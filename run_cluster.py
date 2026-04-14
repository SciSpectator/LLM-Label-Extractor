#!/usr/bin/env python3
"""
run_cluster.py — HPC/SLURM batch runner for LLM-Label-Extractor v2.1

Designed for HPC clusters (OSCER, Walnut, etc.) using either:
  - Ollama (local GPU)
  - fake_ollama_lb.py (llama_cpp servers behind a load balancer)

Supports all pipeline phases: Phase 1 → 1b → 1c → 2 (collapse)

Environment variables:
  PLATFORM_ONLY    - comma-separated GPL IDs (e.g. "GPL570,GPL96")
  PLATFORM_START   - start index into sorted platform list (default: 0)
  PLATFORM_END     - end index into sorted platform list (default: 99999)
  SKIP_GPLS        - comma-separated GPL IDs to skip
  PHASES           - which phases to run (default: "1,1b,1c,2" = all)
                     Examples: "1" (extract only), "1,1b" (no collapse),
                     "1,1b,1c" (extract+infer+full, no collapse),
                     "2" (collapse only, requires existing Phase 1 checkpoint)
  HARMONIZED_DIR   - output directory (default: ./NEW_RESULTS)
  GEODB_PATH       - path to GEOmetadb.sqlite (default: ./GEOmetadb.sqlite)
  USE_FAKE_OLLAMA  - "1" to skip ollama startup (use pre-started fake_ollama_lb)
  OLLAMA_URL       - ollama API URL (default: http://localhost:11434)
  NUM_WORKERS      - override auto-detected worker count

Usage:
  # Standard (local Ollama GPU):
  python run_cluster.py

  # HPC with fake_ollama_lb:
  USE_FAKE_OLLAMA=1 SKIP_PHASE2=1 PLATFORM_START=0 PLATFORM_END=200 python run_cluster.py

  # Specific platforms only:
  PLATFORM_ONLY=GPL570,GPL96 python run_cluster.py

SLURM example:
  #!/bin/bash
  #SBATCH --job-name=tails
  #SBATCH --cpus-per-task=8
  #SBATCH --mem=32G
  #SBATCH --gres=gpu:1
  #SBATCH --time=48:00:00

  module load python/3.10
  source venv/bin/activate
  ollama serve &
  sleep 10
  ollama pull gemma4:e2b
  python run_cluster.py
"""

import os, sys, re, time, queue, threading, signal

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)
sys.path.insert(0, SCRIPT_DIR)

import llm_extractor as G

# ── Configuration from environment ──────────────────────────────────────────
SPECIES         = "Homo sapiens"
TECH_MODE       = "Expression Microarray"
MIN_SAMPLES     = 5
MODEL           = G.DEFAULT_MODEL
OLLAMA_URL      = os.environ.get("OLLAMA_URL", G.DEFAULT_URL)
HARMONIZED      = os.environ.get("HARMONIZED_DIR", os.path.join(SCRIPT_DIR, "NEW_RESULTS"))
DB_PATH_SQLITE  = os.environ.get("GEODB_PATH", os.path.join(SCRIPT_DIR, "GEOmetadb.sqlite"))
DB_PATH_GZ      = os.path.join(SCRIPT_DIR, "GEOmetadb.sqlite.gz")
DB_PATH         = DB_PATH_SQLITE if os.path.isfile(DB_PATH_SQLITE) else DB_PATH_GZ

PLATFORM_ONLY   = set(os.environ.get("PLATFORM_ONLY", "").split(",")) - {""}
PLATFORM_START  = int(os.environ.get("PLATFORM_START", 0))
PLATFORM_END    = int(os.environ.get("PLATFORM_END", 99999))
SKIP_GPLS       = set(os.environ.get("SKIP_GPLS", "").split(",")) - {""}
# Phase selection: comma-separated list of phases to run
# "1,1b,1c,2" = all (default), "1" = extract only, "2" = collapse only, etc.
_PHASES_STR     = os.environ.get("PHASES", "1,1b,1c,2")
ENABLED_PHASES  = set(p.strip() for p in _PHASES_STR.split(","))
SKIP_PHASE2     = "2" not in ENABLED_PHASES
SKIP_PHASE1B    = "1b" not in ENABLED_PHASES
SKIP_PHASE1C    = "1c" not in ENABLED_PHASES
SKIP_PHASE1     = "1" not in ENABLED_PHASES

USE_FAKE_OLLAMA = os.environ.get("USE_FAKE_OLLAMA", "0") == "1"
NUM_WORKERS     = int(os.environ.get("NUM_WORKERS", 0)) or None  # None = auto-detect

LOG_FILE = os.path.join(SCRIPT_DIR, "batch_run.log")

# ── Expression platform filter ──────────────────────────────────────────────
_SEQ_EXCLUDE = re.compile(
    r"sequenc|hiseq|miseq|nextseq|novaseq|ion torrent|solid|pacbio|"
    r"bgiseq|dnbseq|genome analyzer|454 gs|"
    r"cytoscan|snp|genotyp|copy number|cgh|tiling|"
    r"methylat|bisulfite|rrbs|"
    r"chipseq|chip-seq|mirna|microrna|ncrna|lncrna|"
    r"exome|16s|metagenom|"
    r"mapping\d|mapping array|splicing|"
    r"miRBase|RNAi|shRNA|siRNA",
    re.IGNORECASE
)

_EXPRESSION_TECHNOLOGIES = {
    "in situ oligonucleotide",
    "spotted DNA/cDNA",
    "spotted oligonucleotide",
    "oligonucleotide beads",
}

# ── Graceful shutdown ────────────────────────────────────────────────────────
_stop = threading.Event()

def _sig(s, f):
    print("\n[SIGINT] Stopping gracefully …")
    _stop.set()
    signal.signal(signal.SIGINT, signal.SIG_DFL)

signal.signal(signal.SIGINT, _sig)


def vram_monitor():
    """Background thread: prints GPU/RAM/Ollama status every 60s."""
    import psutil
    while not _stop.is_set():
        try:
            u, t, pct = G._get_vram_usage()
            gpu_str = f"VRAM {u:,}/{t:,} MB ({pct:.0f}%)" if t else "No GPU"
        except Exception:
            gpu_str = "GPU: N/A"
        try:
            ok = G.ollama_server_ok(OLLAMA_URL, timeout=2)
            oll_str = "Ollama: OK" if ok else "Ollama: DOWN"
        except Exception:
            oll_str = "Ollama: ?"
        ram = psutil.virtual_memory()
        ram_str = f"RAM {ram.used//(1024**3)}/{ram.total//(1024**3)} GB ({ram.percent:.0f}%)"
        print(f"  [{time.strftime('%H:%M:%S')}] {gpu_str}  |  {ram_str}  |  {oll_str}", flush=True)
        _stop.wait(60)


def queue_consumer(q, log_fh):
    """Background thread: drains pipeline queue messages."""
    while True:
        try:
            msg = q.get(timeout=1)
        except queue.Empty:
            continue
        if msg is None:
            break
        mtype = msg.get("type", "")
        if mtype == "log":
            text = msg.get("msg", "")
            print(text, flush=True)
            log_fh.write(text + "\n")
            log_fh.flush()
        elif mtype == "progress":
            pct = msg.get("pct", 0)
            label = msg.get("label", "")
            if label:
                print(f"  [{pct:3.0f}%] {label}", flush=True)
        elif mtype == "done":
            status = "SUCCESS" if msg.get("success") else "FAILED"
            print(f"\n{'='*60}\n Pipeline: {status}\n{'='*60}", flush=True)
            log_fh.write(f"\nPipeline: {status}\n")
            log_fh.flush()
            break


def main():
    print(f"{'='*60}")
    print(f"  TAILS v2.1 — HPC Cluster Runner")
    print(f"  Species: {SPECIES}  |  Model: {MODEL}")
    print(f"  DB: {os.path.basename(DB_PATH)}")
    print(f"  Output: {HARMONIZED}")
    print(f"  PLATFORM_START={PLATFORM_START}  PLATFORM_END={PLATFORM_END}")
    if PLATFORM_ONLY:
        print(f"  PLATFORM_ONLY: {PLATFORM_ONLY}")
    if SKIP_GPLS:
        print(f"  SKIP_GPLS: {SKIP_GPLS}")
    if NUM_WORKERS:
        print(f"  NUM_WORKERS: {NUM_WORKERS} (override)")
    print(f"  PHASES={_PHASES_STR}  USE_FAKE_OLLAMA={USE_FAKE_OLLAMA}")
    print(f"{'='*60}\n", flush=True)

    os.makedirs(HARMONIZED, exist_ok=True)

    if not os.path.isfile(DB_PATH):
        print(f"[ERROR] GEOmetadb not found at: {DB_PATH}")
        sys.exit(1)

    # ── Ollama startup ──
    if not USE_FAKE_OLLAMA:
        print("Killing any stale Ollama processes …")
        G._kill_ollama(print)
        time.sleep(2)

    print("Computing optimal parallel slots …")
    num_parallel, gpu_w, cpu_w = G.compute_ollama_parallel(MODEL)
    if NUM_WORKERS:
        num_parallel = NUM_WORKERS
    print(f"  Workers: {num_parallel} total ({gpu_w} GPU + {cpu_w} CPU)")

    server_proc = None
    if USE_FAKE_OLLAMA:
        print("  Using pre-started fake_ollama_lb — skipping ollama startup")
    else:
        print("Starting Ollama server …")
        server_proc = G.start_ollama_server_blocking(print, num_parallel)
        if server_proc is None:
            print("[ERROR] Failed to start Ollama server")
            sys.exit(1)

    # ── Ensure models ──
    for mdl in set([MODEL, G.EXTRACTION_MODEL]):
        if not G.model_available(mdl, OLLAMA_URL):
            print(f"Pulling {mdl} …")
            G.pull_model_blocking(mdl, print)
        else:
            print(f"  {mdl} — ready")

    # ── Discover platforms ──
    print(f"\nLoading GEOmetadb …")
    conn = G.load_db_to_memory(DB_PATH, print)
    import sqlite3
    cur = conn.cursor()
    tech_list = ",".join(f"'{t}'" for t in _EXPRESSION_TECHNOLOGIES)
    cur.execute(f"""
        SELECT g.gpl, g.title, g.technology, COUNT(s.gsm) AS n
        FROM gpl g JOIN gsm s ON s.gpl = g.gpl
        WHERE g.organism = ? AND g.technology IN ({tech_list})
        GROUP BY g.gpl HAVING n >= ? ORDER BY n DESC
    """, (SPECIES, MIN_SAMPLES))
    platforms_raw = [{"gpl": r[0], "title": r[1] or "", "technology": r[2] or "", "sample_count": r[3]}
                     for r in cur.fetchall()]
    conn.close()

    platforms_raw.sort(key=lambda p: p["sample_count"], reverse=True)

    # Filter
    filtered = [p for p in platforms_raw if not _SEQ_EXCLUDE.search(p["title"])]
    if PLATFORM_ONLY:
        filtered = [p for p in filtered if p["gpl"] in PLATFORM_ONLY]
    filtered = filtered[PLATFORM_START:PLATFORM_END]
    if SKIP_GPLS:
        filtered = [p for p in filtered if p["gpl"] not in SKIP_GPLS]

    if not filtered:
        print("[ERROR] No platforms to process after filtering")
        if not USE_FAKE_OLLAMA:
            G._kill_ollama(print)
        sys.exit(0)

    total_samples = sum(p["sample_count"] for p in filtered)
    print(f"\n  Platforms: {len(filtered)}  |  Samples: {total_samples:,}")
    for p in filtered[:10]:
        print(f"    {p['gpl']:12s} {p['sample_count']:>8,}  {p['title'][:50]}")
    if len(filtered) > 10:
        print(f"    ... and {len(filtered)-10} more")

    # ── Check completions ──
    def _is_complete(gpl):
        rd = os.path.join(HARMONIZED, f"{gpl}_NS_repaired_final_results")
        if SKIP_PHASE2:
            return os.path.isfile(os.path.join(rd, "labels_phase1b_1c.csv"))
        return os.path.isfile(os.path.join(rd, "labels_final.csv"))

    remaining = [p for p in filtered if not _is_complete(p["gpl"])]
    skipped = len(filtered) - len(remaining)
    if skipped:
        print(f"  Skipping {skipped} already-completed platforms")
    print(f"  Processing {len(remaining)} platforms\n")

    if not remaining:
        print("All platforms already complete.")
        if not USE_FAKE_OLLAMA:
            G._kill_ollama(print)
        return

    # ── Run pipeline ──
    log_fh = open(LOG_FILE, "a", encoding="utf-8")
    log_fh.write(f"\n{'='*60}\n")
    log_fh.write(f"  Batch: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    log_fh.write(f"  Model: {MODEL}  |  Platforms: {len(remaining)}  |  Samples: {sum(p['sample_count'] for p in remaining):,}\n")
    log_fh.write(f"  SKIP_PHASE2={SKIP_PHASE2}\n{'='*60}\n\n")

    mon_t = threading.Thread(target=vram_monitor, daemon=True)
    mon_t.start()

    q = queue.Queue()
    consumer_t = threading.Thread(target=queue_consumer, args=(q, log_fh), daemon=True)
    consumer_t.start()

    try:
        scratch_tuples = [(p["gpl"], p["title"], p["sample_count"]) for p in remaining]
        config = {
            "db_path":        DB_PATH,
            "platform":       scratch_tuples[0][0],
            "platforms":      scratch_tuples,
            "model":          MODEL,
            "ollama_url":     OLLAMA_URL,
            "harmonized_dir": HARMONIZED,
            "limit":          None,
            "num_workers":    NUM_WORKERS,
            "skip_install":   True,
            "skip_phase1":    SKIP_PHASE1,
            "skip_phase1b":   SKIP_PHASE1B,
            "skip_phase1c":   SKIP_PHASE1C,
            "skip_phase2":    SKIP_PHASE2,
            "gsm_list_file":  "",
            "server_proc":    server_proc,
        }
        G.pipeline_multi(config, q)
    except KeyboardInterrupt:
        print("\n[INTERRUPTED]")
    except Exception as exc:
        import traceback
        print(f"\n[ERROR] {exc}")
        traceback.print_exc()
    finally:
        q.put(None)
        _stop.set()
        consumer_t.join(timeout=5)
        log_fh.close()
        if not USE_FAKE_OLLAMA:
            G._kill_ollama(print)
        print("Done.")


if __name__ == "__main__":
    main()
