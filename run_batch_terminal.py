#!/usr/bin/env python3
"""
Terminal batch runner for llm_extractor.py — v3 (2026-03-29)

Architecture:
  Phase 1:  Raw extraction (gemma2:2b) — extract Tissue/Condition/Treatment
            from GSM metadata + GSE context. Parallel, ~174ms/sample.
  Phase 1b: NS inference (gemma2:2b) — for fields still NS after Phase 1,
            re-infer from GSE experiment description. Single LLM call.
  Phase 2:  Collapse (gemma2:2b) — map raw labels to cluster names via
            Memory Agent (clusters.db). Treatment stays raw (no clusters yet).

All phases use gemma2:2b — no model swaps, maximum GPU parallelism.
GSE descriptions fetched fresh from NCBI (full view, not brief).
Checkpoints saved every 5000 samples (Phase 1) and 1000 samples (Phase 2).

Platform order:
  1) CSV platforms (GPL570, GPL10558, GPL96, GPL6947) — repair NS only
  2) Scratch platforms (2364 GPLs from GEOmetadb) — annotate from scratch

Usage:  python run_batch_terminal.py
Monitor: bash monitor.sh
"""

import os, sys, time, queue, threading, signal, subprocess, re

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)

sys.path.insert(0, SCRIPT_DIR)
import llm_extractor as G

# ── Configuration ─────────────────────────────────────────────────────────────
SPECIES       = "Homo sapiens"
TECH_MODE     = "Expression Microarray"
MIN_SAMPLES   = 5
MODEL         = G.DEFAULT_MODEL        # gemma2:2b (single model for all phases)
OLLAMA_URL    = G.DEFAULT_URL          # http://localhost:11434
HARMONIZED    = SCRIPT_DIR
LOG_FILE      = os.path.join(SCRIPT_DIR, "batch_run.log")

# Use pre-decompressed .sqlite if available
DB_PATH_SQLITE = os.path.join(SCRIPT_DIR, "GEOmetadb.sqlite")
DB_PATH_GZ     = os.path.join(SCRIPT_DIR, "GEOmetadb.sqlite.gz")
DB_PATH        = DB_PATH_SQLITE if os.path.isfile(DB_PATH_SQLITE) else DB_PATH_GZ

# Keywords that indicate a platform is NOT gene expression microarray
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

# Known gene expression platforms missing from gpl table
_KNOWN_EXPRESSION_GPLS = {
    "GPL570":   ("[HG-U133_Plus_2] Affymetrix Human Genome U133 Plus 2.0 Array",
                 "in situ oligonucleotide"),
    "GPL10558": ("Illumina HumanHT-12 V4.0 expression beadchip",
                 "oligonucleotide beads"),
}

# Technologies that correspond to gene expression arrays
_EXPRESSION_TECHNOLOGIES = {
    "in situ oligonucleotide",
    "spotted DNA/cDNA",
    "spotted oligonucleotide",
    "oligonucleotide beads",
}

# ── Graceful shutdown ─────────────────────────────────────────────────────────
_stop = threading.Event()
def _sig(s, f):
    print("\n[SIGINT] Stopping gracefully … (Ctrl-C again to force)")
    _stop.set()
    signal.signal(signal.SIGINT, signal.SIG_DFL)
signal.signal(signal.SIGINT, _sig)


def vram_monitor():
    """Print VRAM + Ollama status every 60 seconds."""
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
        import psutil
        ram = psutil.virtual_memory()
        ram_str = f"RAM {ram.used // (1024**3)}/{ram.total // (1024**3)} GB ({ram.percent:.0f}%)"
        ts = time.strftime("%H:%M:%S")
        print(f"  [{ts}] {gpu_str}  |  {ram_str}  |  {oll_str}", flush=True)
        _stop.wait(60)


def queue_consumer(q, log_fh):
    """Drain the queue and print/log messages."""
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
                print(f"  [{pct:3d}%] {label}", flush=True)
        elif mtype == "done":
            ok = msg.get("success", False)
            status = "SUCCESS" if ok else "FAILED"
            print(f"\n{'='*60}\n  Pipeline finished: {status}\n{'='*60}", flush=True)
            log_fh.write(f"\nPipeline finished: {status}\n")
            log_fh.flush()
            break
        elif mtype == "watchdog":
            text = msg.get("msg", "")
            print(f"  [WATCHDOG] {text}", flush=True)


def main():
    print(f"{'='*60}")
    print(f"  BATCH TERMINAL RUNNER v3 — llm_extractor")
    print(f"  Species: {SPECIES}  |  Tech: {TECH_MODE}")
    print(f"  Min samples/GPL: {MIN_SAMPLES}")
    print(f"  Model: {MODEL}  |  Ollama: {OLLAMA_URL}")
    print(f"  DB: {os.path.basename(DB_PATH)}")
    print(f"  Phases: P1 (extract) → P1b (NS infer from GSE) → P2 (collapse)")
    print(f"{'='*60}\n")

    if not os.path.isfile(DB_PATH):
        print(f"[ERROR] GEOmetadb not found at: {DB_PATH}")
        sys.exit(1)

    # ── Step 1: Kill stale Ollama, start fresh ────────────────────────────────
    print("Killing any stale Ollama processes …")
    G._kill_ollama(print)
    time.sleep(2)

    # ── Step 2: Start Ollama server ───────────────────────────────────────────
    print("\nComputing optimal parallel slots …")
    num_parallel, gpu_w, cpu_w = G.compute_ollama_parallel(MODEL)
    print(f"  Workers: {num_parallel} total ({gpu_w} GPU + {cpu_w} CPU)")

    print("\nStarting Ollama server …")
    server_proc = G.start_ollama_server_blocking(print, num_parallel)
    if server_proc is None:
        print("[ERROR] Failed to start Ollama server")
        sys.exit(1)

    # ── Step 3: Ensure models are pulled ──────────────────────────────────────
    for mdl in [MODEL, G.EXTRACTION_MODEL]:
        if not G.model_available(mdl, OLLAMA_URL):
            print(f"Pulling {mdl} …")
            G.pull_model_blocking(mdl, print)
        else:
            print(f"  {mdl} — ready")

    # ── Step 4: Discover gene expression platforms from GEOmetadb ────────────
    print(f"\nLoading GEOmetadb into memory …")
    conn = G.load_db_to_memory(DB_PATH, print)

    import sqlite3
    cur = conn.cursor()
    tech_list = ",".join(f"'{t}'" for t in _EXPRESSION_TECHNOLOGIES)
    cur.execute(f"""
        SELECT g.gpl, g.title, g.technology, COUNT(s.gsm) AS sample_count
        FROM gpl g
        JOIN gsm s ON s.gpl = g.gpl
        WHERE g.organism = ?
          AND g.technology IN ({tech_list})
        GROUP BY g.gpl
        HAVING sample_count >= ?
        ORDER BY sample_count DESC
    """, (SPECIES, MIN_SAMPLES))
    platforms_raw = []
    for row in cur.fetchall():
        platforms_raw.append({
            "gpl": row[0], "title": row[1] or "",
            "technology": row[2] or "", "sample_count": row[3]
        })
    print(f"  Found {len(platforms_raw)} platforms with expression technology")

    # Add known missing platforms
    seen_gpls = {p["gpl"] for p in platforms_raw}
    for gpl, (title, tech) in _KNOWN_EXPRESSION_GPLS.items():
        if gpl not in seen_gpls:
            cur.execute("SELECT COUNT(*) FROM gsm WHERE gpl = ? AND organism_ch1 = ?",
                        (gpl, SPECIES))
            n = cur.fetchone()[0]
            if n >= MIN_SAMPLES:
                platforms_raw.append({
                    "gpl": gpl, "title": title, "technology": tech, "sample_count": n
                })
                print(f"  Added known platform: {gpl} ({n:,} samples)")
    conn.close()

    if not platforms_raw:
        print("[ERROR] No platforms discovered")
        G._kill_ollama(print)
        sys.exit(1)

    platforms_raw.sort(key=lambda p: p["sample_count"], reverse=True)

    # Filter non-expression
    filtered = [p for p in platforms_raw if not _SEQ_EXCLUDE.search(p["title"])]
    excluded = len(platforms_raw) - len(filtered)
    print(f"  Excluded {excluded} non-expression platforms")
    print(f"  After filter: {len(filtered)} gene expression platforms")

    # ── Split CSV vs scratch ──────────────────────────────────────────────────
    ns_words = {"not specified", "n/a", "none", "unknown", "na",
                "not available", "not applicable", "unclear",
                "unspecified", "missing", "undetermined", ""}
    csv_platforms = []
    scratch_platforms = []
    for p in filtered:
        tp = os.path.join(HARMONIZED, f"matrix_tissue_{p['gpl']}.csv")
        cp = os.path.join(HARMONIZED, f"matrix_condition_annotated_{p['gpl']}.csv.gz")
        if os.path.isfile(tp) and os.path.isfile(cp):
            csv_platforms.append(p)
        else:
            scratch_platforms.append(p)

    import pandas as _pd
    csv_ns_total = 0
    print(f"\n── Platform split ──")
    print(f"  CSV platforms (repair NS only): {len(csv_platforms)}")
    for p in csv_platforms:
        try:
            _t = _pd.read_csv(os.path.join(HARMONIZED, f"matrix_tissue_{p['gpl']}.csv"))
            _c = _pd.read_csv(os.path.join(HARMONIZED,
                    f"matrix_condition_annotated_{p['gpl']}.csv.gz"))
            _ns_t = _t["Tissue"].astype(str).str.strip().str.lower().isin(ns_words).sum()
            _ns_c = (_c.get("Condition", "") == "Not Specified").sum()
            _ns = max(_ns_t, _ns_c)
            csv_ns_total += _ns
            print(f"    {p['gpl']:12s} {p['sample_count']:>8,} total | "
                  f"Tissue NS: {_ns_t:>6,} | Condition NS: {_ns_c:>6,} | "
                  f"actual work: {_ns:,}")
        except Exception:
            csv_ns_total += p["sample_count"]
            print(f"    {p['gpl']:12s} {p['sample_count']:>8,} total (CSV read error)")

    scratch_total = sum(p["sample_count"] for p in scratch_platforms)
    print(f"  Scratch platforms: {len(scratch_platforms)} ({scratch_total:,} samples)")

    scratch_tuples = [(p["gpl"], p["title"], p["sample_count"])
                      for p in scratch_platforms]

    # ── ETA estimate ──────────────────────────────────────────────────────────
    def _fmt(s):
        h, m = int(s // 3600), int((s % 3600) // 60)
        return f"{h}h {m}m" if h else f"{m}m"

    # All phases use gemma2:2b: P1 ~174ms, P1b ~200ms (30% of samples), P2 ~200ms
    total_samples = csv_ns_total + scratch_total
    avg_per_sample = 0.174 + 0.30 * 0.200 + 0.200  # ~434ms
    eta_s = total_samples * avg_per_sample
    overhead = len(scratch_platforms) * 5  # DB + NCBI per platform
    total_eta = eta_s + overhead

    print(f"\n{'='*60}")
    print(f"  PLAN:")
    print(f"  1) Repair {len(csv_platforms)} CSV platforms ({csv_ns_total:,} NS samples)")
    print(f"  2) Scratch {len(scratch_platforms)} platforms ({scratch_total:,} samples)")
    print(f"  TOTAL: {total_samples:,} samples | ETA: ~{_fmt(total_eta)}")
    print(f"  Phases: P1 (extract) → P1b (GSE inference) → P2 (collapse)")
    print(f"{'='*60}")
    all_tuples = [(p["gpl"], p["title"], p["sample_count"]) for p in filtered]
    for i, (gpl, title, n) in enumerate(all_tuples[:30], 1):
        mode = "REPAIR" if any(p["gpl"] == gpl for p in csv_platforms) else "SCRATCH"
        print(f"  {i:3d}. [{mode:7s}] {gpl:12s} {n:>8,}  {title[:45]}")
    if len(all_tuples) > 30:
        print(f"  ... and {len(all_tuples) - 30} more platforms")
    print(f"{'='*60}\n")

    # ── Step 5: Run the pipeline ──────────────────────────────────────────────
    log_fh = open(LOG_FILE, "a", encoding="utf-8")
    log_fh.write(f"\n{'='*60}\n")
    log_fh.write(f"  Batch run started: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    log_fh.write(f"  Model: {MODEL} (single model, no swaps)\n")
    log_fh.write(f"  CSV: {len(csv_platforms)} platforms, {csv_ns_total:,} NS\n")
    log_fh.write(f"  Scratch: {len(scratch_platforms)} platforms, {scratch_total:,}\n")
    log_fh.write(f"  ETA: ~{_fmt(total_eta)}\n")
    log_fh.write(f"{'='*60}\n\n")

    mon_t = threading.Thread(target=vram_monitor, daemon=True)
    mon_t.start()

    consumer_t = threading.Thread(target=queue_consumer,
                                   args=(q := queue.Queue(), log_fh), daemon=True)
    consumer_t.start()

    print(f"Pipeline started — logging to {LOG_FILE}")
    print(f"Monitor: bash monitor.sh\n")

    try:
        # ── Phase A: Repair CSV platforms (NS fields only) ────────────────
        # Prioritise GPL10558 (has checkpoint to resume), skip completed ones
        def _is_complete(gpl):
            """A platform is complete if NS_repaired.csv (final) exists."""
            rd = os.path.join(HARMONIZED, f"{gpl}_NS_repaired_final_results")
            return os.path.isfile(os.path.join(rd, "NS_repaired.csv"))

        # Move GPL10558 to front so it resumes first
        csv_platforms.sort(key=lambda p: (
            0 if p["gpl"] == "GPL10558" else 1,
            -p["sample_count"]
        ))

        for idx, p in enumerate(csv_platforms, 1):
            gpl = p["gpl"]
            if _is_complete(gpl):
                print(f"\n  [{idx}/{len(csv_platforms)}] SKIP: {gpl} — already complete "
                      f"(NS_repaired.csv exists)")
                continue

            print(f"\n{'━'*60}")
            print(f"  [{idx}/{len(csv_platforms)}] REPAIR: {gpl} — {p['title'][:50]}")
            print(f"{'━'*60}")

            config_repair = {
                "db_path":          DB_PATH,
                "platform":         gpl,
                "model":            MODEL,
                "ollama_url":       OLLAMA_URL,
                "harmonized_dir":   HARMONIZED,
                "limit":            None,
                "num_workers":      None,
                "skip_install":     True,
                "gsm_list_file":    "",
                "server_proc":      server_proc,
            }
            try:
                G.pipeline(config_repair, q)
            except Exception as exc:
                import traceback
                print(f"[ERROR] {gpl} repair failed: {exc}")
                traceback.print_exc()

        # ── Phase B: Scratch-annotate remaining platforms ─────────────────
        if scratch_platforms:
            # Filter out already-completed scratch platforms
            remaining_scratch = [p for p in scratch_platforms
                                 if not _is_complete(p["gpl"])]
            skipped_scratch = len(scratch_platforms) - len(remaining_scratch)

            print(f"\n{'='*60}")
            print(f"  Starting scratch annotation: {len(remaining_scratch)} platforms"
                  f" ({skipped_scratch} already complete, skipped)")
            print(f"{'='*60}")

            if remaining_scratch:
                scratch_tuples_rem = [(p["gpl"], p["title"], p["sample_count"])
                                      for p in remaining_scratch]
                config_scratch = {
                    "db_path":          DB_PATH,
                    "platform":         scratch_tuples_rem[0][0],
                    "platforms":        scratch_tuples_rem,
                    "model":            MODEL,
                    "ollama_url":       OLLAMA_URL,
                    "harmonized_dir":   HARMONIZED,
                    "limit":            None,
                    "num_workers":      None,
                    "skip_install":     True,
                    "gsm_list_file":    "",
                    "server_proc":      server_proc,
                }
                G.pipeline_multi(config_scratch, q)

    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Stopping …")
    except Exception as exc:
        import traceback
        print(f"\n[ERROR] {exc}")
        traceback.print_exc()
    finally:
        q.put(None)
        _stop.set()
        consumer_t.join(timeout=5)
        log_fh.close()

        print("\nCleaning up Ollama …")
        G._kill_ollama(print)
        print("Done.")


if __name__ == "__main__":
    main()
