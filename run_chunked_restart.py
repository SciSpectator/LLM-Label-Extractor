#!/usr/bin/env python3
"""
Chunked auto-restart runner — FULL PIPELINE.

Phase A: Repair CSV platforms (GPL10558, GPL96, GPL6947) — NS fields only
Phase B: Scratch-annotate ALL remaining human gene expression GPLs from GEOmetadb

Processes ~2000 new samples → checkpoint → exit → cooldown → restart.
Resumes from last checkpoint each time.
"""

import os, sys, time, queue, threading, signal, re, sqlite3

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)
sys.path.insert(0, SCRIPT_DIR)

import llm_extractor as G
import pandas as pd

# ── Configuration ─────────────────────────────────────────────────────────────
BATCH_SIZE    = 10000
SPECIES       = "Homo sapiens"
MIN_SAMPLES   = 5
MODEL         = G.DEFAULT_MODEL
OLLAMA_URL    = G.DEFAULT_URL
HARMONIZED    = SCRIPT_DIR
LOG_FILE      = os.path.join(SCRIPT_DIR, "chunked_run.log")
DONE_MARKER   = os.path.join(SCRIPT_DIR, ".chunked_done")

DB_PATH_SQLITE = os.path.join(SCRIPT_DIR, "GEOmetadb.sqlite")
DB_PATH_GZ     = os.path.join(SCRIPT_DIR, "GEOmetadb.sqlite.gz")
DB_PATH        = DB_PATH_SQLITE if os.path.isfile(DB_PATH_SQLITE) else DB_PATH_GZ

# CSV platforms to repair first (in order)
CSV_PLATFORMS = ["GPL10558", "GPL96", "GPL6947"]

NS_WORDS = {"not specified", "n/a", "none", "unknown", "na",
            "not available", "not applicable", "unclear",
            "unspecified", "missing", "undetermined", ""}

_EXPRESSION_TECHNOLOGIES = {
    "in situ oligonucleotide", "spotted DNA/cDNA",
    "spotted oligonucleotide", "oligonucleotide beads",
}
_KNOWN_EXPRESSION_GPLS = {
    "GPL570":   ("[HG-U133_Plus_2] Affymetrix Human Genome U133 Plus 2.0 Array", "in situ oligonucleotide"),
    "GPL10558": ("Illumina HumanHT-12 V4.0 expression beadchip", "oligonucleotide beads"),
}
_SEQ_EXCLUDE = re.compile(
    r"sequenc|hiseq|miseq|nextseq|novaseq|ion torrent|solid|pacbio|"
    r"bgiseq|dnbseq|genome analyzer|454 gs|"
    r"cytoscan|snp|genotyp|copy number|cgh|tiling|"
    r"methylat|bisulfite|rrbs|chipseq|chip-seq|mirna|microrna|ncrna|lncrna|"
    r"exome|16s|metagenom|mapping\\d|mapping array|splicing|miRBase|RNAi|shRNA|siRNA",
    re.IGNORECASE)

_stop = threading.Event()
signal.signal(signal.SIGINT, lambda s, f: (
    _stop.set(), signal.signal(signal.SIGINT, signal.SIG_DFL)))


def is_platform_complete(gpl):
    """Check if platform has final NS_repaired.csv."""
    rd = os.path.join(HARMONIZED, f"{gpl}_NS_repaired_final_results")
    return os.path.isfile(os.path.join(rd, "NS_repaired.csv"))


def count_done_samples(gpl):
    """Count samples already in live CSV checkpoint."""
    live_csv = os.path.join(
        HARMONIZED, f"{gpl}_NS_repaired_final_results", "NS_repaired_live.csv")
    if os.path.isfile(live_csv):
        try:
            df = pd.read_csv(live_csv, usecols=["gsm"])
            return len(df[df["gsm"] != "gsm"])
        except Exception:
            return 0
    return 0


def count_ns_samples(gpl):
    """Count total NS samples for a CSV platform.
    Uses the UNION of rows where ANY of Tissue/Condition/Treatment is NS,
    matching how the pipeline itself counts NS rows.
    """
    tp = os.path.join(HARMONIZED, f"matrix_tissue_{gpl}.csv")
    cp = os.path.join(HARMONIZED, f"matrix_condition_annotated_{gpl}.csv.gz")
    if not (os.path.isfile(tp) and os.path.isfile(cp)):
        return 0
    try:
        t_df = pd.read_csv(tp, dtype=str).fillna("")
        c_df = pd.read_csv(cp, dtype=str).fillna("")
        merge_cols = ["gsm", "Condition"]
        if "Treatment" in c_df.columns:
            merge_cols.append("Treatment")
        merged = t_df.merge(c_df[merge_cols], on="gsm", how="inner")
        ns_t = merged["Tissue"].str.strip().str.lower().isin(NS_WORDS)
        ns_c = merged["Condition"].str.strip().str.lower().isin(NS_WORDS)
        if "Treatment" in merged.columns:
            ns_tr = merged["Treatment"].str.strip().str.lower().isin(NS_WORDS)
        else:
            ns_tr = pd.Series(False, index=merged.index)
        ns_any = ns_t | ns_c | ns_tr
        return int(ns_any.sum())
    except Exception:
        return 0


def discover_scratch_platforms():
    """Discover all human gene expression platforms from GEOmetadb."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    tech_list = ",".join(f"'{t}'" for t in _EXPRESSION_TECHNOLOGIES)
    cur.execute(f"""
        SELECT g.gpl, g.title, g.technology, COUNT(s.gsm) AS n
        FROM gpl g JOIN gsm s ON s.gpl = g.gpl
        WHERE g.organism = ? AND g.technology IN ({tech_list})
        GROUP BY g.gpl HAVING n >= ?
        ORDER BY n DESC
    """, (SPECIES, MIN_SAMPLES))
    platforms = [{"gpl": r[0], "title": r[1] or "", "n": r[3]} for r in cur.fetchall()]
    seen = {p["gpl"] for p in platforms}
    for gpl, (title, tech) in _KNOWN_EXPRESSION_GPLS.items():
        if gpl not in seen:
            cur.execute("SELECT COUNT(*) FROM gsm WHERE gpl=? AND organism_ch1=?",
                        (gpl, SPECIES))
            n = cur.fetchone()[0]
            if n >= MIN_SAMPLES:
                platforms.append({"gpl": gpl, "title": title, "n": n})
    conn.close()

    # Filter non-expression
    filtered = [p for p in platforms if not _SEQ_EXCLUDE.search(p["title"])]
    filtered.sort(key=lambda p: p["n"], reverse=True)

    # Exclude CSV platforms (handled separately)
    csv_set = set(CSV_PLATFORMS) | {"GPL570"}
    scratch = [p for p in filtered if p["gpl"] not in csv_set]
    return scratch


def find_next_platform():
    """
    Find next platform that needs work.
    Returns (gpl, total_samples, done_samples, mode) or (None,0,0,None).
    Mode is 'repair' or 'scratch'.
    """
    # Phase A: CSV platforms
    for gpl in CSV_PLATFORMS:
        if is_platform_complete(gpl):
            continue
        total_ns = count_ns_samples(gpl)
        done = count_done_samples(gpl)
        if done < total_ns:
            return gpl, total_ns, done, "repair"

    # Phase B: Scratch platforms (largest first)
    scratch = discover_scratch_platforms()
    for p in scratch:
        gpl = p["gpl"]
        if is_platform_complete(gpl):
            continue
        return gpl, p["n"], count_done_samples(gpl), "scratch"

    return None, 0, 0, None


def queue_consumer(q, log_fh):
    """Drain the queue, print/log messages."""
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
            label = msg.get("label", "")
            pct = msg.get("pct", 0)
            if label:
                print(f"  [{pct:3d}%] {label}", flush=True)
        elif mtype == "done":
            ok = msg.get("success", False)
            print(f"\n  Pipeline: {'SUCCESS' if ok else 'FAILED'}", flush=True)
            break


def main():
    ts = time.strftime("%Y-%m-%d %H:%M:%S")

    # ── Find next platform ────────────────────────────────────────────────────
    gpl, total_samples, done_before, mode = find_next_platform()
    if gpl is None:
        print(f"[{ts}] ALL PLATFORMS COMPLETE!")
        with open(DONE_MARKER, "w") as f:
            f.write("done\n")
        return 0

    remaining = total_samples - done_before

    # Count overall progress
    scratch = discover_scratch_platforms()
    total_all = sum(p["n"] for p in scratch)
    done_all = sum(1 for p in scratch if is_platform_complete(p["gpl"]))

    print(f"\n{'='*60}")
    print(f"  CHUNKED RUNNER — {ts}")
    print(f"  Platform: {gpl} [{mode.upper()}]")
    print(f"  Samples:  {done_before:,}/{total_samples:,} done, {remaining:,} remaining")
    print(f"  This chunk: ~{min(BATCH_SIZE, remaining)} samples")
    print(f"  Overall:  {done_all}/{len(scratch)+len(CSV_PLATFORMS)} platforms done")
    print(f"  Model: {MODEL}")
    print(f"{'='*60}\n")

    # Remove done marker
    if os.path.isfile(DONE_MARKER):
        os.remove(DONE_MARKER)

    # ── Start Ollama ──────────────────────────────────────────────────────────
    print("Killing stale Ollama processes …")
    G._kill_ollama(print)
    time.sleep(2)

    print("Computing optimal workers …")
    num_parallel, gpu_w, cpu_w = G.compute_ollama_parallel(MODEL)
    print(f"  Workers: {num_parallel} total ({gpu_w} GPU + {cpu_w} CPU)")

    print("Starting Ollama server …")
    server_proc = G.start_ollama_server_blocking(print, num_parallel)
    if server_proc is None:
        print("[ERROR] Failed to start Ollama server")
        return 1

    for mdl in [MODEL, G.EXTRACTION_MODEL]:
        if not G.model_available(mdl, OLLAMA_URL):
            print(f"Pulling {mdl} …")
            G.pull_model_blocking(mdl, print)
        else:
            print(f"  {mdl} — ready")

    # More frequent checkpoints
    G.CKPT_EVERY = 500

    # ── Batch limit monitor ───────────────────────────────────────────────────
    _batch_start_count = done_before
    _batch_exit_triggered = threading.Event()

    def _patched_pipeline(pipeline_fn, config, q_arg):
        def _monitor():
            while not _batch_exit_triggered.is_set():
                current = count_done_samples(gpl)
                new_done = current - _batch_start_count
                if new_done >= BATCH_SIZE:
                    print(f"\n  >>> BATCH LIMIT: {new_done:,} new samples → graceful exit")
                    _batch_exit_triggered.set()
                    os.kill(os.getpid(), signal.SIGINT)
                    return
                time.sleep(10)

        mon = threading.Thread(target=_monitor, daemon=True)
        mon.start()
        try:
            pipeline_fn(config, q_arg)
        except KeyboardInterrupt:
            if _batch_exit_triggered.is_set():
                print("  Batch limit graceful exit.")
            else:
                raise

    # ── Run pipeline ──────────────────────────────────────────────────────────
    log_fh = open(LOG_FILE, "a", encoding="utf-8")
    log_fh.write(f"\n{'='*60}\n")
    log_fh.write(f"  Chunk: {ts} | {gpl} [{mode}] | done={done_before:,} rem={remaining:,}\n")
    log_fh.write(f"{'='*60}\n\n")

    q = queue.Queue()
    consumer_t = threading.Thread(target=queue_consumer, args=(q, log_fh), daemon=True)
    consumer_t.start()

    if mode == "scratch":
        config = {
            "db_path":        DB_PATH,
            "platform":       gpl,
            "model":          MODEL,
            "ollama_url":     OLLAMA_URL,
            "harmonized_dir": HARMONIZED,
            "limit":          None,
            "num_workers":    None,
            "skip_install":   True,
            "gsm_list_file":  "",
            "server_proc":    server_proc,
            "_db_platform_mode": True,  # load from GEOmetadb, not CSV
        }
    else:
        config = {
            "db_path":        DB_PATH,
            "platform":       gpl,
            "model":          MODEL,
            "ollama_url":     OLLAMA_URL,
            "harmonized_dir": HARMONIZED,
            "limit":          None,
            "num_workers":    None,
            "skip_install":   True,
            "gsm_list_file":  "",
            "server_proc":    server_proc,
        }

    exit_code = 0
    try:
        _patched_pipeline(G.pipeline, config, q)
    except KeyboardInterrupt:
        if not _batch_exit_triggered.is_set():
            print("\n[INTERRUPTED by user]")
            exit_code = 2
    except Exception as exc:
        import traceback
        print(f"[ERROR] {exc}")
        traceback.print_exc()
        exit_code = 1
    finally:
        q.put(None)
        consumer_t.join(timeout=5)
        log_fh.close()

    # ── Cleanup ───────────────────────────────────────────────────────────────
    print("\nKilling Ollama …")
    G._kill_ollama(print)
    time.sleep(2)

    done_after = count_done_samples(gpl)
    new_processed = done_after - done_before

    print(f"\n{'='*60}")
    print(f"  Chunk done: {gpl} [{mode}]")
    print(f"  New this chunk: {new_processed:,}")
    print(f"  Total done: {done_after:,}/{total_samples:,}")

    if is_platform_complete(gpl):
        print(f"  >>> {gpl} COMPLETE!")
    else:
        print(f"  {total_samples - done_after:,} remaining — restart will continue")

    # Check if ALL done
    next_gpl, _, _, _ = find_next_platform()
    if next_gpl is None:
        print(f"\n  >>> ALL PLATFORMS DONE!")
        with open(DONE_MARKER, "w") as f:
            f.write("done\n")
    else:
        print(f"  Next up: {next_gpl}")

    print(f"{'='*60}\n")
    return exit_code


if __name__ == "__main__":
    sys.exit(main() or 0)
