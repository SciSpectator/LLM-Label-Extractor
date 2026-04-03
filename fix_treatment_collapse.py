#!/usr/bin/env python3
"""
Treatment-only re-collapse for platforms that already completed Tissue+Condition collapse.
Reads existing NS_repaired CSV, collapses only Treatment_after, saves back.
Does NOT re-run Phase 1, Phase 1b, or Tissue/Condition collapse.
"""
import sys, os, time, json, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import llm_extractor as G
import pandas as pd

OLLAMA_URL = "http://localhost:11434"
BASE = os.path.dirname(os.path.abspath(__file__))

def fix_treatment(gpl, csv_path):
    """Re-collapse Treatment column in an existing results CSV."""
    print(f"\n{'='*60}")
    print(f"  TREATMENT RE-COLLAPSE: {gpl}")
    print(f"  Source: {os.path.basename(csv_path)}")
    print(f"{'='*60}")

    df = pd.read_csv(csv_path)
    print(f"  Rows: {len(df):,}")

    # Find rows where Treatment_after is NOT "Not Specified" and NOT already collapsed
    # We re-collapse ALL non-NS Treatment values through the cluster map
    treat_col = "Treatment_after"
    if treat_col not in df.columns:
        print(f"  [SKIP] No {treat_col} column")
        return

    non_ns_mask = ~df[treat_col].astype(str).str.strip().str.lower().isin(
        {"not specified", "n/a", "none", "unknown", "na", ""})
    to_fix = df[non_ns_mask].copy()
    print(f"  Non-NS Treatment values: {len(to_fix):,}")

    if len(to_fix) == 0:
        print(f"  Nothing to collapse — all Treatment is NS")
        return

    # Load memory agent
    print(f"  Loading MemoryAgent...")
    ma = G.MemoryAgent(os.path.join(BASE, "biomedical_memory.db"), OLLAMA_URL)
    ma.load_cache_all(log_fn=lambda m: print(f"    {m}"))
    print(f"  Treatment ready: {ma.is_ready('Treatment')}")

    wd = G.Watchdog(log_fn=lambda m: None)
    cw = G.CollapseWorker("gemma2:2b", OLLAMA_URL, ma, watchdog=wd, log_fn=lambda m: None)

    # Phase 1: deterministic-only pass (no LLM, instant)
    print(f"\n  Pass 1: Deterministic cluster_lookup (no LLM)...")
    t0 = time.time()
    det_matched = 0
    det_new_vals = {}

    for idx, row in to_fix.iterrows():
        raw = str(row[treat_col]).strip()
        cluster = ma.cluster_lookup("Treatment", raw)
        if cluster:
            det_new_vals[idx] = cluster
            det_matched += 1

    elapsed = time.time() - t0
    print(f"  Deterministic: {det_matched:,}/{len(to_fix):,} matched "
          f"({100*det_matched/len(to_fix):.1f}%) in {elapsed:.1f}s")

    # Apply deterministic results
    for idx, val in det_new_vals.items():
        df.at[idx, treat_col] = val

    # Phase 2: LLM collapse for remaining unmatched
    remaining = to_fix[~to_fix.index.isin(det_new_vals.keys())]
    print(f"\n  Pass 2: LLM collapse for {len(remaining):,} remaining...")

    if len(remaining) > 0:
        llm_matched = 0
        llm_rejected = 0
        llm_new = 0
        t1 = time.time()
        done = [0]
        lock = threading.Lock()

        def collapse_one(idx_row):
            idx, row = idx_row
            raw = str(row[treat_col]).strip()
            ctx = G.GSEContext(str(row.get("series_id", "")))
            final, collapsed, rule, _ = cw.collapse_field(
                gsm=str(row.get("gsm", "")), col="Treatment",
                raw_label=raw, gse_ctx=ctx, raw={},
                platform=gpl)
            return idx, final, rule

        tasks = list(remaining.iterrows())
        # Limit workers to avoid CPU overheating — use 50% of cores max
        num_workers = min(max(4, (os.cpu_count() or 4) // 2), len(tasks))
        print(f"    Using {num_workers} workers (safe limit for CPU temp)")

        with ThreadPoolExecutor(max_workers=num_workers, thread_name_prefix="TFix") as ex:
            futs = {ex.submit(collapse_one, t): t[0] for t in tasks}
            for fut in as_completed(futs):
                try:
                    idx, final, rule = fut.result()
                    if not G.is_ns(final):
                        df.at[idx, treat_col] = final
                        if "new_cluster" in str(rule):
                            llm_new += 1
                        else:
                            llm_matched += 1
                    else:
                        llm_rejected += 1
                except Exception:
                    llm_rejected += 1

                with lock:
                    done[0] += 1
                    if done[0] % 200 == 0 or done[0] == len(tasks):
                        elapsed2 = time.time() - t1
                        rate = done[0] / elapsed2 if elapsed2 > 0 else 0
                        eta = int((len(tasks) - done[0]) / rate) if rate > 0 else 0
                        print(f"    [{done[0]:,}/{len(tasks):,}] "
                              f"matched:{llm_matched} new:{llm_new} rejected:{llm_rejected} "
                              f"ETA:{timedelta(seconds=eta)}")

        elapsed2 = time.time() - t1
        print(f"  LLM pass: matched:{llm_matched} new:{llm_new} "
              f"rejected:{llm_rejected} in {elapsed2:.0f}s")

    # Save
    out_path = csv_path.replace("_OLD.csv", ".csv")
    if out_path == csv_path:
        out_path = csv_path.replace(".csv", "_treatment_fixed.csv")
    df.to_csv(out_path, index=False)
    print(f"\n  Saved: {out_path} ({len(df):,} rows)")

    # Stats
    ns_after = df[treat_col].astype(str).str.strip().str.lower().isin(
        {"not specified", "n/a", "none", "unknown", "na", ""}).sum()
    print(f"  Treatment NS: {ns_after:,}/{len(df):,} ({100*ns_after/len(df):.1f}%)")
    print(f"  Treatment resolved: {len(df)-ns_after:,}/{len(df):,}")
    return out_path


def main():
    print(f"{'='*60}")
    print(f"  TREATMENT-ONLY RE-COLLAPSE")
    print(f"  Fixes Treatment_after in already-completed platforms")
    print(f"  Tissue + Condition are NOT touched")
    print(f"{'='*60}")

    # Ensure Ollama is running
    if not G.ollama_server_ok(OLLAMA_URL):
        print("\nStarting Ollama...")
        G.start_ollama_server_blocking(print, 210)

    # Find platforms with old results (Treatment not collapsed)
    platforms = []
    for gpl in ["GPL570", "GPL10558", "GPL96", "GPL6947"]:
        d = os.path.join(BASE, f"{gpl}_NS_repaired_final_results")
        old = os.path.join(d, "NS_repaired_OLD.csv")
        live_old = os.path.join(d, "NS_repaired_live_OLD.csv")
        live = os.path.join(d, "NS_repaired_live.csv")
        # Prefer OLD backup (was the completed run without Treatment collapse)
        if os.path.isfile(old):
            platforms.append((gpl, old))
        elif os.path.isfile(live_old):
            platforms.append((gpl, live_old))
        elif os.path.isfile(live):
            platforms.append((gpl, live))

    if not platforms:
        print("\nNo platforms with old results found.")
        return

    print(f"\nPlatforms to fix: {len(platforms)}")
    for gpl, path in platforms:
        n = sum(1 for _ in open(path)) - 1
        print(f"  {gpl}: {n:,} rows in {os.path.basename(path)}")

    t_total = time.time()
    for gpl, path in platforms:
        fix_treatment(gpl, path)

    elapsed = time.time() - t_total
    print(f"\n{'='*60}")
    print(f"  ALL DONE in {elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
