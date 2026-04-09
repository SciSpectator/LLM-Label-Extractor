#!/usr/bin/env python3
"""
CREEDS Benchmark Evaluation — Reproducible for Publication
==========================================================
Downloads CREEDS disease signatures, samples 1000 GSMs, runs them through
the TAILS pipeline, and computes Tissue + Condition metrics against
CREEDS-curated ground truth.

Usage:
    python eval_creeds_benchmark.py

Output:
    eval_creeds_results/
        creeds_ground_truth.csv       — 1000 GSMs with CREEDS-curated labels
        pipeline_labels.csv           — pipeline extraction results
        evaluation_report.csv         — per-sample comparison
        metrics_summary.txt           — precision, recall, F1
        confusion_tissue.csv          — tissue confusion matrix (top labels)
        confusion_condition.csv       — condition confusion matrix (top labels)

Reproducibility: random seed = 42, CREEDS v1.0, gemma4:e2b with think=false.
"""

import os, sys, json, time, random, queue, threading, re
import pandas as pd
import numpy as np
from collections import Counter

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)

# ── Configuration ────────────────────────────────────────────────────────────
SEED            = 42
N_SAMPLES       = 1000
CREEDS_URL      = "https://maayanlab.cloud/CREEDS/download/disease_signatures-v1.0.json"
CREEDS_CACHE    = os.path.join(SCRIPT_DIR, ".creeds_disease_signatures.json")
EVAL_DIR        = os.path.join(SCRIPT_DIR, "eval_creeds_results")
MODEL           = "gemma4:e2b"
NUM_WORKERS     = 20      # user-requested high parallelism
OLLAMA_URL      = "http://localhost:11434"

os.environ["OLLAMA_NUM_PARALLEL"] = str(NUM_WORKERS)
os.environ["OLLAMA_FLASH_ATTENTION"] = "1"
os.environ["OLLAMA_MAX_LOADED_MODELS"] = "1"

os.makedirs(EVAL_DIR, exist_ok=True)


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 1: Download and parse CREEDS
# ═════════════════════════════════════════════════════════════════════════════

def download_creeds():
    """Download CREEDS disease signatures (cached locally)."""
    if os.path.isfile(CREEDS_CACHE):
        print(f"  CREEDS cached: {CREEDS_CACHE}")
        with open(CREEDS_CACHE) as f:
            return json.load(f)

    print(f"  Downloading CREEDS from {CREEDS_URL} ...")
    import requests
    resp = requests.get(CREEDS_URL, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    with open(CREEDS_CACHE, "w") as f:
        json.dump(data, f)
    print(f"  Downloaded {len(data)} disease signatures")
    return data


def parse_creeds_to_gsm_labels(signatures):
    """
    Convert CREEDS disease signatures to per-GSM ground truth labels.

    For each signature:
      - pert_ids (disease samples): Tissue = cell_type, Condition = disease_name
      - ctrl_ids (control samples): Tissue = cell_type, Condition = Control

    Returns DataFrame with columns: gsm, gse, tissue_creeds, condition_creeds, signature_id
    """
    rows = []
    for sig in signatures:
        if sig.get("organism", "").lower() != "human":
            continue

        cell_type = (sig.get("cell_type") or "").strip()
        disease   = (sig.get("disease_name") or "").strip()
        gse       = (sig.get("geo_id") or "").strip()
        sig_id    = sig.get("id", "")

        if not cell_type and not disease:
            continue

        # Disease/perturbation samples
        for gsm in (sig.get("pert_ids") or []):
            gsm = gsm.strip()
            if gsm.startswith("GSM"):
                rows.append({
                    "gsm": gsm, "gse": gse,
                    "tissue_creeds": cell_type if cell_type else "Not Specified",
                    "condition_creeds": disease if disease else "Not Specified",
                    "sample_type": "disease",
                    "signature_id": sig_id,
                })

        # Control samples
        for gsm in (sig.get("ctrl_ids") or []):
            gsm = gsm.strip()
            if gsm.startswith("GSM"):
                rows.append({
                    "gsm": gsm, "gse": gse,
                    "tissue_creeds": cell_type if cell_type else "Not Specified",
                    "condition_creeds": "Control",
                    "sample_type": "control",
                    "signature_id": sig_id,
                })

    df = pd.DataFrame(rows)
    # Deduplicate: same GSM may appear in multiple signatures — keep first
    df = df.drop_duplicates(subset=["gsm"], keep="first")
    return df


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 2: Sample 1000 GSMs
# ═════════════════════════════════════════════════════════════════════════════

def sample_gsms(creeds_df, n=N_SAMPLES, seed=SEED):
    """Randomly sample N GSMs from CREEDS ground truth."""
    random.seed(seed)
    np.random.seed(seed)

    # Ensure we have enough
    if len(creeds_df) < n:
        print(f"  WARNING: Only {len(creeds_df)} GSMs available, using all")
        return creeds_df.copy()

    sampled = creeds_df.sample(n=n, random_state=seed).reset_index(drop=True)
    return sampled


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 3: Run pipeline on sampled GSMs
# ═════════════════════════════════════════════════════════════════════════════

def run_pipeline(gsm_list_path):
    """Run the TAILS pipeline on the GSM list."""
    sys.path.insert(0, SCRIPT_DIR)
    import llm_extractor as ext

    config = {
        "db_path":        os.path.join(SCRIPT_DIR, "GEOmetadb.sqlite"),
        "platform":       "CREEDS_EVAL",
        "model":          MODEL,
        "ollama_url":     OLLAMA_URL,
        "harmonized_dir": EVAL_DIR,
        "limit":          None,
        "num_workers":    NUM_WORKERS,
        "skip_install":   True,
        "gsm_list_file":  gsm_list_path,
        "server_proc":    None,
    }

    q = queue.Queue()

    def log(msg):
        q.put({"type": "log", "msg": msg})

    def drain():
        while True:
            try:
                item = q.get(timeout=1)
            except queue.Empty:
                continue
            if item.get("type") == "log":
                print(item["msg"], flush=True)
            elif item.get("type") == "progress":
                lbl = item.get("label", "")
                pct = item.get("pct", 0)
                if lbl:
                    print(f"  [{pct:.0f}%] {lbl}", flush=True)
            elif item.get("type") == "done":
                ok = item.get("success", False)
                print(f"\n  Pipeline: {'SUCCESS' if ok else 'FAILED'}", flush=True)
                return

    drain_thread = threading.Thread(target=drain, daemon=True)
    drain_thread.start()

    # Start Ollama
    server_proc = None
    if not ext.ollama_server_ok(OLLAMA_URL):
        print("  Starting Ollama ...")
        server_proc = ext.start_ollama_server_blocking(log, NUM_WORKERS)
    else:
        ext._kill_ollama(log)
        time.sleep(2)
        server_proc = ext.start_ollama_server_blocking(log, NUM_WORKERS)

    # Ensure model
    if not ext.model_available(MODEL, OLLAMA_URL):
        print(f"  Pulling {MODEL} ...")
        ext.pull_model_blocking(MODEL, log)

    config["server_proc"] = server_proc

    t0 = time.time()
    try:
        ext.pipeline(config, q)
    except Exception as exc:
        import traceback
        print(f"  [ERROR] {exc}")
        traceback.print_exc()
    finally:
        ext._kill_ollama()

    elapsed = time.time() - t0
    drain_thread.join(timeout=10)
    print(f"\n  Pipeline elapsed: {elapsed/60:.1f} min")


# ═════════════════════════════════════════════════════════════════════════════
#  STEP 4: Compute evaluation metrics
# ═════════════════════════════════════════════════════════════════════════════

def normalize_label(text):
    """Normalize a label for comparison: lowercase, strip, remove punctuation."""
    if not text or pd.isna(text):
        return ""
    text = str(text).strip().lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def fuzzy_match(pred, gold):
    """
    Fuzzy matching for biomedical labels.
    Returns: 'exact', 'contained', 'partial', or 'mismatch'
    """
    p = normalize_label(pred)
    g = normalize_label(gold)

    if not p or not g:
        return "mismatch"
    if p == g:
        return "exact"
    if p in g or g in p:
        return "contained"

    # Token overlap
    p_tokens = set(p.split())
    g_tokens = set(g.split())
    if p_tokens and g_tokens:
        overlap = len(p_tokens & g_tokens) / max(len(p_tokens), len(g_tokens))
        if overlap >= 0.5:
            return "partial"

    return "mismatch"


def is_ns(val):
    """Check if a value is Not Specified or equivalent."""
    if not val or pd.isna(val):
        return True
    return normalize_label(val) in (
        "not specified", "n/a", "none", "unknown", "not available",
        "unspecified", "missing", ""
    )


def compute_metrics(eval_df, col_pred, col_gold, label_name):
    """
    Compute evaluation metrics for one label column.
    Returns dict of metrics.
    """
    # Filter out rows where CREEDS ground truth is empty/NS
    valid = eval_df[~eval_df[col_gold].apply(is_ns)].copy()
    total = len(valid)

    if total == 0:
        return {"label": label_name, "total": 0, "note": "no valid ground truth"}

    # Match categories
    valid["_match"] = valid.apply(lambda r: fuzzy_match(r[col_pred], r[col_gold]), axis=1)

    exact     = (valid["_match"] == "exact").sum()
    contained = (valid["_match"] == "contained").sum()
    partial   = (valid["_match"] == "partial").sum()
    mismatch  = (valid["_match"] == "mismatch").sum()

    # Precision: of resolved predictions, how many match?
    resolved = valid[~valid[col_pred].apply(is_ns)]
    n_resolved = len(resolved)
    if n_resolved > 0:
        resolved_matches = resolved["_match"].isin(["exact", "contained", "partial"]).sum()
        precision = resolved_matches / n_resolved
    else:
        precision = 0.0

    # Recall: of ground truth labels, how many did we get right?
    all_matches = exact + contained + partial
    recall = all_matches / total if total > 0 else 0.0

    # F1
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    # Strict metrics (exact match only)
    strict_precision = exact / n_resolved if n_resolved > 0 else 0.0
    strict_recall = exact / total if total > 0 else 0.0
    strict_f1 = (2 * strict_precision * strict_recall /
                 (strict_precision + strict_recall)
                 if (strict_precision + strict_recall) > 0 else 0.0)

    # Resolution rate
    pred_ns = valid[col_pred].apply(is_ns).sum()
    resolution_rate = (total - pred_ns) / total if total > 0 else 0.0

    return {
        "label": label_name,
        "total_with_ground_truth": total,
        "pipeline_resolved": n_resolved,
        "pipeline_ns": total - n_resolved,
        "resolution_rate": f"{resolution_rate:.1%}",
        "exact_match": exact,
        "contained_match": contained,
        "partial_match": partial,
        "mismatch": mismatch,
        "lenient_precision": f"{precision:.3f}",
        "lenient_recall": f"{recall:.3f}",
        "lenient_f1": f"{f1:.3f}",
        "strict_precision": f"{strict_precision:.3f}",
        "strict_recall": f"{strict_recall:.3f}",
        "strict_f1": f"{strict_f1:.3f}",
    }


def build_confusion(eval_df, col_pred, col_gold, top_n=15):
    """Build a confusion-style comparison of top predicted vs gold labels."""
    valid = eval_df[~eval_df[col_gold].apply(is_ns)].copy()
    valid["pred_norm"] = valid[col_pred].apply(normalize_label)
    valid["gold_norm"] = valid[col_gold].apply(normalize_label)

    pairs = valid.groupby(["gold_norm", "pred_norm"]).size().reset_index(name="count")
    pairs = pairs.sort_values("count", ascending=False)
    return pairs.head(top_n * 3)


# ═════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("  CREEDS BENCHMARK EVALUATION")
    print(f"  Samples: {N_SAMPLES} | Seed: {SEED} | Model: {MODEL}")
    print("=" * 70)

    # ── Step 1: Download CREEDS ──────────────────────────────────────────
    print("\n[1/5] Downloading CREEDS disease signatures ...")
    signatures = download_creeds()
    print(f"  Total signatures: {len(signatures)}")

    # ── Step 2: Parse to GSM-level ground truth ──────────────────────────
    print("\n[2/5] Parsing CREEDS to per-GSM ground truth ...")
    creeds_df = parse_creeds_to_gsm_labels(signatures)
    print(f"  Human GSMs with labels: {len(creeds_df):,}")
    print(f"  Unique tissues: {creeds_df['tissue_creeds'].nunique()}")
    print(f"  Unique conditions: {creeds_df['condition_creeds'].nunique()}")
    print(f"  Disease samples: {(creeds_df['sample_type']=='disease').sum():,}")
    print(f"  Control samples: {(creeds_df['sample_type']=='control').sum():,}")

    # ── Step 3: Sample 1000 GSMs ─────────────────────────────────────────
    print(f"\n[3/5] Sampling {N_SAMPLES} GSMs (seed={SEED}) ...")
    sampled = sample_gsms(creeds_df, N_SAMPLES, SEED)
    print(f"  Sampled: {len(sampled)} GSMs from {sampled['gse'].nunique()} GSEs")

    # Save ground truth
    gt_path = os.path.join(EVAL_DIR, "creeds_ground_truth.csv")
    sampled.to_csv(gt_path, index=False)
    print(f"  Ground truth saved: {gt_path}")

    # Save GSM list for pipeline (format: index,gsm)
    gsm_list_path = os.path.join(EVAL_DIR, "creeds_gsm_list.csv")
    gsm_for_pipeline = sampled[["gsm"]].reset_index()
    gsm_for_pipeline.columns = ["", "gsm"]
    gsm_for_pipeline.to_csv(gsm_list_path, index=False)
    print(f"  GSM list saved: {gsm_list_path}")

    # ── Step 4: Run pipeline ─────────────────────────────────────────────
    print(f"\n[4/5] Running TAILS pipeline on {len(sampled)} GSMs ...")
    print(f"  Model: {MODEL} | Workers: {NUM_WORKERS}")

    run_pipeline(gsm_list_path)

    # ── Step 5: Load results and compute metrics per phase ──────────────
    print(f"\n[5/5] Computing evaluation metrics (3 phases) ...")

    # Find pipeline output directory
    results_dir = None
    for d in os.listdir(EVAL_DIR):
        full = os.path.join(EVAL_DIR, d)
        if os.path.isdir(full) and "final_results" in d:
            if os.path.isfile(os.path.join(full, "labels_final.csv")):
                results_dir = full
                break

    if not results_dir:
        print("  [ERROR] Pipeline results not found in eval_creeds_results/")
        print("  Looking for labels_final.csv ...")
        sys.exit(1)

    print(f"  Results dir: {results_dir}")

    # ── Load all 3 phase outputs ──
    # Phase 1: raw extraction only
    # Phase 1b: extraction + NS inference (before collapse)
    # Phase 2: after collapse/normalization (final)
    phase_files = {
        "Phase 1 (Extract)":      "labels_raw.csv",        # Phase 1 only (from phase1_only checkpoint)
        "Phase 1b (NS Infer)":    "labels_phase1b.csv",    # Phase 1 + 1b (after GSE inference)
        "Phase 2 (Collapse)":     "labels_final.csv",      # After normalization/collapse
    }

    all_phase_metrics = {}

    # Metrics summary file
    metrics_path = os.path.join(EVAL_DIR, "metrics_summary.txt")
    with open(metrics_path, "w") as mf:
        mf.write("CREEDS Benchmark Evaluation — TAILS Pipeline (Per-Phase)\n")
        mf.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        mf.write(f"Samples: {N_SAMPLES} | Seed: {SEED} | Model: {MODEL}\n")
        mf.write(f"CREEDS version: disease_signatures-v1.0\n")
        mf.write(f"Pipeline: llm_extractor.py with gemma4:e2b, think=false\n")
        mf.write(f"Workers: {NUM_WORKERS}\n\n")

        for phase_name, csv_name in phase_files.items():
            csv_path = os.path.join(results_dir, csv_name)
            phase_tag = csv_name.replace("labels_", "").replace(".csv", "")

            if not os.path.isfile(csv_path):
                print(f"\n  [SKIP] {phase_name}: {csv_name} not found")
                mf.write(f"\n{'='*60}\n  {phase_name}: FILE NOT FOUND\n{'='*60}\n")
                continue

            phase_df = pd.read_csv(csv_path)
            print(f"\n  ── {phase_name} ({len(phase_df)} rows) ──")

            # Merge with ground truth
            eval_df = sampled.merge(phase_df, on="gsm", how="left", suffixes=("", "_pipeline"))
            matched = eval_df["Tissue"].notna().sum()
            print(f"    Matched: {matched} / {len(eval_df)} GSMs")

            # Save per-phase evaluation report
            eval_path = os.path.join(EVAL_DIR, f"evaluation_report_{phase_tag}.csv")
            eval_df.to_csv(eval_path, index=False)

            # Compute metrics for Tissue and Condition
            tissue_m = compute_metrics(eval_df, "Tissue", "tissue_creeds", "Tissue")
            condition_m = compute_metrics(eval_df, "Condition", "condition_creeds", "Condition")
            all_phase_metrics[phase_name] = [tissue_m, condition_m]

            # Print to console
            print(f"\n    {'METRIC':<32s} {'TISSUE':>10s} {'CONDITION':>10s}")
            print(f"    {'-'*54}")
            for key in tissue_m:
                if key == "label":
                    continue
                tv = tissue_m[key]
                cv = condition_m[key]
                print(f"    {key:<32s} {str(tv):>10s} {str(cv):>10s}")

            # Write to metrics file
            mf.write(f"\n{'='*60}\n")
            mf.write(f"  {phase_name}\n")
            mf.write(f"{'='*60}\n\n")
            for m in [tissue_m, condition_m]:
                mf.write(f"  ── {m['label']} ──\n")
                for k, v in m.items():
                    if k != "label":
                        mf.write(f"    {k:30s}: {v}\n")
                mf.write("\n")

            # Confusion matrices per phase
            for col_pred, col_gold, name in [
                ("Tissue", "tissue_creeds", "tissue"),
                ("Condition", "condition_creeds", "condition"),
            ]:
                conf = build_confusion(eval_df, col_pred, col_gold)
                conf_path = os.path.join(EVAL_DIR, f"confusion_{name}_{phase_tag}.csv")
                conf.to_csv(conf_path, index=False)

            # Print top mismatches for final phase only
            if csv_name == "labels_final.csv":
                for col_pred, col_gold, name in [
                    ("Tissue", "tissue_creeds", "Tissue"),
                    ("Condition", "condition_creeds", "Condition"),
                ]:
                    valid = eval_df[~eval_df[col_gold].apply(is_ns)].copy()
                    valid["_match"] = valid.apply(
                        lambda r: fuzzy_match(r[col_pred], r[col_gold]), axis=1
                    )
                    mismatches = valid[valid["_match"] == "mismatch"]
                    if len(mismatches) > 0:
                        print(f"\n    Top {name} mismatches (pipeline vs CREEDS):")
                        top_mm = mismatches.head(10)
                        for _, row in top_mm.iterrows():
                            print(f"      {row['gsm']}: pipeline='{row[col_pred]}' vs CREEDS='{row[col_gold]}'")

    # ── Cross-phase comparison table ──
    print(f"\n{'='*70}")
    print(f"  CROSS-PHASE COMPARISON")
    print(f"{'='*70}")

    # Build comparison table for key metrics
    comparison_rows = []
    key_metrics = [
        "resolution_rate", "lenient_precision", "lenient_recall", "lenient_f1",
        "strict_precision", "strict_recall", "strict_f1",
    ]

    for label_type in ["Tissue", "Condition"]:
        print(f"\n  ── {label_type} ──")
        header = f"    {'METRIC':<24s}"
        for phase_name in phase_files:
            short = phase_name.split("(")[1].rstrip(")")
            header += f" {short:>14s}"
        print(header)
        print(f"    {'-'*(24 + 15 * len(phase_files))}")

        idx = 0 if label_type == "Tissue" else 1
        for key in key_metrics:
            row = f"    {key:<24s}"
            row_data = {"metric": key, "label": label_type}
            for phase_name in phase_files:
                if phase_name in all_phase_metrics:
                    val = all_phase_metrics[phase_name][idx].get(key, "N/A")
                    row += f" {str(val):>14s}"
                    row_data[phase_name] = val
                else:
                    row += f" {'N/A':>14s}"
            print(row)
            comparison_rows.append(row_data)

    # Save comparison CSV
    comp_df = pd.DataFrame(comparison_rows)
    comp_path = os.path.join(EVAL_DIR, "phase_comparison.csv")
    comp_df.to_csv(comp_path, index=False)

    print(f"\n  Metrics saved: {metrics_path}")
    print(f"  Phase comparison: {comp_path}")
    print(f"\n{'='*70}")
    print(f"  All results saved to: {EVAL_DIR}/")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
