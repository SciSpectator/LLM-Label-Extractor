#!/usr/bin/env python3
"""
Diagnostic profiler v2: test NEW per-label agent architecture on 20 samples.
Each phase uses independent per-label LLM agents with per-GSE context.
"""
import sys, os, time, json, traceback, threading
import psutil
from concurrent.futures import ThreadPoolExecutor as TPE

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import llm_extractor as ext

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(SCRIPT_DIR, "GEOmetadb.sqlite")
GSM_FILE   = "/home/mwinn99/Downloads/GSMsforMateusz.csv"
MODEL      = ext.DEFAULT_MODEL
OLLAMA_URL = ext.DEFAULT_URL
N_SAMPLES  = 20

def mem_mb():
    return psutil.Process().memory_info().rss / 1024**2

def gpu_vram():
    try:
        u, t, pct = ext._get_vram_usage()
        return u, t
    except:
        return 0, 0

def phase_timer(name):
    class Timer:
        def __enter__(self):
            self.t0 = time.time()
            self.mem0 = mem_mb()
            self.gpu0 = gpu_vram()
            print(f"\n{'='*60}")
            print(f"  START: {name}")
            print(f"  RAM: {self.mem0:.0f} MB  |  GPU VRAM: {self.gpu0[0]:,}/{self.gpu0[1]:,} MB")
            print(f"{'='*60}")
            return self
        def __exit__(self, *args):
            dt = time.time() - self.t0
            mem1 = mem_mb()
            gpu1 = gpu_vram()
            print(f"\n{'─'*60}")
            print(f"  DONE: {name}")
            print(f"  Time: {dt:.1f}s  ({dt/max(N_SAMPLES,1):.2f}s/sample)")
            print(f"  RAM: {self.mem0:.0f} → {mem1:.0f} MB (Δ{mem1-self.mem0:+.0f} MB)")
            print(f"  GPU: {self.gpu0[0]:,} → {gpu1[0]:,} MB (Δ{gpu1[0]-self.gpu0[0]:+,} MB)")
            print(f"{'─'*60}")
    return Timer()

def noop_log(msg): pass

def main():
    print(f"PROFILING v2 — {N_SAMPLES} SAMPLES — PER-LABEL AGENTS")
    print(f"PID: {os.getpid()}  |  Initial RAM: {mem_mb():.0f} MB")

    # ── Ollama startup ──
    with phase_timer("Ollama startup"):
        ext._kill_ollama(noop_log)
        time.sleep(1)
        num_p, _, _ = ext.compute_ollama_parallel(MODEL)
        num_p = min(num_p, 3)
        server_proc = ext.start_ollama_server_blocking(noop_log, num_p)
        for mdl in [MODEL, ext.EXTRACTION_MODEL]:
            if not ext.model_available(mdl, OLLAMA_URL):
                ext.pull_model_blocking(mdl, noop_log)
        if ext._OLLAMA_LIB_OK:
            ext._ollama_lib.chat(model=ext.EXTRACTION_MODEL,
                                 messages=[{"role":"user","content":"1"}],
                                 options={"num_predict":1,"num_ctx":512},
                                 keep_alive=-1, stream=False)
        print(f"  Ollama ready with {num_p} parallel slots")

    # ── Load data ──
    import pandas as pd
    with phase_timer("Load GSM list + GEOmetadb + metadata"):
        target = ext.load_gsm_list(GSM_FILE, "SCRATCH_profile")
        target = target.head(N_SAMPLES).copy()
        mem_conn = ext.load_db_to_memory(DB_PATH, noop_log)
        all_gsms = target["gsm"].tolist()
        ph = ",".join("?" * len(all_gsms))
        gse_map_df = pd.read_sql_query(
            f"SELECT gsm, series_id FROM gsm WHERE gsm IN ({ph})",
            mem_conn, params=all_gsms)
        gse_map = dict(zip(gse_map_df["gsm"], gse_map_df["series_id"]))
        target["series_id"] = target["gsm"].map(gse_map).fillna("UNKNOWN")
        raw_map = ext.fetch_gsm_raw(mem_conn, all_gsms)
        mem_conn.close()
        gse_ids = [g for g in target["series_id"].unique() if g and g != "UNKNOWN"]
        gse_meta = ext.scrape_gse_meta(gse_ids, noop_log)
        print(f"  {len(target)} samples, {len(gse_ids)} GSEs, {len(raw_map)} raw records")

    # ── Memory Agent ──
    with phase_timer("Build Memory Agent"):
        mem_db_path = os.path.join(SCRIPT_DIR, ext.MEM_DB_NAME)
        mem_agent = ext.MemoryAgent(mem_db_path, OLLAMA_URL)
        mem_agent.load_cache_all(log_fn=noop_log)
        stats = mem_agent.stats()
        print(f"  Clusters: {stats.get('clusters',{})}")

    # ── Build GSEContexts ──
    _cols = ext.LABEL_COLS_SCRATCH
    NS = ext.NS
    with phase_timer("Build GSEContexts"):
        gse_contexts = {}
        for gse, grp in target.groupby("series_id"):
            if not gse or gse == "UNKNOWN": continue
            ctx = ext.GSEContext(str(gse))
            meta = gse_meta.get(str(gse), {})
            ctx.set_meta(meta.get("gse_title",""),
                         meta.get("gse_summary",""),
                         meta.get("gse_design",""))
            for _, row in grp.iterrows():
                gsm = str(row.get("gsm","")).strip()
                labels = {c: str(row.get(c, NS)).strip() for c in _cols}
                ctx.add_sample(gsm, labels, mem_agent=mem_agent)
            gse_contexts[gse] = ctx
        print(f"  {len(gse_contexts)} GSE contexts (each has its OWN sibling labels)")

    # ════════════════════════════════════════════════════════════════════
    # PHASE 1: Per-label extraction (3 independent LLM agents per sample)
    # ════════════════════════════════════════════════════════════════════
    phase1_extracted = {}
    gsm_to_gse = dict(zip(target["gsm"], target["series_id"]))

    # Build workers per GSE
    p1_workers = {}
    for gse_ in gse_contexts:
        p1_workers[gse_] = ext.GSEWorker(
            gse_, gse_contexts[gse_], MODEL, OLLAMA_URL,
            None, mem_agent=mem_agent, platform="PROFILE")

    with phase_timer("PHASE 1: Per-label extraction (3 agents × 20 samples)"):
        for idx, (_, row) in enumerate(target.iterrows()):
            gsm = str(row["gsm"]).strip()
            gse = str(row.get("series_id","")).strip()
            raw = raw_map.get(gsm, {})
            worker = p1_workers.get(gse) or ext.GSEWorker(
                gse, ext.GSEContext(gse), MODEL, OLLAMA_URL,
                None, mem_agent=mem_agent, platform="PROFILE")

            _title = str(raw.get("gsm_title","")).strip()[:80]
            _source = str(raw.get("source_name","")).strip()[:80]
            _char = str(raw.get("characteristics","")).replace("\t"," ").strip()[:300]
            _treat = str(raw.get("treatment_protocol","")).replace("\t"," ").strip()[:200]
            _desc = str(raw.get("description","")).replace("\t"," ").strip()[:200]
            _gse_info = gse_meta.get(gse, {})
            _gse_ctx = ""
            if _gse_info.get("title"):
                _gse_ctx += f"Experiment: {_gse_info['title'][:120]}\n"
            if _gse_info.get("summary"):
                _gse_ctx += f"Summary: {_gse_info['summary'][:250]}\n"

            t_sample = time.time()
            result = {c: NS for c in _cols}
            col_times = {}

            def _call_one(col_):
                t0 = time.time()
                prompt_ = (ext._PER_LABEL_EXTRACT_PROMPTS[col_]
                    .replace("{TITLE}", _title)
                    .replace("{SOURCE}", _source)
                    .replace("{CHAR}", _char))
                if col_ == "Treatment" and _treat:
                    prompt_ += f"\nTreatment protocol: {_treat}"
                if _desc:
                    prompt_ += f"\nDescription: {_desc}"
                if _gse_ctx:
                    prompt_ += f"\n{_gse_ctx}"
                text_ = worker._llm_with_model(
                    prompt_, model=ext.EXTRACTION_MODEL,
                    max_tokens=60, system="")
                val = ext._parse_single_label(text_)
                return col_, val, time.time() - t0

            with TPE(max_workers=3) as ex:
                for col_r, val_r, dt_r in ex.map(_call_one, _cols):
                    result[col_r] = val_r
                    col_times[col_r] = dt_r

            phase1_extracted[gsm] = result
            dt_s = time.time() - t_sample
            detail = " | ".join(f"{c}={result[c][:25]}({col_times.get(c,0):.2f}s)" for c in _cols)
            print(f"  [{idx+1}/{N_SAMPLES}] {gsm} {dt_s:.2f}s — {detail}")

    # ════════════════════════════════════════════════════════════════════
    # PHASE 1b: Per-label NS inference (each label has its OWN GSE context)
    # ════════════════════════════════════════════════════════════════════
    ns_after_p1 = [(gsm, labs) for gsm, labs in phase1_extracted.items()
                   if any(ext.is_ns(labs.get(c, NS)) for c in _cols)]

    with phase_timer("PHASE 1b: Per-label GSE inference (independent agents)"):
        print(f"  {len(ns_after_p1)} samples still have NS fields")
        gsm_to_gse_map = dict(zip(target["gsm"].astype(str),
                                   target["series_id"].astype(str)))
        # One GSEInferencer PER GSE — each has its OWN experiment context
        p1b_inferencers = {}
        for gse_ in set(gsm_to_gse_map.values()):
            gi = gse_meta.get(gse_, {})
            if gi.get("gse_title") or gi.get("title"):
                p1b_inferencers[gse_] = ext.GSEInferencer(
                    gse_, gi, OLLAMA_URL, watchdog=None, log_fn=noop_log)
        print(f"  {len(p1b_inferencers)} GSEInferencers (one per GSE, 3 label agents each)")

        for idx, (gsm, current_labels) in enumerate(ns_after_p1):
            t_sample = time.time()
            ns_fields = [c for c in _cols if ext.is_ns(current_labels.get(c, NS))]
            gse = gsm_to_gse_map.get(gsm, "")
            inferencer = p1b_inferencers.get(gse)
            if inferencer:
                raw = raw_map.get(gsm, {})
                updated = inferencer.infer_sample(gsm, raw, current_labels, _cols)
                phase1_extracted[gsm] = updated
            dt_s = time.time() - t_sample
            resolved = [c for c in ns_fields if not ext.is_ns(phase1_extracted[gsm].get(c, NS))]
            print(f"  [{idx+1}/{len(ns_after_p1)}] {gsm} {dt_s:.2f}s — NS:{ns_fields} → resolved:{resolved}")

    # ── Seed GSEContexts ──
    with phase_timer("Seed GSEContexts with Phase 1 labels"):
        seeded = 0
        for gsm, labels in phase1_extracted.items():
            gse_of_gsm = gsm_to_gse.get(gsm, "")
            ctx = gse_contexts.get(gse_of_gsm)
            if ctx is None: continue
            for col in _cols:
                val = labels.get(col, NS)
                if not ext.is_ns(val):
                    cased = mem_agent.cluster_lookup(col, val) if mem_agent else None
                    val = cased if cased else val
                    ctx.label_counts[col][val] += 1
                    seeded += 1
        print(f"  Seeded {seeded} labels into GSE sibling contexts")

    # ════════════════════════════════════════════════════════════════════
    # PHASE 2: Per-label collapse (single LLM call, NOT ReAct)
    # ════════════════════════════════════════════════════════════════════
    with phase_timer("PHASE 2: Per-label collapse (single LLM, no ReAct)"):
        cw = ext.CollapseWorker(MODEL, OLLAMA_URL, mem_agent,
                                watchdog=None, log_fn=noop_log)

        total_deterministic = 0
        total_llm = 0
        total_no_match = 0
        per_sample_times = []

        for idx, (_, row) in enumerate(target.iterrows()):
            gsm = str(row["gsm"]).strip()
            gse = str(row.get("series_id","")).strip()
            raw = raw_map.get(gsm, {})
            pre = phase1_extracted.get(gsm, {})
            gse_ctx = gse_contexts.get(gse, ext.GSEContext(gse))
            current = {c: NS for c in _cols}

            t_sample = time.time()
            col_details = []
            for col in _cols:
                t_col = time.time()
                raw_label = pre.get(col, NS)
                final, collapsed, rule, audit = cw.collapse_field(
                    gsm=gsm, col=col, raw_label=raw_label,
                    gse_ctx=gse_ctx, raw=raw, platform="PROFILE")
                dt_col = time.time() - t_col

                if "llm" in rule:
                    total_llm += 1
                elif collapsed:
                    total_deterministic += 1
                else:
                    total_no_match += 1

                col_details.append(f"{col}={final[:20]}({rule},{dt_col:.3f}s)")

            dt_s = time.time() - t_sample
            per_sample_times.append(dt_s)
            print(f"  [{idx+1}/{N_SAMPLES}] {gsm} {dt_s:.3f}s — {' | '.join(col_details)}")

        avg_t = sum(per_sample_times) / len(per_sample_times) if per_sample_times else 0
        print(f"\n  Phase 2 SUMMARY:")
        print(f"    Avg time/sample: {avg_t:.3f}s")
        print(f"    Deterministic:   {total_deterministic}")
        print(f"    LLM collapse:    {total_llm}")
        print(f"    No match:        {total_no_match}")

    # ── Cleanup ──
    ext._kill_ollama(noop_log)
    print(f"\n{'='*60}")
    print(f"  PROFILING v2 COMPLETE — Final RAM: {mem_mb():.0f} MB")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
