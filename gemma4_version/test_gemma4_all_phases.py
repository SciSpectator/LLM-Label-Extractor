#!/usr/bin/env python3
"""Test gemma4:e2b across ALL 3 phases on 20 diverse samples."""
import sys, os, time, requests, json
from concurrent.futures import ThreadPoolExecutor as TPE
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import llm_extractor as ext

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GEMMA4 = 'gemma4:e2b'
OLLAMA_URL = 'http://localhost:11434'
NS = ext.NS
_cols = ext.LABEL_COLS_SCRATCH
N = 20


def call_gemma4(prompt, max_tokens=60, system=""):
    """gemma4:e2b via shared pipeline helper — think=false."""
    return ext._llm_call_think_off(GEMMA4, prompt, OLLAMA_URL,
                                    max_tokens=max_tokens, system=system)


def mem_mb():
    import psutil
    return psutil.Process().memory_info().rss / 1024**2


def main():
    print(f"ALL PHASES — gemma4:e2b think=false — {N} samples")
    print(f"Initial RAM: {mem_mb():.0f} MB\n")

    # Startup
    ext._kill_ollama(lambda m: None)
    time.sleep(2)
    ext.start_ollama_server_blocking(lambda m: None, 3)
    # Pre-load gemma4
    call_gemma4("test", 1)
    print("gemma4:e2b loaded.\n")

    # Load data — pick diverse samples
    import pandas as pd
    mem_conn = ext.load_db_to_memory(os.path.join(SCRIPT_DIR, 'GEOmetadb.sqlite'), lambda m: None)
    target_full = ext.load_gsm_list('/home/mwinn99/Downloads/GSMsforMateusz.csv', 'TEST')
    all_gsms = target_full['gsm'].tolist()
    ph = ','.join('?' * len(all_gsms))
    gse_df = pd.read_sql_query(f'SELECT gsm, series_id FROM gsm WHERE gsm IN ({ph})', mem_conn, params=all_gsms)
    gse_map = dict(zip(gse_df['gsm'], gse_df['series_id']))
    target_full['series_id'] = target_full['gsm'].map(gse_map).fillna('UNKNOWN')

    # Pick 2 from top 10 GSEs
    gse_counts = target_full['series_id'].value_counts()
    diverse_gsms = []
    for gse in gse_counts.index[:10]:
        diverse_gsms.extend(target_full[target_full['series_id'] == gse]['gsm'].head(2).tolist())
    diverse_gsms = diverse_gsms[:N]

    raw_map = ext.fetch_gsm_raw(mem_conn, diverse_gsms)
    mem_conn.close()

    gse_ids = list(set(gse_map[g] for g in diverse_gsms if g in gse_map))
    gse_meta = ext.scrape_gse_meta(gse_ids, lambda m: None)

    # Memory agent
    mem_agent = ext.MemoryAgent(os.path.join(SCRIPT_DIR, ext.MEM_DB_NAME), OLLAMA_URL)
    mem_agent.load_cache_all(log_fn=lambda m: None)

    # GSE contexts
    gse_contexts = {}
    for gse_ in set(gse_map[g] for g in diverse_gsms if g in gse_map):
        ctx = ext.GSEContext(str(gse_))
        meta = gse_meta.get(str(gse_), {})
        ctx.set_meta(meta.get('gse_title', ''), meta.get('gse_summary', ''), meta.get('gse_design', ''))
        gse_contexts[gse_] = ctx

    # ════════════════════════════════════════════════════════════════
    # PHASE 1: Per-label extraction (3 parallel agents per sample)
    # ════════════════════════════════════════════════════════════════
    print("=" * 80)
    print("  PHASE 1: Per-label extraction (gemma4:e2b, think=false)")
    print("=" * 80)
    phase1 = {}
    t_phase1 = time.time()

    for idx, gsm in enumerate(diverse_gsms):
        raw = raw_map.get(gsm, {})
        gse = gse_map.get(gsm, '')
        _title = str(raw.get('gsm_title', '')).strip()[:80]
        _source = str(raw.get('source_name', '')).strip()[:80]
        _char = str(raw.get('characteristics', '')).replace('\t', ' ').strip()[:300]
        _treat = str(raw.get('treatment_protocol', '')).replace('\t', ' ').strip()[:200]
        _desc = str(raw.get('description', '')).replace('\t', ' ').strip()[:200]
        _gse_info = gse_meta.get(gse, {})
        _gse_ctx = ''
        if _gse_info.get('title'): _gse_ctx += f"Experiment: {_gse_info['title'][:120]}\n"
        if _gse_info.get('summary'): _gse_ctx += f"Summary: {_gse_info['summary'][:250]}\n"

        def _extract(col_):
            p = (ext._PER_LABEL_EXTRACT_PROMPTS[col_]
                 .replace('{TITLE}', _title).replace('{SOURCE}', _source).replace('{CHAR}', _char))
            if col_ == 'Treatment' and _treat: p += f'\nTreatment protocol: {_treat}'
            if _desc: p += f'\nDescription: {_desc}'
            if _gse_ctx: p += f'\n{_gse_ctx}'
            return col_, ext._parse_single_label(call_gemma4(p, 60))

        t0 = time.time()
        result = {c: NS for c in _cols}
        with TPE(max_workers=3) as ex:
            for col_r, val_r in ex.map(_extract, _cols):
                result[col_r] = val_r
        phase1[gsm] = result
        dt = time.time() - t0
        vals = " | ".join(f"{c}={result[c][:20]}" for c in _cols)
        print(f"  [{idx+1}/{N}] {gsm} {dt:.2f}s — {vals}")

    dt_p1 = time.time() - t_phase1
    print(f"\n  Phase 1 TOTAL: {dt_p1:.1f}s  ({dt_p1/N:.2f}s/sample)\n")

    # ════════════════════════════════════════════════════════════════
    # PHASE 1b: Per-label GSE inference for NS fields
    # ════════════════════════════════════════════════════════════════
    ns_samples = [(gsm, labs) for gsm, labs in phase1.items()
                  if any(ext.is_ns(labs.get(c, NS)) for c in _cols)]
    print("=" * 80)
    print(f"  PHASE 1b: GSE inference ({len(ns_samples)} samples with NS fields)")
    print("=" * 80)
    t_phase1b = time.time()

    for idx, (gsm, current) in enumerate(ns_samples):
        gse = gse_map.get(gsm, '')
        raw = raw_map.get(gsm, {})
        ns_fields = [c for c in _cols if ext.is_ns(current.get(c, NS))]
        _gse_info = gse_meta.get(gse, {})
        _title = str(raw.get('gsm_title', '')).strip()[:80]
        _source = str(raw.get('source_name', '')).strip()[:80]
        _char = str(raw.get('characteristics', '')).replace('\t', ' ').strip()[:300]

        def _infer(col_):
            sys_prompt = (ext._PER_LABEL_INFER_SYSTEMS[col_]
                          .replace('{GSE_TITLE}', (_gse_info.get('gse_title') or _gse_info.get('title', ''))[:200])
                          .replace('{GSE_SUMMARY}', (_gse_info.get('gse_summary') or _gse_info.get('summary', ''))[:400])
                          .replace('{GSE_DESIGN}', (_gse_info.get('gse_design') or _gse_info.get('design', ''))[:300]))
            user_msg = f"Title: {_title}\nSource: {_source}\nCharacteristics: {_char}\nANSWER:"
            return col_, ext._parse_single_label(call_gemma4(user_msg, 60, system=sys_prompt))

        t0 = time.time()
        with TPE(max_workers=len(ns_fields)) as ex:
            for col_r, val_r in ex.map(_infer, ns_fields):
                if not ext.is_ns(val_r):
                    phase1[gsm][col_r] = val_r
        dt = time.time() - t0
        resolved = [c for c in ns_fields if not ext.is_ns(phase1[gsm].get(c, NS))]
        print(f"  [{idx+1}/{len(ns_samples)}] {gsm} {dt:.2f}s — NS:{ns_fields} resolved:{resolved}")

    dt_p1b = time.time() - t_phase1b
    print(f"\n  Phase 1b TOTAL: {dt_p1b:.1f}s  ({dt_p1b/max(len(ns_samples),1):.2f}s/sample)\n")

    # Seed GSE contexts
    for gsm, labels in phase1.items():
        gse = gse_map.get(gsm, '')
        ctx = gse_contexts.get(gse)
        if not ctx: continue
        for col in _cols:
            val = labels.get(col, NS)
            if not ext.is_ns(val):
                cased = mem_agent.cluster_lookup(col, val) if mem_agent else None
                ctx.label_counts[col][cased or val] += 1

    # ════════════════════════════════════════════════════════════════
    # PHASE 2: Per-label collapse (single LLM call per label)
    # ════════════════════════════════════════════════════════════════
    print("=" * 80)
    print("  PHASE 2: Per-label collapse (gemma4:e2b, think=false)")
    print("=" * 80)

    # Patch CollapseWorker to use gemma4 via HTTP
    cw = ext.CollapseWorker(ext.DEFAULT_MODEL, OLLAMA_URL, mem_agent,
                            watchdog=None, log_fn=lambda m: None)
    # Override _llm_single to use gemma4
    def _llm_gemma4(prompt, max_tokens=60):
        return call_gemma4(prompt, max_tokens)
    cw._llm_single = _llm_gemma4

    t_phase2 = time.time()
    total_det = 0
    total_llm = 0
    total_nomatch = 0

    for idx, gsm in enumerate(diverse_gsms):
        gse = gse_map.get(gsm, '')
        raw = raw_map.get(gsm, {})
        pre = phase1.get(gsm, {})
        gse_ctx = gse_contexts.get(gse, ext.GSEContext(gse))

        t0 = time.time()
        col_details = []
        for col in _cols:
            raw_label = pre.get(col, NS)
            final, collapsed, rule, audit = cw.collapse_field(
                gsm=gsm, col=col, raw_label=raw_label,
                gse_ctx=gse_ctx, raw=raw, platform="TEST")
            if 'llm' in rule: total_llm += 1
            elif collapsed: total_det += 1
            else: total_nomatch += 1
            col_details.append(f"{col}={final[:18]}({rule[:15]})")
        dt = time.time() - t0
        print(f"  [{idx+1}/{N}] {gsm} {dt:.2f}s — {' | '.join(col_details)}")

    dt_p2 = time.time() - t_phase2
    print(f"\n  Phase 2 TOTAL: {dt_p2:.1f}s  ({dt_p2/N:.2f}s/sample)")
    print(f"  Deterministic: {total_det}  LLM: {total_llm}  No match: {total_nomatch}")

    # ════════════════════════════════════════════════════════════════
    # SUMMARY
    # ════════════════════════════════════════════════════════════════
    total = dt_p1 + dt_p1b + dt_p2
    print(f"\n{'='*80}")
    print(f"  TOTAL ALL PHASES: {total:.1f}s  ({total/N:.2f}s/sample)")
    print(f"    Phase 1:  {dt_p1:.1f}s  ({dt_p1/N:.2f}s/sample)")
    print(f"    Phase 1b: {dt_p1b:.1f}s  ({dt_p1b/max(len(ns_samples),1):.2f}s/sample)")
    print(f"    Phase 2:  {dt_p2:.1f}s  ({dt_p2/N:.2f}s/sample)")
    print(f"  ETA for 1,983 samples (3 workers): ~{1983 * (total/N) / 3 / 60:.0f} min")
    print(f"  RAM: {mem_mb():.0f} MB")
    print(f"{'='*80}")

    ext._kill_ollama(lambda m: None)


if __name__ == '__main__':
    main()
