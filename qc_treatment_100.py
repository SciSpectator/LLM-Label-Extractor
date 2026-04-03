#!/usr/bin/env python3
"""Quick QC: 100-sample Treatment collapse test.  Run when pipeline is idle."""
import sys, time, json, random, statistics
sys.path.insert(0, ".")
import llm_extractor as G

OLLAMA_URL = "http://localhost:11434"

print("Loading MemoryAgent...")
ma = G.MemoryAgent("biomedical_memory.db", OLLAMA_URL)
ma.load_cache_all(log_fn=print)
print(f"is_ready: {', '.join(f'{c}={ma.is_ready(c)}' for c in G.LABEL_COLS)}")

print("\nLoading GPL570 Phase 1 checkpoint...")
with open("GPL570_NS_repaired_final_results/checkpoints/phase1_extracted.json") as f:
    p1 = json.load(f)

random.seed(42)
cands = [(g, l) for g, l in p1.items()
         if any(not G.is_ns(l.get(c, G.NS)) for c in G.LABEL_COLS)]
test = random.sample(cands, 100)
print(f"  {len(test)} samples selected\n")

wd = G.Watchdog(log_fn=lambda m: None)
cw = G.CollapseWorker("gemma2:2b", OLLAMA_URL, ma, watchdog=wd, log_fn=lambda m: None)

print(f"{'='*70}")
print(f"  QC — 100 samples × 3 fields")
print(f"{'='*70}\n")

st = {c: {"m": 0, "n": 0, "ni": 0, "r": 0, "tot": 0, "t": [], "ru": {}}
      for c in G.LABEL_COLS}

for i, (gsm, labs) in enumerate(test):
    ctx = G.GSEContext("QC")
    for col in G.LABEL_COLS:
        raw = labs.get(col, G.NS)
        st[col]["tot"] += 1
        if G.is_ns(raw):
            st[col]["ni"] += 1
            continue
        t0 = time.perf_counter()
        final, collapsed, rule, audit = cw.collapse_field(
            gsm=gsm, col=col, raw_label=raw, gse_ctx=ctx, raw={}, platform="GPL570")
        ms = (time.perf_counter() - t0) * 1000
        st[col]["t"].append(ms)
        st[col]["ru"][rule] = st[col]["ru"].get(rule, 0) + 1
        if G.is_ns(final):
            st[col]["r"] += 1; tag = "REJ"
        elif "new_cluster" in str(rule):
            st[col]["n"] += 1; tag = "NEW"
        else:
            st[col]["m"] += 1; tag = "OK"
        if i < 8 or tag == "NEW":
            print(f"  [{i+1:3d}] {col:10s} {tag:3s} {ms:5.0f}ms  "
                  f'"{raw[:28]}" -> "{final[:28]}"  [{rule}]')
    if i == 7:
        print("  ... (remaining 92 samples)\n")

print(f"\n{'='*70}")
print(f"  RESULTS")
print(f"{'='*70}")
for col in G.LABEL_COLS:
    s = st[col]
    nn = s["tot"] - s["ni"]
    rs = s["m"] + s["n"]
    pct = 100 * rs / nn if nn else 0
    avg = statistics.mean(s["t"]) if s["t"] else 0
    print(f"  {col:12s}  matched:{s['m']:3d}  new:{s['n']:2d}  "
          f"rejected:{s['r']:3d}  ns_input:{s['ni']:3d}  "
          f"resolution:{pct:.0f}%  mean:{avg:.0f}ms")
    print(f"               rules: "
          f"{dict(sorted(s['ru'].items(), key=lambda x: -x[1]))}")

at = [t for s in st.values() for t in s["t"]]
tn = sum(s["tot"] - s["ni"] for s in st.values())
tr = sum(s["m"] + s["n"] for s in st.values())
rj = sum(s["r"] for s in st.values())
print(f"\n  OVERALL: {tr}/{tn} = {100*tr/tn:.1f}%  "
      f"|  rejected: {rj}  |  mean={statistics.mean(at):.0f}ms")
tf = sum(1 for l in p1.values()
         for c in G.LABEL_COLS if not G.is_ns(l.get(c, G.NS)))
print(f"  GPL570: {tf:,} fields | 210 workers: "
      f"~{tf * statistics.mean(at) / 210 / 60000:.1f} min")
