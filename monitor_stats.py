#!/usr/bin/env python3
"""Quick stats for monitor.sh — uses cached platform list for speed."""
import os, sys, datetime

BASE = os.path.dirname(os.path.abspath(__file__))
P1B_RATE = int(sys.argv[1]) if len(sys.argv) > 1 else 320
P1B_DONE = int(sys.argv[2]) if len(sys.argv) > 2 else 0
P1B_TOTAL = int(sys.argv[3]) if len(sys.argv) > 3 else 0
MODE = sys.argv[4] if len(sys.argv) > 4 else "CHUNKED"

CACHE = os.path.join(BASE, ".platform_cache.txt")
if not os.path.isfile(CACHE):
    print("NO_DB"); sys.exit()

# Load cached platform list (gpl\tsamples per line)
platforms = []
with open(CACHE) as f:
    for line in f:
        parts = line.strip().split("\t")
        if len(parts) == 2:
            platforms.append((parts[0], int(parts[1])))

P1_MS, P1B_MS, P2_MS = 174, max(P1B_RATE, 300), 296
plats_done = 0
plats_total = len(platforms)
samples_total = sum(n for _, n in platforms)
samples_done = 0
remaining_sec = 0.0

for gpl, n in platforms:
    rd = os.path.join(BASE, f"{gpl}_NS_repaired_final_results")
    final = os.path.join(rd, "NS_repaired.csv")
    live = os.path.join(rd, "NS_repaired_live.csv")

    if os.path.isfile(final):
        plats_done += 1
        samples_done += n
        continue

    # Live CSV progress
    live_done = 0
    if os.path.isfile(live):
        with open(live) as f:
            live_done = max(0, sum(1 for _ in f) - 1)
    samples_done += live_done

    # P1 checkpoint?
    p1_ckpt = os.path.join(rd, "checkpoints", "phase1_extracted.json")
    has_p1 = os.path.isfile(p1_ckpt)

    p2_rem = max(0, n - live_done)
    if not has_p1:
        remaining_sec += n * P1_MS / 1000
        remaining_sec += int(n * 0.28) * P1B_MS / 1000
    else:
        if P1B_TOTAL > 0 and P1B_DONE < P1B_TOTAL:
            remaining_sec += max(0, P1B_TOTAL - P1B_DONE) * P1B_MS / 1000
        else:
            remaining_sec += int(n * 0.10) * P1B_MS / 1000
    remaining_sec += p2_rem * P2_MS / 1000

# Chunk overhead
chunks = 0
if MODE == "CHUNKED":
    chunks = max(1, int(remaining_sec / 3000) + 2)  # ~50min processing per 10K chunk
    remaining_sec += chunks * 211  # 210s setup + 1s cooldown per chunk

samples_rem = samples_total - samples_done
pct = int(samples_done * 100 / samples_total) if samples_total > 0 else 0
h = int(remaining_sec // 3600)
m = int((remaining_sec % 3600) // 60)
finish = datetime.datetime.now() + datetime.timedelta(seconds=remaining_sec)
days = remaining_sec / 86400

print(f"{plats_done}|{plats_total}|{samples_done}|{samples_total}|{samples_rem}|{pct}|{h}h {m}m|{finish.strftime('%a %b %d %H:%M')}|{chunks}|{days:.1f}")
