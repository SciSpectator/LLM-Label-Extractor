#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Chunked auto-restart loop
#
# Runs the pipeline in chunks of ~2000 samples, then:
#   1. Saves checkpoint
#   2. Kills Ollama + GPU processes
#   3. Cools down (30 seconds)
#   4. Restarts automatically
#
# This prevents overheating by giving the hardware regular breaks.
#
# Usage:   bash run_chunked_loop.sh
# Stop:    Ctrl-C (or touch .chunked_stop to stop after current chunk)
# Monitor: tail -f chunked_run.log
# ─────────────────────────────────────────────────────────────────────────────

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COOLDOWN=1          # seconds between chunks (minimal cooldown)
MAX_RUNTIME=0       # no timeout — run until batch limit or completion
DONE_MARKER=".chunked_done"
STOP_MARKER=".chunked_stop"
LOOP_LOG="chunked_loop.log"

# Clean markers
rm -f "$DONE_MARKER" "$STOP_MARKER"

CHUNK=0

echo "============================================================"
echo "  CHUNKED AUTO-RESTART LOOP"
echo "  Batch: ~2000 samples per chunk"
echo "  Cooldown: ${COOLDOWN}s between chunks"
echo "  No safety timeout — runs until batch limit"
echo "  Stop gracefully: touch $STOP_MARKER"
echo "============================================================"
echo ""

while true; do
    CHUNK=$((CHUNK + 1))
    TS=$(date '+%Y-%m-%d %H:%M:%S')

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  CHUNK #${CHUNK} starting at ${TS}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "[${TS}] Chunk #${CHUNK} starting" >> "$LOOP_LOG"

    # Run the chunked python script — no timeout
    set +e
    python3 run_chunked_restart.py 2>&1 | tee -a "$LOOP_LOG"
    EXIT_CODE=${PIPESTATUS[0]}
    set -e

    TS_END=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[${TS_END}] Chunk #${CHUNK} finished (exit=$EXIT_CODE)" >> "$LOOP_LOG"

    # Check if all done
    if [ -f "$DONE_MARKER" ]; then
        echo ""
        echo "============================================================"
        echo "  ALL PLATFORMS COMPLETE!"
        echo "  Finished at ${TS_END} after ${CHUNK} chunks"
        echo "============================================================"
        break
    fi

    # Check if user wants to stop
    if [ -f "$STOP_MARKER" ]; then
        echo ""
        echo "  Stop marker found — halting loop."
        rm -f "$STOP_MARKER"
        break
    fi

    # If exit code 2 = user Ctrl-C, don't restart
    if [ "$EXIT_CODE" -eq 130 ] || [ "$EXIT_CODE" -eq 2 ]; then
        echo ""
        echo "  User interrupted — stopping loop."
        break
    fi

    # ── Cleanup between chunks ────────────────────────────────────────────
    echo ""
    echo "  Ensuring all GPU processes are dead …"
    pkill -f "ollama" 2>/dev/null || true
    sleep 2

    # Check GPU temp if nvidia-smi available
    if command -v nvidia-smi &>/dev/null; then
        GPU_TEMP=$(nvidia-smi --query-gpu=temperature.gpu --format=csv,noheader 2>/dev/null || echo "?")
        echo "  GPU temperature: ${GPU_TEMP}°C"

        # If GPU is very hot (>80°C), extend cooldown
        if [ "$GPU_TEMP" != "?" ] && [ "$GPU_TEMP" -gt 80 ] 2>/dev/null; then
            EXTRA_COOL=$((COOLDOWN * 2))
            echo "  GPU HOT! Extended cooldown: ${EXTRA_COOL}s"
            COOLDOWN_THIS=$EXTRA_COOL
        else
            COOLDOWN_THIS=$COOLDOWN
        fi
    else
        COOLDOWN_THIS=$COOLDOWN
    fi

    echo "  Cooldown ${COOLDOWN_THIS}s …"
    sleep "$COOLDOWN_THIS"
    echo "  Restarting now!"
    echo ""

done

echo ""
echo "Loop finished. Check chunked_run.log and chunked_loop.log for details."
