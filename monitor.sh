#!/bin/bash
# Live progress monitor  —  refreshes every 5 seconds
# Shows SEPARATE progress bars + ETAs for each pipeline phase
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="${SCRIPT_DIR}/batch_stdout.log"
BASE="${SCRIPT_DIR}"

# ── Helper: format seconds as "Xh Ym Zs" ──
fmt_time() {
    local s=$1
    local h=$((s / 3600))
    local m=$(( (s % 3600) / 60 ))
    local sec=$((s % 60))
    if [ "$h" -gt 0 ]; then
        echo "${h}h ${m}m"
    elif [ "$m" -gt 0 ]; then
        echo "${m}m ${sec}s"
    else
        echo "${sec}s"
    fi
}

# ── Helper: draw progress bar [████░░░░] ──
draw_bar() {
    local pct=$1
    local width=${2:-30}
    local fill=$((pct * width / 100))
    local empty=$((width - fill))
    [ "$fill" -lt 0 ] && fill=0
    [ "$empty" -lt 0 ] && empty=0
    local bar=""
    [ "$fill" -gt 0 ] && bar=$(printf '%0.s█' $(seq 1 $fill 2>/dev/null))
    local spc=""
    [ "$empty" -gt 0 ] && spc=$(printf '%0.s░' $(seq 1 $empty 2>/dev/null))
    echo "${bar}${spc}"
}

# ── Helper: format number with commas ──
comma() {
    printf '%s' "$1" | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta'
}

while true; do
    clear
    NOW_EPOCH=$(date +%s)

    echo "═══════════════════════════════════════════════════════════════════════"
    echo "  LLM-LABEL-EXTRACTOR MONITOR  $(date '+%Y-%m-%d %H:%M:%S')"
    echo "═══════════════════════════════════════════════════════════════════════"

    # Current platform
    PLAT=$(grep -E "^\s*\[.*/.*\] REPAIR|^\s*\[.*/.*\] SCRATCH" "$LOG" 2>/dev/null | tail -1 | sed 's/^  *//')
    [ -n "$PLAT" ] && echo "  $PLAT"
    echo ""

    # ══════════════════════════════════════════════════════════════════════
    #  SYSTEM RESOURCES
    # ══════════════════════════════════════════════════════════════════════
    RAM_PCT=$(free | awk '/Mem:/ {printf "%.0f", $3/$2*100}')
    RAM_USED=$(free -g | awk '/Mem:/ {print $3}')
    RAM_TOTAL=$(free -g | awk '/Mem:/ {print $2}')
    CPU_PCT=$(awk '{u=$2+$4; t=$2+$4+$5; if (NR==1){u1=u;t1=t} else {printf "%.0f", (u-u1)/(t-t1)*100}}' <(grep 'cpu ' /proc/stat) <(sleep 0.3 && grep 'cpu ' /proc/stat) 2>/dev/null)
    [ -z "$CPU_PCT" ] && CPU_PCT="?"

    GPU_INFO=$(nvidia-smi --query-gpu=memory.used,memory.total,temperature.gpu --format=csv,noheader,nounits 2>/dev/null | head -1)
    if [ -n "$GPU_INFO" ]; then
        VRAM_USED=$(echo "$GPU_INFO" | cut -d',' -f1 | tr -d ' ')
        VRAM_TOTAL=$(echo "$GPU_INFO" | cut -d',' -f2 | tr -d ' ')
        GPU_TEMP=$(echo "$GPU_INFO" | cut -d',' -f3 | tr -d ' ')
        VRAM_PCT=$((VRAM_USED * 100 / VRAM_TOTAL))
    fi

    CPU_TEMP=""
    if [ -f /sys/class/thermal/thermal_zone0/temp ]; then
        CPU_TEMP_RAW=$(cat /sys/class/thermal/thermal_zone0/temp 2>/dev/null)
        [ -n "$CPU_TEMP_RAW" ] && CPU_TEMP=$((CPU_TEMP_RAW / 1000))
    fi
    [ -z "$CPU_TEMP" ] && command -v sensors &>/dev/null && CPU_TEMP=$(sensors 2>/dev/null | grep -oP 'Core 0.*?\+\K[\d.]+' | head -1 | cut -d. -f1)

    # RAM
    RAM_TAG=""
    [ "$RAM_PCT" -ge 95 ] 2>/dev/null && RAM_TAG="PAUSED"
    [ "$RAM_PCT" -ge 85 ] 2>/dev/null && [ "$RAM_PCT" -lt 95 ] 2>/dev/null && RAM_TAG="HIGH"
    printf "  RAM  [%s] %3d%%  %sG / %sG  %s\n" "$(draw_bar $RAM_PCT 30)" "$RAM_PCT" "$RAM_USED" "$RAM_TOTAL" "$RAM_TAG"

    # CPU
    if [ "$CPU_PCT" != "?" ]; then
        CPU_TAG=""
        [ "$CPU_PCT" -ge 95 ] 2>/dev/null && CPU_TAG="PAUSED"
        [ "$CPU_PCT" -ge 80 ] 2>/dev/null && [ "$CPU_PCT" -lt 95 ] 2>/dev/null && CPU_TAG="HIGH"
        TEMP_STR=""
        [ -n "$CPU_TEMP" ] && TEMP_STR="${CPU_TEMP}C"
        printf "  CPU  [%s] %3d%%  %s cores  %s  %s\n" "$(draw_bar $CPU_PCT 30)" "$CPU_PCT" "$(nproc)" "$TEMP_STR" "$CPU_TAG"
    fi

    # VRAM
    if [ -n "$GPU_INFO" ]; then
        GPU_TEMP_STR=""
        [ -n "$GPU_TEMP" ] && GPU_TEMP_STR="${GPU_TEMP}C"
        printf "  VRAM [%s] %3d%%  %s / %s MB  %s\n" "$(draw_bar $VRAM_PCT 30)" "$VRAM_PCT" "$VRAM_USED" "$VRAM_TOTAL" "$GPU_TEMP_STR"
    fi

    # ── Watchdog status + Fluid Workers ──
    WD=$(grep -E "\[WATCHDOG\]" "$LOG" 2>/dev/null | tail -1 | sed 's/.*\[WATCHDOG\] //')
    if [ -n "$WD" ]; then
        # Extract fluid worker count: W:126/210
        WK_NOW=$(echo "$WD" | grep -oP 'W:\K\d+' | head -1)
        WK_MAX=$(echo "$WD" | grep -oP 'W:\d+/\K\d+' | head -1)
        LLM_MIN=$(echo "$WD" | grep -oP 'LLM/min:\K\d+')
        WD_STATE=$(echo "$WD" | grep -oP '(running|PAUSED[^|]*)' | head -1)

        if [ -n "$WK_NOW" ] && [ -n "$WK_MAX" ]; then
            WK_PCT=$((WK_NOW * 100 / WK_MAX))
            if [ "$WK_NOW" -lt "$WK_MAX" ] 2>/dev/null; then
                WK_TAG="SCALED DOWN"
            else
                WK_TAG=""
            fi
            printf "  WKRS [%s] %3d%%  %s / %s workers  LLM: %s/min  %s  %s\n" \
                "$(draw_bar $WK_PCT 30)" "$WK_PCT" "$WK_NOW" "$WK_MAX" \
                "${LLM_MIN:-?}" "${WD_STATE:-?}" "$WK_TAG"
        fi
    fi

    # Watchdog alerts (pause/resume/scale)
    WD_ALERT=$(grep -E "PAUSED|RESUMED|Workers:.*→" "$LOG" 2>/dev/null | tail -1)
    if [ -n "$WD_ALERT" ]; then
        # Only show if recent (within last 60 seconds)
        echo "  $(echo "$WD_ALERT" | sed 's/^  *//')"
    fi

    # ══════════════════════════════════════════════════════════════════════
    #  TREATMENT FIX (fix_treatment_collapse.py — separate process)
    # ══════════════════════════════════════════════════════════════════════
    FIX_LOG="$BASE/fix_treatment.log"
    if pgrep -f "fix_treatment_collapse" >/dev/null 2>&1 && [ -f "$FIX_LOG" ]; then
        echo ""
        echo "  ─────────────────────────────────────────────────────────────────"
        echo "  TREATMENT RE-COLLAPSE (fix_treatment_collapse.py)"
        echo "  ─────────────────────────────────────────────────────────────────"

        # Which platform?
        FIX_PLAT=$(grep "TREATMENT RE-COLLAPSE:" "$FIX_LOG" 2>/dev/null | tail -1 | grep -oP 'GPL\S+')
        FIX_ROWS=$(grep "^  Rows:" "$FIX_LOG" 2>/dev/null | tail -1 | grep -oP '[0-9,]+' | head -1 | tr -d ',')
        FIX_DET=$(grep "Deterministic:" "$FIX_LOG" 2>/dev/null | tail -1)
        FIX_LLM=$(grep -P '^\s+\[[0-9,]+/[0-9,]+\]' "$FIX_LOG" 2>/dev/null | tail -1)

        [ -n "$FIX_PLAT" ] && echo "  Platform: $FIX_PLAT ($FIX_ROWS rows)"

        # Deterministic pass
        if [ -n "$FIX_DET" ]; then
            DET_MATCH=$(echo "$FIX_DET" | grep -oP '[\d,]+/[\d,]+' | head -1)
            DET_PCT=$(echo "$FIX_DET" | grep -oP '[\d.]+%' | head -1)
            echo "  Pass 1 (deterministic): $DET_MATCH = $DET_PCT"
        fi

        # LLM pass progress bar
        if [ -n "$FIX_LLM" ]; then
            LLM_DONE=$(echo "$FIX_LLM" | grep -oP '\[\K[0-9,]+' | head -1 | tr -d ',')
            LLM_TOTAL=$(echo "$FIX_LLM" | grep -oP '/\K[0-9,]+' | head -1 | tr -d ',')
            LLM_ETA=$(echo "$FIX_LLM" | grep -oP 'ETA:\K[^\s]+')
            LLM_MATCHED=$(echo "$FIX_LLM" | grep -oP 'matched:\K[0-9]+')
            LLM_NEW=$(echo "$FIX_LLM" | grep -oP 'new:\K[0-9]+')
            LLM_REJ=$(echo "$FIX_LLM" | grep -oP 'rejected:\K[0-9]+')

            if [ -n "$LLM_DONE" ] && [ -n "$LLM_TOTAL" ] && [ "$LLM_TOTAL" -gt 0 ] 2>/dev/null; then
                LLM_PCT=$((LLM_DONE * 100 / LLM_TOTAL))
                echo "  Pass 2 (LLM agent):"
                printf "  [%s] %3d%%  %s / %s\n" "$(draw_bar $LLM_PCT 40)" "$LLM_PCT" "$(comma $LLM_DONE)" "$(comma $LLM_TOTAL)"
                printf "     matched:%s  new:%s  rejected:%s  |  ETA: %s\n" \
                    "${LLM_MATCHED:-0}" "${LLM_NEW:-0}" "${LLM_REJ:-0}" "${LLM_ETA:-?}"
            fi
        elif [ -n "$FIX_DET" ]; then
            echo "  Pass 2 (LLM): starting..."
        fi
        echo ""
    fi

    echo ""
    echo "  ─────────────────────────────────────────────────────────────────"
    echo "  CURRENT PLATFORM — PER-PHASE PROGRESS"
    echo "  ─────────────────────────────────────────────────────────────────"

    # ══════════════════════════════════════════════════════════════════════
    #  PHASE 1 — Raw Extraction
    # ══════════════════════════════════════════════════════════════════════
    P1_COMPLETE_FLAG=0
    P1_DONE=0
    P1_TOTAL=0
    P1_RATE_MS=0
    P1_ETA_S=0

    # Check if Phase 1 was restored from checkpoint
    P1_CKPT_MSG=$(grep "Phase 1 checkpoint loaded" "$LOG" 2>/dev/null | tail -1)
    P1_ALL_DONE_MSG=$(grep "Phase 1.*all.*extracted\|Phase 1 complete" "$LOG" 2>/dev/null | tail -1)

    if [ -n "$P1_ALL_DONE_MSG" ] || [ -n "$P1_CKPT_MSG" ]; then
        P1_COMPLETE_FLAG=1
        # Get total from checkpoint message
        P1_TOTAL=$(echo "$P1_CKPT_MSG $P1_ALL_DONE_MSG" | grep -oP '[\d,]+' | head -1 | tr -d ',')
        P1_DONE=$P1_TOTAL
        echo ""
        printf "  PHASE 1  Raw Extraction (gemma2:2b)                    DONE\n"
        printf "  [%s] 100%%  %s/%s samples\n" "$(draw_bar 100 40)" "$(comma ${P1_DONE:-0})" "$(comma ${P1_TOTAL:-0})"
    else
        # Phase 1 in progress — parse latest line
        P1_LINE=$(grep "Phase 1:" "$LOG" 2>/dev/null | tail -1)
        if [ -n "$P1_LINE" ]; then
            P1_DONE=$(echo "$P1_LINE" | grep -oP 'Phase 1: \K[\d,]+' | tr -d ',')
            P1_TOTAL=$(echo "$P1_LINE" | grep -oP '/\K[\d,]+' | head -1 | tr -d ',')
            P1_RATE_MS=$(echo "$P1_LINE" | grep -oP '[\d]+ms' | head -1 | tr -d 'ms')
            P1_ETA_RAW=$(echo "$P1_LINE" | grep -oP 'ETA \K[^\s]+')
            [ -z "$P1_DONE" ] && P1_DONE=0
            [ -z "$P1_TOTAL" ] && P1_TOTAL=0

            if [ "$P1_TOTAL" -gt 0 ] 2>/dev/null; then
                P1_PCT=$((P1_DONE * 100 / P1_TOTAL))
                P1_REM=$((P1_TOTAL - P1_DONE))
                if [ -n "$P1_RATE_MS" ] && [ "$P1_RATE_MS" -gt 0 ] 2>/dev/null; then
                    P1_ETA_S=$((P1_REM * P1_RATE_MS / 1000))
                fi
                echo ""
                printf "  PHASE 1  Raw Extraction (gemma2:2b)                    ACTIVE\n"
                printf "  [%s] %3d%%  %s / %s samples\n" \
                    "$(draw_bar $P1_PCT 40)" "$P1_PCT" "$(comma $P1_DONE)" "$(comma $P1_TOTAL)"
                printf "     Speed: %s ms/sample  |  ETA: %s" "${P1_RATE_MS:-?}" "$(fmt_time ${P1_ETA_S:-0})"
                [ -n "$P1_ETA_RAW" ] && printf "  (%s)" "$P1_ETA_RAW"
                echo ""
            fi
        else
            # Check if Phase 1 hasn't started yet (NCBI scraping or loading)
            NCBI_TOTAL=$(grep "Fetching.*GSEs\|Fetching NCBI" "$LOG" 2>/dev/null | tail -1 | grep -oP '\d+' | head -1)
            NCBI_DONE=$(grep "fetched" "$LOG" 2>/dev/null | tail -1 | grep -oP '^\s*\K\d+')
            if [ -n "$NCBI_DONE" ] && [ -n "$NCBI_TOTAL" ] && [ "$NCBI_TOTAL" -gt 0 ] && [ "$NCBI_DONE" -lt "$NCBI_TOTAL" ]; then
                NCBI_PCT=$((NCBI_DONE * 100 / NCBI_TOTAL))
                echo ""
                printf "  PHASE 0  NCBI Scraping (GSE metadata)                  ACTIVE\n"
                printf "  [%s] %3d%%  %s / %s GSEs\n" \
                    "$(draw_bar $NCBI_PCT 40)" "$NCBI_PCT" "$NCBI_DONE" "$NCBI_TOTAL"
            else
                LOADING=$(grep -E "Loading|Building|Memory Agent|GEOmetadb" "$LOG" 2>/dev/null | tail -1 | sed 's/^  *//')
                if [ -n "$LOADING" ]; then
                    echo ""
                    echo "  PHASE 0  Initialising...                                 LOADING"
                    echo "  $LOADING"
                fi
            fi
        fi
    fi

    # ══════════════════════════════════════════════════════════════════════
    #  PHASE 1b — NS Inference from GSE Context
    # ══════════════════════════════════════════════════════════════════════
    P1B_COMPLETE_FLAG=0
    P1B_DONE_N=0
    P1B_TOTAL_N=0
    P1B_ETA_SEC=0

    P1B_COMPLETE_MSG=$(grep "Phase 1b complete" "$LOG" 2>/dev/null | tail -1)
    P1B_LINE=$(grep "P1b " "$LOG" 2>/dev/null | tail -1)

    if [ -n "$P1B_COMPLETE_MSG" ]; then
        P1B_COMPLETE_FLAG=1
        P1B_RESOLVED=$(echo "$P1B_COMPLETE_MSG" | grep -oP '[\d,]+' | head -1 | tr -d ',')
        echo ""
        printf "  PHASE 1b NS Inference (KV-cached, gemma2:2b)           DONE\n"
        printf "  [%s] 100%%  +%s fields resolved from GSE context\n" "$(draw_bar 100 40)" "$(comma ${P1B_RESOLVED:-0})"
    elif [ -n "$P1B_LINE" ] && [ "$P1_COMPLETE_FLAG" -eq 1 ]; then
        P1B_DONE_N=$(echo "$P1B_LINE" | grep -oP 'P1b \K[\d,]+' | tr -d ',' | head -1)
        P1B_TOTAL_N=$(echo "$P1B_LINE" | grep -oP '/\K[\d,]+' | head -1 | tr -d ',')
        P1B_RATE=$(echo "$P1B_LINE" | grep -oP '[\d]+ms/sample')
        P1B_RATE_MS_N=$(echo "$P1B_RATE" | grep -oP '\d+')
        P1B_ETA_SEC_RAW=$(echo "$P1B_LINE" | grep -oP 'ETA \K\d+')
        [ -z "$P1B_DONE_N" ] && P1B_DONE_N=0
        [ -z "$P1B_TOTAL_N" ] && P1B_TOTAL_N=0

        if [ "$P1B_TOTAL_N" -gt 0 ] 2>/dev/null; then
            P1B_PCT=$((P1B_DONE_N * 100 / P1B_TOTAL_N))
            P1B_REM=$((P1B_TOTAL_N - P1B_DONE_N))
            if [ -n "$P1B_RATE_MS_N" ] && [ "$P1B_RATE_MS_N" -gt 0 ] 2>/dev/null; then
                P1B_ETA_SEC=$((P1B_REM * P1B_RATE_MS_N / 1000))
            elif [ -n "$P1B_ETA_SEC_RAW" ] 2>/dev/null; then
                P1B_ETA_SEC=$P1B_ETA_SEC_RAW
            fi
            echo ""
            printf "  PHASE 1b NS Inference (KV-cached, gemma2:2b)           ACTIVE\n"
            printf "  [%s] %3d%%  %s / %s samples\n" \
                "$(draw_bar $P1B_PCT 40)" "$P1B_PCT" "$(comma $P1B_DONE_N)" "$(comma $P1B_TOTAL_N)"
            printf "     Speed: %s ms/sample  |  ETA: %s\n" "${P1B_RATE_MS_N:-?}" "$(fmt_time ${P1B_ETA_SEC:-0})"
        fi
    elif [ "$P1_COMPLETE_FLAG" -eq 1 ]; then
        # Phase 1 done but Phase 1b not started yet — waiting
        echo ""
        echo "  PHASE 1b NS Inference (KV-cached, gemma2:2b)           WAITING"
    fi

    # ══════════════════════════════════════════════════════════════════════
    #  PHASE 2 — CollapseWorker
    # ══════════════════════════════════════════════════════════════════════
    P2_DONE_N=0
    P2_TOTAL_N=0
    P2_ETA_SEC=0
    P2_ACTIVE=0

    P2_RESUME=$(grep "Phase 2 checkpoint loaded" "$LOG" 2>/dev/null | tail -1)

    P2_LINE=$(grep -E "\[\s*[\d,]+/\s*[\d,]+\] samples\s+fixed:" "$LOG" 2>/dev/null | tail -1)
    [ -z "$P2_LINE" ] && P2_LINE=$(grep -E "\[.*GSEs.*\[.*samples" "$LOG" 2>/dev/null | tail -1)

    if [ -n "$P2_LINE" ]; then
        P2_ACTIVE=1
        P2_DONE_N=$(echo "$P2_LINE" | grep -oP '\[\s*\K[\d,]+' | tail -1 | tr -d ',')
        P2_TOTAL_N=$(echo "$P2_LINE" | grep -oP '/\s*\K[\d,]+' | tail -1 | tr -d ',')
        P2_FIXED=$(echo "$P2_LINE" | grep -oP 'fixed:\K[\d,]+' | tr -d ',')
        P2_RATE_STR=$(echo "$P2_LINE" | grep -oP '[\d.]+[m]?s/sample')
        P2_ETA_RAW=$(echo "$P2_LINE" | grep -oP 'ETA:\K[^\s]+')
        [ -z "$P2_DONE_N" ] && P2_DONE_N=0
        [ -z "$P2_TOTAL_N" ] && P2_TOTAL_N=0

        if [ "$P2_TOTAL_N" -gt 0 ] 2>/dev/null; then
            P2_PCT=$((P2_DONE_N * 100 / P2_TOTAL_N))

            # Compute ETA from rate
            P2_RATE_MS=$(echo "$P2_LINE" | grep -oP '([\d.]+)ms/sample' | head -1 | grep -oP '[\d.]+')
            P2_RATE_S=$(echo "$P2_LINE" | grep -oP '([\d.]+)s/sample' | head -1 | grep -oP '[\d.]+')
            P2_REM=$((P2_TOTAL_N - P2_DONE_N))
            if [ -n "$P2_RATE_MS" ] 2>/dev/null; then
                P2_ETA_SEC=$(echo "$P2_REM $P2_RATE_MS" | awk '{printf "%.0f", $1 * $2 / 1000}')
            elif [ -n "$P2_RATE_S" ] 2>/dev/null; then
                P2_ETA_SEC=$(echo "$P2_REM $P2_RATE_S" | awk '{printf "%.0f", $1 * $2}')
            fi

            [ -n "$P2_RESUME" ] && RESUME_TAG="  (resumed)" || RESUME_TAG=""
            echo ""
            printf "  PHASE 2  CollapseWorker (4-tier memory, gemma2:2b)     ACTIVE%s\n" "$RESUME_TAG"
            printf "  [%s] %3d%%  %s / %s samples\n" \
                "$(draw_bar $P2_PCT 40)" "$P2_PCT" "$(comma $P2_DONE_N)" "$(comma $P2_TOTAL_N)"
            printf "     Speed: %s  |  ETA: %s" "$P2_RATE_STR" "$(fmt_time ${P2_ETA_SEC:-0})"
            [ -n "$P2_ETA_RAW" ] && printf "  (%s)" "$P2_ETA_RAW"
            printf "  |  Fixed: %s\n" "$(comma ${P2_FIXED:-0})"

            # Per-field bars
            for COL in Tissue Condition Treatment; do
                CLINE=$(grep "    $COL " "$LOG" 2>/dev/null | tail -1)
                if [ -n "$CLINE" ]; then
                    echo "    $CLINE" | sed 's/^  */    /'
                fi
            done
        fi
    elif [ "$P1B_COMPLETE_FLAG" -eq 1 ] || ([ "$P1_COMPLETE_FLAG" -eq 1 ] && [ -z "$P1B_LINE" ]); then
        echo ""
        echo "  PHASE 2  CollapseWorker (4-tier memory, gemma2:2b)     WAITING"
    fi

    # ══════════════════════════════════════════════════════════════════════
    #  CURRENT PLATFORM SUMMARY
    # ══════════════════════════════════════════════════════════════════════
    CUR_PLAT_LINE=$(grep -E "\[.*/.*\] (REPAIR|SCRATCH):" "$LOG" 2>/dev/null | tail -1)
    CUR_PLAT_ID=$(echo "$CUR_PLAT_LINE" | grep -oP 'GPL\S+' | head -1)
    CUR_PLAT_NUM=$(echo "$CUR_PLAT_LINE" | grep -oP '\[\K\d+' | head -1)
    CUR_PLAT_TOTAL_N=$(echo "$CUR_PLAT_LINE" | grep -oP '/\K\d+' | head -1)

    # Current platform elapsed
    CUR_ELAPSED=""
    CUR_ELAPSED_S=0
    if [ -n "$CUR_PLAT_ID" ]; then
        CUR_LINE_NUM=$(grep -n "\[.*\] .*${CUR_PLAT_ID}" "$LOG" 2>/dev/null | head -1 | cut -d: -f1)
        CUR_PLAT_START_TS=""
        if [ -n "$CUR_LINE_NUM" ]; then
            CUR_PLAT_START_TS=$(head -n "$CUR_LINE_NUM" "$LOG" 2>/dev/null | grep -oP '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}' | tail -1)
        fi
        if [ -z "$CUR_PLAT_START_TS" ]; then
            CKPT_DIR="$BASE/${CUR_PLAT_ID}_NS_repaired_final_results/checkpoints"
            [ -d "$CKPT_DIR" ] && CUR_PLAT_START_TS=$(stat -c '%Y' "$CKPT_DIR" 2>/dev/null)
        fi
        if [ -n "$CUR_PLAT_START_TS" ]; then
            if echo "$CUR_PLAT_START_TS" | grep -qP '^\d{4}-'; then
                CUR_START_EPOCH=$(date -d "$CUR_PLAT_START_TS" +%s 2>/dev/null)
            else
                CUR_START_EPOCH="$CUR_PLAT_START_TS"
            fi
            [ -n "$CUR_START_EPOCH" ] && CUR_ELAPSED_S=$((NOW_EPOCH - CUR_START_EPOCH))
            CUR_ELAPSED="$(fmt_time $CUR_ELAPSED_S)"
        fi
    fi

    # Combined current platform ETA = remaining P1 + remaining P1b + remaining P2
    CUR_TOTAL_ETA_S=0
    # Add remaining Phase 1 if active
    [ "$P1_COMPLETE_FLAG" -eq 0 ] && [ "$P1_ETA_S" -gt 0 ] 2>/dev/null && CUR_TOTAL_ETA_S=$((CUR_TOTAL_ETA_S + P1_ETA_S))
    # Add remaining Phase 1b if active
    [ "$P1B_COMPLETE_FLAG" -eq 0 ] && [ "$P1B_ETA_SEC" -gt 0 ] 2>/dev/null && CUR_TOTAL_ETA_S=$((CUR_TOTAL_ETA_S + P1B_ETA_SEC))
    # If P1b hasn't started yet but P1 is done, estimate P1b from P1 total
    if [ "$P1_COMPLETE_FLAG" -eq 1 ] && [ "$P1B_COMPLETE_FLAG" -eq 0 ] && [ -z "$P1B_LINE" ] && [ "$P1_TOTAL" -gt 0 ] 2>/dev/null; then
        # ~30% of samples need P1b, ~210ms each
        EST_P1B=$((P1_TOTAL * 30 / 100 * 210 / 1000))
        CUR_TOTAL_ETA_S=$((CUR_TOTAL_ETA_S + EST_P1B))
    fi
    # Add remaining Phase 2 if active
    [ "$P2_ACTIVE" -eq 1 ] && [ "$P2_ETA_SEC" -gt 0 ] 2>/dev/null && CUR_TOTAL_ETA_S=$((CUR_TOTAL_ETA_S + P2_ETA_SEC))
    # If P2 hasn't started yet, estimate from total samples ~42ms/sample
    if [ "$P2_ACTIVE" -eq 0 ] && [ "$P1_TOTAL" -gt 0 ] 2>/dev/null; then
        EST_P2=$((P1_TOTAL * 42 / 1000))
        CUR_TOTAL_ETA_S=$((CUR_TOTAL_ETA_S + EST_P2))
    fi

    echo ""
    echo "  ─────────────────────────────────────────────────────────────────"
    printf "  CURRENT: %s  [%s/%s]" "${CUR_PLAT_ID:-?}" "${CUR_PLAT_NUM:-?}" "${CUR_PLAT_TOTAL_N:-?}"
    printf "  |  Elapsed: %s" "${CUR_ELAPSED:-?}"
    if [ "$CUR_TOTAL_ETA_S" -gt 0 ] 2>/dev/null; then
        printf "  |  ETA: %s" "$(fmt_time $CUR_TOTAL_ETA_S)"
        FINISH_CUR=$((NOW_EPOCH + CUR_TOTAL_ETA_S))
        printf "  |  Done: %s" "$(date -d "@$FINISH_CUR" '+%H:%M' 2>/dev/null)"
    fi
    echo ""

    # ══════════════════════════════════════════════════════════════════════
    #  ALL PLATFORMS — BATCH PROGRESS
    # ══════════════════════════════════════════════════════════════════════
    echo ""
    echo "  ═══════════════════════════════════════════════════════════════"
    echo "  ALL PLATFORMS — BATCH PROGRESS"
    echo "  ═══════════════════════════════════════════════════════════════"

    # Batch elapsed
    BATCH_START_TS=$(grep -oP '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}' "$LOG" 2>/dev/null | head -1)
    [ -z "$BATCH_START_TS" ] && BATCH_START_TS=$(stat -c '%Y' "$LOG" 2>/dev/null)
    BATCH_ELAPSED_S=0
    if [ -n "$BATCH_START_TS" ]; then
        if echo "$BATCH_START_TS" | grep -qP '^\d{4}-'; then
            BATCH_START_EPOCH=$(date -d "$BATCH_START_TS" +%s 2>/dev/null)
        else
            BATCH_START_EPOCH="$BATCH_START_TS"
        fi
        [ -n "$BATCH_START_EPOCH" ] && BATCH_ELAPSED_S=$((NOW_EPOCH - BATCH_START_EPOCH))
    fi

    BATCH_TOTAL_SAMP=$(grep "TOTAL:.*samples" "$LOG" 2>/dev/null | head -1 | grep -oP '[\d,]+' | head -1 | tr -d ',')
    BATCH_TOTAL_PLATS=$(grep "After filter:" "$LOG" 2>/dev/null | head -1 | grep -oP '\d+' | head -1)
    COMPLETED_PLATS=$(grep -c "REPAIR SUMMARY\|Pipeline finished: SUCCESS" "$LOG" 2>/dev/null)

    # ── Count TOTAL samples done across ALL phases (not just Phase 2 CSV) ──

    # 1) Completed platforms: count rows from NS_repaired_live.csv
    P2_ROWS_TOTAL=0
    for RDIR in "$BASE"/*_NS_repaired_final_results/NS_repaired_live.csv; do
        if [ -f "$RDIR" ]; then
            RC=$(wc -l < "$RDIR" 2>/dev/null)
            [ -n "$RC" ] && [ "$RC" -gt 1 ] && P2_ROWS_TOTAL=$((P2_ROWS_TOTAL + RC - 1))
        fi
    done

    # 2) Current platform Phase 1 progress (from checkpoint JSONs)
    #    Count keys in phase1_extracted.json as P1 samples done
    P1_CKPT_DONE=0
    for CKPT in "$BASE"/*_NS_repaired_final_results/checkpoints/phase1_extracted.json; do
        if [ -f "$CKPT" ]; then
            # Count top-level keys — each is one extracted sample
            # Fast: count occurrences of "GSM" at top level
            KC=$(python3 -c "import json; print(len(json.load(open('$CKPT'))))" 2>/dev/null)
            [ -n "$KC" ] && P1_CKPT_DONE=$((P1_CKPT_DONE + KC))
        fi
    done

    # 3) Current platform in-progress Phase 1 from log (if not yet checkpointed)
    CUR_P1_PROGRESS=0
    [ "$P1_COMPLETE_FLAG" -eq 0 ] && [ "$P1_DONE" -gt 0 ] 2>/dev/null && CUR_P1_PROGRESS=$P1_DONE
    CUR_P1B_PROGRESS=0
    [ "$P1B_COMPLETE_FLAG" -eq 0 ] && [ "$P1B_DONE_N" -gt 0 ] 2>/dev/null && CUR_P1B_PROGRESS=$P1B_DONE_N

    # Total: max(P2 rows, P1 checkpoint) + in-progress from log
    # P2 rows already include completed platforms, P1 checkpoint may overlap with current
    # Use: completed CSV rows + current platform Phase 1 (if no Phase 2 yet)
    if [ "$P2_ACTIVE" -eq 1 ] && [ "$P2_DONE_N" -gt 0 ] 2>/dev/null; then
        # Phase 2 active: count P2 rows (includes current platform live CSV rows)
        TOTAL_EFFECTIVE=$P2_ROWS_TOTAL
    else
        # Phase 1 or 1b active: use checkpoint + in-progress
        # Completed platforms (P2 rows) + current platform P1 progress
        TOTAL_EFFECTIVE=$((P2_ROWS_TOTAL + CUR_P1_PROGRESS + CUR_P1B_PROGRESS))
        # Also include P1 checkpoints from platforms that may have P1 done but no P2 CSV yet
        if [ "$P1_CKPT_DONE" -gt "$P2_ROWS_TOTAL" ] 2>/dev/null; then
            TOTAL_EFFECTIVE=$P1_CKPT_DONE
        fi
    fi

    [ -z "$BATCH_TOTAL_SAMP" ] && BATCH_TOTAL_SAMP=0

    if [ "$BATCH_TOTAL_SAMP" -gt 0 ] 2>/dev/null && [ "$TOTAL_EFFECTIVE" -gt 0 ] 2>/dev/null && [ "$BATCH_ELAPSED_S" -gt 60 ] 2>/dev/null; then
        BATCH_PCT=$((TOTAL_EFFECTIVE * 100 / BATCH_TOTAL_SAMP))
        BATCH_RATE_SPS=$(echo "$TOTAL_EFFECTIVE $BATCH_ELAPSED_S" | awk '{if($2>0) printf "%.2f", $1/$2; else print "0"}')
        BATCH_REMAINING=$((BATCH_TOTAL_SAMP - TOTAL_EFFECTIVE))
        BATCH_REM_S=0
        if [ "$BATCH_REMAINING" -gt 0 ] 2>/dev/null; then
            BATCH_REM_S=$(echo "$BATCH_REMAINING $BATCH_RATE_SPS" | awk '{if($2>0) printf "%.0f", $1/$2; else print "0"}')
        fi

        echo ""
        printf "  Platforms: %s done / %s total\n" "${COMPLETED_PLATS:-0}" "${BATCH_TOTAL_PLATS:-?}"
        echo ""
        printf "  [%s] %3d%%\n" "$(draw_bar $BATCH_PCT 50)" "$BATCH_PCT"
        printf "  %s / %s samples" "$(comma $TOTAL_EFFECTIVE)" "$(comma $BATCH_TOTAL_SAMP)"
        if [ "$P2_ROWS_TOTAL" -ne "$TOTAL_EFFECTIVE" ] 2>/dev/null; then
            printf "  (P2 saved: %s, P1 in-flight: %s)" "$(comma $P2_ROWS_TOTAL)" "$(comma $((TOTAL_EFFECTIVE - P2_ROWS_TOTAL)))"
        fi
        echo ""
        echo ""
        printf "  Elapsed: %s" "$(fmt_time $BATCH_ELAPSED_S)"
        printf "  |  Rate: %s samp/s" "$BATCH_RATE_SPS"
        if [ "$BATCH_REM_S" -gt 0 ] 2>/dev/null; then
            printf "  |  ETA: %s" "$(fmt_time $BATCH_REM_S)"
            FINISH_EPOCH=$((NOW_EPOCH + BATCH_REM_S))
            FINISH_TIME=$(date -d "@$FINISH_EPOCH" '+%a %b %d %H:%M' 2>/dev/null)
            [ -n "$FINISH_TIME" ] && printf "  |  Done: %s" "$FINISH_TIME"
        fi
        echo ""
    else
        echo ""
        printf "  Platforms: %s done / %s total\n" "${COMPLETED_PLATS:-0}" "${BATCH_TOTAL_PLATS:-?}"
        printf "  Samples: %s done  |  Elapsed: %s  |  ETA: calculating...\n" \
            "$(comma $TOTAL_EFFECTIVE)" "$(fmt_time ${BATCH_ELAPSED_S:-0})"
    fi

    # Active workers
    echo ""
    CW_THREADS=$(ps -eLf 2>/dev/null | grep -c "CW-\|P1b-\|Phase1" 2>/dev/null)
    [ "$CW_THREADS" -gt 0 ] && echo "  Active workers: $CW_THREADS threads"
    AGENT_LINE=$(grep -E "GSEInferencers created|CollapseWorker created|Thread pool:" "$LOG" 2>/dev/null | tail -1)
    [ -n "$AGENT_LINE" ] && echo "  $(echo "$AGENT_LINE" | sed 's/^  *//')"

    echo ""
    echo "═══════════════════════════════════════════════════════════════════════"
    echo "  Pipeline: GSMExtractor -> GSEInferencer -> CollapseWorker"
    echo "  Watchdog: 95% RAM / 95% CPU / 88C CPU / 85C GPU  |  Ctrl+C to stop"
    sleep 5
done
