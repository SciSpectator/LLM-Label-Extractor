#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# TAILS (LLM-Label-Extractor) — HPC / SLURM batch script
#
# This is a SLURM submission script you can also run directly with bash
# (the #SBATCH lines are comments to bash but directives to sbatch).
#
# Submit:   sbatch run_cluster.sbatch
# Run now:  bash   run_cluster.sbatch
# Tail log: tail -f  slurm-*.out   (SLURM) or    tail -f batch_run.log
#
# Most behavior is controlled by environment variables consumed by
# run_cluster.py (see its docstring). You can override any of them by
# exporting them before `sbatch`, e.g.:
#   PHASES=2 PLATFORM_ONLY=GPL570 sbatch run_cluster.sbatch
# ─────────────────────────────────────────────────────────────────────────────

#SBATCH --job-name=tails
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --gres=gpu:1
#SBATCH --time=48:00:00

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================================"
echo "  TAILS HPC runner  |  $(date)"
echo "  Host: $(hostname)  |  SLURM_JOB_ID=${SLURM_JOB_ID:-none}"
echo "  Dir:  $SCRIPT_DIR"
echo "============================================================"

# ── 1. Load modules (edit for your cluster) ───────────────────────────────────
if command -v module >/dev/null 2>&1; then
    module load python/3.10 || true
    module load cuda/12.1   || true
fi

# ── 2. Activate virtual environment (create if missing) ───────────────────────
if [ ! -d "venv" ]; then
    echo "Creating virtual environment …"
    python3 -m venv venv
fi
# shellcheck disable=SC1091
source venv/bin/activate

# ── 3. Install / update Python requirements ──────────────────────────────────
echo "Installing requirements …"
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

# Torch is pulled in transitively by sentence-transformers; if your cluster
# needs a specific CUDA-linked torch build, install it explicitly BEFORE this
# script, e.g.:
#   pip install torch --index-url https://download.pytorch.org/whl/cu121

# ── 4. Ensure Ollama is running (skip if you use fake_ollama_lb) ─────────────
USE_FAKE_OLLAMA="${USE_FAKE_OLLAMA:-0}"
OLLAMA_URL="${OLLAMA_URL:-http://localhost:11434}"
MODEL="${MODEL:-gemma4:e2b}"

if [ "$USE_FAKE_OLLAMA" != "1" ]; then
    if ! pgrep -x ollama >/dev/null; then
        echo "Starting Ollama server …"
        # Cap concurrent Ollama requests — matches pipeline's num_workers=3 cap.
        # Raising this without raising pipeline worker count wastes VRAM;
        # raising BOTH past 3 is known to freeze single-GPU workstations.
        OLLAMA_NUM_PARALLEL=3 \
        OLLAMA_KEEP_ALIVE=-1 \
        ollama serve >ollama.log 2>&1 &
        OLLAMA_PID=$!
        echo "  ollama PID=$OLLAMA_PID"
        # Wait for server to come up
        for _ in $(seq 1 30); do
            if curl -sf "$OLLAMA_URL/api/tags" >/dev/null; then
                break
            fi
            sleep 1
        done
    fi

    # Pull the model if missing
    if ! ollama list 2>/dev/null | grep -q "^${MODEL%%:*}.*${MODEL##*:}"; then
        echo "Pulling model $MODEL …"
        ollama pull "$MODEL"
    fi
fi

# ── 5. Verify GEOmetadb ──────────────────────────────────────────────────────
GEODB_PATH="${GEODB_PATH:-$SCRIPT_DIR/GEOmetadb.sqlite}"
if [ ! -f "$GEODB_PATH" ] && [ ! -f "${GEODB_PATH}.gz" ]; then
    echo "[ERROR] GEOmetadb not found at: $GEODB_PATH"
    echo "        Download: wget https://gbnci.cancer.gov/geo/GEOmetadb.sqlite.gz"
    exit 1
fi
export GEODB_PATH

# ── 6. Phase selection + output dir (all have defaults in run_cluster.py) ────
export PHASES="${PHASES:-1,1b,1c,2}"
export HARMONIZED_DIR="${HARMONIZED_DIR:-$SCRIPT_DIR/NEW_RESULTS}"
export OLLAMA_URL MODEL USE_FAKE_OLLAMA

echo "  PHASES=$PHASES"
echo "  HARMONIZED_DIR=$HARMONIZED_DIR"
echo "  MODEL=$MODEL  OLLAMA_URL=$OLLAMA_URL  USE_FAKE_OLLAMA=$USE_FAKE_OLLAMA"
echo

# ── 7. Run the pipeline ──────────────────────────────────────────────────────
python3 run_cluster.py

echo
echo "============================================================"
echo "  Finished $(date)"
echo "============================================================"
