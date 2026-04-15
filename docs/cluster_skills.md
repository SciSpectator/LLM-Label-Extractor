# TAILS — HPC Cluster Deployment Guide

## Repository

```
https://github.com/SciSpectator/LLM-Label-Extractor.git
```

---

## Architecture Overview

TAILS (Tissue, condItion & treatment Label extraction System) is a multi-agent pipeline that extracts and normalizes biomedical metadata labels from GEO microarray data using local LLMs via Ollama.

### Pipeline Phases

```
Phase 1  → Raw extraction (3 per-label agents: Tissue, Condition, Treatment)
Phase 1b → NS inference (GSE-context-aware, KV-cached)
Phase 1c → Full re-extraction (zero char limits for remaining NS)
Phase 2  → Collapse (BioLORD-2023 retrieval + LLM reasoning + per-GSE working memory)
```

### Output Structure

```
{PlatformID}_llm_extraction_results/
├── Phase1_results/
│   ├── {PlatformID}_phase1_labels.csv      # raw extraction
│   └── {PlatformID}_phase1b_labels.csv     # after NS inference
├── Final_results/
│   ├── {PlatformID}_final_labels.csv       # PRIMARY OUTPUT (after collapse)
│   ├── {PlatformID}_collapse_audit.csv     # before/after audit trail
│   └── {PlatformID}_full_platform.csv      # full platform with all columns
└── checkpoints/
    ├── phase1_only_extracted.json
    └── phase1_extracted.json
```

---

## Files in Repository

### Core Files

| File | Purpose |
|------|---------|
| `llm_extractor.py` | Main pipeline engine (~8500 lines). Contains all agents, prompts, memory system, phases, GUI. |
| `run_cluster.py` | **HPC/SLURM batch runner**. Environment-variable config. Auto-discovers platforms. |
| `run_gui.py` | GUI launcher (tkinter). Not used on cluster. |
| `requirements.txt` | Python dependencies: pandas, requests, psutil, numpy, sentence-transformers |
| `LLM_memory/` | Canonical cluster vocabulary files (.txt) |
| `biomedical_memory.db` | SQLite memory database (clusters, embeddings, KG triples) |
| `GEOmetadb.sqlite` | NCBI GEO metadata database (~19GB, download separately) |

### run_cluster.py — The HPC Entry Point

```python
#!/usr/bin/env python3
"""
run_cluster.py — HPC/SLURM batch runner for LLM-Label-Extractor v2.1

Designed for HPC clusters (OSCER, Walnut, etc.) using either:
  - Ollama (local GPU)
  - fake_ollama_lb.py (llama_cpp servers behind a load balancer)

Supports all pipeline phases: Phase 1 → 1b → 1c → 2 (collapse)

Environment variables:
  PLATFORM_ONLY    - comma-separated GPL IDs (e.g. "GPL570,GPL96")
  PLATFORM_START   - start index into sorted platform list (default: 0)
  PLATFORM_END     - end index into sorted platform list (default: 99999)
  SKIP_GPLS        - comma-separated GPL IDs to skip
  PHASES           - which phases to run (default: "1,1b,1c,2" = all)
                     Examples: "1" (extract only), "1,1b" (no collapse),
                     "1,1b,1c" (extract+infer+full, no collapse),
                     "2" (collapse only, requires existing Phase 1 checkpoint)
  HARMONIZED_DIR   - output directory (default: ./NEW_RESULTS)
  GEODB_PATH       - path to GEOmetadb.sqlite (default: ./GEOmetadb.sqlite)
  USE_FAKE_OLLAMA  - "1" to skip ollama startup (use pre-started fake_ollama_lb)
  OLLAMA_URL       - ollama API URL (default: http://localhost:11434)
  NUM_WORKERS      - override auto-detected worker count
  LLM_NUM_CTX      - context window size (default: 4096, lower for low-VRAM GPUs)

Usage:
  # All phases, all platforms:
  python run_cluster.py

  # Specific platforms, extraction only:
  PLATFORM_ONLY=GPL570,GPL96 PHASES=1,1b,1c python run_cluster.py

  # HPC with fake_ollama_lb:
  USE_FAKE_OLLAMA=1 OLLAMA_URL=http://localhost:8080 python run_cluster.py

  # SLURM parallel jobs:
  PLATFORM_START=0 PLATFORM_END=100 python run_cluster.py     # job 1
  PLATFORM_START=100 PLATFORM_END=200 python run_cluster.py   # job 2
"""
```

**Key behavior:**
- Auto-discovers all human expression microarray platforms from GEOmetadb
- Filters out sequencing/SNP/methylation platforms
- Skips already-completed platforms (checks for output files)
- Resumes from checkpoints if interrupted
- Graceful SIGINT handling
- VRAM/RAM/Ollama status monitoring thread (every 60s)

---

## Cluster Setup: OSCER (Schooner)

### 1. Clone and Install

```bash
# SSH to schooner
ssh mateuszclus@schooner.oscer.ou.edu

# Navigate to scratch (large storage)
cd /scratch/mateuszclus

# Clone repo
git clone https://github.com/SciSpectator/LLM-Label-Extractor.git
cd LLM-Label-Extractor

# Load modules
module load Python/3.10.8-GCCcore-12.2.0
module load CUDA/12.0.0

# Create venv
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Install Ollama

```bash
# Install Ollama (user-level, no sudo)
curl -fsSL https://ollama.com/install.sh | sh

# Or if no sudo, download binary directly:
mkdir -p ~/bin
curl -L https://ollama.com/download/ollama-linux-amd64 -o ~/bin/ollama
chmod +x ~/bin/ollama
export PATH=$HOME/bin:$PATH
```

### 3. Download GEOmetadb

```bash
wget https://gbnci.cancer.gov/geo/GEOmetadb.sqlite.gz
gunzip GEOmetadb.sqlite.gz
# ~19 GB after decompression
```

### 4. Pull Model

```bash
ollama serve &
sleep 10
ollama pull gemma4:e2b
# ~4.5 GB download
```

### 5. SLURM Job Script

Create `submit_tails.sh`:

```bash
#!/bin/bash
#SBATCH --job-name=tails
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=48:00:00
#SBATCH --output=tails_%j.log
#SBATCH --error=tails_%j.err

# Load modules
module load Python/3.10.8-GCCcore-12.2.0
module load CUDA/12.0.0

# Activate venv
cd /scratch/mateuszclus/LLM-Label-Extractor
source venv/bin/activate

# Start Ollama
export PATH=$HOME/bin:$PATH
OLLAMA_NUM_PARALLEL=20 OLLAMA_FLASH_ATTENTION=1 ollama serve &
sleep 15
ollama pull gemma4:e2b

# Run extraction
# All phases, specific platforms:
PLATFORM_ONLY=GPL570,GPL96 \
PHASES=1,1b,1c,2 \
HARMONIZED_DIR=/scratch/mateuszclus/results \
NUM_WORKERS=20 \
python run_cluster.py

# Kill Ollama
pkill ollama
```

Submit:
```bash
sbatch submit_tails.sh
```

### 6. Parallel Jobs (Split by Platform Range)

```bash
# Job 1: platforms 0-99 (largest)
sbatch --export=ALL,PLATFORM_START=0,PLATFORM_END=100 submit_tails.sh

# Job 2: platforms 100-199
sbatch --export=ALL,PLATFORM_START=100,PLATFORM_END=200 submit_tails.sh

# Job 3: platforms 200-499
sbatch --export=ALL,PLATFORM_START=200,PLATFORM_END=500 submit_tails.sh
```

---

## Cluster Setup: Walnut (OMRF)

### 1. Clone and Install

```bash
ssh szczepaniak@walnut.rc.lan.omrf.org

cd /data/szczepaniak
git clone https://github.com/SciSpectator/LLM-Label-Extractor.git
cd LLM-Label-Extractor

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Ollama Setup

```bash
# If Ollama is already installed system-wide:
ollama serve &
sleep 10
ollama pull gemma4:e2b

# If not, install to user dir:
mkdir -p ~/bin
curl -L https://ollama.com/download/ollama-linux-amd64 -o ~/bin/ollama
chmod +x ~/bin/ollama
export PATH=$HOME/bin:$PATH
```

### 3. Run with fake_ollama_lb (if using llama_cpp backend)

```bash
# Start llama_cpp servers first (on GPU nodes)
# Then use fake_ollama_lb as load balancer:
USE_FAKE_OLLAMA=1 \
OLLAMA_URL=http://localhost:8080 \
PLATFORM_ONLY=GPL570 \
PHASES=1,1b,1c \
python run_cluster.py
```

### 4. Direct Run (with Ollama on GPU node)

```bash
# Request a GPU node interactively
srun --partition=gpu --gres=gpu:1 --mem=32G --time=24:00:00 --pty bash

# Then run:
cd /data/szczepaniak/LLM-Label-Extractor
source venv/bin/activate
ollama serve &
sleep 10
PLATFORM_ONLY=GPL570,GPL96,GPL6244 \
PHASES=1,1b,1c,2 \
NUM_WORKERS=20 \
python run_cluster.py
```

---

## Key Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PHASES` | `1,1b,1c,2` | Phases to run. `1`=extract, `1b`=infer, `1c`=full re-extract, `2`=collapse |
| `PLATFORM_ONLY` | all | Comma-separated GPL IDs |
| `PLATFORM_START` | 0 | Start index for parallel jobs |
| `PLATFORM_END` | 99999 | End index for parallel jobs |
| `SKIP_GPLS` | none | GPL IDs to skip |
| `NUM_WORKERS` | auto | Override worker count |
| `LLM_NUM_CTX` | 4096 | Context window (lower for small GPUs) |
| `USE_FAKE_OLLAMA` | 0 | Use fake_ollama_lb backend |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama API endpoint |
| `HARMONIZED_DIR` | `./NEW_RESULTS` | Output directory |
| `GEODB_PATH` | `./GEOmetadb.sqlite` | GEOmetadb path |

### Phase Selection Examples

```bash
PHASES=1           # Phase 1 only (raw extraction)
PHASES=1,1b        # Phase 1 + NS inference
PHASES=1,1b,1c     # All extraction, no collapse
PHASES=1,1b,1c,2   # All phases (default)
PHASES=2           # Collapse only (needs existing Phase 1 checkpoint)
```

---

## Extraction Agents (from llm_extractor.py)

### Tissue Agent

```
Extract the TISSUE / CELL TYPE / CELL LINE from this GEO sample.
Read ALL fields (Title, Source, Characteristics, Description, Experiment).
Priority: Cell Line > Cell Type > Tissue. Most specific term.
Tumor samples → extract the TISSUE (breast tumor → Breast).
If MULTIPLE tissues → list ALL separated by semicolon.
If absent → Not Specified. Title Case.
```

### Condition Agent

```
Extract the CONDITION / DISEASE from this GEO sample.
Read ALL fields — disease hides in Description, Title, Characteristics, Experiment.
Cancer/infection/syndrome/phenotype all count. Abbreviations valid (CAIS, AML, HCC).

IMPORTANT — GEO metadata often uses CODED values (0/1, Y/N, Yes/No, True/False)
to indicate presence or absence of a condition. The field NAME tells you WHAT
the condition is, and the VALUE tells you whether this sample HAS it or not.
If the value indicates ABSENCE (0, N, No, None, negative, False) → Control.
If the value indicates PRESENCE (1, Y, Yes, positive, True) → extract the condition
name FROM THE FIELD NAME, not the coded value itself.

Normal/Control/Healthy → Control.
If MULTIPLE conditions → list ALL separated by semicolon.
If absent → Not Specified. Title Case.
```

### Treatment Agent

```
Extract the TREATMENT / DRUG / INTERVENTION applied to this GEO sample.
Read ALL fields. Treatment = something DONE TO the patient/sample.
NOT treatments: diseases, tissues, lab protocols (Illumina, TRIzol, FFPE).

IMPORTANT — coded values (0/1, Y/N, None) in treatment fields indicate
presence or absence. If the value means NO treatment → Not Specified.
If the value means treatment WAS applied → extract the treatment name from context.

Vehicle (DMSO, PBS) → Untreated. smoking:0 → Not Specified.
If MULTIPLE treatments → list ALL separated by semicolon.
If no treatment → Not Specified. Title Case.
```

### Collapse Agent (Phase 2)

```
Biomedical label normalizer with per-GSE working memory.
- Receives raw label + top-20 BioLORD-2023 candidates
- Knows experiment context (GSE title, summary)
- Remembers recent resolutions in this experiment
- Preserves specificity (Cerebellum ≠ Brain)
- Normalizes naming only (HL-60 cell → Cell Line: Hl-60 Cells)
```

---

## Memory Architecture

```
Phase 2 Collapse Memory Hierarchy:

1. Working Memory (per-GSE)        <1ms   ~30% of labels
   └── Resolutions from THIS experiment

2. Episodic Cache (global)         <1ms   ~25% of labels
   └── Resolutions from ALL experiments

3. BioLORD-2023 Retrieval          ~17ms  candidate generation
   └── 768-dim biomedical embeddings (CPU, no VRAM)
   └── Cosine similarity → top-20 candidates

4. LLM Reasoning (gemma4:e2b)      ~700ms ~45% of labels
   └── Picks best canonical cluster from candidates
   └── GSE context + recent resolutions in system prompt
```

---

## Performance Estimates

### Per-Phase Latency

| Phase | Per-Sample | GPU Requirement |
|-------|-----------|-----------------|
| Phase 1 (Extract) | ~1-2s | 8+ GB VRAM |
| Phase 1b (Infer) | ~0.5s | 8+ GB VRAM |
| Phase 1c (Full) | ~2-3s | 8+ GB VRAM |
| Phase 2 (Collapse) | ~21ms avg* | 8+ GB VRAM + CPU |

*Phase 2 avg includes episodic hits (<1ms, ~55%) and LLM calls (~700ms, ~45%).

### Platform ETAs (A100 80GB, 20 workers)

| Platform | Samples | Phase 1+1b+1c | Phase 2 | Total |
|----------|---------|---------------|---------|-------|
| GPL570 | 110,993 | ~30h | ~40min | ~31h |
| GPL10558 | 69,830 | ~19h | ~25min | ~20h |
| GPL96 | 30,265 | ~8h | ~10min | ~9h |
| GPL6244 | 38,328 | ~10h | ~13min | ~11h |

---

## Monitoring

```bash
# Watch log in real time
tail -f batch_run.log

# Check GPU usage
watch -n 5 nvidia-smi

# Check Ollama status
curl -s http://localhost:11434/api/ps | python3 -c "import sys,json; d=json.load(sys.stdin); [print(f'{m[\"name\"]}: {m.get(\"size_vram\",0)/1e9:.1f}GB') for m in d.get('models',[])]"

# Check progress (how many platforms done)
ls -d *_llm_extraction_results/Final_results/ 2>/dev/null | wc -l
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Ollama timeout | Restart: `pkill ollama; sleep 5; ollama serve &` |
| VRAM OOM | Lower workers: `NUM_WORKERS=5` or `LLM_NUM_CTX=512` |
| Model not found | `ollama pull gemma4:e2b` |
| GEOmetadb not found | Set `GEODB_PATH=/path/to/GEOmetadb.sqlite` |
| Pipeline stuck | Check `batch_run.log`, kill and restart (checkpoints resume) |
| Permission denied | Use user-level Ollama install in `~/bin/` |
