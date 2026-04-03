<p align="center">
  <br>
  <img src="https://img.shields.io/badge/LLM--Label--Extractor-v1.0-blueviolet?style=for-the-badge" alt="LLM-Label-Extractor v1.0">
  <br><br>
  <strong>LLM-Label-Extractor</strong><br>
  <em>Multi-agent pipeline for extracting and normalizing biomedical metadata labels from GEO microarray data</em>
</p>

<p align="center">
  <a href="#-license-1"><img src="https://img.shields.io/badge/License-MIT-green.svg?logo=opensourceinitiative&logoColor=white" alt="License: MIT"></a>
  <img src="https://img.shields.io/badge/Python-3.8%2B-blue.svg?logo=python&logoColor=white" alt="Python 3.8+">
  <img src="https://img.shields.io/badge/Backend-Ollama-orange.svg?logo=ollama&logoColor=white" alt="Backend: Ollama">
  <img src="https://img.shields.io/badge/Model-gemma2%3A2b-ff6f00?logo=google&logoColor=white" alt="Model: gemma2:2b">
  <img src="https://img.shields.io/badge/Status-Stable-brightgreen.svg" alt="Status: Stable">
  <img src="https://img.shields.io/badge/Platform-Linux%20%7C%20macOS%20%7C%20Windows-lightgrey.svg" alt="Platform">
  <img src="https://img.shields.io/badge/GUI-tkinter-4B8BBE.svg" alt="GUI: tkinter">
</p>

---

## 📋 Overview

**LLM-Label-Extractor** is a multi-agent pipeline that extracts and normalizes **Tissue**, **Condition**, and **Treatment** metadata labels from [Gene Expression Omnibus (GEO)](https://www.ncbi.nlm.nih.gov/geo/) microarray data using local LLMs. It runs entirely offline via [Ollama](https://ollama.com/) with the `gemma2:2b` model -- no API keys, no cloud, no data leaves your machine.

---

## 🛠️ Features

| | Feature | Description |
|---|---|---|
| :brain: | **4-Tier Memory System** | Cluster map, semantic, episodic, and knowledge graph memory for consistent label normalization |
| :robot: | **Multi-Agent Swarm** | One GSEWorker agent per experiment, parallel across all available hardware |
| :bar_chart: | **Fluid Worker Scaling** | Auto-scales 4--210 workers based on real-time CPU, RAM, and GPU utilization |
| :microscope: | **3-Phase Pipeline** | Extract, Infer, Collapse -- progressively refined label assignment |
| :desktop_computer: | **Dark-Theme GUI** | Modern tkinter interface with per-phase progress bars and live resource monitoring |
| :keyboard: | **Headless CLI Mode** | `run_batch_terminal.py` for servers, HPC, and batch processing |
| :dna: | **Multi-Species Support** | Homo sapiens, Mus musculus, Rattus norvegicus, and 8 more organisms |
| :lock: | **Fully Local** | All inference runs on your hardware via Ollama -- zero data exfiltration |

---

## 🏗️ Architecture

<p align="center">
  <img src="docs/architecture.svg" alt="LLM-Label-Extractor Pipeline Architecture" width="100%">
</p>

---

## 🤖 Agent Workflow

The pipeline uses a **multi-agent swarm** where each GEO experiment (GSE) is handled by an independent worker agent. Here is the step-by-step workflow:

1. **Platform Discovery** -- The pipeline queries `GEOmetadb.sqlite` to find all GPL platforms matching the user's species, technology, and minimum sample filters.

2. **GSE Queue Build** -- For each discovered platform, all associated GSE experiments and their GSM samples are queued for processing.

3. **Phase 1: Extract** -- Worker agents are spawned in parallel (4--210 concurrent). Each agent:
   - Reads the GSM sample title, characteristics, and description
   - Reads the parent GSE experiment summary for context
   - Sends a structured prompt to `gemma2:2b` via Ollama
   - Extracts verbatim **Tissue**, **Condition**, and **Treatment** labels
   - Labels with insufficient information are marked `Not Specified`
   - Results are checkpointed every 5,000 samples

4. **Phase 1b: Infer** -- For fields still marked `Not Specified`, the pipeline makes a single KV-cached LLM call per GSE to re-infer labels from the full experiment description, amortizing context across all samples in that experiment.

5. **Phase 2: Collapse** -- The 4-Tier Memory Agent normalizes raw extracted labels to canonical cluster names:
   - **Step 1:** Check Episodic Memory for a cached resolution
   - **Step 2:** Check the Knowledge Graph for known synonyms/variants
   - **Step 3:** Embed the label and search Semantic Memory (~6,920 vectors) via cosine similarity
   - **Step 4:** If no match, query the LLM with the top-50 Core Memory labels as few-shot context
   - **Step 5:** Apply deterministic fallback rules
   - Results are checkpointed every 1,000 samples

6. **Output** -- Per-platform CSV files are written with repaired labels, full annotations, and collapse reports.

---

## 💻 Minimum Requirements

| Resource | Minimum | Recommended |
|---|---|---|
| **CPU** | 4 cores | 8+ cores |
| **RAM** | 8 GB | 16+ GB |
| **Disk** | 25 GB free (GEOmetadb + model) | 50+ GB free |
| **GPU** | Not required (CPU-only works) | NVIDIA GPU with 4+ GB VRAM |
| **OS** | Linux, macOS, or Windows 10+ | Ubuntu 22.04+ / macOS 13+ |
| **Python** | 3.8+ | 3.10+ |
| **Ollama** | Latest stable | Latest stable |

> **Note:** GPU is optional but significantly speeds up LLM inference. Without GPU, the pipeline runs on CPU via Ollama and will be slower but fully functional.

---

## ⚙️ Installation & Setup

### Prerequisites

- **Python 3.8+**
- **Ollama** (local LLM runtime)
- **GEOmetadb.sqlite** (~19 GB, downloaded separately)

### 1. Clone the Repository

```bash
git clone https://github.com/SciSpectator/LLM-Label-Extractor.git
cd LLM-Label-Extractor
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Install Ollama & Pull the Model

#### Ubuntu / Debian

```bash
# System dependencies
sudo apt update && sudo apt install -y python3 python3-pip python3-tk

# Install Ollama
curl -fsSL https://ollama.com/install.sh | sh

# Pull the model
ollama pull gemma2:2b
```

#### macOS

```bash
# Homebrew + Python
brew install python python-tk

# Install Ollama
brew install ollama

# Start Ollama service
ollama serve &

# Pull the model
ollama pull gemma2:2b
```

#### Windows

```powershell
# Install Python (via winget or chocolatey)
winget install Python.Python.3.11
# OR: choco install python

# Install Ollama
winget install Ollama.Ollama
# OR download from https://ollama.com/download/windows

# Pull the model
ollama pull gemma2:2b
```

### 4. Download GEOmetadb

The pipeline requires the GEOmetadb SQLite database (~19 GB) from NCBI.

#### Linux / macOS

```bash
# Method 1: Direct download
wget https://gbnci.cancer.gov/geo/GEOmetadb.sqlite.gz

# Method 2: Via R/Bioconductor
Rscript -e 'library(GEOmetadb); getSQLiteFile()'

# Decompress
gunzip GEOmetadb.sqlite.gz

# Place in the project root
mv GEOmetadb.sqlite /path/to/LLM-Label-Extractor/
```

#### Windows

```powershell
# Method 1: Download with PowerShell
Invoke-WebRequest -Uri "https://gbnci.cancer.gov/geo/GEOmetadb.sqlite.gz" -OutFile "GEOmetadb.sqlite.gz"

# Method 2: Via R/Bioconductor (if R is installed)
Rscript -e "library(GEOmetadb); getSQLiteFile()"

# Decompress using 7-Zip (install from https://www.7-zip.org if needed)
7z x GEOmetadb.sqlite.gz

# OR decompress using Python (no extra tools needed)
python -c "import gzip, shutil; shutil.copyfileobj(gzip.open('GEOmetadb.sqlite.gz','rb'), open('GEOmetadb.sqlite','wb'))"

# Move to the project root
move GEOmetadb.sqlite C:\path\to\LLM-Label-Extractor\
```

> **Note:** The pipeline will automatically detect either `GEOmetadb.sqlite` or `GEOmetadb.sqlite.gz` in the project directory.

### 5. Verification (Optional but Recommended)

```bash
# Verify Ollama is running
ollama ps

# Verify model is available
ollama list

# Test a quick run
python run_gui.py
```

---

## 🚀 Quick Start

### GUI Mode

```bash
python run_gui.py
```

1. Select the **GEOmetadb.sqlite** file path
2. Choose **Species** (any organism available in GEOmetadb)
3. Choose **Technology** (any platform technology available in GEOmetadb)
4. Click **Start** -- the pipeline will discover platforms, then process each through all three phases

### CLI / Terminal Batch Mode

```bash
python run_batch_terminal.py
```

Edit the configuration block at the top of `run_batch_terminal.py` to set:

```python
SPECIES     = "Homo sapiens"
TECH_MODE   = "Expression Microarray"
```

Monitor progress in a separate terminal:

```bash
bash monitor.sh
```

---

## 🖥️ GUI Walkthrough

The GUI features a modern dark theme with the following panels:

| Panel | Description |
|---|---|
| **Configuration** | Database path, species, technology, model URL |
| **Platform Queue** | Discovered platforms with sample counts, processing status |
| **Phase Progress** | Individual progress bars for Phase 1, Phase 1b, and Phase 2 |
| **Resource Monitor** | Real-time CPU, RAM, GPU utilization and worker count |
| **Live Log** | Scrolling log of agent activity, label assignments, and errors |

---

## 🔬 Pipeline Phases

### Phase 1: Extract

Raw label extraction from GSM sample metadata and GSE experiment context. Each GSM record is processed by a dedicated agent that reads the sample title, characteristics, description, and parent GSE summary. The LLM extracts verbatim **Tissue**, **Condition**, and **Treatment** labels. Fields with insufficient information are marked `Not Specified`.

- **Parallelism:** Fluid worker pool (4--210 concurrent agents)
- **Speed:** ~174 ms per sample
- **Checkpoints:** Every 5,000 samples

### Phase 1b: Infer

For fields still marked `Not Specified` after Phase 1, the pipeline re-infers labels from the full GSE experiment description. This uses a single KV-cached LLM call per GSE, amortizing context loading across all samples in the experiment.

### Phase 2: Collapse

Raw extracted labels are normalized to canonical cluster names via the 4-tier Memory Agent. The cluster map files contain pre-curated biomedical term groupings:

| Category | Cluster File | Clusters |
|---|---|---|
| Tissue | `LLM_memory/Tissues_clusters_db_ready.txt` | 1,512 |
| Condition | `LLM_memory/Conditions_clusters_db_ready.txt` | 2,689 |
| Treatment | `LLM_memory/treatment_clusters_db_ready.txt` | 2,719 |

**Checkpoints:** Every 1,000 samples

---

## 🧠 Memory System

The 4-tier memory system (`biomedical_memory.db`) ensures consistent label normalization across millions of samples:

### Tier 1: Core Memory (Cluster Map)
The top-50 most frequent labels are injected into every LLM prompt as few-shot examples, ensuring the most common terms are always resolved without retrieval.

### Tier 2: Semantic Memory
~6,920 biomedical labels are embedded as vectors. At resolution time, the raw label is embedded and matched to the nearest cluster via cosine similarity.

### Tier 3: Episodic Memory
Every past label resolution is logged with confidence scores. When the same raw label appears again, the cached resolution is returned instantly.

### Tier 4: Knowledge Graph
Synonym and variant relationships stored as triples in SQLite (e.g., `"liver" -> IS_A -> "Liver"`, `"hepatic tissue" -> SYNONYM -> "Liver"`). This handles abbreviations, alternative spellings, and domain-specific naming conventions.

**Resolution priority:** Episodic > Knowledge Graph > Semantic + LLM > Deterministic rules

---

## 📈 Fluid Worker Scaling

The pipeline dynamically adjusts its concurrency based on real-time system metrics:

| Metric | Low Utilization | High Utilization |
|---|---|---|
| CPU | Scale up workers | Scale down |
| RAM | Scale up workers | Scale down |
| GPU VRAM | Scale up workers | Scale down |
| **Range** | **4 workers (min)** | **210 workers (max)** |

The scaler samples system metrics every few seconds and adjusts the thread pool size to maximize throughput without causing OOM kills or GPU memory exhaustion.

---

## 💾 Input / Output

### Input

- **GEOmetadb.sqlite** -- the full GEO metadata database from NCBI (~19 GB)
- **Existing CSV files** (optional) -- previously annotated platform files for NS-repair mode

### Output (per platform)

| File | Description |
|---|---|
| `{GPL}_NS_repaired.csv` | Samples that had `Not Specified` labels repaired |
| `{GPL}_full_repaired.csv` | Complete annotated sample table |
| `{GPL}_collapse_report.csv` | Mapping of raw labels to collapsed cluster names |

---

## 🧬 Supported Species

The pipeline supports **all organisms available in GEOmetadb**, including but not limited to:

| Species | Common Name | GEO Platforms |
|---|---|---|
| Homo sapiens | Human | 4,000+ |
| Mus musculus | Mouse | 2,000+ |
| Rattus norvegicus | Rat | 500+ |
| Drosophila melanogaster | Fruit fly | 200+ |
| Danio rerio | Zebrafish | 100+ |
| Caenorhabditis elegans | Nematode | 50+ |
| Saccharomyces cerevisiae | Yeast | 100+ |
| Arabidopsis thaliana | Thale cress | 100+ |
| Sus scrofa | Pig | 50+ |
| Bos taurus | Cattle | 50+ |
| Gallus gallus | Chicken | 30+ |

> **Note:** Any species present in GEOmetadb can be selected. The table above shows commonly used organisms. The full list is dynamically queried from the database at runtime.

## 🔧 Supported Technologies

The pipeline supports **all platform technologies catalogued in GEOmetadb**. Common categories include:

| Technology | Example Array Types |
|---|---|
| Expression Microarray | in situ oligonucleotide, spotted DNA/cDNA, spotted oligonucleotide, oligonucleotide beads |
| RNA-Seq | high-throughput sequencing |
| Methylation Array | methylation profiling by array |
| SNP/Genotyping | SNP genotyping by array, genotyping by array |
| miRNA | miRNA profiling by array |
| All Technologies | no filter (processes all platforms) |

> **Note:** Technology categories are queried directly from GEOmetadb. As NCBI adds new platform types, they become automatically available.

---

## ⚡ Configuration

Key configuration is set at runtime in the GUI or at the top of `run_batch_terminal.py`:

| Parameter | Default | Description |
|---|---|---|
| `SPECIES` | `Homo sapiens` | Target organism (any species in GEOmetadb) |
| `TECH_MODE` | `Expression Microarray` | Platform technology filter (any technology in GEOmetadb) |
| `MODEL` | `gemma2:2b` | Ollama model name |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama API endpoint |

---

## 🐛 Troubleshooting

### Ollama not running

```
ConnectionError: Cannot connect to Ollama at http://localhost:11434
```

**Fix:** Start the Ollama service:
```bash
ollama serve      # foreground
# OR
systemctl start ollama   # systemd (Linux)
```

### Model not found

```
Error: model "gemma2:2b" not found
```

**Fix:** Pull the model:
```bash
ollama pull gemma2:2b
```

### GEOmetadb not found

```
FileNotFoundError: GEOmetadb.sqlite not found
```

**Fix:** Download and place in the project directory. See [Download GEOmetadb](#4-download-geometadb).

### Out of memory during Phase 2

The collapse phase loads cluster files into memory. If you run out of RAM:
- Reduce worker count manually
- Close other applications
- The fluid scaler will automatically reduce workers on high memory pressure

### tkinter not available (Linux)

```
ModuleNotFoundError: No module named 'tkinter'
```

**Fix:**
```bash
sudo apt install python3-tk    # Debian/Ubuntu
sudo dnf install python3-tkinter  # Fedora
```

### GPU not detected

Ollama automatically uses GPU if available. Verify with:
```bash
ollama ps       # shows running models and GPU layers
nvidia-smi      # NVIDIA GPU status
```

---

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📖 Citation

If you use this software in your research, please cite:

```bibtex
@software{llm_label_extractor2026,
  title   = {LLM-Label-Extractor: Multi-Agent GEO Metadata Extraction Pipeline},
  author  = {Szczepaniak, Mateusz},
  year    = {2026},
  url     = {https://github.com/SciSpectator/LLM-Label-Extractor}
}
```

---

## 📜 License

This project is licensed under the **MIT License**. See [LICENSE](LICENSE) for details.

<p align="center">
  <sub>Built with Ollama + gemma2:2b | Runs entirely on your hardware</sub>
</p>
