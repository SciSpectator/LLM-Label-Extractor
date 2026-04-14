#!/usr/bin/env python3
"""
GEO NS Repair  v2    GSE-Context-Aware Raw Extraction

Architecture
   Swarm of agents  one GSEWorker agent per GSE experiment
      Each agent handles its own GSE (full context, 3 tools),
      finishes, then the pool assigns it the next pending GSE.
      Agents run in parallel (N slots = VRAM / model size).
   Two-step LLM per NS field:
      1. Raw extraction   extract exactly what the text states, no renaming
      2. Phase 1.5 collapse via 4-tier Memory Agent (see below)

  Memory Agent  (biomedical_memory.db  shared across all platforms/runs)
  
    Tier 1  Core memory     top-50 labels injected into every prompt 
    Tier 2  Semantic memory ~3800 labels embedded, cosine RAG query  
    Tier 3  Episodic memory log of every past resolution + conf.     
    Tier 4  Knowledge graph synonym / variant triples (SQLite)       
  
  Phase 1.5 priority: Episodic  KG  Semantic+LLM  Deterministic rules

   GSEContext (MemGPT-style rolling memory)
       as each NS sample is resolved, the context is updated live
       subsequent samples in the same GSE see the newly assigned labels
   NEVER normalises biology  (HSV  HIV, ALS  AD, etc.)

Run:   python llm_extractor.py
"""

import faulthandler; faulthandler.enable()  # show C-level crash traceback
import os, re, sys, json, gzip, time, sqlite3, shutil, signal, glob
import subprocess, threading, queue
import platform as _platform
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
import numpy as np

# ── auto-install missing packages ─────────────────────────────────────────────
def _ensure_pkg(pkg, import_name=None):
    try:
        __import__(import_name or pkg)
    except ImportError:
        print(f"[SETUP] Installing {pkg} ")
        subprocess.run([sys.executable, "-m", "pip", "install", pkg,
                        "--break-system-packages", "-q"], check=False)

_ensure_pkg("pandas")
_ensure_pkg("requests")
_ensure_pkg("psutil")

import pandas as pd
import requests
import psutil
try:
    import ollama as _ollama_lib
    _OLLAMA_LIB_OK = True
except ImportError:
    _ollama_lib = None
    _OLLAMA_LIB_OK = False

# ── GUI imports ───────────────────────────────────────────────────────────────
import tkinter as tk
from tkinter import filedialog, messagebox, ttk, font as tkfont
from tkinter import scrolledtext




# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════
SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
GSE_CACHE_FILE = os.path.join(SCRIPT_DIR, ".gse_meta_cache.json")

NS            = "Not Specified"
LABEL_COLS         = ["Tissue", "Condition", "Treatment"]  # repair NS mode (all 3 fields collapsed via clusters)
LABEL_COLS_SCRATCH = ["Tissue", "Condition", "Treatment"]  # annotate from scratch mode

# ── Universal extraction system prompt — sent ONCE per worker via Ollama system role ──
# Ollama caches the KV of the system prompt across calls from the same session.
# Only the user message (raw metadata) changes per sample — saves ~40% latency.
# ─────────────────────────────────────────────────────────────────────────────
# PER-LABEL EXTRACTION PROMPTS  (gemma2:2b, Phase 1)
# Each label has its OWN LLM agent with a focused, domain-specific prompt.
# 3 independent calls run in parallel — one per label column.
# Purpose: READ the raw GEO text and COPY the value out as-is.
# No reasoning, no vocabulary, no normalisation — just structured extraction.
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# PER-LABEL SYSTEM PROMPTS (KV-cached by Ollama — sent once per column)
# These define each agent's role, expertise, and extraction rules.
# The user message contains ONLY the sample metadata.
# ─────────────────────────────────────────────────────────────────────────────

_TISSUE_SYSTEM_PROMPT = (
    "You are a biomedical tissue extraction specialist. Your sole task is to "
    "identify the biological tissue, cell type, or cell line from GEO sample metadata.\n"
    "\n"
    "YOUR EXPERTISE:\n"
    "- You know all human anatomical tissues and organs\n"
    "- You know cell types: immune cells, epithelial, endothelial, stem cells, etc.\n"
    "- You know cell lines: HeLa (cervix), MCF-7 (breast), A549 (lung), HL-60 (blood),\n"
    "  THP-1 (monocyte), Jurkat (T-cell), K562 (CML), HEK-293 (kidney), etc.\n"
    "- You understand brain subregions: hippocampus, prefrontal cortex, cerebellum, etc.\n"
    "\n"
    "HOW TO READ THE METADATA:\n"
    "- Title: sample name, may contain tissue info or just an ID\n"
    "- Source: MOST RELIABLE field for tissue — check this first\n"
    "- Characteristics: key-value pairs, look for 'tissue:', 'cell type:', 'cell line:'\n"
    "- Description: free-text, may describe the biological material\n"
    "- Experiment context: the parent study's title and summary\n"
    "\n"
    "EXTRACTION RULES:\n"
    "1. Extract the MOST SPECIFIC tissue/cell type present\n"
    "2. Priority: Cell Line > Cell Type > Tissue/Organ\n"
    "3. Copy exactly what the text says — do NOT rename or generalize\n"
    "4. For tumor/cancer samples, extract the TISSUE not the disease\n"
    "   (e.g. 'breast tumor' → Breast, 'colon carcinoma tissue' → Colon)\n"
    "5. If NOTHING in any field indicates a tissue → Not Specified\n"
    "6. NEVER extract sample IDs, patient codes, or batch numbers from Title\n"
    "7. Title Case. One answer only. No explanation.\n"
    "\n"
    "EXAMPLES:\n"
    "  'source: whole blood' → Whole Blood\n"
    "  'cell type: CD4+ T cells' → CD4+ T Cells\n"
    "  'tissue: liver biopsy' → Liver Biopsy\n"
    "  'MCF-7 cell line treated with...' → MCF-7\n"
    "  'description: RNA from prefrontal cortex' → Prefrontal Cortex\n"
    "  'description: pancreatic adenocarcinoma patient' → Pancreas\n"
    "  Title is just 'Sample_001', no other info → Not Specified"
)

_CONDITION_SYSTEM_PROMPT = (
    "You are a biomedical condition extraction specialist. Your sole task is to "
    "identify the disease state, condition, or phenotype from GEO sample metadata.\n"
    "\n"
    "YOUR EXPERTISE:\n"
    "- You know all major human diseases, syndromes, and conditions\n"
    "- You recognize cancer types: carcinoma, adenocarcinoma, sarcoma, lymphoma,\n"
    "  leukemia, melanoma, glioma, neuroblastoma, myeloma\n"
    "- You recognize infections: bacterial (Neisseria, Staphylococcus), viral (HIV,\n"
    "  HCV, influenza), parasitic\n"
    "- You recognize genetic conditions: cystic fibrosis (CFTR mutations), Down\n"
    "  syndrome, Marfan syndrome, CAIS, Huntington's disease\n"
    "- You recognize phenotypes: obese, diabetic, hypertensive, smoker\n"
    "- 'Normal', 'Control', 'Healthy', 'Unaffected', 'Wild-type' ALL → Control\n"
    "\n"
    "HOW TO READ THE METADATA:\n"
    "- Title: may contain disease name, patient ID, or condition code\n"
    "- Source: rarely has condition info but check anyway\n"
    "- Characteristics: look for 'disease state:', 'diagnosis:', 'condition:',\n"
    "  'group:', 'subject status:', 'disease:', 'phenotype:', 'smoking:'\n"
    "- Description: free-text — diseases often described here\n"
    "- Experiment context: the parent study reveals the disease being studied\n"
    "\n"
    "CRITICAL: Scan ALL fields. Disease info can hide ANYWHERE:\n"
    "  'tumor tissue of pancreatic adenocarcinoma patient' → in Description\n"
    "  'CAIS' → abbreviation in Title\n"
    "  'infected with Neisseria meningitidis' → infection in Description\n"
    "\n"
    "EXTRACTION RULES:\n"
    "1. Extract the MOST SPECIFIC condition/disease present\n"
    "2. Copy exactly what the text states — do NOT rename or generalize\n"
    "3. Abbreviations are valid: 'CAIS', 'AML', 'HCC', 'T2D' — extract them\n"
    "4. Mutations that imply disease: 'CFTR D508' → CFTR D508\n"
    "5. Smoking: 'smoking: 0' → Never Smoker, '1' → Former, '2' → Current\n"
    "6. If sample is healthy/control/normal → Control\n"
    "7. If NO disease/condition info in ANY field → Not Specified\n"
    "8. Title Case. One answer only. No explanation.\n"
    "\n"
    "EXAMPLES:\n"
    "  'disease state: hepatocellular carcinoma' → Hepatocellular Carcinoma\n"
    "  'description: infected with Neisseria meningitidis' → Neisseria Meningitidis Infection\n"
    "  Title: 'ARD842, CAIS, F, GOF' → CAIS\n"
    "  'description: pancreatic ductal adenocarcinoma patient' → Pancreatic Ductal Adenocarcinoma\n"
    "  'group: control' → Control\n"
    "  No disease info anywhere → Not Specified"
)

_TREATMENT_SYSTEM_PROMPT = (
    "You are a biomedical treatment extraction specialist. Your sole task is to "
    "identify any drug, compound, intervention, or exposure applied to a GEO sample.\n"
    "Treatment = something DONE TO or GIVEN TO the patient/sample.\n"
    "\n"
    "YOUR EXPERTISE:\n"
    "- You know pharmaceutical drugs: Dexamethasone, Tamoxifen, Methotrexate, etc.\n"
    "- You know compounds: DMSO (vehicle), estradiol, retinoic acid, TGF-beta\n"
    "- You know interventions: surgery, radiation, gene knockdown (siRNA, shRNA, CRISPR)\n"
    "- You know exposures: smoking, UV irradiation, hypoxia, heat shock\n"
    "- Infections as experimental treatment: 'infected with virus X'\n"
    "- Vehicle controls: DMSO, PBS, saline, ethanol → Untreated\n"
    "\n"
    "HOW TO READ THE METADATA:\n"
    "- Characteristics: look for 'treatment:', 'compound:', 'drug:', 'agent:',\n"
    "  'stimulus:', 'exposure:', 'transfection:'\n"
    "- Description: free-text — often describes treatment in detail\n"
    "- Title: may encode treatment (e.g. 'MCF7_Tamoxifen_24h')\n"
    "- Experiment context: describes the treatment protocol\n"
    "\n"
    "NOT treatments (output Not Specified):\n"
    "- Diseases/conditions (HIV+, Depression, cancer)\n"
    "- Tissues/organs (blood, liver, brain)\n"
    "- Lab protocols (Illumina, TRIzol, RNA extraction, FFPE, RNAlater)\n"
    "- Sample IDs, batch codes, patient identifiers\n"
    "\n"
    "EXTRACTION RULES:\n"
    "1. Extract drug/compound/intervention with dose if present\n"
    "2. Vehicle controls (DMSO, PBS, saline) → Untreated\n"
    "3. 'smoking: 0' or 'never smoker' → Not Specified (no exposure)\n"
    "4. If NO treatment applied → Not Specified\n"
    "5. Title Case. One answer only. No explanation.\n"
    "\n"
    "EXAMPLES:\n"
    "  'treatment: Dexamethasone 10nM' → Dexamethasone 10nM\n"
    "  'compound: Carfilzomib' → Carfilzomib\n"
    "  'infected with influenza A' → Influenza A Infection\n"
    "  'treatment: vehicle (DMSO)' → Untreated\n"
    "  No treatment mentioned → Not Specified"
)

# System prompts indexed by column name
_PER_LABEL_SYSTEM_PROMPTS = {
    "Tissue":    _TISSUE_SYSTEM_PROMPT,
    "Condition": _CONDITION_SYSTEM_PROMPT,
    "Treatment": _TREATMENT_SYSTEM_PROMPT,
}

# User prompt template — metadata only (filled per sample)
_EXTRACT_USER_TEMPLATE = (
    "Title: {TITLE}\n"
    "Source: {SOURCE}\n"
    "Characteristics: {CHAR}"
)

# Legacy combined prompt — kept for backward compatibility with GSEWorker.repair_one
_EXTRACTION_PROMPT_TEMPLATE = (
    "TASK: Read the metadata below and extract exactly what is written.\n"
    "Do NOT normalise, generalise, or map to any vocabulary — copy the specific term.\n"
    "FIELDS:\n"
    "  Tissue    : anatomical tissue, organ, cell type, or cell line as written\n"
    "  Condition : disease, phenotype, or health status as written\n"
    "  Treatment : drug or stimulus as written. None/vehicle = Untreated.\n"
    "RULES:\n"
    "  - Copy the most specific term present (e.g. Alveolar Macrophages not Lung)\n"
    "  - If a cell type is named, use the cell type (e.g. NK cells not PBMC)\n"
    "  - Unknown or absent field = Not Specified\n"
    "  - Title Case. Output JSON only.\n"
    "METADATA: Title: {TITLE}\nSource: {SOURCE}\nCharacteristics: {CHAR}\n"
    "JSON SCHEMA: {\"Tissue\":\"\", \"Condition\":\"\", \"Treatment\":\"\"}"
)
_EXTRACTION_SYSTEM_PROMPT = ""  # not used

# ─────────────────────────────────────────────────────────────────────────────
# PER-LABEL NS INFERENCE PROMPTS  (gemma2:2b, Phase 1b)
# Each label has its OWN GSE-context inference agent.
# GSE experiment context is SYSTEM prompt (KV cached by Ollama).
# ─────────────────────────────────────────────────────────────────────────────
_TISSUE_INFER_SYSTEM = (
    "You are a tissue/cell-type annotator. This sample is part of "
    "the following experiment:\n\n"
    "EXPERIMENT TITLE: {GSE_TITLE}\n"
    "EXPERIMENT SUMMARY: {GSE_SUMMARY}\n"
    "EXPERIMENT DESIGN: {GSE_DESIGN}\n\n"
    "Based on this experiment context, INFER the tissue or cell type.\n"
    "If the experiment uses a specific tissue, this sample is from that tissue.\n"
    "Be specific. Unknown = Not Specified. One answer only."
)
_CONDITION_INFER_SYSTEM = (
    "You are a disease/condition annotator. This sample is part of "
    "the following experiment:\n\n"
    "EXPERIMENT TITLE: {GSE_TITLE}\n"
    "EXPERIMENT SUMMARY: {GSE_SUMMARY}\n"
    "EXPERIMENT DESIGN: {GSE_DESIGN}\n\n"
    "Based on this experiment context, INFER the disease or condition.\n"
    "If the experiment studies a disease, this sample has that condition.\n"
    "Control/healthy in disease study = Control.\n"
    "Be specific. Unknown = Not Specified. One answer only."
)
_TREATMENT_INFER_SYSTEM = (
    "You are a treatment/drug annotator. This sample is part of "
    "the following experiment:\n\n"
    "EXPERIMENT TITLE: {GSE_TITLE}\n"
    "EXPERIMENT SUMMARY: {GSE_SUMMARY}\n"
    "EXPERIMENT DESIGN: {GSE_DESIGN}\n\n"
    "Was a drug, compound, or exposure APPLIED in this experiment?\n"
    "Treatment = drug, compound, surgical procedure, or active exposure (e.g. smoking).\n"
    "Disease names are NEVER treatments: HIV, Depression, Down Syndrome, MS,\n"
    "Cancer, Asthma, NAFLD, PSP, Obesity = these are CONDITIONS not treatments.\n"
    "'Control' or 'Normal' = NOT a treatment.\n"
    "Observational disease-vs-control studies have NO treatment → Not Specified.\n"
    "Vehicle/DMSO/PBS alone = Untreated. No drug applied = Not Specified.\n"
    "One answer only."
)
_PER_LABEL_INFER_SYSTEMS = {
    "Tissue":    _TISSUE_INFER_SYSTEM,
    "Condition": _CONDITION_INFER_SYSTEM,
    "Treatment": _TREATMENT_INFER_SYSTEM,
}

# Legacy combined prompt — kept for backward compatibility
_NS_INFERENCE_PROMPT_TEMPLATE = (
    "TASK: This sample is part of a specific experiment. Based on the experiment\n"
    "context below, INFER the missing fields for this sample.\n"
    "If the experiment studies a specific disease, this sample has that condition.\n"
    "If the experiment uses a specific tissue, this sample is from that tissue.\n"
    "If the experiment applies a treatment/drug, this sample received that treatment.\n"
    "\n"
    "FIELDS TO INFER:\n"
    "  Tissue    : anatomical tissue, organ, cell type, or cell line\n"
    "  Condition : disease, phenotype, or health status\n"
    "  Treatment : drug, stimulus, or intervention. If none = Untreated.\n"
    "\n"
    "EXPERIMENT TITLE: {GSE_TITLE}\n"
    "EXPERIMENT SUMMARY: {GSE_SUMMARY}\n"
    "EXPERIMENT DESIGN: {GSE_DESIGN}\n"
    "\n"
    "SAMPLE METADATA:\n"
    "  Title: {TITLE}\n"
    "  Source: {SOURCE}\n"
    "  Characteristics: {CHAR}\n"
    "\n"
    "RULES:\n"
    "  - Be specific: use the exact disease name (e.g. Psoriasis not Skin Disease)\n"
    "  - If the sample is a control/healthy in a disease study, Condition = Control\n"
    "  - Only infer from the experiment context — do not guess\n"
    "  - If truly cannot determine = Not Specified\n"
    "  - Output JSON only.\n"
    "JSON: {\"Tissue\":\"\", \"Condition\":\"\", \"Treatment\":\"\"}"
)

# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 collapse prompts are now built dynamically by
# CollapseWorker._build_system_prompt() with per-GSE context injection.
def _parse_json_extraction(text: str, cols: list) -> dict:
    """
    Parse JSON from LLM response.
    Uses GREEDY regex (same as proven working old script) to capture full JSON.
    """
    import re as _re, json as _json
    result = {c: NS for c in cols}
    if not text:
        return result
    try:
        # GREEDY match — same as old working script: re.search(r'\{.*\}', text, re.DOTALL)
        m = _re.search(r'\{.*\}', text, _re.DOTALL)
        if m:
            data = _json.loads(m.group(0))
            for col in cols:
                # Try exact key and variants (Tissue, tissue, Cell_Type etc.)
                for key in [col, col.replace(' ', '_'), col.lower(),
                            col.replace(' ', '_').lower()]:
                    if key in data:
                        val = str(data[key]).strip()
                        if val and val.lower() not in ('none', 'null', '', 'not specified'):
                            result[col] = val
                        break
            return result
    except Exception:
        pass
    return parse_combined(text, cols)


def _parse_single_label(text: str) -> str:
    """Parse a single label from a per-label LLM agent response.
    The agent outputs just one label (no JSON), so we clean it up.
    Falls back to JSON parsing if the response contains braces.
    """
    if not text:
        return NS
    text = text.strip()
    # If response contains JSON, extract the value
    if '{' in text:
        import re as _re, json as _json
        m = _re.search(r'\{.*\}', text, _re.DOTALL)
        if m:
            try:
                data = _json.loads(m.group(0))
                # Return the first non-empty value
                for v in data.values():
                    v = str(v).strip()
                    if v and v.lower() not in ('none', 'null', '', 'not specified',
                                                'n/a', 'unknown'):
                        return v
            except Exception:
                pass
    # Plain text response — take first non-empty line
    for line in text.splitlines():
        line = line.strip().rstrip('.')
        # Skip lines that are just instructions/labels
        if line.lower().startswith(('answer:', 'tissue:', 'condition:',
                                     'treatment:', 'note:', 'rules:')):
            line = line.split(':', 1)[1].strip() if ':' in line else ''
        if line and line.lower() not in ('none', 'null', '', 'not specified',
                                          'n/a', 'unknown', 'untreated'):
            return line
    return NS


ALL_GPLS      = ["GPL6947", "GPL96", "GPL570", "GPL10558"]
BATCH_SIZE    = 200

# Species available for platform discovery from GEOmetadb
SPECIES_LIST = [
    "Homo sapiens",
    "Mus musculus",
    "Rattus norvegicus",
    "Danio rerio",
    "Drosophila melanogaster",
    "Caenorhabditis elegans",
    "Sus scrofa",
    "Bos taurus",
    "Gallus gallus",
    "Arabidopsis thaliana",
    "Saccharomyces cerevisiae",
]

# Title keywords to EXCLUDE non-expression platforms (used only in "Expression Microarray" mode)
_PLATFORM_EXCLUDE_KEYWORDS = [
    "%SNP%", "%ethylat%", "%miRNA%", "%Exome%", "%Genotyp%",
    "%Mapping%", "%CGH%", "%Copy Number%", "%Tiling%", "%ChIP%",
    "%Promoter%", "%Splicing%", "%ncRNA%", "%lncRNA%", "%16S%",
]
# Sequencing-specific keywords (excluded in microarray mode, included in RNA-seq mode)
_SEQ_KEYWORDS = [
    "%Sequencing%", "%HiSeq%", "%MiSeq%", "%NextSeq%", "%NovaSeq%",
    "%Ion Torrent%", "%SOLiD%", "%454%", "%PacBio%",
]

# Technology filter options for the GUI
TECHNOLOGY_FILTERS = {
    "All (any technology)":       {"tech_filter": None,  "exclude_seq": False, "exclude_nonexpr": False},
    "Expression Microarray":      {"tech_filter": None,  "exclude_seq": True,  "exclude_nonexpr": True},
    "RNA-seq / Sequencing":       {"tech_filter": "high-throughput sequencing", "exclude_seq": False, "exclude_nonexpr": True},
    "Methylation":                {"tech_filter": None,  "exclude_seq": True,  "exclude_nonexpr": False,
                                   "title_require": ["%ethylat%"]},
    "miRNA":                      {"tech_filter": None,  "exclude_seq": False, "exclude_nonexpr": False,
                                   "title_require": ["%miRNA%"]},
}

MIN_SAMPLES_DEFAULT = 100   # minimum samples for a platform to show up

# ══════════════════════════════════════════════════════════════════════════════
#  MEMORY AGENT  —  Persistent biomedical label memory (4 tiers)
#
#  Architecture (see diagram):
#    Tier 1 — Core memory       : top-N most frequent labels, always injected
#    Tier 2 — Semantic memory   : ~3800 labels embedded, cosine query (RAG)
#    Tier 3 — Episodic memory   : log of every past resolution + confidence
#    Tier 4 — Knowledge graph   : synonym/hierarchy triples (SQLite)
#
#  Persisted in:  {harmonized_dir}/biomedical_memory.db  (SQLite)
#  Shared across: all platforms, all runs, all agents — one DB for all.
#
#  Cross-agent API (tools exposed):
#    search_tissue(query)    → top-k + episodic hits
#    search_condition(query) → top-k + episodic hits
#    log_resolution(raw, canonical, col, confidence)  → writes episodic tier
#
#  Memory type rationale:
#    3800 labels → in-context impossible (~15k tokens).
#    Rebuilt-each-run RAG → loses all past resolution knowledge.
#    This agent persists embeddings + episodic log across runs and platforms.
# ══════════════════════════════════════════════════════════════════════════════

MEM_TOP_K          = 10     # semantic neighbors retrieved per query
MEM_MATCH_THRESH   = 0.72   # cosine similarity floor (lowered for cell line variants)
MEM_EMBED_BATCH    = 32     # labels per /api/embed call
MEM_CORE_N         = 99999  # store ALL clusters — no cap (not injected into prompts)
MEM_DB_NAME        = "biomedical_memory.db"
MEM_EMBED_MODEL    = "nomic-embed-text"
LLM_MEMORY_DIR     = "LLM_memory"   # folder with cluster files
CLUSTER_FILE = {"Tissue":     "Tissues_clusters_db_ready.txt",
                "Condition":  "Conditions_clusters_db_ready.txt",
                "Treatment":  "treatment_clusters_db_ready.txt"}


class MemoryAgent:
    """
    Persistent 4-tier biomedical label memory agent.

    Lifecycle:
      1. MemoryAgent(db_path, ollama_url)   open / create DB
      2. build(all_dfs, log_fn)             populate tiers from input CSVs
                                             (skips labels already embedded)
      3. search(col, text)                  ranked candidates
      4. log_resolution(raw, canonical, col, confidence)   episodic write
      5. core_labels(col)                   kept for stats only, NOT injected into prompts

    Thread-safe: read ops use shared connection pool, writes are serialised
    through a lock.
    """

    # ── Init / DB setup ───────────────────────────────────────────────────────

    def __init__(self, db_path: str, ollama_url: str):
        self.db_path    = db_path
        self.ollama_url = ollama_url
        self._lock      = threading.Lock()
        # Track every new cluster created this run for the report
        self._new_cluster_log: list = []   # list of dicts

        # In-RAM vector cache: {col: (labels_list, np.ndarray)}
        self._vec_cache:   Dict[str, tuple] = {}
        self._cache_ok:    Dict[str, bool]  = {c: False for c in LABEL_COLS}
        # Embed query cache — avoids repeated HTTP calls for same text
        # Key: text string  Value: L2-normalised float32 numpy array
        self._embed_cache: Dict[str, "np.ndarray"] = {}

        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        c.execute("PRAGMA journal_mode=WAL")
        return c

    def _init_db(self):
        """Create tables if they don't exist."""
        with self._lock, self._conn() as c:
            c.executescript("""
            -- Tier 1: Core  top-N most frequent labels (updated on build)
            CREATE TABLE IF NOT EXISTS core_labels (
                col    TEXT NOT NULL,
                label  TEXT NOT NULL,
                freq   INTEGER DEFAULT 1,
                PRIMARY KEY (col, label)
            );

            -- Tier 2: Semantic  one row per unique canonical label
            CREATE TABLE IF NOT EXISTS semantic_labels (
                col        TEXT NOT NULL,
                label      TEXT NOT NULL,
                embedding  BLOB,          -- float32 numpy array serialised
                freq       INTEGER DEFAULT 1,
                PRIMARY KEY (col, label)
            );

            -- Tier 3: Episodic  every resolution ever made
            CREATE TABLE IF NOT EXISTS episodic_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                col         TEXT NOT NULL,
                raw_label   TEXT NOT NULL,
                canonical   TEXT NOT NULL,
                confidence  REAL DEFAULT 1.0,
                platform    TEXT DEFAULT '',
                gse         TEXT DEFAULT '',
                gsm         TEXT DEFAULT '',
                ts          TEXT DEFAULT (datetime('now')),
                collapse_rule TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS ep_raw ON episodic_log(col, raw_label);
            CREATE INDEX IF NOT EXISTS ep_can ON episodic_log(col, canonical);

            -- Tier 4: Knowledge graph  synonym / hierarchy triples
            CREATE TABLE IF NOT EXISTS kg_triples (
                col      TEXT NOT NULL,
                subject  TEXT NOT NULL,
                relation TEXT NOT NULL,   -- 'synonym_of', 'is_a', 'variant_of'
                object   TEXT NOT NULL,
                weight   REAL DEFAULT 1.0,
                PRIMARY KEY (col, subject, relation, object)
            );
            CREATE INDEX IF NOT EXISTS kg_sub ON kg_triples(col, subject);

            -- Cluster map  raw label  canonical cluster name (O(1) lookup)
            -- This is the PRIMARY resolution table. Cluster names are the
            -- human-approved canonical labels from LLM_memory/ files.
            CREATE TABLE IF NOT EXISTS cluster_map (
                col     TEXT NOT NULL,
                raw     TEXT NOT NULL,
                cluster TEXT NOT NULL,
                PRIMARY KEY (col, raw)
            );
            CREATE INDEX IF NOT EXISTS cm_raw ON cluster_map(col, raw);
            """)



    #  Cluster file parsing 

    @staticmethod
    def parse_cluster_file(path: str, col: str) -> Dict[str, List[str]]:
        """
        Parse a cluster file from LLM_memory/ folder.
        Returns {cluster_name: [raw_label, ...]} dict.

        Handles two formats:

        Tissues format:
            CLUSTER: Name (TOTAL: N)
              - Raw Label 1
              - Raw Label 2

        Conditions format:
            ...
            CLUSTER: NAME
              Total: N | Platforms: X/4
               GPL...csv.gz (N samples)
                    - Raw Label    | count
        """
        clusters: Dict[str, List[str]] = {}
        if not os.path.exists(path):
            return clusters

        current_cluster = None
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line_s = line.rstrip()
                stripped = line_s.strip()

                # Both formats start a new cluster with "CLUSTER: ..."
                if stripped.upper().startswith("CLUSTER:"):
                    # Extract name  strip trailing (TOTAL: N) if present
                    name = stripped[len("CLUSTER:"):].strip()
                    name = re.sub(r"\s*\(TOTAL:.*$", "", name, flags=re.IGNORECASE).strip()
                    # Skip the NS cluster  we only want canonical targets
                    if name.upper() == "NOT SPECIFIED":
                        current_cluster = None
                        continue
                    # Normalise ALL-CAPS cluster names to title case
                    # so output labels are properly cased:
                    # CONTROL  Control, ALZHEIMER DISEASE  Alzheimer Disease
                    # Mixed-case names (Cell Line: Mcf7 Cells) stay as-is.
                    if name == name.upper() and len(name) > 1:
                        name = name.title()
                    current_cluster = name
                    if current_cluster not in clusters:
                        clusters[current_cluster] = []

                elif current_cluster is not None:
                    # Tissues format: "  - Raw Label"
                    # Conditions format: "        - Raw Label   | count"
                    if re.match(r"\s+-\s+", line_s):
                        # Strip leading "  - " and trailing "| count"
                        raw = re.sub(r"\s*\|.*$", "", line_s)
                        raw = re.sub(r"^\s*-\s*", "", raw).strip()
                        if raw:
                            clusters[current_cluster].append(raw)

        return clusters

    #  Build from cluster files (PRIMARY build path) 

    def build_from_clusters(self, llm_memory_dir: str, log_fn=print) -> None:
        # .txt files are read ONCE here to seed the DB. After this,
        # the DB is the sole source of truth — .txt files are never touched again.
        """
        Populate all memory tiers from the cluster files in LLM_memory/.

        What gets stored:
          cluster_map  : raw_label (lowercased)  cluster_name  (O(1) lookup)
          kg_triples   : raw_label assigned_to cluster_name     (provenance)
          semantic     : CLUSTER NAMES only  these are the collapse targets
          core_labels  : top-N cluster names  kept for stats/UI, NOT injected into prompts
                         (GSE experiment label context is the relevant signal, not global freq)

        The cluster files contain human-approved canonical labels (clusters)
        with all raw labels that were assigned to each cluster. The memory
        agent collapses NS-resolved labels to CLUSTER NAMES, never to raw labels.
        """
        for col in LABEL_COLS:
            fname = CLUSTER_FILE.get(col)
            if not fname:
                continue
            path = os.path.join(llm_memory_dir, fname)
            if not os.path.exists(path):
                log_fn(f"  [MemoryAgent] {col}: cluster file not found: {path}")
                continue

            log_fn(f"  [MemoryAgent] {col}: parsing {fname} ")
            clusters = self.parse_cluster_file(path, col)
            if not clusters:
                log_fn(f"  [MemoryAgent] {col}: no clusters parsed")
                continue

            log_fn(f"  [MemoryAgent] {col}: {len(clusters):,} clusters, "
                   f"{sum(len(v) for v in clusters.values()):,} raw label mappings")

            #  Populate cluster_map + kg_triples 
            cm_rows = []
            kg_rows = []
            for cluster, raws in clusters.items():
                for raw in raws:
                    raw_lower = raw.lower().strip()
                    if not raw_lower:
                        continue
                    cm_rows.append((col, raw_lower, cluster))
                    # Also map exact-case version
                    cm_rows.append((col, raw.strip(), cluster))
                    kg_rows.append((col, raw.strip(), "assigned_to", cluster, 1.0))

                # Cluster name maps to itself (exact canonical lookup)
                cm_rows.append((col, cluster.lower(), cluster))
                cm_rows.append((col, cluster, cluster))

            # Store ALL normalised forms at build time  fast O(1) lookup later
            # Includes: exact, lower, norm, prefix-stripped, singular/plural
            extra_rows = []
            seen = {(c1, r) for c1, r, _ in cm_rows}
            for col_val, raw, cluster in cm_rows:
                for form in MemoryAgent._all_forms(raw):
                    if form and (col_val, form) not in seen:
                        extra_rows.append((col_val, form, cluster))
                        seen.add((col_val, form))

            with self._lock, self._conn() as c:
                c.executemany(
                    "INSERT OR REPLACE INTO cluster_map (col, raw, cluster) "
                    "VALUES (?,?,?)", cm_rows + extra_rows)
                c.executemany(
                    "INSERT OR REPLACE INTO kg_triples "
                    "(col, subject, relation, object, weight) VALUES (?,?,?,?,?)"
                    , kg_rows)
            log_fn(f"  [MemoryAgent] {col}: {len(cm_rows) + len(extra_rows):,} "
                   f"cluster_map entries ({len(extra_rows):,} extra normalised forms)")

            #  Embed CLUSTER NAMES only (these are the canonical targets) 
            cluster_names = list(clusters.keys())

            # Find which cluster names are not yet embedded
            with self._conn() as c:
                existing = {r[0] for r in c.execute(
                    "SELECT label FROM semantic_labels WHERE col=?", (col,))}
            new_clusters = [cn for cn in cluster_names if cn not in existing]

            if new_clusters:
                log_fn(f"  [MemoryAgent] {col}: embedding "
                       f"{len(new_clusters):,} new cluster names ")
                vecs = self._embed_batch(new_clusters, log_fn)
                if vecs is not None:
                    # Frequency = number of raw labels assigned to this cluster
                    with self._lock, self._conn() as c:
                        c.executemany(
                            "INSERT OR REPLACE INTO semantic_labels "
                            "(col, label, embedding, freq) VALUES (?,?,?,?)",
                            [(col, cn,
                              vecs[i].astype(np.float32).tobytes(),
                              len(clusters[cn]))
                             for i, cn in enumerate(new_clusters)])
                    log_fn(f"  [MemoryAgent] {col}: {len(new_clusters):,} "
                           f"cluster names embedded")
                else:
                    log_fn(f"  [MemoryAgent] {col}: embedding failed  "
                           f"falling back to cluster_map only")

            #  Core labels: top-N clusters by raw label count 
            top = sorted(clusters.items(),
                         key=lambda x: len(x[1]), reverse=True)
            with self._lock, self._conn() as c:
                c.execute("DELETE FROM core_labels WHERE col=?", (col,))
                c.executemany(
                    "INSERT INTO core_labels (col, label, freq) VALUES (?,?,?)",
                    [(col, cn, len(raws)) for cn, raws in top])

            # Load into RAM cache
            self._load_cache(col, log_fn)

            n_sem = len(self._vec_cache.get(col, ([], None))[0])
            sem_status = (f"{n_sem:,} names embedded"
                          if n_sem > 0
                          else "semantic DISABLED (nomic-embed-text not pulled)")
            log_fn(f"  [MemoryAgent] {col}:  "
                   f"{len(cluster_names):,} clusters | "
                   f"core={len(cluster_names)} | "
                   f"semantic={sem_status}")

    #  Cluster lookup (fastest path  O(1) dict hit) 

    @staticmethod
    def _norm_raw(text: str) -> str:
        """
        Normalise a raw label for cluster lookup.
        Strips hyphens/underscores/extra spaces and lowercases
        so 'MCF-7 CELL LINE' matches 'Mcf7 Cell Line' in the DB.
        """
        t = text.lower().strip()
        t = re.sub(r"[-_/]", " ", t)          # hyphens/underscores  space
        t = re.sub(r"\s+", " ", t).strip()   # collapse spaces
        return t

    @staticmethod
    def _all_forms(text: str) -> List[str]:
        """
        All normalised lookup forms for a raw label.
        Returns: exact, lowercase, normalised, prefix-stripped,
        parenthetical-stripped, suffix-stripped, and plural/singular variants.
        """
        t = text.strip()
        forms = [
            t,
            t.lower(),
            MemoryAgent._norm_raw(t),
        ]
        # Strip "Cell Line:", "Cell Type:" etc prefixes
        stripped = MemoryAgent._strip_cell_prefix(t)
        if stripped != t:
            forms += [stripped, stripped.lower(),
                      MemoryAgent._norm_raw(stripped)]

        # Strip parenthetical suffixes: "Abdominal Aortic Aneurysm (AAA)" → "Abdominal Aortic Aneurysm"
        no_paren = re.sub(r'\s*\([^)]*\)\s*$', '', t).strip()
        if no_paren and no_paren != t:
            forms += [no_paren, no_paren.lower(), MemoryAgent._norm_raw(no_paren)]

        # Strip common biomedical suffixes for tissue lookups
        for suffix in (" cells", " cell", " tissue", " cell line",
                       " cell lines", " tissues", " sample", " samples",
                       " biopsy", " biopsies", " specimen"):
            tl = t.lower()
            if tl.endswith(suffix) and len(tl) > len(suffix) + 2:
                base = t[:len(t) - len(suffix)].strip()
                forms += [base, base.lower(), MemoryAgent._norm_raw(base)]

        # Plural/singular: add trailing 's' or strip it
        tl = t.lower().strip()
        if tl.endswith('s') and len(tl) > 3:
            forms.append(tl[:-1])
        elif not tl.endswith('s') and len(tl) > 2:
            forms.append(tl + 's')

        return [f for f in forms if f]

    def cluster_lookup(self, col: str, raw_label: str) -> Optional[str]:
        """
        Look up raw_label in cluster_map using all normalised forms.
        Tries exact, lowercase, normalised, prefix-stripped, parenthetical-stripped,
        suffix-stripped, and plural/singular variants.
        If cluster_map returns a self-mapping (orphan cluster), continues to fuzzy matching.
        Falls back to case-insensitive matching on semantic_labels cluster names.
        """
        try:
            with self._conn() as c:
                # All normalised forms
                first_hit = None
                for attempt in self._all_forms(raw_label):
                    row = c.execute(
                        "SELECT cluster FROM cluster_map WHERE col=? AND raw=?",
                        (col, attempt)).fetchone()
                    if row:
                        # Check if this cluster is a REAL canonical cluster
                        # (exists in semantic_labels with an embedding)
                        is_canon = c.execute(
                            "SELECT 1 FROM semantic_labels WHERE col=? AND LOWER(label)=LOWER(?) "
                            "AND embedding IS NOT NULL AND LENGTH(embedding) > 0",
                            (col, row[0])).fetchone()
                        if is_canon:
                            return row[0]
                        # Self-mapping orphan — save as fallback but keep looking
                        if first_hit is None:
                            first_hit = row[0]

                # Case-insensitive cluster name match
                row = c.execute(
                    "SELECT label FROM semantic_labels "
                    "WHERE col=? AND LOWER(label)=LOWER(?)",
                    (col, raw_label.strip())).fetchone()
                if row:
                    return row[0]

                # Parenthetical stripping: "Abdominal Aortic Aneurysm (AAA)" → "..."
                no_paren = re.sub(r'\s*\([^)]*\)\s*$', '', raw_label).strip()
                if no_paren != raw_label and no_paren:
                    row = c.execute(
                        "SELECT label FROM semantic_labels "
                        "WHERE col=? AND LOWER(label)=LOWER(?)",
                        (col, no_paren)).fetchone()
                    if row:
                        return row[0]
                    # Also check cluster_map for the cleaned form
                    for attempt in self._all_forms(no_paren):
                        row = c.execute(
                            "SELECT cluster FROM cluster_map WHERE col=? AND raw=?",
                            (col, attempt)).fetchone()
                        if row:
                            is_canon = c.execute(
                                "SELECT 1 FROM semantic_labels WHERE col=? AND LOWER(label)=LOWER(?) "
                                "AND embedding IS NOT NULL AND LENGTH(embedding) > 0",
                                (col, row[0])).fetchone()
                            if is_canon:
                                return row[0]

                # Suffix-stripped match: "Alveolar Epithelial Cells" → "Alveolar Epithelial" → "Alveolar"
                stripped_base = MemoryAgent._strip_cell_prefix(raw_label)
                for suffix in (" cells", " cell", " tissue", " cell line",
                               " cell lines", " tissues", " sample", " samples",
                               " biopsy", " biopsies", "-derived stem cells",
                               " stem cells", " progenitor cells"):
                    sl = stripped_base.lower()
                    if sl.endswith(suffix) and len(sl) > len(suffix) + 2:
                        base = stripped_base[:len(stripped_base) - len(suffix)].strip()
                        # Try semantic_labels
                        row = c.execute(
                            "SELECT label FROM semantic_labels "
                            "WHERE col=? AND LOWER(label)=LOWER(?)",
                            (col, base)).fetchone()
                        if row:
                            return row[0]
                        # Try cluster_map
                        for attempt in self._all_forms(base):
                            row = c.execute(
                                "SELECT cluster FROM cluster_map WHERE col=? AND raw=?",
                                (col, attempt)).fetchone()
                            if row:
                                is_canon = c.execute(
                                    "SELECT 1 FROM semantic_labels WHERE col=? AND LOWER(label)=LOWER(?)",
                                    (col, row[0])).fetchone()
                                if is_canon:
                                    return row[0]

                # Progressive word stripping from right:
                # "Alveolar Epithelial Cells" → "Alveolar Epithelial" → "Alveolar"
                # Stop when we find a canonical match or reach 1 word
                stripped_base = MemoryAgent._strip_cell_prefix(raw_label)
                no_p = re.sub(r'\s*\([^)]*\)\s*$', '', stripped_base).strip()
                words = no_p.split()
                if len(words) >= 2:
                    for n_drop in range(1, len(words)):
                        partial = " ".join(words[:len(words) - n_drop])
                        if len(partial) < 3:
                            break
                        row = c.execute(
                            "SELECT label FROM semantic_labels "
                            "WHERE col=? AND LOWER(label)=LOWER(?)",
                            (col, partial)).fetchone()
                        if row:
                            return row[0]

                # Return the first hit even if orphan (better than None)
                if first_hit is not None:
                    return first_hit

        except Exception:
            pass
        return None

    #  Tier 2: Build / embed
        return None

    #  Tier 2: Build / embed 

    def build(self, all_dfs: Dict[str, "pd.DataFrame"], log_fn=print) -> None:
        """
        Populate Tier 1 (core) and Tier 2 (semantic) from input DataFrames.

        INPUT RULE (Memory Lifecycle  Aggregation & Ingestion):
          Only rows where the field is NOT "Not Specified" are ingested.
          Tissue and Condition vocabularies are kept strictly separate.
          This ensures the memory only contains verified canonical labels
          from the harmonized input  never NS placeholders or repaired guesses.
        """
        for col in LABEL_COLS:
            #  Aggregation & Ingestion 
            # Strictly filter: only confirmed non-NS values from this category.
            # Tissue labels never contaminate the Condition vocabulary and vice versa.
            freq: Counter = Counter()
            for platform, df in all_dfs.items():
                if col not in df.columns:
                    continue
                # Only rows where THIS specific column has a real label
                mask = df[col].notna() & (df[col] != NS) & (df[col].str.strip() != "")
                confirmed = df.loc[mask, col].tolist()
                freq.update(confirmed)
                log_fn(f"  [MemoryAgent] {col}  {platform}: "
                       f"{len(confirmed):,} confirmed labels "
                       f"({df[col].notna().sum():,} total rows)")
            if not freq:
                log_fn(f"  [MemoryAgent] {col}: no confirmed labels found  skipping")
                continue
            log_fn(f"  [MemoryAgent] {col}: {len(freq):,} unique canonical labels ingested")

            # NOTE: build() does NOT write to semantic_labels.
            # semantic_labels contains ONLY cluster names from LLM_memory/
            # written by build_from_clusters(). Raw platform labels must never
            # enter semantic_labels  that would make is_cluster_name() return
            # True for raw labels like "CELECOXIB (.00001 M) FOR 6 H", letting
            # them pass the cluster gate as valid output labels.

            # Update frequencies for existing labels too
            with self._lock, self._conn() as c:
                c.executemany(
                    "UPDATE semantic_labels SET freq=? WHERE col=? AND label=?",
                    [(freq[lbl], col, lbl) for lbl in existing if lbl in freq]
                )

            # Tier 1: refresh core labels (top-N by frequency)
            top = freq.most_common()  # store all clusters, no cap
            with self._lock, self._conn() as c:
                c.execute("DELETE FROM core_labels WHERE col=?", (col,))
                c.executemany(
                    "INSERT INTO core_labels (col, label, freq) VALUES (?,?,?)",
                    [(col, lbl, cnt) for lbl, cnt in top]
                )
            log_fn(f"  [MemoryAgent] {col}:  core={len(freq)} "
                   f"semantic={len(freq):,} labels")

            # Load vector cache into RAM for fast query
            self._load_cache(col, log_fn)

    def _detect_embed_model(self) -> str:
        """
        Query Ollama for available models and return the best one for embedding.
        Priority: nomic-embed-text > mxbai-embed-large > any *embed* model >
                  snowflake-arctic-embed > all-minilm > chat model as last resort.
        Result is cached after first call.
        """
        if getattr(self, "_embed_model_detected", None):
            return self._embed_model_detected

        preferred = [
            "nomic-embed-text", "mxbai-embed-large",
            "snowflake-arctic-embed", "all-minilm",
        ]
        try:
            resp = requests.get(
                self.ollama_url.rstrip("/") + "/api/tags", timeout=5)
            resp.raise_for_status()
            available = [m["name"].split(":")[0]
                         for m in resp.json().get("models", [])]
            # Exact match first
            for p in preferred:
                if any(p in a for a in available):
                    found = next(a for a in available if p in a)
                    self._embed_model_detected = found
                    return found
            # Any *embed* model
            embed_models = [a for a in available if "embed" in a.lower()]
            if embed_models:
                self._embed_model_detected = embed_models[0]
                return embed_models[0]
        except Exception:
            pass
        # Fall back to configured model
        self._embed_model_detected = MEM_EMBED_MODEL
        return MEM_EMBED_MODEL

    # Lazy-loaded sentence-transformers model (CPU, no VRAM needed)
    _st_model = None
    _st_lock  = threading.Lock()

    @classmethod
    def _get_st_model(cls):
        """Lazy-load sentence-transformers model on first use."""
        if cls._st_model is None:
            with cls._st_lock:
                if cls._st_model is None:
                    try:
                        from sentence_transformers import SentenceTransformer
                        cls._st_model = SentenceTransformer("FremyCompany/BioLORD-2023-C")
                    except ImportError:
                        pass
        return cls._st_model

    def _call_embed(self, texts: List[str]) -> "Optional[List[List[float]]]":
        """
        Embed texts using local sentence-transformers (CPU, no VRAM).
        Falls back to Ollama /api/embed if sentence-transformers unavailable.
        """
        # Prefer local sentence-transformers (no VRAM competition)
        st = self._get_st_model()
        if st is not None:
            try:
                vecs = st.encode(texts, batch_size=128, show_progress_bar=False,
                                 normalize_embeddings=True)
                return vecs.tolist()
            except Exception:
                pass

        # Fallback: Ollama embedding endpoint
        base  = self.ollama_url.rstrip("/")
        model = self._detect_embed_model()
        try:
            resp = requests.post(
                base + "/api/embed",
                json={"model": model, "input": texts},
                timeout=60)
            if resp.status_code != 404:
                resp.raise_for_status()
                data = resp.json()
                if "embeddings" in data:
                    return data["embeddings"]
        except Exception:
            pass
        # Endpoint 2: /api/embeddings  single text, older Ollama
        results = []
        for text in texts:
            try:
                resp = requests.post(
                    base + "/api/embeddings",
                    json={"model": model, "prompt": text},
                    timeout=30)
                resp.raise_for_status()
                data = resp.json()
                if "embedding" in data:
                    results.append(data["embedding"])
                else:
                    return None
            except Exception:
                return None
        return results if results else None

    def _embed_batch(self, labels: List[str], log_fn) -> "Optional[np.ndarray]":
        all_vecs = []
        for i in range(0, len(labels), MEM_EMBED_BATCH):
            chunk = labels[i:i + MEM_EMBED_BATCH]
            vecs = self._call_embed(chunk)
            if vecs is None:
                model_used = self._detect_embed_model()
                log_fn(f"  [MemoryAgent] embed error batch {i}: "
                       f"both /api/embed and /api/embeddings failed for "
                       f"model '{model_used}'.  "
                       f"Fix: ollama pull {model_used}  "
                       f"  (or pull any embed model: ollama pull nomic-embed-text)")
                return None
            all_vecs.append(np.array(vecs, dtype=np.float32))
        mat   = np.ascontiguousarray(np.vstack(all_vecs), dtype=np.float32)
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1.0, norms)
        return np.ascontiguousarray(mat / norms, dtype=np.float32)

    def _load_cache(self, col: str, log_fn=print):
        """Load semantic embeddings from DB into RAM numpy array."""
        try:
            with self._conn() as c:
                rows = c.execute(
                    "SELECT label, embedding FROM semantic_labels "
                    "WHERE col=? AND embedding IS NOT NULL ORDER BY label",
                    (col,)).fetchall()
            if not rows:
                return
            labels = [r[0] for r in rows]
            mat    = np.ascontiguousarray(np.stack([
                np.frombuffer(r[1], dtype=np.float32).copy()
                for r in rows]), dtype=np.float32)
            # Re-normalise in case of DB corruption
            norms = np.linalg.norm(mat, axis=1, keepdims=True)
            norms = np.where(norms == 0, 1.0, norms)
            mat   = np.ascontiguousarray(mat / norms, dtype=np.float32)
            with self._lock:
                self._vec_cache[col] = (labels, mat)
                self._cache_ok[col]  = True
            log_fn(f"  [MemoryAgent] {col}: loaded {len(labels):,} vectors into RAM")
        except Exception as e:
            log_fn(f"  [MemoryAgent] cache load error ({col}): {e}")

    def load_cache_all(self, log_fn=print):
        """Load all columns from DB  RAM (called at startup if DB already exists)."""
        for col in LABEL_COLS:
            self._load_cache(col, log_fn)

    #  Tier 2: Semantic search 

    def _embed_one(self, text: str) -> "Optional[np.ndarray]":
        """Embed a single text string with an in-process cache.
        The HTTP call to Ollama costs 300-500ms  caching avoids
        repeating it for the same extracted label seen across multiple
        NS samples in a run.
        """
        cached = self._embed_cache.get(text)
        if cached is not None:
            return cached
        vecs = self._call_embed([text])
        if vecs is None:
            return None
        vec = np.ascontiguousarray(
            np.array(vecs[0], dtype=np.float32))
        n = np.linalg.norm(vec)
        result = vec / n if n > 0 else None
        if result is not None:
            # Cache up to 10k entries  covers a full platform run
            if len(self._embed_cache) < 10_000:
                self._embed_cache[text] = result
        return result

    @staticmethod
    def _strip_cell_prefix(text: str) -> str:
        """Strip structural prefix: Cell Line: Mcf7 Cells -> Mcf7 Cells."""
        tl = text.lower().strip()
        for pfx in ("cell line:", "cell type:", "tissue:", "organ:",
                    "cell line :", "cell type :", "tissue :"):
            if tl.startswith(pfx):
                return text[len(pfx):].strip()
        return text


    def _safe_dot(self, mat: "np.ndarray", vec: "np.ndarray") -> "Optional[np.ndarray]":
        """Safe dot product  catches numpy segfaults via shape validation."""
        try:
            if mat is None or vec is None:
                return None
            if mat.ndim != 2 or vec.ndim != 1:
                return None
            if mat.shape[1] != vec.shape[0]:
                return None
            # Ensure contiguous float32  prevents BLAS segfaults
            mat_c = np.ascontiguousarray(mat, dtype=np.float32)
            vec_c = np.ascontiguousarray(vec, dtype=np.float32)
            return mat_c @ vec_c
        except Exception:
            return None

    def semantic_search(self, col: str, text: str,
                        k: int = MEM_TOP_K) -> List[tuple]:
        """
        Return [(label, cosine_sim), ...] top-k filtered to >= MEM_MATCH_THRESH.
        """
        try:
            if not self._cache_ok.get(col):
                return []
            with self._lock:
                labels, mat = self._vec_cache[col]
            if mat is None or len(labels) == 0:
                return []

            results = {}

            def _search_one(query_text: str):
                vec = self._embed_one(query_text)
                if vec is None:
                    return
                sims = self._safe_dot(mat, vec)
                if sims is None:
                    return
                for i in np.argsort(sims)[::-1][:k]:
                    if sims[i] >= MEM_MATCH_THRESH:
                        lbl = labels[i]
                        results[lbl] = max(results.get(lbl, 0.0), float(sims[i]))

            # Query 1: text as-is
            _search_one(text)

            # Query 2: strip cell prefix (helps cell line matching)
            stripped = self._strip_cell_prefix(text)
            if stripped != text:
                _search_one(stripped)

            return sorted(results.items(), key=lambda x: x[1], reverse=True)[:k]
        except Exception:
            return []

    #  Tier 3: Episodic search + write 

    def episodic_search(self, col: str, raw_label: str) -> List[dict]:
        """
        Look up past resolutions for this exact raw label.
        Returns list of {canonical, confidence, count, last_ts}.
        """
        try:
            with self._conn() as c:
                rows = c.execute("""
                    SELECT canonical,
                           AVG(confidence)  AS avg_conf,
                           COUNT(*)         AS cnt,
                           MAX(ts)          AS last_ts
                    FROM   episodic_log
                    WHERE  col=? AND raw_label=?
                    GROUP  BY canonical
                    ORDER  BY cnt DESC, avg_conf DESC
                    LIMIT  5
                """, (col, raw_label)).fetchall()
            return [{"canonical": r[0], "confidence": r[1],
                     "count": r[2], "last_ts": r[3]} for r in rows]
        except Exception:
            return []

    def log_resolution(self, col: str, raw_label: str, canonical: str,
                       confidence: float = 1.0, platform: str = "",
                       gse: str = "", gsm: str = "",
                       collapse_rule: str = "") -> None:
        """
        Write a resolution event to Tier 3 (episodic log).
        Also upserts a synonym triple into Tier 4 if raw != canonical.
        """
        try:
            with self._lock, self._conn() as c:
                c.execute("""
                    INSERT INTO episodic_log
                      (col, raw_label, canonical, confidence,
                       platform, gse, gsm, collapse_rule)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (col, raw_label, canonical, confidence,
                      platform, gse, gsm, collapse_rule))
                # Tier 4: auto-populate knowledge graph with variant_of triple
                if raw_label != canonical:
                    c.execute("""
                        INSERT OR REPLACE INTO kg_triples
                          (col, subject, relation, object, weight)
                        VALUES (?,?,?,?,?)
                    """, (col, raw_label, "variant_of", canonical, confidence))
        except Exception:
            pass   # never crash the extraction pipeline on a memory write

    #  Tier 4: Knowledge graph lookup 

    def kg_lookup(self, col: str, label: str) -> List[tuple]:
        """
        Return [(object, relation, weight), ...] for triples where
        subject=label  finds known synonyms / variants.
        """
        try:
            with self._conn() as c:
                return c.execute("""
                    SELECT object, relation, weight
                    FROM   kg_triples
                    WHERE  col=? AND subject=?
                    ORDER  BY weight DESC
                    LIMIT  5
                """, (col, label)).fetchall()
        except Exception:
            return []

    #  Tier 1: Core labels 

    def core_labels(self, col: str, n: int = MEM_CORE_N) -> List[str]:
        """Return top-N most frequent labels for in-context injection."""
        try:
            with self._conn() as c:
                return [r[0] for r in c.execute(
                    "SELECT label FROM core_labels WHERE col=? "
                    "ORDER BY freq DESC LIMIT ?", (col, n))]
        except Exception:
            return []

    #  Unified search (all tiers) 

    def search(self, col: str, text: str) -> dict:
        """
        Full multi-tier search. Priority:
          0. cluster_map   O(1) direct rawcluster lookup (fastest, most certain)
          1. episodic      past resolutions with confidence
          2. semantic      cosine similarity against cluster names
          3. kg            knowledge graph triples

        Returns {
            "cluster":   str or None   direct cluster match (use immediately),
            "episodic":  [...],
            "semantic":  [(cluster_name, sim), ...],
            "kg":        [(object, relation, weight), ...],
        }
        """
        return {
            "cluster":  self.cluster_lookup(col, text),
            "episodic": self.episodic_search(col, text),
            "semantic": self.semantic_search(col, text),
            "kg":       self.kg_lookup(col, text),
        }

    def is_cluster_name(self, col: str, label: str) -> bool:
        """
        Returns True if label is a cluster name in LLM_memory.
        Case-insensitive — 'Pcb', 'PBC', 'pbc' all match 'PBC'.
        """
        try:
            with self._conn() as c:
                row = c.execute(
                    "SELECT 1 FROM semantic_labels WHERE col=? AND LOWER(label)=LOWER(?)",
                    (col, label.strip())).fetchone()
                return row is not None
        except Exception:
            return False


    def register_new_cluster(self, col: str, cluster_name: str,
                              raw_label: str,
                              log_fn=print) -> None:
        """
        Create a brand-new cluster for a label that has no match anywhere.

        Called when ALL 5 memory tiers fail and the extracted label is a
        specific, unique entity (e.g. a novel cell line) that deserves its
        own cluster rather than being discarded as Not Specified.

        Everything is written to the .db only — .txt files are never touched
        after the initial build. The DB is the sole source of truth.

        What this does:
          1. Writes the new cluster name + raw mapping to cluster_map (O(1) lookup)
          2. Embeds the cluster name and adds it to semantic_labels (so future
             similarity searches can find it)
          3. Logs a KG triple: raw_label assigned_to cluster_name
          4. Updates the in-RAM vector cache for this column

        The cluster name IS the raw label (title-cased and stripped).
        It maps to itself — a singleton cluster — until a human merges it.
        """
        cluster_name = cluster_name.strip()
        raw_lower    = raw_label.lower().strip()
        if not cluster_name or not raw_lower:
            return

        # 1. Write to cluster_map
        with self._lock, self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO cluster_map (col, raw, cluster) VALUES (?,?,?)",
                (col, raw_lower, cluster_name))
            c.execute(
                "INSERT OR REPLACE INTO cluster_map (col, raw, cluster) VALUES (?,?,?)",
                (col, cluster_name.lower(), cluster_name))
            c.execute(
                "INSERT OR REPLACE INTO cluster_map (col, raw, cluster) VALUES (?,?,?)",
                (col, cluster_name, cluster_name))
            # 3. KG triple
            c.execute(
                "INSERT OR REPLACE INTO kg_triples "
                "(col, subject, relation, object, weight) VALUES (?,?,?,?,?)",
                (col, raw_label, "assigned_to", cluster_name, 1.0))

        # Track for run report
        import datetime as _dt
        self._new_cluster_log.append({
            "col":          col,
            "cluster_name": cluster_name,
            "raw_label":    raw_label,
            "ts":           _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

        # 2. Embed + add to semantic_labels so is_cluster_name() returns True
        vecs = self._embed_batch([cluster_name], log_fn)
        if vecs is not None:
            with self._lock, self._conn() as c:
                c.execute(
                    "INSERT OR REPLACE INTO semantic_labels "
                    "(col, label, embedding, freq) VALUES (?,?,?,?)",
                    (col, cluster_name,
                     vecs[0].astype("float32").tobytes(), 1))
            # 4. Reload RAM cache so this cluster is immediately queryable
            self._load_cache(col, log_fn)
            log_fn(f"  [NEW CLUSTER] {col}: '{cluster_name}' registered "
                   f"(from '{raw_label}') → DB only")
        else:
            log_fn(f"  [NEW CLUSTER] {col}: '{cluster_name}' registered "
                   f"(embedding unavailable — cluster_map only)")

    def get_new_cluster_log(self) -> list:
        """Return all new clusters created during this run."""
        return list(self._new_cluster_log)

    def is_ready(self, col: str) -> bool:
        return self._cache_ok.get(col, False)

    #  Memory Store Awareness (Image 3: via System Prompt) 

    def memory_system_prompt(self, col: str) -> str:
        """
        Returns the agent system prompt describing its own memory stores.
        Cluster frequency across platforms is NOT injected  only the GSE
        experiment's own label context (via tool_gse_context) is the relevant
        frequency signal. Platform-wide frequency is irrelevant to a single
        experiment's decision.
        """
        stats = self.stats()
        n_ep  = stats.get("episodic", {}).get(col, 0)
        n_sem = stats.get("semantic", {}).get(col, 0)
        n_kg  = stats.get("kg_triples", 0)

        return (
            "=== MEMORY AWARE AGENT - SYSTEM INSTRUCTIONS ===\n"
            f"You are a biomedical metadata normalization agent for GEO field: {col}.\n"
            "Your ONLY job: map the extracted label to one approved CLUSTER NAME.\n\n"
            "CRITICAL RULE: Cluster names are human-approved canonical labels.\n"
            "  Output ONLY a cluster name returned by your tools  nothing else.\n"
            "  NEVER output a raw label, abbreviation, or free-form text.\n"
            "  NEVER invent or modify a cluster name.\n\n"
            "MEMORY STORE INVENTORY:\n"
            "  Cluster map  (Tier 0) : direct raw->cluster lookups from LLM_memory/ files\n"
            f"  Tier 2 - Semantic     : {n_sem:,} cluster names, vector-indexed\n"
            f"  Tier 3 - Episodic     : {n_ep:,} past resolutions logged with confidence\n"
            f"  Tier 4 - KG triples   : {n_kg:,} raw->cluster assignment records\n\n"
            "CONTEXT WINDOW SEGMENTS BELOW:\n"
            "  [ENTITY MEMORY]   - past resolutions for this exact extracted label\n"
            "  [WORKFLOW MEMORY] - cluster assignment triples from KG\n"
            "  [KNOWLEDGE BASE]  - top-k semantically similar CLUSTER NAMES (valid outputs)\n"
            "  [USER PROMPT]     - the extracted label + decision rules\n"
            "=== END SYSTEM INSTRUCTIONS ==="
        )

    #  Memory lifecycle reasoning (agent-triggered) 

    def should_log(self, col: str, raw: str, canonical: str,
                   collapse_rule: str) -> tuple:
        """
        Memory lifecycle reasoning: decide whether and with what confidence
        to write this resolution to episodic memory.
        Returns (should_log: bool, confidence: float, reason: str).
        Per Image 3: Memory lifecycle reasoning + agent-triggered memory operations.
        """
        # Never log identity mappings
        if raw == canonical:
            return False, 0.0, "identity"
        # Never log low-quality rules
        if collapse_rule in ("", "vocab_exact"):
            return False, 0.0, "no_change"
        # High confidence  episodic hit confirmed again
        if collapse_rule == "episodic":
            return True, 0.98, "episodic_confirmed"
        # KG match  well-structured synonym
        if collapse_rule.startswith("kg_"):
            return True, 0.95, "kg_verified"
        # Semantic + LLM  good but not certain
        if collapse_rule == "semantic_vocab":
            return True, 0.88, "semantic_llm"
        # Deterministic rules  reliable
        if collapse_rule in ("exact_match", "abbreviation"):
            return True, 0.92, "deterministic"
        return True, 0.80, "other"

    #  DB stats 

    def stats(self) -> dict:
        try:
            with self._conn() as c:
                sem  = {r[0]: r[1] for r in c.execute(
                    "SELECT col, COUNT(*) FROM semantic_labels GROUP BY col")}
                epi  = {r[0]: r[1] for r in c.execute(
                    "SELECT col, COUNT(*) FROM episodic_log GROUP BY col")}
                kg   = c.execute("SELECT COUNT(*) FROM kg_triples").fetchone()[0]
                cm   = {r[0]: r[1] for r in c.execute(
                    "SELECT col, COUNT(DISTINCT cluster) FROM cluster_map GROUP BY col")}
            return {"semantic": sem, "episodic": epi,
                    "kg_triples": kg, "clusters": cm}
        except Exception:
            return {}


#  Memory Aware Agent  context-window-segmented collapse prompt 
#
#  Per Image 1 (Context Engineering): context window is explicitly structured
#  into segments so the LLM knows exactly what memory type each section is.
#  Per Image 3 (Memory Aware Agent): agent sees its own memory store inventory
#  via the system prompt before reasoning about the label.

def prompt_semantic_collapse(col: str, extracted: str,
                              candidates: List[str],
                              episodic_hits: List[dict] = None,
                              kg_hits: List[tuple] = None,
                              system_prompt: str = "") -> str:
    """
    Fully segmented context window prompt for the Memory Aware Agent.

    Context window layout (per Image 1  Context Engineering):
      [SYSTEM INSTRUCTIONS]  memory store awareness + lifecycle rules
      [ENTITY MEMORY]        episodic hits (Tier 3)
      [WORKFLOW MEMORY]      knowledge graph triples (Tier 4)
      [KNOWLEDGE BASE]       semantic candidates (Tier 2)
      [USER PROMPT]          the label to normalize

    The agent sees its own memory inventory before reasoning, making it
    Memory Aware rather than just Memory Augmented (Image 3).
    """
    #  [SYSTEM INSTRUCTIONS] 
    sys_block = system_prompt if system_prompt else (
        f"You are a biomedical metadata normalization agent for GEO field: {col}. "
        f"Use the memory segments below in priority order (Entity  Workflow  KB)."
    )

    #  [ENTITY MEMORY]  Tier 3 episodic 
    if episodic_hits:
        ep_lines = []
        for h in episodic_hits[:3]:
            ep_lines.append(
                f"  canonical={h['canonical']}  "
                f"count={h['count']}  confidence={h['confidence']:.2f}  "
                f"last_seen={h.get('last_ts','?')[:10]}")
        ep_block = ("[ENTITY MEMORY - Tier 3 Episodic]\n"
                    "Past resolutions for this exact raw label:\n"
                    + "\n".join(ep_lines))
    else:
        ep_block = "[ENTITY MEMORY - Tier 3 Episodic]\nNo past resolutions found."

    #  [WORKFLOW MEMORY]  Tier 4 KG 
    if kg_hits:
        kg_lines = [f"  {subj} --{rel}--> {obj}  (weight={wgt:.2f})"
                    for subj, rel, obj, wgt in
                    [(extracted, r[1], r[0], r[2]) for r in kg_hits[:3]]]
        kg_block = ("[WORKFLOW MEMORY - Tier 4 Knowledge Graph]\n"
                    "Known synonym/variant triples:\n" + "\n".join(kg_lines))
    else:
        kg_block = "[WORKFLOW MEMORY - Tier 4 Knowledge Graph]\nNo KG triples found."

    #  [KNOWLEDGE BASE]  Tier 2 semantic hits 
    cand_lines = "\n".join(
        f"  {i+1}. {c}" for i, c in enumerate(candidates))
    kb_block = (f"[KNOWLEDGE BASE - Tier 2 Semantic Memory]\n"
                f"Top-k vector similarity candidates for \"{extracted}\":\n"
                f"{cand_lines}")

    #  [USER PROMPT] 
    user_block = (
        f"[USER PROMPT]\n"
        f"Normalize this extracted {col} label: {extracted!r}\n\n"
        "DECISION RULES (apply in order):\n"
        "  1. If Entity Memory has confident hits (count>=3, conf>=0.85)"
        " prefer that canonical.\n"
        "  2. If Workflow Memory has a high-weight triple apply it.\n"
        "  3. Pick the Knowledge Base candidate meaning EXACTLY the same"
        " biological entity. Abbreviations, plurals, added words like"
        " cell line / cells / tissue that do not change identity pick match.\n"
        "  4. If extracted label is MORE SPECIFIC than all candidates NO_MATCH.\n"
        "  5. If ambiguous or nothing fits NO_MATCH.\n\n"
        "Reply with ONLY the exact candidate label string, or NO_MATCH.\n"
        "Answer:"
    )
    return "\n\n".join([sys_block, ep_block, kg_block, kb_block, user_block])




CKPT_EVERY    = 1000        # checkpoint every N resolved NS samples
DEFAULT_MODEL = "gemma2:2b"   # single model for extraction + collapse — max GPU parallelism
DEFAULT_URL   = "http://localhost:11434"
NCBI_WORKERS  = 5
NCBI_DELAY    = 0.35

# colour palette
BG      = "#1e1e2e"; BG2 = "#2a2a3e"; BG3 = "#313145"
ACCENT  = "#7c5cbf"; ACCENT2 = "#5c9fd4"
SUCCESS = "#4caf76"; WARNING = "#e0a84a"; ERROR = "#e05c5c"
FG      = "#e0e0f0"; FG2 = "#a0a0c0"
MONO    = ("DejaVu Sans Mono", "Courier New", "monospace")


# 
#  GPU DETECTION  &  PARALLEL SLOT CALCULATION
# 
MODEL_RAM_GB = {
    #  Ollama model names 
    "gemma2:2b":                         2.0,
    "gemma2:2b-q4_0":                    1.8,
    "gemma2:9b":                         5.4,
    "gemma2:9b-q4_0":                    5.0,
    "gemma2:9b-q8_0":                    9.5,
    "gemma2:27b":                        18.0,
    "llama3:8b":                         5.5,
    "llama3.1:8b":                       5.5,
    "llama3:70b":                        48.0,
    "mistral:7b":                        4.8,
    "mistral:7b-q4_0":                   4.5,
    "qwen2.5:7b":                        4.4,
    #  GGUF filenames (llama-cpp-python backend) 
    "gemma-2-9b-it-q4_k_m.gguf":        5.4,
    "gemma-2-9b-it-q4_0.gguf":          5.0,
    "gemma-2-9b-it-q8_0.gguf":          9.5,
    "gemma-2-9b-it-q5_k_m.gguf":        6.5,
    "gemma-2-9b-it-q6_k.gguf":          7.8,
    "gemma-2-27b-it-q4_k_m.gguf":       17.5,
    "meta-llama-3.1-8b-instruct-q4_k_m.gguf": 5.5,
    "mistral-7b-instruct-v0.3-q4_k_m.gguf":   4.8,
    "qwen2.5-7b-instruct-q4_k_m.gguf":        4.4,
}
DEFAULT_MODEL_GB = 5.4   # assume Q4_K_M for unknowns


def detect_gpus():
    gpus = []
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=index,name,memory.total,memory.free",
             "--format=csv,noheader,nounits"],
            stderr=subprocess.DEVNULL, text=True, timeout=5)
        for line in out.strip().splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) == 4:
                gpus.append({"id": int(parts[0]), "name": parts[1],
                             "vram_gb":      round(int(parts[2]) / 1024, 1),
                             "free_vram_gb": round(int(parts[3]) / 1024, 1),
                             "type": "nvidia"})
    except Exception:
        pass
    if not gpus:
        try:
            out = subprocess.check_output(
                ["rocm-smi", "--showmeminfo", "vram", "--csv"],
                stderr=subprocess.DEVNULL, text=True, timeout=5)
            for i, line in enumerate(out.strip().splitlines()[1:]):
                parts = line.split(",")
                if len(parts) >= 2:
                    gpus.append({"id": i, "name": f"AMD GPU {i}",
                                 "vram_gb": round(int(parts[-1].strip()) / 1e6, 1),
                                 "free_vram_gb": 0, "type": "amd"})
        except Exception:
            pass
    return gpus


def compute_ollama_parallel(model: str,
                             reserve_gb: float = 4.0,
                             extra_vram_gb: float = 0.0) -> tuple:
    """
    Compute HYBRID worker count: GPU workers + CPU workers combined.

    Ollama routes requests to GPU first (fastest). When all GPU slots are
    busy it spills to CPU RAM for additional workers automatically 
    you just set OLLAMA_NUM_PARALLEL to the total combined count.

    Returns (total_workers, gpu_workers, cpu_workers) so the caller
    can log the breakdown clearly.

    Example  11GB GPU + 600GB RAM + 12 CPUs, gemma2:9b (5.4GB):
        gpu_workers  = int(10 / 5.4)       = 1   (10GB free after OS)
        ram_after    = 600 - 4 - 1*5.4     = 590.6 GB still free
        ram_slots    = int(590.6 / 5.4)    = 109  (RAM is not the limit)
        cpu_slots    = (12-2) // 2         = 5    (CPU threads are)
        cpu_workers  = min(109, 5)         = 5
        total        = 1 + 5               = 6 workers
                       (1 on GPU, 5 on CPU RAM)
    """
    try:
        gpus     = detect_gpus()
        # Accept both Ollama model name and GGUF filename
        model_key = os.path.basename(model).strip().lower()
        slot_gb   = MODEL_RAM_GB.get(model_key,
                    MODEL_RAM_GB.get(model.strip().lower(),
                    DEFAULT_MODEL_GB))
        free_gb  = psutil.virtual_memory().available / 1e9

        #  GPU workers 
        # Query Ollama /api/ps to see how much VRAM the loaded model actually uses.
        # This is more accurate than free_vram (which drops once model is loaded)
        # or total_vram (which ignores real OS overhead).
        # Strategy: total_vram - actual_model_vram_footprint - 1GB_OS_reserve
        #           = headroom available for KV cache of parallel slots.
        # KV cache per parallel slot  slot_gb * 0.15 (empirical for gemma2:9b)
        if gpus:
            total_vram  = sum(g["vram_gb"] for g in gpus)
            # Try to get actual model VRAM from Ollama
            try:
                import requests as _req
                ps = _req.get(f"{DEFAULT_URL}/api/ps", timeout=2).json()
                loaded_vram_gb = sum(
                    m.get("size_vram", 0) / 1e9
                    for m in ps.get("models", []))
            except Exception:
                loaded_vram_gb = slot_gb   # assume one copy loaded

            # KV cache per VRAM-resident slot (fits in GPU memory)
            kv_per_slot_vram = max(0.015, slot_gb * 0.01)
            os_reserve    = 1.0
            headroom      = total_vram - loaded_vram_gb - os_reserve - extra_vram_gb
            vram_slots    = max(1, int(headroom / kv_per_slot_vram))

            # Ollama spills excess parallel slots into system RAM.
            # Each spilled slot uses ~0.5 GB of RAM for KV cache — NOT 0.01.
            # Cap total workers so RAM-spilled slots stay within safe budget.
            kv_per_slot_ram = 0.5  # empirical: RAM-resident KV per slot
            ram_budget_gb   = min(free_gb * 0.40, 200.0)  # use at most 40% of free RAM
            ram_spill_slots = max(0, int(ram_budget_gb / kv_per_slot_ram))

            # Total = VRAM slots + RAM-spill slots, hard cap at 80
            gpu_workers = max(1, min(80, vram_slots + ram_spill_slots))
        else:
            gpu_workers = 0

        # CPU workers DISABLED — causes system freezing
        cpu_workers = 0

        total = max(1, gpu_workers)
        return total, gpu_workers, cpu_workers

    except Exception:
        return 1, 0, 1


def check_ollama_gpu(base_url=DEFAULT_URL):
    try:
        r = requests.get(f"{base_url}/api/ps", timeout=5)
        if r.status_code == 200:
            models = r.json().get("models", [])
            if models:
                vram  = models[0].get("size_vram", 0)
                total = models[0].get("size", 1)
                if vram > total * 0.5:
                    return "gpu", round(vram / 1e9, 1)
                return "cpu", 0
    except Exception:
        pass
    return "unknown", 0


def _get_vram_usage():
    try:
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=memory.used,memory.total",
             "--format=csv,noheader,nounits"],
            stderr=subprocess.DEVNULL, text=True, timeout=3)
        parts = [p.strip() for p in out.strip().splitlines()[0].split(",")]
        u, t  = int(parts[0]), int(parts[1])
        return u, t, 100.0 * u / t if t else 0.0
    except Exception:
        return 0, 0, 0.0


# ══════════════════════════════════════════════════════════════════════════════
#  OLLAMA SERVER MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════
def ollama_server_ok(base_url=DEFAULT_URL, timeout=3):
    try:
        return requests.get(f"{base_url}/api/tags", timeout=timeout).status_code == 200
    except Exception:
        return False

def ollama_binary_exists():
    return shutil.which("ollama") is not None

def model_available(model, base_url=DEFAULT_URL):
    try:
        r = requests.get(f"{base_url}/api/tags", timeout=5)
        if r.status_code == 200:
            names = [m.get("name", "") for m in r.json().get("models", [])]
            return any(model.split(":")[0] in n for n in names)
    except Exception:
        pass
    return False

def install_ollama_blocking(log_fn):
    os_name = _platform.system().lower()
    if os_name not in ("linux", "darwin"):
        log_fn("[ERROR] Auto-install supported only on Linux/macOS.")
        log_fn("  Download manually: https://ollama.com/download")
        return False
    log_fn(" Installing Ollama via official script ")
    proc = subprocess.Popen(
        "curl -fsSL https://ollama.com/install.sh | sh",
        shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in proc.stdout:
        log_fn("  " + line.rstrip())
    proc.wait()
    if proc.returncode != 0:
        log_fn("[ERROR] Install failed."); return False
    log_fn(" Ollama installed."); return True

def start_ollama_server_blocking(log_fn, num_parallel: int = 1):
    gpus = detect_gpus()
    if gpus:
        gpu_ids = ",".join(str(g["id"]) for g in gpus)
        names   = " + ".join(f"{g['name']} ({g['vram_gb']}GB)" for g in gpus)
        log_fn(f"  GPU(s): {names}")
    else:
        gpu_ids = "0"
        log_fn("  No GPU via nvidia-smi  attempting anyway")

    env = os.environ.copy()
    env["CUDA_VISIBLE_DEVICES"]     = gpu_ids
    env["OLLAMA_NUM_PARALLEL"]      = str(num_parallel)
    # Do NOT force OLLAMA_GPU_LAYERS — let Ollama decide how many layers
    # fit in VRAM. With 999 set, Ollama refuses to run when VRAM is full
    # instead of spilling overflow layers to CPU RAM.
    env["OLLAMA_FLASH_ATTENTION"]   = "1"
    env["OLLAMA_KEEP_ALIVE"]        = "5m"  # free VRAM after 5 min idle (was -1 = forever)
    env["OLLAMA_MAX_LOADED_MODELS"] = "1"   # one model at a time — swap between 2b and 9b
    env["HSA_OVERRIDE_GFX_VERSION"] = "10.3.0"

    log_fn(f" Starting Ollama  |  GPU={gpu_ids}  PARALLEL={num_parallel}  CPU-offload=auto ")
    if _platform.system().lower() == "windows":
        proc = subprocess.Popen(["ollama", "serve"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=env, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
    else:
        proc = subprocess.Popen(["ollama", "serve"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=env, preexec_fn=os.setsid)
    for i in range(40):
        time.sleep(1)
        if ollama_server_ok():
            log_fn(f" Ollama ready ({i+1}s) | {num_parallel} parallel slots")
            return proc
        if i % 5 == 4:
            log_fn(f"   waiting ({i+1}s)")
    log_fn("[ERROR] Server did not start in 40 s.")
    proc.terminate(); return None


CPU_OLLAMA_URL  = "http://localhost:11435"   # second instance — CPU only
_cpu_server_proc = None                       # global handle so we can kill it

def start_ollama_cpu_server(log_fn, num_parallel: int = 2) -> object:
    """
    Launch a second Ollama instance on port 11435 that uses CPU RAM only.
    CUDA_VISIBLE_DEVICES="" forces it off the GPU entirely.
    Workers that detect GPU saturation route new requests here.
    Returns the Popen handle or None on failure.
    """
    import platform as _plt
    env = os.environ.copy()
    env["CUDA_VISIBLE_DEVICES"]     = ""          # no GPU — pure CPU
    env["OLLAMA_HOST"]              = "0.0.0.0:11435"
    env["OLLAMA_NUM_PARALLEL"]      = str(num_parallel)
    env["OLLAMA_KEEP_ALIVE"]        = "5m"  # free RAM after 5 min idle
    env["OLLAMA_MAX_LOADED_MODELS"] = "1"
    env["OLLAMA_FLASH_ATTENTION"]   = "0"         # not useful on CPU
    # Give it its own model storage dir so it doesn't fight GPU instance
    env["OLLAMA_MODELS"]            = os.path.expanduser("~/.ollama/models")

    log_fn(f"  🖥️  Starting CPU Ollama on port 11435 ({num_parallel} workers) …")
    try:
        if _plt.system().lower() == "windows":
            proc = subprocess.Popen(
                ["ollama", "serve"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                env=env, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
        else:
            proc = subprocess.Popen(
                ["ollama", "serve"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                env=env, preexec_fn=os.setsid)

        for i in range(30):
            time.sleep(1)
            if ollama_server_ok(CPU_OLLAMA_URL):
                log_fn(f"  🖥️  CPU Ollama ready ({i+1}s) — port 11435")
                return proc
            if i % 5 == 4:
                log_fn(f"    waiting ({i+1}s) …")
        log_fn("  [WARN] CPU Ollama did not start in 30s — CPU swarm disabled")
        proc.terminate()
        return None
    except Exception as _e:
        log_fn(f"  [WARN] Could not start CPU Ollama: {_e} — CPU swarm disabled")
        return None


def vram_utilisation_pct() -> float:
    """Return current VRAM % used (0-100). Returns 0 if no GPU."""
    try:
        gpus = detect_gpus()
        if not gpus:
            return 0.0
        used = sum(g.get("used_vram_gb", 0) for g in gpus)
        total = sum(g.get("vram_gb", 1) for g in gpus)
        return 100.0 * used / total if total else 0.0
    except Exception:
        return 0.0


def _kill_ollama(log_fn=None):
    """
    Kill any running Ollama serve process using multiple strategies.
    Also prevents systemd from auto-restarting it so our instance wins.
    """
    def _log(m):
        if log_fn: log_fn(m)

    # Step 1: stop systemd service AND prevent auto-restart
    try:
        subprocess.run(["sudo", "-n", "systemctl", "stop", "ollama"],
                       capture_output=True, timeout=5)
    except Exception:
        pass

    # Step 2: kill ollama processes on the DEFAULT port only
    # Avoid killing Ollama instances on other ports (e.g. 11435 for parallel runs)
    import psutil as _ps
    for proc in _ps.process_iter(["pid", "name", "cmdline"]):
        try:
            cmd = " ".join(proc.info["cmdline"] or [])
            if "ollama" not in cmd:
                continue
            # Skip if this is an Ollama bound to a different port
            if "OLLAMA_HOST" in cmd and ":11434" not in cmd:
                continue
            # Kill runners spawned by any Ollama (they use random ports)
            # but only if no other Ollama on a non-default port owns them
            if "ollama runner" in cmd:
                parent = proc.parent()
                if parent:
                    pcmd = " ".join(parent.cmdline() or [])
                    if "OLLAMA_HOST" in pcmd and ":11434" not in pcmd:
                        continue
            for sig in ["TERM", "KILL"]:
                try:
                    proc.send_signal(getattr(signal, f"SIG{sig}"))
                    break
                except Exception:
                    pass
        except Exception:
            pass

    # Step 3: wait until port 11434 is actually free (up to 15s)
    import socket as _sock
    for i in range(15):
        try:
            s = _sock.create_connection(("127.0.0.1", 11434), timeout=0.5)
            s.close()
            time.sleep(1)   # still running
        except Exception:
            break            # port free
    return True

def _unload_all_models(base_url=DEFAULT_URL, log_fn=None):
    """Unload all models currently loaded in Ollama to free VRAM.
    Sends keep_alive=0 to each loaded model, causing immediate eviction."""
    try:
        ps = requests.get(f"{base_url}/api/ps", timeout=5).json()
        for m in ps.get("models", []):
            mname = m.get("name") or m.get("model", "")
            if not mname:
                continue
            try:
                requests.post(f"{base_url}/api/generate",
                              json={"model": mname, "keep_alive": 0},
                              timeout=10)
                if log_fn:
                    log_fn(f"  Unloaded {mname} from VRAM")
            except Exception:
                pass
        time.sleep(2)  # give Ollama time to release VRAM
    except Exception as _e:
        if log_fn:
            log_fn(f"  [WARN] Could not unload models: {_e}")


def pull_model_blocking(model, log_fn, progress_fn=None):
    log_fn(f" Pulling model '{model}' ")
    try:
        with requests.post(f"{DEFAULT_URL}/api/pull",
                           json={"name": model}, stream=True, timeout=3600) as r:
            r.raise_for_status(); last = ""
            for line in r.iter_lines():
                if not line: continue
                try: d = json.loads(line)
                except: continue
                status = d.get("status", "")
                total  = d.get("total", 0); done = d.get("completed", 0)
                if status != last:
                    log_fn(f"  {status}"); last = status
                if progress_fn and total and done:
                    progress_fn(int(100 * done / total))
        log_fn(f" Model '{model}' ready."); return True
    except Exception as exc:
        log_fn(f"[ERROR] Pull failed: {exc}"); return False


# ══════════════════════════════════════════════════════════════════════════════
#  NCBI GEO SCRAPER
# ══════════════════════════════════════════════════════════════════════════════
def _fetch_one_gse(gse, retries=3):
    # Use full view to get ALL GSE metadata
    url = (f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi"
           f"?acc={gse}&targ=self&form=text&view=full")
    with requests.Session() as session:
        for attempt in range(1, retries + 1):
            try:
                time.sleep(NCBI_DELAY)
                r = session.get(url, timeout=30,
                                headers={"User-Agent": "GEO-LabelRepairBot/2.0"})
                r.raise_for_status()
                text = r.text

                # Parse all Series fields
                def _collect(prefix):
                    lines = re.findall(rf"{prefix}\s*=\s*(.+)", text)
                    return " ".join(s.strip() for s in lines if s.strip())

                result = {
                    "gse":            gse,
                    "gse_title":      _collect("!Series_title"),
                    "gse_summary":    _collect("!Series_summary"),
                    "gse_design":     _collect("!Series_overall_design"),
                    "gse_type":       _collect("!Series_type"),
                    "gse_pubmed":     _collect("!Series_pubmed_id"),
                }

                # Only return if we got SOMETHING — don't cache empty results
                if result["gse_title"]:
                    return result

                # Title empty — try brief view as fallback
                r2 = session.get(url.replace("view=full", "view=brief"),
                                 timeout=30,
                                 headers={"User-Agent": "GEO-LabelRepairBot/2.0"})
                text2 = r2.text
                title_m = re.search(r"!Series_title\s*=\s*(.+)", text2)
                if title_m:
                    result["gse_title"] = title_m.group(1).strip()
                    result["gse_summary"] = " ".join(
                        re.findall(r"!Series_summary\s*=\s*(.+)", text2))
                    result["gse_design"] = " ".join(
                        re.findall(r"!Series_overall_design\s*=\s*(.+)", text2))

                return result

            except Exception:
                if attempt == retries:
                    # Return None on failure — caller will NOT cache empty results
                    return None
                time.sleep(1.5 * attempt)
    return None


def scrape_gse_meta(gse_ids, log_fn, progress_fn=None):
    cache = {}
    if os.path.exists(GSE_CACHE_FILE):
        try:
            with open(GSE_CACHE_FILE) as f: cache = json.load(f)
            log_fn(f"  Cache: {len(cache):,} GSEs stored.")
        except Exception: cache = {}
    # Re-fetch GSEs that are missing OR have empty titles (previous failed scrapes)
    need = [g for g in gse_ids if g not in cache or not cache[g].get("gse_title")]
    cached_ok = len(gse_ids) - len(need)
    if not need:
        log_fn(f"  All {len(gse_ids):,} GSEs in cache with data  no web requests.")
        return cache
    log_fn(f"  {cached_ok:,} cached with data, fetching {len(need):,} from NCBI GEO …")
    done = 0; fetched = 0
    with ThreadPoolExecutor(max_workers=NCBI_WORKERS) as pool:
        futures = {pool.submit(_fetch_one_gse, gse): gse for gse in need}
        for future in as_completed(futures):
            gse_id = futures[future]
            result = future.result()
            done += 1
            # Only cache successful results with data — never cache empty/failed
            if result and result.get("gse_title"):
                cache[result["gse"]] = result
                fetched += 1
            if progress_fn: progress_fn(int(100 * done / len(need)))
            if done % 50 == 0: log_fn(f"   {done}/{len(need)} fetched ({fetched} with data)")
    gse_set = set(gse_ids)
    titles  = sum(1 for g, v in cache.items() if g in gse_set and v.get("gse_title"))
    log_fn(f"   {titles:,}/{len(gse_ids):,} GSEs have titles")
    try:
        # Clean empty entries before saving
        clean_cache = {k: v for k, v in cache.items() if v.get("gse_title")}
        with open(GSE_CACHE_FILE, "w") as f: json.dump(clean_cache, f)
        log_fn(f"  Cache saved  {os.path.basename(GSE_CACHE_FILE)} "
               f"({len(clean_cache):,} entries with data)")
    except Exception as e: log_fn(f"  [WARN] Cache save failed: {e}")
    return cache


# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE  —  load GEOmetadb fully into RAM
# ══════════════════════════════════════════════════════════════════════════════
def load_db_to_memory(db_path: str, log_fn=print) -> sqlite3.Connection:
    """
    Load GEOmetadb into RAM using streaming decompression.
    Writes to a temp file in chunks (never holds entire DB in Python memory)
    then uses sqlite3.backup() which streams page-by-page.
    Temp file is deleted immediately after backup completes.
    """
    import tempfile, shutil
    mem_conn = sqlite3.connect(":memory:", check_same_thread=False)
    mem_conn.row_factory = sqlite3.Row

    if db_path.endswith(".gz"):
        gz_mb = os.path.getsize(db_path) / 1e6
        log_fn(f" Decompressing {os.path.basename(db_path)} ({gz_mb:.0f} MB) ")
        # Write to temp file in 64MB chunks — never hold entire DB in RAM
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".sqlite")
        try:
            with gzip.open(db_path, "rb") as gz_in,                  os.fdopen(tmp_fd, "wb") as tmp_out:
                shutil.copyfileobj(gz_in, tmp_out, length=64 * 1024 * 1024)
            # Backup page-by-page into memory
            disk_conn = sqlite3.connect(tmp_path)
            disk_conn.backup(mem_conn)
            disk_conn.close()
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    else:
        log_fn(f" Loading {os.path.basename(db_path)} into RAM ")
        disk_conn = sqlite3.connect(db_path)
        disk_conn.backup(mem_conn)
        disk_conn.close()

    try:
        ram_mb = psutil.Process().memory_info().rss / 1e6
        log_fn(f" GEOmetadb in RAM (process RSS: {ram_mb:.0f} MB)")
    except Exception:
        log_fn(f" GEOmetadb in RAM")
    return mem_conn


def fetch_gsm_raw(conn, gsm_ids: List[str]) -> Dict[str, dict]:
    out = {}
    for i in range(0, len(gsm_ids), BATCH_SIZE):
        chunk = gsm_ids[i:i + BATCH_SIZE]
        ph    = ",".join("?" * len(chunk))
        q     = f"""SELECT gsm,
                           title                  AS gsm_title,
                           source_name_ch1        AS source_name,
                           characteristics_ch1    AS characteristics,
                           treatment_protocol_ch1 AS treatment_protocol,
                           description,
                           organism_ch1           AS organism
                    FROM gsm WHERE gsm IN ({ph})"""
        df = pd.read_sql_query(q, conn, params=chunk)
        for _, row in df.iterrows():
            out[row["gsm"]] = {
                k: (str(v).strip() if v and str(v) not in ("nan", "None") else "")
                for k, v in row.items()
            }
    return out


# ── NCBI GSM scraper — fallback when GEOmetadb has no record ─────────────────
GSM_CACHE_FILE = os.path.join(SCRIPT_DIR, ".gsm_raw_cache.json")

def _fetch_one_gsm(gsm: str, retries: int = 3) -> dict:
    """
    Scrape a single GSM record from the NCBI GEO soft text endpoint.
    Parses: title, source_name, characteristics, organism, description.
    Returns a dict with a '_error' key if all attempts failed.
    """
    url = (f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi"
           f"?acc={gsm}&targ=self&form=text&view=brief")
    last_err = ""
    for attempt in range(1, retries + 1):
        try:
            time.sleep(NCBI_DELAY)
            r = requests.get(url, timeout=30,
                             headers={"User-Agent": "GEO-LabelRepairBot/2.0"})
            r.raise_for_status()
            text = r.text

            def _val(pattern):
                m = re.search(pattern, text, re.MULTILINE)
                return m.group(1).strip() if m else ""

            chars = re.findall(r"!Sample_characteristics_ch1\s*=\s*(.+)", text)
            chars_str = "; ".join(c.strip() for c in chars if c.strip())

            return {
                "gsm":               gsm,
                "gsm_title":         _val(r"!Sample_title\s*=\s*(.+)"),
                "source_name":       _val(r"!Sample_source_name_ch1\s*=\s*(.+)"),
                "characteristics":   chars_str,
                "treatment_protocol":_val(r"!Sample_treatment_protocol_ch1\s*=\s*(.+)"),
                "description":       _val(r"!Sample_description\s*=\s*(.+)"),
                "organism":          _val(r"!Sample_organism_ch1\s*=\s*(.+)"),
            }
        except Exception as e:
            last_err = str(e)
            if attempt < retries:
                time.sleep(1.5 * attempt)

    # All retries exhausted — return empty with error marker
    return {"gsm": gsm, "gsm_title": "", "source_name": "",
            "characteristics": "", "treatment_protocol": "",
            "description": "", "organism": "", "_error": last_err}


def scrape_gsm_raw(missing_gsms: List[str], log_fn,
                   progress_fn=None) -> Dict[str, dict]:
    """
    Fetch raw metadata for GSMs not found in GEOmetadb.
    Results are cached to GSM_CACHE_FILE so re-runs skip already-scraped GSMs.
    """
    # Load disk cache
    cache: Dict[str, dict] = {}
    if os.path.exists(GSM_CACHE_FILE):
        try:
            with open(GSM_CACHE_FILE) as f:
                cache = json.load(f)
            log_fn(f"  GSM cache: {len(cache):,} records already stored.")
        except Exception:
            cache = {}

    need = [g for g in missing_gsms if g not in cache]
    if not need:
        log_fn(f"  All {len(missing_gsms):,} missing GSMs found in cache.")
        return {g: cache[g] for g in missing_gsms if g in cache}

    log_fn(f"  Scraping {len(need):,} GSMs from NCBI GEO "
           f"(not in GEOmetadb) ")
    done = 0
    n_errors = 0
    first_error = ""
    with ThreadPoolExecutor(max_workers=NCBI_WORKERS) as pool:
        futures = {pool.submit(_fetch_one_gsm, gsm): gsm for gsm in need}
        for future in as_completed(futures):
            rec  = future.result()
            gsm  = rec["gsm"]
            if rec.get("_error"):
                n_errors += 1
                if not first_error:
                    first_error = rec["_error"]
            cache[gsm] = {k: v for k, v in rec.items() if k != "_error"}
            done += 1
            if progress_fn:
                progress_fn(int(100 * done / len(need)))
            if done % 100 == 0 or done == len(need):
                log_fn(f"   {done:,}/{len(need):,} scraped"
                       f"  errors:{n_errors:,}")

    if n_errors > 0:
        log_fn(f"  [WARN] {n_errors:,}/{len(need):,} NCBI requests failed.")
        log_fn(f"  [WARN] First error: {first_error}")
        log_fn(f"  [WARN] Check network access to www.ncbi.nlm.nih.gov")

    # Persist cache
    try:
        with open(GSM_CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
        log_fn(f"  GSM cache saved  {os.path.basename(GSM_CACHE_FILE)}")
    except Exception as e:
        log_fn(f"  [WARN] GSM cache save failed: {e}")

    # Return only the requested GSMs
    out = {}
    for g in missing_gsms:
        if g in cache:
            out[g] = cache[g]
    has_text = sum(
        1 for v in out.values()
        if any(v.get(k) for k in ("gsm_title","source_name","characteristics"))
    )
    log_fn(f"   {has_text:,}/{len(out):,} scraped GSMs have usable text"
           f"  ({len(out)-has_text:,} still empty after scraping)")
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  INPUT LOADER
# ══════════════════════════════════════════════════════════════════════════════
def load_platform(gpl, base_dir=None):
    d  = base_dir or SCRIPT_DIR
    tp = os.path.join(d, f"matrix_tissue_{gpl}.csv")
    cp = os.path.join(d, f"matrix_condition_annotated_{gpl}.csv.gz")
    if not os.path.exists(tp) or not os.path.exists(cp): return pd.DataFrame()
    tissue = pd.read_csv(tp, dtype=str)
    with gzip.open(cp, "rt") as f: cond = pd.read_csv(f, dtype=str)
    # Normalise gsm column name to lowercase in both files
    tissue.columns = [c.lower() if c.lower() == "gsm" else c for c in tissue.columns]
    cond.columns   = [c.lower() if c.lower() == "gsm" else c for c in cond.columns]
    tissue["Tissue"] = tissue["Tissue"].fillna(NS).replace("NOT SPECIFIED", NS).str.strip()
    merged = cond.merge(tissue[["gsm", "Tissue"]], on="gsm", how="left")
    merged["Tissue"] = merged["Tissue"].fillna(NS)
    for col in LABEL_COLS:
        if col in merged.columns:
            merged[col] = merged[col].fillna(NS).astype(str).str.strip()
        else:
            merged[col] = NS
    # Ensure gsm column is lowercase
    if "GSM" in merged.columns and "gsm" not in merged.columns:
        merged = merged.rename(columns={"GSM": "gsm"})
    return merged

def load_all(base_dir=None):
    dfs = {}
    for gpl in ALL_GPLS:
        df = load_platform(gpl, base_dir)
        if not df.empty: dfs[gpl] = df
    return dfs


def load_gsm_list(gsm_file: str, platform_id: str = "CUSTOM") -> pd.DataFrame:
    """
    Build a synthetic target DataFrame from a plain text file or CSV of GSM IDs.
    Every sample gets Tissue=NS and Condition=NS.

    Accepted formats:
      - Plain text: one GSM ID per line  (GSM123456 or bare 123456)
      - CSV/TSV: any file with a column named "gsm" or "GSM" (case-insensitive)
    """
    gsms = []
    try:
        # Try CSV first — look for a gsm column (case-insensitive)
        if gsm_file.lower().endswith((".csv", ".tsv", ".txt")):
            try:
                sep = "\t" if gsm_file.lower().endswith(".tsv") else ","
                df_in = pd.read_csv(gsm_file, dtype=str, sep=sep)
                # Find gsm column case-insensitively
                col_map = {c.lower(): c for c in df_in.columns}
                if "gsm" in col_map:
                    gsm_col = col_map["gsm"]
                    gsms = df_in[gsm_col].dropna().str.strip().tolist()
                    # Ensure GSM prefix
                    gsms = [g if g.upper().startswith("GSM") else "GSM"+g
                            for g in gsms if g]
                    gsms = [g.upper() for g in gsms]
            except Exception:
                pass   # fall through to line-by-line parsing

        if not gsms:
            # Plain text — one GSM per line
            with open(gsm_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    token = line.split()[0].strip()
                    if not token.upper().startswith("GSM"):
                        token = "GSM" + token
                    gsms.append(token.upper())
    except Exception as e:
        return pd.DataFrame()

    if not gsms:
        return pd.DataFrame()

    # load_gsm_list is only called in scratch mode — use LABEL_COLS_SCRATCH
    # so Treatment column exists in target and gets picked up by repair_one
    row = {"gsm": gsms, "series_id": "", "platform": platform_id}
    for col in LABEL_COLS_SCRATCH:
        row[col] = NS
    return pd.DataFrame(row)


# ══════════════════════════════════════════════════════════════════════════════
#  PLATFORM DISCOVERY  —  query GEOmetadb for platforms by species
# ══════════════════════════════════════════════════════════════════════════════

def discover_platforms(conn: sqlite3.Connection, species: str,
                       min_samples: int = MIN_SAMPLES_DEFAULT,
                       tech_mode: str = "Expression Microarray",
                       log_fn=print) -> list:
    """
    Query GEOmetadb for platforms of a given species, filtered by technology.
    tech_mode: key from TECHNOLOGY_FILTERS dict.
    Returns list of dicts: [{gpl, title, technology, sample_count}, ...]
    sorted by sample_count descending.
    """
    tf = TECHNOLOGY_FILTERS.get(tech_mode, TECHNOLOGY_FILTERS["Expression Microarray"])

    # Build WHERE clauses based on technology filter
    where_parts = ["g.organism = ?"]

    # Exclude non-expression keywords (SNP, methylation, etc.)
    if tf.get("exclude_nonexpr"):
        for kw in _PLATFORM_EXCLUDE_KEYWORDS:
            where_parts.append(f"g.title NOT LIKE '{kw}'")

    # Exclude sequencing platforms
    if tf.get("exclude_seq"):
        for kw in _SEQ_KEYWORDS:
            where_parts.append(f"g.title NOT LIKE '{kw}'")

    # Filter by specific technology column value
    if tf.get("tech_filter"):
        where_parts.append(f"g.technology = '{tf['tech_filter']}'")

    # Require specific title keywords (e.g. methylation, miRNA)
    if tf.get("title_require"):
        req_clauses = " OR ".join(f"g.title LIKE '{kw}'"
                                   for kw in tf["title_require"])
        where_parts.append(f"({req_clauses})")

    where_sql = " AND ".join(where_parts)

    # Primary query: platforms with entries in the gpl table
    sql_primary = f"""
        SELECT g.gpl, g.title, g.technology,
               COUNT(s.gsm) AS sample_count
        FROM gpl g
        JOIN gsm s ON s.gpl = g.gpl
        WHERE {where_sql}
        GROUP BY g.gpl
        HAVING sample_count >= ?
        ORDER BY sample_count DESC
    """

    # Fallback: platforms in gsm but missing from gpl table
    sql_fallback = f"""
        SELECT s.gpl, '(title unavailable)' AS title,
               '' AS technology,
               COUNT(s.gsm) AS sample_count
        FROM gsm s
        LEFT JOIN gpl g ON g.gpl = s.gpl
        WHERE g.gpl IS NULL
          AND s.organism_ch1 = ?
        GROUP BY s.gpl
        HAVING sample_count >= ?
        ORDER BY sample_count DESC
    """

    results = []
    try:
        cur = conn.cursor()
        # Primary
        cur.execute(sql_primary, (species, min_samples))
        for row in cur.fetchall():
            results.append({
                "gpl":          row[0],
                "title":        row[1] or "",
                "technology":   row[2] or "",
                "sample_count": row[3],
            })
        # Fallback for platforms missing from gpl table (only in "All" mode)
        if not tf.get("tech_filter"):
            cur.execute(sql_fallback, (species, min_samples))
            seen = {r["gpl"] for r in results}
            for row in cur.fetchall():
                if row[0] not in seen:
                    results.append({
                        "gpl":          row[0],
                        "title":        row[1] or "",
                        "technology":   row[2] or "",
                        "sample_count": row[3],
                    })
    except Exception as e:
        log_fn(f"[WARN] Platform discovery query failed: {e}")

    # Sort by sample count descending
    results.sort(key=lambda r: r["sample_count"], reverse=True)
    total = sum(r["sample_count"] for r in results)
    log_fn(f"  Discovered {len(results)} {species} [{tech_mode}] platforms "
           f"({total:,} total samples, min {min_samples} samples/platform)")
    return results


def load_platform_from_db(gpl: str, conn: sqlite3.Connection,
                          log_fn=print) -> pd.DataFrame:
    """
    Build a target DataFrame for a platform directly from GEOmetadb.
    Used when no harmonized CSV files exist for this platform.
    All labels are set to NS — full annotation from scratch.
    """
    try:
        df = pd.read_sql_query(
            "SELECT gsm, series_id, gpl AS platform FROM gsm WHERE gpl = ?",
            conn, params=[gpl])
    except Exception as e:
        log_fn(f"[ERROR] Could not load platform {gpl} from GEOmetadb: {e}")
        return pd.DataFrame()
    if df.empty:
        return df
    # Normalise column names
    df.columns = [c.lower() for c in df.columns]
    # Set all labels to NS
    for col in LABEL_COLS_SCRATCH:
        df[col] = NS
    log_fn(f"  {gpl}: {len(df):,} GSMs loaded from GEOmetadb (all NS)")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  GSEContext  —  MemGPT-style rolling memory for one experiment
# ══════════════════════════════════════════════════════════════════════════════
class GSEContext:
    """
    Complete memory block for one GEO experiment (GSE).

    Seeded once at startup from the full platform DataFrame (all samples,
    labeled + NS).  Updated live as the GSEWorker resolves NS samples, so
    every subsequent sample in the same experiment sees the freshly assigned
    labels  exactly like MemGPT's rolling external memory.

    Thread-safe via a per-instance lock (one worker per GSE means the lock
    is only contested in the rare case of parallel workers on the same GSE,
    which never happens by design  but lock is kept for safety).
    """

    def __init__(self, gse_id: str):
        self.gse_id        = gse_id
        self.title         = ""
        self.summary       = ""
        self.design        = ""
        self._samples      : List[Dict] = []
        # Use full scratch cols  superset of repair cols, harmless extra key
        self.label_counts  : Dict[str, Counter] = {c: Counter() for c in LABEL_COLS_SCRATCH}
        self._ns_count     : Dict[str, int]     = {c: 0          for c in LABEL_COLS_SCRATCH}
        self.total         = 0
        self._lock         = threading.Lock()

    #  Population 
    def add_sample(self, gsm: str, labels: Dict[str, str],
                   mem_agent=None):
        """
        Load one sample into context.
        If mem_agent provided, normalise label casing via cluster_lookup 
        so ALL CAPS platform labels become correctly-cased cluster names
        in the sibling label pool shown to the agent.
        """
        rec = {"gsm": gsm}
        for col in self.label_counts:  # covers all cols including Treatment
            val = labels.get(col, NS)
            # Normalise casing: "LIVER"  "Liver", "ALZHEIMER DISEASE"  "Alzheimer Disease"
            if val != NS and mem_agent is not None:
                cased = mem_agent.cluster_lookup(col, val)
                if cased:
                    val = cased
            rec[col] = val
            if val != NS:
                self.label_counts[col][val] += 1
            else:
                self._ns_count[col] += 1
        self._samples.append(rec)
        self.total += 1

    def set_meta(self, title: str, summary: str, design: str = ""):
        self.title   = (title   or "").strip()
        self.summary = (summary or "").strip()
        self.design  = (design  or "").strip()

    #  Live update (called after each NS resolved) 
    def update_label(self, gsm: str, col: str, new_val: str):
        """
        No-op  label_counts is a STATIC snapshot of all pre-existing labels
        loaded from the CSV at startup via add_sample().
        All 300 labels (or however many exist in this GSE) are visible to every
        NS sample from the very first call. We do NOT grow the pool incrementally
        as NS samples are resolved  that would give sample 1 fewer siblings
        than sample 40, which is wrong. The full experiment label vocabulary
        is known at load time and never changes during a run.
        """
        pass  # intentional no-op  static label context

    #  Accessors 
    def labeled_count(self, col: str) -> int:
        return sum(self.label_counts[col].values())

    def diverse_examples(self, col: str, n: int = 5) -> List[Dict]:
        """Return up to n examples covering distinct labels (for prompt diversity)."""
        seen, examples = set(), []
        for s in self._samples:
            v = s.get(col, NS)
            if v != NS and v not in seen:
                examples.append(s); seen.add(v)
            if len(examples) >= n: break
        return examples

    #  Context block injected into every LLM prompt 
    def context_block(self, col: str) -> str:
        lc  = self.labeled_count(col)
        ns  = self._ns_count[col]

        lines = []
        if self.title:
            lines.append(f"Experiment title  : {self.title}")
        if self.summary:
            lines.append(f"Experiment summary: {self.summary}")
        if self.design:
            lines.append(f"Overall design    : {self.design}")
        lines.append(
            f"Total samples    : {self.total}"
            f"  |  Labeled {col}: {lc}"
            f"  |  Still NS     : {ns}"
        )

        #  Label distribution 
        if self.label_counts[col]:
            lines.append(f"\nKnown {col} labels in this experiment:")
            for label, count in self.label_counts[col].most_common():
                lines.append(f"  [{count:>4}]  {label}")
        else:
            lines.append(f"\nNo {col} labels assigned yet in this experiment.")

        #  Diverse labeled examples 
        examples = self.diverse_examples(col, n=5)
        if examples:
            other = [c for c in LABEL_COLS if c != col][0]
            lines.append(f"\nExample labeled samples (one per unique label):")
            for ex in examples:
                lines.append(
                    f"  {ex['gsm']}    {col}: {ex.get(col, NS)}"
                    f"  |  {other}: {ex.get(other, NS)}"
                )
        return "\n".join(lines)


# 
#  PROMPT BUILDERS    short, direct, gemma2:9b-friendly
#  Rule: keep total prompt under ~600 tokens so num_ctx=2048 is always safe
# 
def _sanitize(text, max_chars: int = 400) -> str:
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', ' ', str(text or ""))
    return text.replace('\r', ' ').strip()[:max_chars]



EXTRACTION_MODEL   = "gemma4:e2b"   # gemma4 edge model — better reasoning, think=false for speed


def format_sample_for_extraction(raw: dict) -> str:
    """
    Format raw GEO metadata as structured compact input for gemma2:2b.
    Matches the IN:/OUT: examples in _EXTRACTION_SYSTEM_PROMPT exactly.
    """
    def _s(v):
        return str(v).strip().replace("\t", " ").replace("\n", " ") if v else ""
    title  = _s(raw.get("gsm_title",       ""))[:80]
    source = _s(raw.get("source_name",      ""))[:60]
    char   = _s(raw.get("characteristics",  ""))[:250]
    treat  = _s(raw.get("treatment_protocol",""))[:80]
    parts  = []
    if title:  parts.append(f"title:{title}")
    if source: parts.append(f"source:{source}")
    if char:   parts.append(f"char:{char}")
    if treat and treat.lower() not in ("none", "n/a", ""):
        parts.append(f"lab_protocol:{treat}")
    return "{" + ", ".join(parts) + "}" if parts else "(no metadata)"


def format_raw_block(raw: dict) -> str:
    fields = [
        ("Title",           raw.get("gsm_title", "")),
        ("Source",          raw.get("source_name", "")),
        ("Characteristics", raw.get("characteristics", "")),
        ("Lab Protocol",    raw.get("treatment_protocol", "")),
        ("Description",     raw.get("description", "")),
    ]
    lines = []
    for label, val in fields:
        val = _sanitize(val)
        if val: lines.append(f"{label}: {val}")
    return "\n".join(lines) or "(no metadata available)"


def _task_prompt(col: str) -> str:
    """Shared task instruction for extraction prompts."""
    if col == "Tissue":
        return (
            "What tissue, organ, cell type, or cell line is this sample from?\n"
            "Answer with the tissue name only (e.g. brain, liver, PBMC, MCF-7, "
            "CD4+ T cells).\n"
            "If not mentioned or not clear: Not Specified"
        )
    else:
        return (
            "What disease, condition, or phenotype does this sample represent?\n"
            "Answer with the condition name only (e.g. Alzheimer Disease, "
            "breast cancer, Control, Normal, HSV-1 infection).\n"
            "Do NOT substitute one disease for another.\n"
            "If not mentioned or not clear: Not Specified"
        )


def prompt_extract_raw(gsm: str, col: str, raw: dict) -> str:
    """
    Step 1a  extract purely from this sample's own raw metadata.
    No GSE context, no sibling labels, no cluster hints.
    The sample must speak for itself first.
    Fields: title, source_name, characteristics, treatment_protocol, description.
    """
    raw_block = format_raw_block(raw)
    return (
        f"Sample {gsm}:\n{raw_block}\n\n"
        f"{_task_prompt(col)}\n\n"
        f"{col}:"
    )


def prompt_extract_with_gse(gsm: str, col: str, raw: dict,
                             ctx: GSEContext,
                             mem_agent: "MemoryAgent" = None) -> str:
    """
    Step 1b  extraction fallback when raw-only returned NS.
    Now brings in:
      - Full GSE description (title + summary + design from NCBI)
      - Sibling label counts from the same experiment
      - Sibling labels from the same experiment (already canonical cluster names)
    The raw block is still included so the LLM has all available evidence.
    """
    raw_block = format_raw_block(raw)

    # Full GSE experiment description from NCBI
    gse_hint = ""
    if ctx.title:
        gse_hint += f"Experiment title  : {ctx.title}\n"
    if getattr(ctx, "summary", ""):
        gse_hint += f"Experiment summary: {ctx.summary[:400]}\n"
    if getattr(ctx, "design", ""):
        gse_hint += f"Overall design    : {ctx.design[:200]}\n"
    if gse_hint:
        gse_hint += "\n"

    # Sibling label counts — other samples in this GSE already labeled
    gse_label_hint = ""
    if ctx.label_counts[col]:
        lines = []
        for lbl, cnt in ctx.label_counts[col].most_common():
            lines.append(f"  {lbl} ({cnt} sample{'s' if cnt > 1 else ''})")
        gse_label_hint = (
            f"Other samples in this experiment are labeled as:\n"
            + "\n".join(lines) + "\n\n"
        )

    # Sibling labels from this GSE ARE the cluster examples — they are
    # already canonical cluster names from the same experiment. Adding
    # random examples from the full vocabulary would be noise.

    return (
        f"{gse_hint}"
        f"{gse_label_hint}"
        f"Sample {gsm}:\n{raw_block}\n\n"
        f"{_task_prompt(col)}\n\n"
        f"{col}:"
    )


def prompt_extract(gsm: str, col: str, raw: dict, ctx: GSEContext,
                   mem_agent: "MemoryAgent" = None) -> str:
    """Legacy wrapper  kept for any callers that use the old signature."""
    return prompt_extract_with_gse(gsm, col, raw, ctx, mem_agent=mem_agent)


def prompt_extract_combined(gsm: str, raw: dict, ctx: GSEContext,
                              ns_cols: List[str],
                              gse_block: str = "") -> str:
    """
    Combined Step 1 prompt  extracts Tissue AND Condition in one LLM call.
    The GSE description (gse_block, pre-built once) and GSM metadata are
    shown once, not repeated per field.
    Sibling labels for EACH field are shown separately under their own heading.
    Returns a prompt whose answer is parsed into {col: value} by parse_combined().
    """
    raw_block = format_raw_block(raw)

    # GSE description — use pre-built block from worker __init__, never rebuild
    gse_hint = gse_block  # already has trailing newlines

    # Sibling labels per field — static snapshot, loaded at startup for this GSE
    sibling_block = ""
    for col in ns_cols:
        if ctx.label_counts[col]:
            lines = [f"  {lbl} ({cnt}x)"
                     for lbl, cnt in ctx.label_counts[col].most_common()]
            sibling_block += (
                f"{col} labels in this experiment:\n"
                + "\n".join(lines) + "\n\n"
            )

    # Answer format instructions
    answer_fmt = "\n".join(
        f"{col}: <value or Not Specified>"
        for col in ns_cols
    )

    return (
        f"{gse_hint}"
        f"{sibling_block}"
        f"Sample {gsm}:\n{raw_block}\n\n"
        f"Extract the following fields from this sample.\n"
        f"Use the sibling labels above as your vocabulary  match their exact "
        f"phrasing when appropriate.\n"
        f"If a field cannot be determined: Not Specified\n\n"
        f"{answer_fmt}"
    )


def parse_combined(text: str, ns_cols: List[str]) -> Dict[str, str]:
    """
    Parse the combined extraction response into {col: value}.
    Expects lines like:  Tissue: Brain   or   Condition: Not Specified
    Falls back to NS for any field not found.
    """
    result = {col: NS for col in ns_cols}
    for line in text.splitlines():
        line = line.strip()
        for col in ns_cols:
            prefix = f"{col}:"
            if line.lower().startswith(prefix.lower()):
                val = line[len(prefix):].strip().strip('"').strip("'")
                val = re.sub(r"^(tissue|condition|treatment)\s*:\s*", "", val,
                             flags=re.IGNORECASE).strip()
                if val:
                    result[col] = val
                break
    return result


def clean_output(text: str) -> str:
    text = re.sub(r"```[a-z]*", "", text).strip().strip("`").strip('"').strip("'")
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return ""
    out = lines[0]
    # gemma2:9b sometimes echoes the field name prefix back, e.g. "Tissue: brain"
    # or "Condition: Alzheimer Disease" — strip it
    for prefix in ("tissue:", "condition:", "treatment:",
                   "tissue :", "condition :", "treatment :"):
        if out.lower().startswith(prefix):
            out = out[len(prefix):].strip()
            break
    # Strip surrounding quotes the model sometimes adds
    out = out.strip('"').strip("'").strip()
    return out
def is_ns(text: str) -> bool:
    return text.lower().strip() in {
        "not specified", "n/a", "none", "unknown", "na",
        "not available", "not applicable", "unclear", "unspecified",
        "missing", "undetermined", "insufficient", "insufficient information",
        "no information", "no data", ""
    }


# 
#  PHASE 1.5    Deterministic GSE-scoped label collapsing
#
#  Scope  : within one GSE only (existing labels from GSEContext)
#  Rule 1 : exact match after case + punctuation normalisation
#           "Alzheimer Disease" == "alzheimer-disease" 
#  Rule 2 : abbreviation / initials match
#           "AD"  == initials("Alzheimer Disease") 
#           "AML" == initials("Acute Myeloid Leukemia") 
#  Guard  : if BOTH labels contain digit sequences and those sequences differ
#            block the merge  (Mut12  Mut10, Gm19141  Gm19144)
#  REMOVED: substring, fuzzy/overlap, synonym dicts  (caused HSVHIV)
#  Fallback: no match found  keep raw extracted label unchanged
# 

def _norm(text: str) -> str:
    """Lowercase, replace non-alphanumeric with space, collapse whitespace."""
    t = text.lower()
    t = re.sub(r'[^a-z0-9]', ' ', t)
    return re.sub(r'\s+', ' ', t).strip()

def _compact(text: str) -> str:
    """_norm without spaces  used for abbreviation comparison."""
    return _norm(text).replace(' ', '')

def _initials(text: str) -> str:
    """First character of every word in the normalised form."""
    return ''.join(w[0] for w in _norm(text).split() if w)

def _numbers(text: str) -> List[str]:
    """All digit sequences extracted from raw text."""
    return re.findall(r'\d+', text)

def _numeric_guard_ok(a: str, b: str) -> bool:
    """
    True = safe to compare (merge allowed from numeric perspective).
    Blocked only when BOTH labels carry digit sequences that differ.
    """
    na, nb = _numbers(a), _numbers(b)
    if not na or not nb:
        return True          # at least one has no numbers  not discriminated by numbers
    return sorted(na) == sorted(nb)

def phase15_collapse(extracted: str,
                     ctx_labels: List[str]) -> tuple:
    """
    Apply Phase 1.5 deterministic rules against the labels already seen in
    this GSE.  Returns (matched_existing_label, rule_name) or (None, None).

    Only collapses when there is positive GSE evidence  a label from another
    GSM in the same experiment that matches by the rules below.
    If ctx_labels is empty  (None, None) always (no context to collapse into).
    """
    if not extracted or not ctx_labels:
        return None, None

    compact_e  = _compact(extracted)
    initials_e = _initials(extracted)

    for existing in ctx_labels:
        if not existing or is_ns(existing):
            continue

        #  Numeric guard 
        if not _numeric_guard_ok(extracted, existing):
            continue          # different numbers  definitely different things

        compact_x  = _compact(existing)
        initials_x = _initials(existing)

        #  Rule 1 : exact match (normalised) 
        if compact_e == compact_x:
            return existing, "exact_match"

        #  Rule 2a : extracted is the abbreviation of existing 
        # e.g. extracted="AD", existing="Alzheimer Disease"
        # initials("Alzheimer Disease") = "ad" == compact("ad") = "ad" 
        if (len(compact_e) <= 6                    # short  looks like abbreviation
                and len(compact_e) >= 2            # ignore single letters
                and len(compact_x) > len(compact_e)# existing is the longer form
                and compact_e == initials_x        # extracted equals initials of existing
                and len(initials_x) >= 2):
            return existing, "abbreviation"

        #  Rule 2b : existing is the abbreviation of extracted 
        # e.g. extracted="Alzheimer Disease", existing="AD"
        if (len(compact_x) <= 6
                and len(compact_x) >= 2
                and len(compact_e) > len(compact_x)
                and compact_x == initials_e
                and len(initials_e) >= 2):
            return existing, "abbreviation"

    return None, None


# 
#  PERSISTENT HTTP SESSION  (one per thread  avoids TCP handshake per call)
# 
_tls = threading.local()

def _get_session():
    if not hasattr(_tls, "s") or _tls.s is None:
        _tls.s = requests.Session()
        a = requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=0)
        _tls.s.mount("http://", a)
    return _tls.s


def _llm_call_think_off(model: str, prompt: str, ollama_url: str = DEFAULT_URL,
                         max_tokens: int = 60, system: str = "") -> str:
    """Call any Ollama model via HTTP API with think=false (gemma4 reasoning off).
    This is the fastest path for gemma4:e2b — no thinking overhead, ~0.5s/call.
    Falls back to standard call for models that don't support think param.
    """
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    url = ollama_url.rstrip("/") + "/api/chat"
    payload = {
        "model": model,
        "messages": messages,
        "options": {"temperature": 0.0, "num_predict": max_tokens,
                    "num_ctx": 1024 if system else 512},
        "think": False,
        "stream": False,
        "keep_alive": -1,
    }
    for attempt in range(1, 4):
        try:
            resp = _get_session().post(url, json=payload, timeout=30)
            resp.raise_for_status()
            return resp.json().get("message", {}).get("content", "").strip()
        except Exception:
            _tls.s = None
            if attempt == 3:
                return ""
            time.sleep(2 * attempt)
    return ""


# ═══════════════════════════════════════════════════════════════════════════════
#  AGENT TOOL REGISTRY  ──  all tools defined once, assigned per agent
# ═══════════════════════════════════════════════════════════════════════════════

class AgentTools:
    """
    Centralised tool registry for the multi-agent system.
    Each tool is a static method that takes explicit dependencies.
    Agents receive only the tools they need via their constructor.

    Tool catalogue:
      DETERMINISTIC (no LLM):
        cluster_lookup   — Tier 0: O(1) raw → canonical cluster name
        gse_context      — Format GSE sibling label distribution
        episodic_search  — Tier 3: past resolutions for a raw label
        kg_lookup        — Tier 4: synonym/variant triples
        phase15_collapse — Deterministic exact/abbreviation match

      LLM-AUGMENTED:
        semantic_search  — Tier 2: cosine similarity vs cluster embeddings
        react_search     — Combines cluster_lookup + semantic + specificity ranking
        react_pick       — Validate + remap a cluster name
        react_new_cluster— Register a new cluster for unique entities

    Agent tool assignments:
      GSMExtractor:   (none — stateless prompt→LLM→parse)
      GSEInferencer:  (none — stateless prompt→LLM→parse, GSE context is in system prompt)
      CollapseWorker: cluster_lookup, gse_context, episodic_search, kg_lookup,
                      phase15_collapse, semantic_search, react_search,
                      react_pick, react_new_cluster
    """

    # ── Tier 0: Cluster Map (deterministic, O(1)) ────────────────────────

    @staticmethod
    def cluster_lookup(mem_agent, col: str, label: str) -> Optional[str]:
        """Direct raw→canonical lookup. Returns None if no match."""
        if not mem_agent or is_ns(label):
            return None
        d = mem_agent.cluster_lookup(col, label)
        return d if (d and mem_agent.is_cluster_name(col, d)) else None

    # ── GSE Context (deterministic) ──────────────────────────────────────

    @staticmethod
    def gse_context(col: str, gse_ctx: "GSEContext",
                    gse_block: str = "") -> str:
        """Format GSE experiment description + sibling label distribution."""
        ctx_counts = dict(gse_ctx.label_counts[col]) if gse_ctx else {}
        lines = [gse_block.rstrip()] if gse_block else []
        if ctx_counts:
            lines.append(f"Sibling labels for {col} in this experiment:")
            for label, count in sorted(ctx_counts.items(), key=lambda x: -x[1])[:15]:
                lines.append(f"  {label!r}: {count}")
        else:
            lines.append(f"No sibling labels for {col} in this experiment.")
        return "\n".join(lines)

    # ── Tier 3: Episodic Memory (deterministic query) ────────────────────

    @staticmethod
    def episodic_search(mem_agent, col: str, raw_label: str) -> str:
        """Past resolutions for this exact raw label, formatted as text."""
        if not mem_agent:
            return "(no episodic memory)"
        results = mem_agent.episodic_search(col, raw_label)
        if not results:
            return f"No past resolutions for {raw_label!r}"
        lines = [f"Past resolutions for {raw_label!r}:"]
        for canonical, count, conf, ts in results[:5]:
            lines.append(f"  → {canonical!r}  (×{count}, conf={conf:.2f})")
        return "\n".join(lines)

    # ── Tier 4: Knowledge Graph (deterministic query) ────────────────────

    @staticmethod
    def kg_lookup(mem_agent, col: str, label: str) -> List[tuple]:
        """Return KG triples: [(object, relation, weight), ...]."""
        if not mem_agent:
            return []
        return mem_agent.kg_lookup(col, label)

    # ── Tier 2: Semantic Search (uses embeddings, no LLM reasoning) ──────

    @staticmethod
    def semantic_search(mem_agent, col: str, query: str, k: int = 10) -> list:
        """Cosine similarity search vs cluster name embeddings."""
        if not mem_agent or not mem_agent.is_ready(col):
            return []
        return mem_agent.semantic_search(col, query.strip(), k=k)

    # ── ReAct Tools (used by CollapseWorker agent loop) ──────────────────

    @staticmethod
    def react_search(mem_agent, col: str, query: str) -> str:
        """Combined cluster_lookup + semantic + specificity ranking."""
        if not mem_agent or not mem_agent.is_ready(col):
            return "OBSERVATION: memory not available"
        hits = mem_agent.semantic_search(col, query.strip(), k=10)
        d = mem_agent.cluster_lookup(col, query.strip())
        if d and not any(l == d for l, _ in hits):
            hits = [(d, 1.0)] + hits
        ranked = _rank_candidates_by_specificity(query.strip(), hits)
        if not ranked:
            return f"OBSERVATION: no candidates for {query!r}"
        lines = [f"OBSERVATION: candidates for {query!r}:"]
        for cl, sim, sc in ranked[:6]:
            lines.append(f"  {cl!r}  sim={sim:.3f}  spec={sc:.1f}")
        return "\n".join(lines)

    @staticmethod
    def react_pick(mem_agent, col: str, cluster_name: str) -> tuple:
        """Validate cluster name. Returns (name, ok, rule)."""
        cn = cluster_name.strip().rstrip(".")
        if mem_agent and mem_agent.is_cluster_name(col, cn):
            return cn, True, "react_pick"
        if mem_agent:
            r = mem_agent.cluster_lookup(col, cn)
            if r and mem_agent.is_cluster_name(col, r):
                return r, True, "react_pick+remap"
        return None, False, "react_pick_invalid"

    @staticmethod
    def react_new_cluster(mem_agent, col: str, name: str,
                          raw_label: str, log_fn=None) -> tuple:
        """Register new cluster — but first try harder to match existing.
        Returns (name, ok, rule)."""
        cn = name.strip().rstrip(".")
        ok = (len(cn) >= 2 and not is_ns(cn) and
              not any(w in cn.lower() for w in
                      ("unknown", "unspecified", "n/a", "none",
                       "other", "not ", "mixed")))
        if not ok:
            return None, False, "new_rejected"

        # Last-ditch: before creating a NEW cluster, try fuzzy substring
        # match against existing cluster names in semantic_labels
        if mem_agent:
            try:
                with mem_agent._conn() as c:
                    cn_lower = cn.lower().strip()
                    # Strip parentheticals and suffixes for matching
                    cn_clean = re.sub(r'\s*\([^)]*\)\s*$', '', cn_lower).strip()
                    for suffix in (" cells", " cell", " tissue", " cell line",
                                   " cell lines", " tissues"):
                        if cn_clean.endswith(suffix) and len(cn_clean) > len(suffix) + 2:
                            cn_clean = cn_clean[:len(cn_clean) - len(suffix)].strip()
                            break

                    # Try exact match on cleaned form
                    row = c.execute(
                        "SELECT label FROM semantic_labels "
                        "WHERE col=? AND LOWER(label)=LOWER(?)",
                        (col, cn_clean)).fetchone()
                    if row:
                        return row[0], True, "gate_fuzzy_remap"

                    # Try containment: does an existing cluster name contain
                    # our label or vice versa? (min 4 chars to avoid false positives)
                    if len(cn_clean) >= 4:
                        row = c.execute(
                            "SELECT label FROM semantic_labels "
                            "WHERE col=? AND LENGTH(label)>=4 "
                            "AND (LOWER(label) LIKE ? OR ? LIKE '%' || LOWER(label) || '%') "
                            "ORDER BY LENGTH(label) LIMIT 1",
                            (col, f"%{cn_clean}%", cn_clean)).fetchone()
                        if row:
                            return row[0], True, "gate_contain_remap"
            except Exception:
                pass

            mem_agent.register_new_cluster(col, cn, raw_label,
                                           log_fn or (lambda m: None))
        return cn, True, "react_new_cluster"

    @staticmethod
    def deterministic_fallback(mem_agent, col: str, label: str,
                               ctx_labels: List[str]) -> Optional[tuple]:
        """Exact match / abbreviation match against sibling labels."""
        if not ctx_labels:
            return None
        matched, rule = phase15_collapse(label, ctx_labels)
        if matched and matched != label:
            if mem_agent and not mem_agent.is_cluster_name(col, matched):
                r = mem_agent.cluster_lookup(col, matched)
                matched = r if (r and mem_agent.is_cluster_name(col, r)) else None
            if matched:
                return matched, rule
        return None

    @staticmethod
    def log_episodic(mem_agent, col: str, raw_label: str, final: str,
                     rule: str, platform: str = "", gse: str = "",
                     gsm: str = ""):
        """Write to episodic log after successful collapse."""
        if not mem_agent or not final or is_ns(final) or final == raw_label:
            return
        conf = {"direct_cluster_map": 0.95, "gse_dominant": 0.92,
                "react_pick": 0.88, "react_pick+remap": 0.88,
                "react_new_cluster": 0.85, "exact_match": 0.92,
                "abbreviation_match": 0.90}.get(rule, 0.80)
        try:
            mem_agent.log_resolution(col, raw_label, final, conf,
                                     platform=platform, gse=gse,
                                     gsm=gsm, rule=rule)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
#  MULTI-AGENT SYSTEM  ──  3 specialised worker classes
# ═══════════════════════════════════════════════════════════════════════════════
#
#  GSMExtractor   — Phase 1  : stateless per-sample raw extraction
#  GSEInferencer  — Phase 1b : per-GSE context inference (KV cache reuse)
#  CollapseWorker — Phase 2  : per-sample collapse with 4-tier memory
#
# Each agent owns its own context and memory scope.
# All agents run in parallel via ThreadPoolExecutor.
# ═══════════════════════════════════════════════════════════════════════════════


class GSMExtractor:
    """
    Phase 1 — Stateless per-sample extraction agent.

    Context : GSM metadata (title, source, characteristics, treatment, description)
              + optional GSE title/summary (appended to prompt)
    Memory  : None (deterministic prompt → LLM → JSON parse)
    Tools   : None
    Model   : gemma2:2b
    """

    def __init__(self, ollama_url: str, watchdog=None, log_fn=None):
        self.url      = ollama_url
        self.watchdog = watchdog
        self._log     = log_fn or (lambda m: None)

    def _llm_call(self, prompt: str, max_tokens: int = 200) -> str:
        if self.watchdog:
            self.watchdog.wait_if_paused()
            self.watchdog.record_call()
        messages = [{"role": "user", "content": prompt}]
        options  = {"temperature": 0.0, "num_predict": max_tokens, "num_ctx": 1024}
        if _OLLAMA_LIB_OK:
            for attempt in range(1, 4):
                try:
                    resp = _ollama_lib.chat(model=EXTRACTION_MODEL, messages=messages,
                                            options=options, stream=False,
                                            keep_alive="5m")
                    if hasattr(resp, "message") and hasattr(resp.message, "content"):
                        return (resp.message.content or "").strip()
                    elif isinstance(resp, dict):
                        return resp.get("message", {}).get("content", "").strip()
                    return ""
                except Exception:
                    if attempt == 3: return ""
                    time.sleep(2 * attempt)
            return ""
        url = self.url.rstrip("/") + "/api/chat"
        payload = {"model": EXTRACTION_MODEL, "stream": False, "options": options,
                   "messages": messages, "keep_alive": "5m"}
        for attempt in range(1, 4):
            try:
                resp = _get_session().post(url, json=payload, timeout=60)
                resp.raise_for_status()
                return resp.json()["message"]["content"].strip()
            except Exception:
                _tls.s = None
                if attempt == 3: return ""
                time.sleep(1)
        return ""

    def extract(self, gsm: str, raw: dict, gse_meta: dict = None,
                cols: list = None) -> Dict[str, str]:
        """Extract labels from one GSM sample. Returns {col: label}."""
        _cols = cols or LABEL_COLS_SCRATCH
        _title  = str(raw.get("gsm_title", "")).strip()[:80]
        _source = str(raw.get("source_name", "")).strip()[:80]
        _char   = str(raw.get("characteristics", "")).replace("\t", " ").strip()[:300]
        _treat  = str(raw.get("treatment_protocol", "")).replace("\t", " ").strip()[:200]
        _desc   = str(raw.get("description", "")).replace("\t", " ").strip()[:200]
        prompt  = (_EXTRACTION_PROMPT_TEMPLATE
                   .replace("{TITLE}", _title)
                   .replace("{SOURCE}", _source)
                   .replace("{CHAR}", _char))
        # treatment_protocol is a lab protocol — not passed to avoid extraction as treatment
        if _desc:
            prompt += f"\nDescription: {_desc}"
        if gse_meta:
            t = gse_meta.get("title") or gse_meta.get("gse_title", "")
            s = gse_meta.get("summary") or gse_meta.get("gse_summary", "")
            if t: prompt += f"\nExperiment: {t[:120]}"
            if s: prompt += f"\nSummary: {s[:250]}"
        text = ""
        for attempt in range(3):
            text = self._llm_call(prompt)
            if text: break
            time.sleep(5 * (attempt + 1))
        return _parse_json_extraction(text, _cols)


class GSEInferencer:
    """
    Phase 1b — Per-GSE, per-label inference agents with KV cache reuse.

    Architecture: 3 independent LLM agents (one per label column), each with
    its OWN system prompt containing THIS GSE's experiment context only.
    Ollama caches the system prompt KV tensors per GSE → ~40% faster.

    Each label agent runs independently in parallel per sample.
    One GSEInferencer instance per GSE — NOT shared across GSEs.
    """

    def __init__(self, gse_id: str, gse_meta: dict, ollama_url: str,
                 watchdog=None, log_fn=None):
        self.gse_id   = gse_id
        self.url      = ollama_url
        self.watchdog = watchdog
        self._log     = log_fn or (lambda m: None)
        _title   = (gse_meta.get("gse_title") or gse_meta.get("title", ""))[:200]
        _summary = (gse_meta.get("gse_summary") or gse_meta.get("summary", ""))[:400]
        _design  = (gse_meta.get("gse_design") or gse_meta.get("design", ""))[:300]
        # Build per-label system prompts — each label gets its own GSE context
        self._systems = {}
        for col in LABEL_COLS_SCRATCH:
            tmpl = _PER_LABEL_INFER_SYSTEMS.get(col, "")
            self._systems[col] = (tmpl
                .replace("{GSE_TITLE}", _title)
                .replace("{GSE_SUMMARY}", _summary)
                .replace("{GSE_DESIGN}", _design))

    def _llm_call(self, system: str, user_msg: str, max_tokens: int = 60) -> str:
        if self.watchdog:
            self.watchdog.wait_if_paused()
            self.watchdog.record_call()
        return _llm_call_think_off(EXTRACTION_MODEL, user_msg, self.url,
                                    max_tokens=max_tokens, system=system)

    def infer_sample(self, gsm: str, raw: dict,
                     current_labels: Dict[str, str],
                     cols: list = None) -> Dict[str, str]:
        """Infer missing labels — 3 independent per-label LLM agents in parallel.
        Each agent gets THIS GSE's context only (not all GSEs)."""
        _cols = cols or LABEL_COLS_SCRATCH
        ns_fields = [c for c in _cols if is_ns(current_labels.get(c, NS))]
        if not ns_fields:
            return current_labels
        _title  = str(raw.get("gsm_title", "")).strip()[:80]
        _source = str(raw.get("source_name", "")).strip()[:80]
        _char   = str(raw.get("characteristics", "")).replace("\t", " ").strip()[:300]
        user_msg = (f"Title: {_title}\nSource: {_source}\n"
                    f"Characteristics: {_char}\n"
                    f"ANSWER:")

        def _infer_one_label(col_):
            sys_ = self._systems.get(col_, "")
            text_ = ""
            for attempt in range(3):
                text_ = self._llm_call(sys_, user_msg, max_tokens=60)
                if text_: break
                time.sleep(3 * (attempt + 1))
            return col_, _parse_single_label(text_)

        updated = current_labels.copy()
        # Run per-label agents in parallel
        from concurrent.futures import ThreadPoolExecutor as _TPE_inner
        with _TPE_inner(max_workers=len(ns_fields),
                        thread_name_prefix="P1bL") as _ex:
            futs = {_ex.submit(_infer_one_label, c): c for c in ns_fields}
            for f in futs:
                try:
                    col_r, val_r = f.result()
                    if not is_ns(val_r):
                        updated[col_r] = val_r
                except Exception:
                    pass
        return updated


class CollapseWorker:
    """
    Phase 2 — Per-GSE retrieval-augmented LLM collapse agent with working memory.

    Architecture: Each GSE is processed as a unit. Within a GSE:
      1. Working memory check — resolutions from earlier samples in THIS GSE (<1ms)
      2. Global episodic cache — resolutions from ALL previous GSEs (<1ms)
      3. BioLORD-2023 embedding retrieval — top-20 candidates (~17ms)
      4. LLM pick with GSE context — gemma4:e2b picks best match (~700ms)
      5. Result cached in working memory + global episodic

    Working memory gives the agent experiment awareness:
    - After resolving "breast tumor" → "Breast" for sample #1, samples #2-100
      in the same GSE resolve instantly without LLM calls.
    - GSE context (title, summary) injected into LLM prompt so the agent
      understands the experiment when disambiguating.

    Model: gemma4:e2b (think=false, ~700ms first encounter, <1ms repeats)
    """

    # Garbage patterns to filter from LLM output
    _GARBAGE = ["[clean", "[disease", "thinking process", "**", "based on",
                "respond with", "none of the", "no match", "[unknown",
                "the raw label", "1.", "2.", "3."]

    def __init__(self, model: str, ollama_url: str,
                 mem_agent: "MemoryAgent", watchdog=None, log_fn=None):
        self.model     = model
        self.url       = ollama_url
        self.mem_agent = mem_agent
        self.watchdog  = watchdog
        self._log      = log_fn or (lambda m: None)
        self._episodic = {}  # {(col, raw_lower): canonical} — global across GSEs
        self._cluster_vecs = {}  # {col: (labels, matrix)}
        self._st_model = None  # lazy-loaded sentence-transformers
        # Per-GSE working memory (set/cleared per GSE)
        self._working_mem = {}  # {(col, raw_lower): canonical}
        self._gse_id = None
        self._gse_title = ""
        self._gse_summary = ""

    def begin_gse(self, gse_id: str, gse_ctx: "GSEContext" = None):
        """Initialize working memory for a new GSE experiment."""
        # Promote working memory to global episodic before clearing
        if self._working_mem:
            self._episodic.update(self._working_mem)
        self._working_mem = {}
        self._gse_id = gse_id
        self._gse_title = ""
        self._gse_summary = ""
        if gse_ctx:
            self._gse_title = (gse_ctx.title or "")[:200]
            self._gse_summary = (gse_ctx.summary or "")[:300]

    def end_gse(self):
        """Finalize GSE — promote working memory to global episodic."""
        if self._working_mem:
            self._episodic.update(self._working_mem)
            self._working_mem = {}
        self._gse_id = None
        self._gse_title = ""
        self._gse_summary = ""

    def _build_system_prompt(self):
        """Build system prompt with GSE context for experiment-aware collapse."""
        parts = [
            "You are a biomedical label normalizer. Given a raw label and candidate clusters, pick the BEST match.",
            "",
            "Rules:",
            "- Pick the MOST SPECIFIC matching cluster — preserve specificity",
            "- Only normalize: standardize names, strip sample IDs, remove \"Normal/Control\" prefixes",
            "- Cell lines → their Cell Line: cluster. Cell types → their Cell Type: cluster",
            "- Abbreviations → expanded form (AD → Alzheimer Disease, HCC → Hepatocellular Carcinoma)",
            "- If NO candidate is a good match, respond: NEW: [clean canonical name]",
        ]
        # Inject GSE context so the agent understands the experiment
        if self._gse_title or self._gse_summary:
            parts.append("")
            parts.append("Experiment context (use to disambiguate):")
            if self._gse_title:
                parts.append(f"  Title: {self._gse_title}")
            if self._gse_summary:
                parts.append(f"  Summary: {self._gse_summary}")
        # Inject working memory (resolutions so far in this GSE)
        if self._working_mem:
            recent = list(self._working_mem.items())[-10:]  # last 10 resolutions
            parts.append("")
            parts.append("Recent resolutions in this experiment:")
            for (col, raw), canonical in recent:
                parts.append(f"  {col}: \"{raw}\" → {canonical}")
        parts.append("")
        parts.append("Respond with ONLY the cluster name (or NEW: name).")
        return "\n".join(parts)

    def _get_st(self):
        """Lazy-load sentence-transformers model."""
        if self._st_model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._st_model = SentenceTransformer("FremyCompany/BioLORD-2023-C")
            except ImportError:
                self._log("  [CollapseWorker] sentence-transformers not available")
        return self._st_model

    def load_cluster_vectors(self, col: str):
        """Load cluster embeddings from DB into RAM for one column."""
        if col in self._cluster_vecs:
            return
        ma = self.mem_agent
        if not ma:
            return
        try:
            import numpy as _np
            with ma._conn() as c:
                rows = c.execute(
                    "SELECT label, embedding FROM semantic_labels "
                    "WHERE col=? AND embedding IS NOT NULL", (col,)).fetchall()
            if not rows:
                return
            labels = [r[0] for r in rows]
            vecs = _np.stack([_np.frombuffer(r[1], dtype=_np.float32) for r in rows])
            self._cluster_vecs[col] = (labels, vecs)
            self._log(f"  [CollapseWorker] {col}: {len(labels)} cluster vectors loaded")
        except Exception as e:
            self._log(f"  [CollapseWorker] {col}: vector load failed: {e}")

    def _retrieve_candidates(self, col: str, raw_label: str, k: int = 20):
        """Retrieve top-k most similar canonical clusters for a raw label using local embeddings."""
        st = self._get_st()
        if st is None or col not in self._cluster_vecs:
            return []
        try:
            import numpy as _np
            labels, mat = self._cluster_vecs[col]
            vec = st.encode([raw_label], normalize_embeddings=True)[0]
            sims = mat @ vec
            top_idx = _np.argsort(sims)[::-1][:k]
            return [(labels[i], float(sims[i])) for i in top_idx]
        except Exception:
            return []

    def _llm_pick(self, col: str, raw_label: str, candidates: list) -> str:
        """Ask LLM to pick the best canonical cluster from candidates.
        System prompt includes GSE context + recent working memory resolutions."""
        if self.watchdog:
            self.watchdog.wait_if_paused()
            self.watchdog.record_call()

        cand_str = "\n".join(f"  - {c} ({s:.2f})" for c, s in candidates)
        user_msg = f'Raw: "{raw_label}"\nCandidates:\n{cand_str}\nBest:'

        try:
            resp = requests.post(f"{self.url}/api/chat", json={
                "model": self.model, "stream": False,
                "messages": [
                    {"role": "system", "content": self._build_system_prompt()},
                    {"role": "user", "content": user_msg}
                ],
                "options": {"temperature": 0, "num_predict": 30, "num_ctx": 2048},
                "think": False,
            }, timeout=30)
            resp.raise_for_status()
            return resp.json().get("message", {}).get("content", "").strip()
        except Exception:
            return raw_label

    # ── Main entry point ─────────────────────────────────────────────────

    def collapse_field(self, gsm: str, col: str, raw_label: str,
                       gse_ctx: "GSEContext", raw: dict = None,
                       platform: str = "") -> tuple:
        """
        Collapse one (gsm, col) pair using working memory + episodic + retrieval + LLM.
        Returns (final_label, collapsed: bool, rule: str, audit: dict).
        """
        # NS passthrough
        if is_ns(raw_label) or not raw_label:
            return NS, False, "ns", {"raw": raw_label, "final": NS}

        key = (col, raw_label.lower().strip())

        # 1. Working memory — resolutions from THIS GSE (<1ms)
        if key in self._working_mem:
            cached = self._working_mem[key]
            return cached, True, "working_mem", {
                "raw": raw_label, "final": cached, "rule": "working_mem"}

        # 2. Global episodic cache — resolutions from ALL GSEs (<1ms)
        if key in self._episodic:
            cached = self._episodic[key]
            # Also cache in working memory for this GSE
            self._working_mem[key] = cached
            return cached, True, "episodic", {
                "raw": raw_label, "final": cached, "rule": "episodic"}

        # 3. Retrieve top-20 candidates via BioLORD-2023 embedding similarity
        candidates = self._retrieve_candidates(col, raw_label, k=20)

        if not candidates:
            return raw_label, False, "no_vectors", {
                "raw": raw_label, "final": raw_label}

        # 4. LLM picks from candidates with GSE context (~700ms)
        answer = self._llm_pick(col, raw_label, candidates)

        # Clean answer
        answer = answer.strip().strip('"').strip("'").rstrip(".")
        rule = "llm_match"

        if answer.upper().startswith("NEW:"):
            answer = answer[4:].strip().strip('"').strip("'").rstrip(".")
            rule = "llm_new"

        # Garbage filter
        if any(g in answer.lower() for g in self._GARBAGE) or len(answer) > 60:
            parts = re.split(r'[,\n;()]', answer)
            cleaned = parts[0].strip().strip('"').rstrip(".")
            if len(cleaned) > 60 or any(g in cleaned.lower() for g in self._GARBAGE):
                answer = raw_label.strip().title()
                rule = "llm_cleaned"
            else:
                answer = cleaned
                rule = "llm_cleaned"

        if not answer or len(answer) < 2:
            answer = raw_label.strip().title()
            rule = "llm_fallback"

        # 5. Cache in BOTH working memory and global episodic
        self._working_mem[key] = answer
        self._episodic[key] = answer
        # Also cache normalized variants
        for variant in [raw_label.lower(), raw_label.title(),
                        re.sub(r'[-_]', ' ', raw_label.lower()),
                        re.sub(r'\s*\([^)]*\)\s*$', '', raw_label).lower().strip()]:
            vkey = (col, variant.strip())
            if vkey not in self._working_mem:
                self._working_mem[vkey] = answer
            if vkey not in self._episodic:
                self._episodic[vkey] = answer

        # 6. Log to MemoryAgent episodic store (persistent across runs)
        ma = self.mem_agent
        if ma and answer != raw_label and not is_ns(answer):
            try:
                conf = {"llm_match": 0.92, "llm_new": 0.85,
                        "llm_cleaned": 0.70}.get(rule, 0.80)
                ma.log_resolution(col, raw_label, answer, conf,
                                  platform=platform,
                                  gse=gse_ctx.gse_id if gse_ctx else "",
                                  gsm=gsm, rule=rule)
            except Exception:
                pass

        audit = {"raw": raw_label, "final": answer,
                 "collapsed": True, "rule": rule,
                 "gse": self._gse_id or "",
                 "top_candidates": [(c, f"{s:.2f}") for c, s in candidates[:3]]}
        return answer, True, rule, audit


#
#  GSEWorker    owns ALL NS samples for one experiment
#  (LEGACY — kept for backward compatibility; new pipeline uses agents above)
#
def _rank_candidates_by_specificity(query: str, candidates: list) -> list:
    """
    Re-rank collapse candidates so the most specific cluster name rises to top.
    Abbreviations are expanded before scoring so NK → Natural Killer Cells
    scores correctly against the full cluster name.
    """
    _ABBREV = {
        "nk":"natural killer cells","pbmc":"peripheral blood mononuclear cell",
        "bm":"bone marrow","sc":"spinal cord","cns":"central nervous system",
        "pns":"peripheral nervous system","lps":"lipopolysaccharide",
        "dc":"dendritic cell","dc":"dendritic cells","th1":"t helper 1 cell",
        "th2":"t helper 2 cell","treg":"regulatory t cell","nkt":"nk t cell",
        "hspc":"hematopoietic stem progenitor cell","msc":"mesenchymal stem cell",
        "ipsc":"induced pluripotent stem cell","esc":"embryonic stem cell",
    }
    def _expand(text):
        words = text.lower().split()
        return " ".join(_ABBREV.get(w.strip("+-()"), w) for w in words)

    CELL_TYPE_WORDS = {
        "cell","cells","macrophage","macrophages","monocyte","monocytes",
        "lymphocyte","lymphocytes","neutrophil","neutrophils","nk","killer",
        "t-cell","b-cell","dendritic","fibroblast","fibroblasts","neuron",
        "neurons","astrocyte","astrocytes","microglia","hepatocyte","hepatocytes",
        "epithelial","endothelial","stem","progenitor","thymocyte","erythrocyte",
        "platelet","eosinophil","basophil","mast","plasma","cd4","cd8","cd3",
        "cd19","cd14","cd56","treg","nkt","kupffer","alveolar","adipocyte",
        "osteoblast","chondrocyte","keratinocyte","melanocyte","sertoli","leydig"
    }
    ORGAN_WORDS = {
        "lung","liver","heart","brain","kidney","spleen","pancreas","colon",
        "blood","bone","muscle","skin","thymus","lymph","breast","prostate",
        "ovary","testis","uterus","stomach","intestine","colon","rectum",
        "esophagus","trachea","bladder","adrenal","thyroid","pituitary"
    }

    q_words = set(_expand(query).split())
    query_has_cell_type = bool(q_words & CELL_TYPE_WORDS)

    scored = []
    for cand_label, sim in candidates:
        c_words = set(_expand(cand_label).split())
        score = sim * 5  # base: semantic similarity

        # +10 per query word covered by candidate (candidate matches query)
        overlap = q_words & c_words
        score += len(overlap) * 10

        # -3 per extra word in candidate not in query (candidate is broader)
        extra = c_words - q_words
        score -= len(extra) * 3

        # +5 if candidate is not longer than query (not a superset)
        if len(c_words) <= len(q_words):
            score += 5

        # -10 if query has a cell type word but candidate is a pure organ
        cand_is_organ = bool(c_words & ORGAN_WORDS) and not bool(c_words & CELL_TYPE_WORDS)
        if query_has_cell_type and cand_is_organ:
            score -= 10

        scored.append((cand_label, sim, round(score, 3)))

    scored.sort(key=lambda x: x[2], reverse=True)
    return scored


_CPU_OLLAMA_ACTIVE = False   # set True once CPU server confirmed up

def _pick_ollama_url(gpu_url: str, vram_threshold: float = 92.0) -> str:
    """
    Route a new GSE worker to GPU or CPU Ollama based on current VRAM load.

    Logic:
      - If VRAM < threshold → GPU instance (fast)
      - If VRAM >= threshold AND CPU instance is running → CPU instance
      - If CPU instance not running → GPU anyway (may queue, but won't crash)

    Called once per GSE at worker construction time — the worker then
    uses that URL for its entire lifetime (no mid-run switching).
    """
    global _CPU_OLLAMA_ACTIVE
    if not _CPU_OLLAMA_ACTIVE:
        return gpu_url
    vpct = vram_utilisation_pct()
    if vpct >= vram_threshold:
        if ollama_server_ok(CPU_OLLAMA_URL):
            return CPU_OLLAMA_URL
    return gpu_url



class GSEWorker:
    """
    Processes every NS sample belonging to ONE GSE.

    Design:
       Receives the GSEContext already seeded with ALL samples (labeled+NS).
       For each NS sample, runs:
          1. prompt_extract      raw label from GEO text (LLM)
          2. phase15_collapse    deterministic GSE-scoped collapsing:
               Rule 1: exact match (case/punctuation normalised)
               Rule 2: abbreviation/initials match (e.g. AD  Alzheimer Disease)
               Guard : differing digit sequences block the merge
               Fallback: no match  keep raw label unchanged
       Step 2 only fires when the GSE already has labeled siblings to compare
        against  no context means no collapse, raw label is kept.
       After each resolved sample, calls ctx.update_label() so subsequent
        samples in the same GSE see freshly assigned labels
        (MemGPT-style rolling context update).
       Workers for different GSEs run in parallel; each has its own context
        and HTTP session  no shared state except the watchdog gate.
    """

    def __init__(self, gse_id: str, ctx: GSEContext,
                 model: str, ollama_url: str, watchdog=None,
                 mem_agent: "MemoryAgent" = None,
                 platform: str = ""):
        self.gse_id     = gse_id
        self.ctx        = ctx
        self.model      = model
        self.url        = ollama_url
        self.watchdog   = watchdog
        self.mem_agent  = mem_agent   # shared Memory Agent (all tiers)
        self.platform   = platform    # for episodic log provenance

        #  GSE description  built ONCE for all NS samples in this GSE 
        # title / summary / overall_design never change during a worker's life.
        # Every NS sample in this GSE reuses this same string  never rebuilt.
        # Sibling label counts (ctx.label_counts) DO change per resolved sample
        # and are read live, not cached here.
        _lines = []
        if ctx.title:
            _lines.append(f"Experiment title  : {ctx.title}")
        if getattr(ctx, "summary", ""):
            _lines.append(f"Experiment summary: {ctx.summary[:500]}")
        if getattr(ctx, "design", ""):
            _lines.append(f"Overall design    : {ctx.design[:300]}")
        self._gse_block: str = "\n".join(_lines) + "\n\n" if _lines else ""

    def _llm(self, prompt: str, max_tokens: int = 80, system: str = "") -> str:
        if self.watchdog:
            self.watchdog.wait_if_paused()
            self.watchdog.record_call()

        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        # num_ctx: use tight window for extraction (fast), full for agent reasoning
        ctx_size = 1024 if max_tokens <= 120 else 4096
        options  = {"temperature": 0.0, "num_predict": max_tokens, "num_ctx": ctx_size}

        # ollama Python library  avoids urllib3/requests segfaults
        if _OLLAMA_LIB_OK:
            for attempt in range(1, 4):
                try:
                    resp = _ollama_lib.chat(model=self.model, messages=messages,
                                            options=options, stream=False,
                                            keep_alive=-1)  # keep model in VRAM always
                    return resp.message.content.strip()
                except Exception as _e:
                    _es = str(_e).lower()
                    if "connection refused" in _es or "disconnected" in _es:
                        if not ollama_server_ok(self.url):
                            time.sleep(10)
                        else:
                            time.sleep(3 * attempt)
                    elif "out of memory" in _es or "cudamalloc" in _es:
                        _unload_all_models(self.url)
                        time.sleep(5)
                    else:
                        time.sleep(2 * attempt)
                    if attempt == 3: return ""
            return ""

        # HTTP fallback
        url = self.url.rstrip("/") + "/api/chat"
        payload = {"model": self.model, "stream": False,
                   "options": options, "messages": messages,
                   "keep_alive": -1}
        for attempt in range(1, 4):
            try:
                resp = _get_session().post(url, json=payload, timeout=180)
                resp.raise_for_status()
                return resp.json()["message"]["content"].strip()
            except Exception:
                _tls.s = None
                if attempt == 3: return ""
                time.sleep(3 * attempt)
        return ""


    def _llm_with_model(self, prompt: str, model: str,
                         max_tokens: int = 80, system: str = "") -> str:
        """Call LLM with think=false for gemma4 speed. Uses shared HTTP helper."""
        if self.watchdog:
            self.watchdog.wait_if_paused()
            self.watchdog.record_call()
        return _llm_call_think_off(model, prompt, self.url,
                                    max_tokens=max_tokens, system=system)

    def repair_one(self, gsm: str, current: Dict[str, str],
                   raw: dict, pre_extracted: Dict[str, str] = None) -> Dict[str, str]:
        """
        Repair all NS fields for one sample.
        If pre_extracted is provided (from Phase 1), skip LLM extraction
        and go straight to collapse — avoids model swap overhead.
        Returns updated label dict with added _agents and _audit keys.
        """
        updated       = current.copy()
        resolved_cols = []
        audit: Dict[str, dict] = {}

        #  Determine which fields need resolving
        _active_cols = list(current.keys()) if current else LABEL_COLS
        _active_cols = [c for c in LABEL_COLS_SCRATCH if c in _active_cols or
                        c in {k for k in current}]
        ns_cols = [c for c in _active_cols if is_ns(current.get(c, NS))]
        if not ns_cols:
            updated["_agents"] = "none"
            updated["_audit"]  = "{}"
            return updated

        # If Phase 1 already extracted labels, use them — skip LLM extraction
        # Phase 2 collapse uses retrieval + LLM reasoning (same model as extraction)
        if pre_extracted:
            raw_extracted = {c: pre_extracted.get(c, NS) for c in ns_cols}
        else:
            #  Step 1a : Combined raw extraction  Tissue + Condition together
            # One LLM call extracts BOTH fields from the same raw GSM metadata.
            _r_title  = str(raw.get("gsm_title","")).strip()[:80]
            _r_source = str(raw.get("source_name","")).strip()[:80]
            _r_char   = str(raw.get("characteristics","")).replace("\t"," ").strip()[:300]
            _r_treat  = str(raw.get("treatment_protocol","")).replace("\t"," ").strip()[:200]
            _r_desc   = str(raw.get("description","")).replace("\t"," ").strip()[:200]
            combined_p1a = (_EXTRACTION_PROMPT_TEMPLATE
                .replace("{TITLE}", _r_title)
                .replace("{SOURCE}", _r_source)
                .replace("{CHAR}", _r_char))
            # treatment_protocol is a lab protocol — not passed to avoid extraction as treatment
            if _r_desc:
                combined_p1a += f"\nDescription: {_r_desc}"
            if self._gse_block:
                combined_p1a += f"\n{self._gse_block[:400]}"
            raw_text_1a = ""
            for _p1a_attempt in range(3):
                raw_text_1a = self._llm_with_model(
                    combined_p1a, model=EXTRACTION_MODEL,
                    max_tokens=200, system="")
                if raw_text_1a:
                    break
                _wait = 5 * (_p1a_attempt + 1)
                self._log(f"  [P1 WARN] {gsm}: Step 1a empty response "
                          f"(attempt {_p1a_attempt+1}/3) — waiting {_wait}s")
                if not ollama_server_ok(self.url):
                    self._log(f"  [P1 WARN] Ollama unreachable — waiting {_wait}s")
                time.sleep(_wait)
            if not raw_text_1a:
                self._log(f"  [P1 WARN] {gsm}: Step 1a failed after 3 attempts — "
                          f"Phase 2 GSE rescue will handle this sample")
            raw_extracted = _parse_json_extraction(raw_text_1a, ns_cols)

            #  Step 1b : Combined fallback with full GSE context
            still_ns = [c for c in ns_cols if is_ns(raw_extracted.get(c, NS))]
            if still_ns and any(self.ctx.label_counts[c] for c in still_ns):
                p1b      = prompt_extract_combined(gsm, raw, self.ctx, still_ns,
                                                        gse_block=self._gse_block)
                raw_text_1b = ""
                for _p1b_attempt in range(3):
                    raw_text_1b = self._llm(p1b, max_tokens=60 * len(still_ns))
                    if raw_text_1b:
                        break
                    _wait = 5 * (_p1b_attempt + 1)
                    self._log(f"  [P1b WARN] {gsm}: Step 1b empty response "
                              f"(attempt {_p1b_attempt+1}/3) — waiting {_wait}s")
                    time.sleep(_wait)
                if not raw_text_1b:
                    self._log(f"  [P1b WARN] {gsm}: Step 1b failed after 3 attempts — "
                              f"Phase 2 GSE rescue will handle remaining NS fields")
                fallback = parse_combined(raw_text_1b, still_ns)
                for c in still_ns:
                    if not is_ns(fallback.get(c, NS)):
                        raw_extracted[c] = fallback[c]

        #  Per-field collapse agents  Tissue and Condition in parallel
        # Each field has its own vocabulary  independent, no shared state.
        # Run concurrently to halve per-sample latency.
        def _run_field(col):
            """Resolve one field in its own thread — Ollama HTTP, truly parallel."""
            audit_f = {}
            out1 = raw_extracted.get(col, NS)

            #  Capitalisation normalisation 
            # Platform CSVs often have ALL CAPS labels (e.g. "LIVER", "BRAIN").
            # Cluster names in LLM_memory use title/sentence case ("Liver").
            # Try a direct cluster_lookup on the raw extracted label first 
            # if found, replace out1 with the correctly-cased cluster name.
            # This handles UPPERCASE before the agent ever sees it.
            ma = self.mem_agent
            if ma and out1 and not is_ns(out1):
                cased = ma.cluster_lookup(col, out1)
                if cased:
                    out1 = cased   # now correctly cased from LLM_memory

            # GSE context rescue — if Phase 1 returned NS but the GSE
            # already has a dominant label from other samples, use it
            # as the starting point for collapse. This is the primary
            # bridge: extraction failed but we know from siblings what
            # this sample likely is.
            ctx_labels = list(self.ctx.label_counts[col].keys())
            ctx_counts = dict(self.ctx.label_counts[col])

            if (is_ns(out1) or not out1) and ctx_labels:
                # Find dominant sibling label — most common in this experiment
                _dom = max(ctx_counts, key=ctx_counts.get)
                _dom_n = ctx_counts[_dom]
                _total_ctx = sum(ctx_counts.values())
                _dom_pct = _dom_n / _total_ctx if _total_ctx else 0
                # If dominant label is highly confident AND validates as cluster
                if _dom_pct >= 0.5 and ma and ma.is_cluster_name(col, _dom):
                    out1 = _dom
                    self._log(f"  [GSE RESCUE] {gsm} {col}: NS rescued "
                              f"from dominant sibling {_dom!r} ({_dom_pct:.0%})")
                elif _dom_pct >= 0.3 and ma:
                    # Weaker signal — try cluster_lookup to get canonical form
                    _cand = ma.cluster_lookup(col, _dom)
                    if _cand and ma.is_cluster_name(col, _cand):
                        out1 = _cand
                        self._log(f"  [GSE RESCUE] {gsm} {col}: NS rescued "
                                  f"from sibling {_cand!r} ({_dom_pct:.0%})")

            # Hard skip only if truly no evidence anywhere
            if (is_ns(out1) or not out1) and not ctx_labels and not ma:
                audit_f[col] = {"raw": raw_extracted.get(col, NS) or NS,
                               "final": NS,
                               "collapsed": False, "context_labels": [],
                               "extraction_path": "both_ns_no_context"}
                return col, audit_f, NS

            #  Phase 1.5 : Multi-tool collapse agent 
            #
            # The agent has THREE independent tools it can call in any order:
            #
            #   tool_gse_context(col)
            #     Returns all labeled GSMs in this experiment with counts.
            #     Strongest signal  if 8/10 samples say "Alzheimer Disease",
            #     this NS sample almost certainly is too.
            #
            #   tool_llm_memory(col, query)
            #     Queries LLM_memory cluster files. Returns direct cluster
            #     hit (cluster_map) + top-k semantic neighbours + KG triples.
            #     These are human-approved canonical labels.
            #
            #   tool_episodic(col, raw)
            #     Checks the episodic log for past resolutions of this exact
            #     raw label. Returns confidence-weighted past decisions.
            #
            # The agent LLM call decides which tools to invoke and synthesises
            # the results into a final cluster name or NO_MATCH.
            # Tool calls are real Python executions  results are injected
            # back into the agent context as tool_result blocks.
            # Max 4 agent turns to prevent infinite loops.

            final         = out1
            collapsed     = False
            collapse_rule = ""
            ma = self.mem_agent

            #  Fast-path: skip LLM entirely if direct cluster_map hit 
            # cluster_lookup is O(1) and covers the majority of cases.
            # Only launch the full agent loop for genuinely ambiguous labels.
            if ma and not is_ns(out1):
                direct = ma.cluster_lookup(col, out1)
                if direct and ma.is_cluster_name(col, direct):
                    final         = direct
                    collapsed     = True
                    collapse_rule = "direct_cluster_map"

            if ma and not collapsed:
                # Also fast-path: single dominant GSE sibling (>70% of samples)
                if ctx_counts:
                    top_label, top_count = max(ctx_counts.items(), key=lambda x: x[1])
                    total_ctx = sum(ctx_counts.values())
                    if total_ctx > 0 and top_count / total_ctx >= 0.70:
                        if ma.is_cluster_name(col, top_label):
                            final         = top_label
                            collapsed     = True
                            collapse_rule = "gse_dominant"

            # Run collapse agent if memory has clusters for this field
            # is_ready() returns True if semantic embeddings are loaded for this col
            # Treatment now has 2,716 clusters — collapse agent runs for all 3 fields
            if ma and not collapsed and ma.is_ready(col):
                final, collapsed, collapse_rule = self._run_collapse_agent(
                    gsm=gsm, col=col, out1=out1,
                    ctx_labels=ctx_labels, ctx_counts=ctx_counts,
                    raw=raw)

            #  Deterministic fallback (no agent / agent returned NO_MATCH) 
            # ctx_labels are already cluster names (from GSEContext.label_counts
            # which is seeded from cluster-validated labels). Safe to use directly.
            if not collapsed and ctx_labels:
                matched, rule = phase15_collapse(out1, ctx_labels)
                if matched and matched != out1:
                    # Validate matched label is a real cluster name
                    if ma and not ma.is_cluster_name(col, matched):
                        remapped = ma.cluster_lookup(col, matched)
                        matched  = remapped if (remapped and
                                   ma.is_cluster_name(col, remapped)) else None
                    if matched:
                        final         = matched
                        collapsed     = True
                        collapse_rule = rule


            #  Cluster gate: ONLY CLUSTER: names from LLM_memory are valid output 
            #  Cluster gate
            # All fields (Tissue, Condition, Treatment): only validated cluster
            # names are valid output. is_ready(col) checks if semantic embeddings
            # are loaded. Treatment now has 2,716 clusters in biomedical_memory.db.
            _col_has_vocab = ma and ma.is_ready(col)
            if not _col_has_vocab:
                # No cluster gate  but USE GSE sibling context to normalise.
                # If siblings already have this label (or a close variant),
                # snap to the most common sibling form for consistency.
                # e.g. "LPS" and "LPS 100ng/ml" in same GSE  pick dominant.
                if final and not is_ns(final):
                    # Try to snap to most common sibling label
                    if ctx_counts:
                        _norm_final = final.lower().strip()
                        _best = None
                        _best_count = 0
                        for sib, cnt in ctx_counts.items():
                            _norm_sib = sib.lower().strip()
                            # Match if one contains the other or they share
                            # significant overlap (>60% of shorter string)
                            _short = min(len(_norm_final), len(_norm_sib))
                            _overlap = sum(
                                1 for w in _norm_final.split()
                                if w in _norm_sib.split())
                            _total_words = max(1, len(_norm_final.split()))
                            if (_norm_sib in _norm_final or
                                _norm_final in _norm_sib or
                                (_overlap / _total_words) >= 0.6):
                                if cnt > _best_count:
                                    _best = sib
                                    _best_count = cnt
                        if _best and _best != final:
                            final         = _best
                            collapse_rule = "treatment_sibling_snap"
                    collapsed     = True
                    collapse_rule = collapse_rule or "raw_extraction"
            elif ma and final != NS and final != out1 and not collapsed:
                # Gate only fires for agent results - not direct_cluster_map/gse_dominant
                # (those already validated via is_cluster_name before setting collapsed)
                if not ma.is_cluster_name(col, final):
                    remapped = ma.cluster_lookup(col, final)
                    if remapped and ma.is_cluster_name(col, remapped):
                        final         = remapped
                        collapse_rule = collapse_rule + "+gate_remap"
                    else:
                        # Not a cluster name — check if it is a unique
                        # specific entity (cell line, novel tissue) that
                        # deserves a new cluster rather than falling to NS
                        _is_novel = (
                            final and len(final.strip()) >= 4 and
                            not is_ns(final) and
                            not any(w in final.lower()
                                    for w in ("unknown","unspecified","n/a","na",
                                              "none","other","not ","mixed")))
                        if _is_novel and ma:
                            _new_cluster = final.strip()
                            ma.register_new_cluster(
                                col, _new_cluster, out1, self._log)
                            collapsed     = True
                            collapse_rule = collapse_rule + "+new_cluster"
                        else:
                            # Truly ambiguous — revert to NS
                            final         = NS
                            collapsed     = False
                            collapse_rule = collapse_rule + "+gate_rejected"

            #  Agent-triggered memory write (after gate) 
            # Writes the post-gate canonical label  never the pre-gate form.
            # should_log() reasons about confidence + rule quality to decide
            # whether this resolution is worth persisting.
            if ma and final != out1 and final != NS:
                do_log, conf, reason = ma.should_log(
                    col, out1, final, collapse_rule)
                if do_log:
                    ma.log_resolution(
                        col=col, raw_label=out1, canonical=final,
                        confidence=conf,
                        platform=self.platform, gse=self.gse_id, gsm=gsm,
                        collapse_rule=collapse_rule)

            audit_f[col] = {"raw": out1, "final": final,
                           "collapsed": collapsed,
                           "collapse_rule": collapse_rule,
                           "context_labels": ctx_labels}

            return col, audit_f, final

            # GSEContext.label_counts is static  no update needed.
            # All pre-existing labels for this GSE were loaded at startup.

        # Run all field agents in parallel threads
        from concurrent.futures import ThreadPoolExecutor as _FEXEC, as_completed as _FAC
        with _FEXEC(max_workers=len(ns_cols)) as _fex:
            _ffuts = {_fex.submit(_run_field, c): c for c in ns_cols}
            for _ff in _FAC(_ffuts):
                try:
                    _col, _audit_f, _final = _ff.result()
                    audit[_col] = _audit_f.get(_col, {})
                    updated[_col] = _final
                    if _final != NS:
                        resolved_cols.append(_col)
                except Exception as _fe:
                    pass
        # Set _agents AFTER executor completes
        updated["_agents"] = ",".join(resolved_cols) if resolved_cols else "none"
        updated["_audit"]  = json.dumps(audit, ensure_ascii=False)
        return updated


    # 
    #  THREE INDEPENDENT TOOLS  called by the collapse agent
    # 

    def _tool_gse_context(self, col: str,
                           ctx_labels: List[str],
                           ctx_counts: Dict[str, int]) -> str:
        """
        TOOL: gse_context
        Returns the FULL GSE description (title + summary + overall design)
        scraped from NCBI, plus the label distribution of OTHER labeled samples
        in this same experiment. This is the strongest signal for NS resolution.
        """
        # _gse_block pre-built once in __init__  same for all samples in this GSE
        lines = ["GSE_CONTEXT:"]
        if self._gse_block:
            lines.append(self._gse_block.rstrip())
        lines.append("")

        if not ctx_labels:
            lines.append("  No labeled samples yet in this experiment.")
            return "\n".join(lines)

        lines.append("Labels of other samples in this experiment:")
        for lbl in sorted(ctx_counts, key=ctx_counts.get, reverse=True):
            lines.append(f"  {lbl!r}  ({ctx_counts[lbl]} sample"
                         f"{'s' if ctx_counts[lbl] > 1 else ''})")
        return "\n".join(lines)

    def _tool_llm_memory(self, col: str, query: str) -> str:
        """
        TOOL: llm_memory
        Queries LLM_memory cluster files for canonical label matches.
        Returns: direct cluster hit, top-k semantic neighbours, KG triples.
        All results are human-approved canonical cluster names.
        """
        ma = self.mem_agent
        if not ma or not ma.is_ready(col):
            return "LLM_MEMORY: Memory index not available."

        lines = [f"LLM_MEMORY results for {query!r}:"]

        # Direct cluster_map hit (highest confidence)
        direct = ma.cluster_lookup(col, query)
        if direct:
            lines.append(f"  DIRECT_CLUSTER: {direct!r}  "
                         f"(exact match in LLM_memory cluster files)")
        else:
            lines.append("  DIRECT_CLUSTER: none")

        # Semantic neighbours (cluster names only)
        sem = ma.semantic_search(col, query, k=6)
        if sem:
            lines.append("  SEMANTIC_NEIGHBOURS (cluster names, cosine sim):")
            for lbl, sim in sem:
                lines.append(f"    {lbl!r}  sim={sim:.3f}")
        else:
            lines.append("  SEMANTIC_NEIGHBOURS: none above threshold")

        # KG triples
        kg = ma.kg_lookup(col, query)
        if kg:
            lines.append("  KG_TRIPLES:")
            for obj, rel, wgt in kg[:3]:
                lines.append(f"    {query!r} --{rel}--> {obj!r}  w={wgt:.2f}")

        return "\n".join(lines)

    def _tool_episodic(self, col: str, raw_label: str) -> str:
        """
        TOOL: episodic_memory
        Checks the episodic log for past resolutions of this exact raw label.
        Returns confidence-weighted history from all past runs.
        """
        ma = self.mem_agent
        if not ma:
            return "EPISODIC_MEMORY: Not available."

        hits = ma.episodic_search(col, raw_label)
        if not hits:
            return (f"EPISODIC_MEMORY: No past resolutions found for "
                    f"{raw_label!r}.")

        lines = [f"EPISODIC_MEMORY: Past resolutions for {raw_label!r}:"]
        for h in hits[:5]:
            lines.append(
                f"  cluster={h['canonical']!r}  "
                f"count={h['count']}x  "
                f"avg_confidence={h['confidence']:.2f}  "
                f"last={h.get('last_ts','?')[:10]}")
        return "\n".join(lines)

    # 
    #  COLLAPSE AGENT  multi-turn tool-calling loop
    # 




    def _run_collapse_agent(self, gsm: str, col: str, out1: str,
                             ctx_labels: List[str],
                             ctx_counts: Dict[str, int],
                             raw: dict = None
                             ) -> tuple:
        """
        ReAct collapse agent — Reason + Act loop, max 3 turns.

        The agent has 3 tools it can call each turn:
          SEARCH: <query>      — semantic search for alternative cluster candidates
          PICK: <cluster name> — finalise with a specific cluster name
          NEW_CLUSTER: <name>  — register a new cluster (unique cell line / cell type)

        Each turn: model reasons (THOUGHT), calls one tool (ACTION),
        gets the result (OBSERVATION), then reasons again.

        Stops when PICK or NEW_CLUSTER is called, or after MAX_TURNS.
        """
        ma        = self.mem_agent
        gsm_block = format_raw_block(raw) if raw else "(no raw metadata)"
        MAX_TURNS = 3

        # ── Tool implementations ──────────────────────────────────────────────
        def tool_search(query: str) -> str:
            """Search cluster DB and return specificity-ranked candidates."""
            if not ma or not ma.is_ready(col):
                return "OBSERVATION: memory not available"
            raw_hits = ma.semantic_search(col, query.strip(), k=10)
            # also try direct cluster lookup
            direct = ma.cluster_lookup(col, query.strip())
            if direct and not any(l == direct for l, _ in raw_hits):
                raw_hits = [(direct, 1.0)] + raw_hits
            ranked = _rank_candidates_by_specificity(query.strip(), raw_hits)
            if not ranked:
                return f"OBSERVATION: no candidates found for {query!r}"
            lines = [f"OBSERVATION: candidates for {query!r} (ranked by specificity):"]
            for cl, sim, sc in ranked[:6]:
                lines.append(f"  {cl!r}  sim={sim:.3f}  specificity={sc:.1f}")
            return "\n".join(lines)

        def tool_pick(cluster_name: str) -> tuple:
            """Validate and return a cluster name."""
            cn = cluster_name.strip().rstrip(".")
            if ma and ma.is_cluster_name(col, cn):
                return cn, True, "react_pick"
            # try remap
            if ma:
                remapped = ma.cluster_lookup(col, cn)
                if remapped and ma.is_cluster_name(col, remapped):
                    return remapped, True, "react_pick+remap"
            return None, False, "react_pick_invalid"

        def tool_new_cluster(name: str) -> tuple:
            """Register a new cluster for a unique entity."""
            cn = name.strip().rstrip(".")
            _is_valid = (len(cn) >= 4 and not is_ns(cn) and
                         not any(w in cn.lower() for w in
                                 ("unknown","unspecified","n/a","na","none",
                                  "other","not ","mixed")))
            if not _is_valid:
                return None, False, "react_new_cluster_rejected"
            if ma:
                ma.register_new_cluster(col, cn, out1, self._log)
            return cn, True, "react_new_cluster"

        # ── Build initial context (pre-loaded, shown once) ────────────────────
        preload_gse      = self._tool_gse_context(col, ctx_labels, ctx_counts)
        preload_episodic = self._tool_episodic(col, out1)
        # If out1 is still NS, seed initial search from GSE context
        # instead of searching for "Not Specified"
        _search_seed = out1
        if is_ns(out1) and ctx_labels:
            _dom_ctx = max(ctx_counts, key=ctx_counts.get) if ctx_counts else None
            if _dom_ctx:
                _search_seed = _dom_ctx
        initial_search   = tool_search(_search_seed)

        system_prompt = (
            f"You are a biomedical label collapse agent for the field: {col}.\n"
            f"Your job: map the extracted label to the MOST SPECIFIC matching cluster "
            f"in the vocabulary, or register a NEW_CLUSTER if it is unique.\n\n"
            f"AVAILABLE TOOLS (call exactly one per turn):\n"
            f"  SEARCH: <query>       search for cluster candidates\n"
            f"  PICK: <cluster name>  select a validated cluster name\n"
            f"  NEW_CLUSTER: <name>   create a new cluster (unique cell line/cell type only)\n\n"
            f"RULES:\n"
            f"1. SPECIFICITY: always pick the most specific match.\n"
            f"   A cell type is NOT the organ it lives in.\n"
            f"   A subtype is NOT the general category it belongs to.\n"
            f"2. If top candidate looks wrong: SEARCH with a better query.\n"
            f"3. If nothing fits after searching: NEW_CLUSTER for unique cell lines/types.\n"
            f"4. For vague labels (unknown, mixed, other): PICK: NO_MATCH\n"
            f"5. FORMAT each turn:\n"
            f"   THOUGHT: <one sentence reasoning>\n"
            f"   ACTION: SEARCH/PICK/NEW_CLUSTER: <value>\n"
        )

        # Turn 0 context shown to model
        _label_hint = (
            " (extracted from raw text)\n\n" if not is_ns(out1) else
            " (raw extraction returned Not Specified — "
            "use GSE context and sibling labels above to infer)\n\n"
        )
        context = (
            f"EXPERIMENT: {self._gse_block}"
            f"SAMPLE {gsm}: {gsm_block[:300]}\n\n"
            f"GSE CONTEXT:\n{preload_gse}\n\n"
            f"PAST RESOLUTIONS:\n{preload_episodic}\n\n"
            f"LABEL TO COLLAPSE: {out1!r}{_label_hint}"
            f"{initial_search}\n\n"
            f"Now reason and act. Start with THOUGHT:"
        )

        messages = [
            {"role": "system",  "content": system_prompt},
            {"role": "user",    "content": context},
        ]

        # ── ReAct loop ────────────────────────────────────────────────────────
        for turn in range(MAX_TURNS):
            # Retry up to 3 times on empty response — Ollama may still be
            # loading gemma2:9b from VRAM after an idle period
            response = ""
            for _llm_attempt in range(3):
                response = self._llm_chat(messages, max_tokens=120)
                if response:
                    break
                wait_s = 5 * (_llm_attempt + 1)
                self._log(f"  [REACT WARN] {gsm} {col}: empty response (attempt {_llm_attempt+1}/3) — waiting {wait_s}s for model to load")
                if not ollama_server_ok(self.url):
                    self._log(f"  [REACT WARN] Ollama not reachable — waiting {wait_s}s")
                time.sleep(wait_s)
            if not response:
                self._log(f"  [REACT ERROR] {gsm} {col}: no response after 3 attempts — Ollama may be overloaded")
                break

            self._log(f"  [REACT t{turn+1}] {gsm} {col}: {response[:150]}")
            messages.append({"role": "assistant", "content": response})

            resp_up = response.upper()

            # ── Parse ACTION ──────────────────────────────────────────────────
            action_line = ""
            for line in response.splitlines():
                if line.strip().upper().startswith("ACTION:"):
                    action_line = line.strip()[len("ACTION:"):].strip()
                    break
            if not action_line:
                # model may have skipped "ACTION:" prefix
                for line in response.splitlines():
                    lu = line.strip().upper()
                    if (lu.startswith("SEARCH:") or lu.startswith("PICK:") or
                            lu.startswith("NEW_CLUSTER:")):
                        action_line = line.strip()
                        break

            if not action_line:
                # no parseable action — treat as done
                break

            action_up = action_line.upper()

            # PICK
            if action_up.startswith("PICK:"):
                val = action_line[len("PICK:"):].strip()
                if val.upper() in ("NO_MATCH", "NOMATCH"):
                    return out1, False, "react_no_match"
                cn, ok, rule = tool_pick(val)
                if ok:
                    return cn, True, rule
                obs = f"OBSERVATION: {val!r} is not a valid cluster name. Try SEARCH or NEW_CLUSTER."
                messages.append({"role": "user", "content": obs})

            # NEW_CLUSTER
            elif action_up.startswith("NEW_CLUSTER:"):
                val = action_line[len("NEW_CLUSTER:"):].strip()
                cn, ok, rule = tool_new_cluster(val)
                if ok:
                    return cn, True, rule
                obs = f"OBSERVATION: {val!r} rejected as new cluster (too vague). Try PICK: NO_MATCH."
                messages.append({"role": "user", "content": obs})

            # SEARCH
            elif action_up.startswith("SEARCH:"):
                query = action_line[len("SEARCH:"):].strip()
                obs   = tool_search(query)
                messages.append({"role": "user", "content": obs})

            else:
                break

        self._log(f"  [REACT] {gsm} {col}: max turns reached — falling to deterministic")
        return out1, False, "react_exhausted"


    def _llm_chat(self, messages: List[dict], max_tokens: int = 200) -> str:
        """Multi-turn chat call used by the collapse agent."""
        if self.watchdog:
            self.watchdog.wait_if_paused()
            self.watchdog.record_call()
        if _OLLAMA_LIB_OK:
            for attempt in range(1, 4):
                try:
                    resp = _ollama_lib.chat(model=self.model, messages=messages,
                                            options={"temperature": 0.0,
                                                     "num_predict": max_tokens,
                                                     "num_ctx": 2048},
                                            stream=False, keep_alive=-1)
                    # Handle both object (new ollama lib) and dict (old lib)
                    if hasattr(resp, "message") and hasattr(resp.message, "content"):
                        return (resp.message.content or "").strip()
                    elif isinstance(resp, dict):
                        return resp.get("message", {}).get("content", "").strip()
                    return ""
                except Exception as _e:
                    _es = str(_e).lower()
                    if "connection refused" in _es or "disconnected" in _es:
                        self._log(f"  [LLM] Ollama disconnected (attempt {attempt})")
                    elif attempt == 1:
                        self._log(f"  [LLM] Error: {str(_e)[:80]}")
                    if attempt == 3: return ""
                    time.sleep(2 * attempt)
            return ""
        url     = self.url.rstrip("/") + "/api/chat"
        payload = {
            "model":  self.model,
            "stream": False,
            "keep_alive": -1,
            "options": {
                "temperature": 0.0,
                "num_predict": max_tokens,
                "num_ctx":     4096,
            },
            "messages": messages,
        }
        for attempt in range(1, 4):
            try:
                resp = _get_session().post(url, json=payload, timeout=150)
                resp.raise_for_status()
                return resp.json()["message"]["content"].strip()
            except Exception:
                _tls.s = None
                if attempt == 3: return ""
                time.sleep(2 * attempt)
        return ""


    def process_all(self, ns_samples: list, raw_map: dict,
                    stop_event: Optional[threading.Event] = None,
                    sample_cb=None,
                    n_threads: int = 1,
                    phase1_results: dict = None) -> list:
        """
        Process every NS sample for this GSE.
        n_threads > 1: samples within this GSE processed in parallel.
        phase1_results: dict {gsm: {col: label}} from Phase 1 — if provided,
            repair_one skips extraction — collapse uses retrieval + LLM reasoning.
        sample_cb(gsm, current, updated, row_dict) called after each sample.
        Returns list of tuples:
          (gsm, gse, gpl, original_labels, updated_labels, row_dict)
        """
        def _do_one(item):
            gsm, gse, gpl, current, row_dict = item
            if stop_event and stop_event.is_set():
                return None
            raw = raw_map.get(gsm, {})
            pre = phase1_results.get(gsm) if phase1_results else None
            try:
                updated = self.repair_one(gsm, current, raw, pre_extracted=pre)
            except Exception as _e:
                # Never let one bad sample kill the whole GSE
                updated = current.copy()
                updated["_agents"] = f"error:{_e}"
                updated["_audit"]  = "{}"
            if sample_cb:
                sample_cb(gsm, current, updated, row_dict)
            return (gsm, gse, gpl, current, updated, row_dict)

        if n_threads <= 1 or len(ns_samples) <= 1:
            # Sequential  llama-cpp path or single sample
            results = []
            for item in ns_samples:
                r = _do_one(item)
                if r is not None:
                    results.append(r)
            return results
        else:
            # Parallel samples within this GSE  Ollama path
            from concurrent.futures import ThreadPoolExecutor as _SE, as_completed as _SC
            results = []
            with _SE(max_workers=n_threads,
                     thread_name_prefix="Sample") as _sex:
                futs = {_sex.submit(_do_one, item): item for item in ns_samples}
                for fut in _SC(futs):
                    if stop_event and stop_event.is_set():
                        break
                    try:
                        r = fut.result()
                        if r is not None:
                            results.append(r)
                    except Exception:
                        pass
            return results


# 
#  WATCHDOG    pauses LLM calls when RAM / VRAM / TEMP  thresholds
#
    # ── Shared throttle config path (GUI writes, Watchdog reads) ──
WATCHDOG_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     ".watchdog_thresholds.json")

def _load_watchdog_config():
    """Load user-set thresholds from disk, or return empty dict."""
    try:
        with open(WATCHDOG_CONFIG_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_watchdog_config(cfg: dict):
    """Save user-set thresholds to disk (atomic write)."""
    tmp = WATCHDOG_CONFIG_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cfg, f, indent=2)
    os.replace(tmp, WATCHDOG_CONFIG_PATH)


class Watchdog:
    CHECK_INTERVAL  = 3
    # ── Fluid scaling thresholds (NEVER pause, scale workers instead) ──
    # Above HIGH: scale down workers.  Below LOW: scale back up.
    RAM_HIGH_PCT    = 50.0;  RAM_LOW_PCT     = 35.0
    CPU_HIGH_PCT    = 90.0;  CPU_LOW_PCT     = 75.0
    VRAM_PAUSE_PCT  = 90.0;  VRAM_RESUME_PCT = 80.0
    # ── Hard pause: protect the machine ──
    RAM_PAUSE_PCT   = 70.0;  RAM_RESUME_PCT  = 50.0
    CPU_PAUSE_PCT   = 95.0;  CPU_RESUME_PCT  = 85.0
    # ── Thermal protection (these still pause — hardware safety) ──
    CPU_TEMP_PAUSE_C  = 88.0;  CPU_TEMP_RESUME_C  = 72.0
    GPU_TEMP_PAUSE_C  = 85.0;  GPU_TEMP_RESUME_C  = 70.0
    # ── Fluid scaling parameters ──
    SCALE_DOWN_FACTOR = 0.5   # multiply current workers by this when above threshold
    SCALE_UP_STEP     = 1     # add this many workers when below threshold
    MIN_WORKERS       = 8     # minimum workers floor
    SCALE_COOLDOWN_S  = 5     # seconds between scale adjustments
    _CONFIG_RELOAD_EVERY = 5  # reload config every N watchdog ticks

    def __init__(self, log_fn=None, stat_fn=None):
        self._log  = log_fn  or (lambda m: None)
        self._stat = stat_fn or (lambda m: None)
        self._gate = threading.Event(); self._gate.set()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._calls: List[float] = []
        self._reason = None
        self._max_workers = 20           # max concurrent GSE workers
        self._last_scale_time = 0.0      # cooldown tracker
        self._config_tick = 0            # reload counter
        self._reload_thresholds()        # apply any saved config on start
        self._thread = threading.Thread(target=self._loop, daemon=True, name="Watchdog")

    def _reload_thresholds(self):
        """Hot-reload thresholds from shared config file (written by GUI)."""
        cfg = _load_watchdog_config()
        if not cfg:
            return
        for key in ("CPU_HIGH_PCT", "CPU_LOW_PCT", "CPU_PAUSE_PCT", "CPU_RESUME_PCT",
                     "VRAM_PAUSE_PCT", "VRAM_RESUME_PCT",
                     "RAM_HIGH_PCT", "RAM_LOW_PCT", "RAM_PAUSE_PCT", "RAM_RESUME_PCT",
                     "CPU_TEMP_PAUSE_C", "GPU_TEMP_PAUSE_C",
                     "SCALE_DOWN_FACTOR", "MIN_WORKERS"):
            if key in cfg:
                setattr(self, key, float(cfg[key]))
        # Live worker count adjustment from GUI
        if "MAX_WORKERS" in cfg:
            new_max = min(int(cfg["MAX_WORKERS"]), len(LABEL_COLS))  # cap at 3
            old_max = getattr(self, "_max_workers", 0)
            if new_max != old_max and new_max >= self.MIN_WORKERS:
                self._max_workers = new_max
                adj_fn = getattr(self, "_adjust_concurrency", None)
                cur = getattr(self, "_target_parallel", old_max)
                if adj_fn and cur > new_max:
                    adj_fn(new_max)
                    self._target_parallel = new_max
                    self._log(f"  🔧 Workers: {cur} → {new_max} (GUI override)")
                elif adj_fn and cur < new_max:
                    adj_fn(new_max)
                    self._target_parallel = new_max
                    self._log(f"  🔧 Workers: {cur} → {new_max} (GUI override)")

    def start(self):    self._thread.start(); return self
    def stop(self):
        self._stop.set()
        self._gate.set()
        self._release_sleep()
    def wait_if_paused(self, timeout: float = 120.0):
        """Block LLM threads while paused. Hard pauses are rare (thermal/OOM only).
        Auto-resumes after timeout (2 min) to prevent silent hang.
        """
        if self._gate.is_set():
            return
        elapsed = 0.0
        interval = 15.0
        while not self._gate.is_set():
            self._log(f"  ⏸  Paused ({self._reason or '?'}) — waiting for resources "
                      f"({elapsed:.0f}s elapsed) …")
            self._gate.wait(timeout=interval)
            elapsed += interval
            if elapsed >= timeout:
                self._log(f"[WATCHDOG] Pause exceeded {timeout:.0f}s — resuming anyway "
                          f"to avoid silent hang.")
                self._gate.set()
                self._reason = None
                return

    def record_call(self):
        with self._lock:
            now = time.time()
            self._calls.append(now)
            self._calls = [t for t in self._calls if now - t <= 60]

    def calls_per_min(self):
        with self._lock:
            now = time.time()
            return len([t for t in self._calls if now - t <= 60])

    def _pause(self, reason, detail):
        if self._gate.is_set():
            self._gate.clear(); self._reason = reason
            self._log(f"⚠️  Watchdog PAUSED: {detail} — LLM calls suspended. "
                      f"Pipeline will auto-resume when resources free up.")
            # Push to GUI progress bar so user sees it
            if self._stat:
                self._stat(f"⏸  PAUSED: {detail}")

    def _resume(self, detail):
        if not self._gate.is_set():
            self._gate.set(); self._reason = None
            self._log(f"✅ Watchdog RESUMED: {detail} — LLM calls restarting.")
            if self._stat:
                self._stat(f"▶  RESUMED: {detail}")

    def _prevent_sleep(self):
        """
        Acquire OS-level sleep/idle inhibitor lock from within this process.
        Held for the entire duration of the watchdog — released when stop() is called.

        Linux (systemd): uses the org.freedesktop.login1 D-Bus Manager.TakeInhibitorLock
        interface which returns a file descriptor. Keeping that fd open holds the lock.
        This is the same mechanism used by Firefox, VLC, etc. during playback.

        Fallback: /sys/power/wake_lock (requires root).
        No-op if neither is available — logs a warning.
        """
        import platform as _plat
        self._inhibit_fd = None   # hold fd to keep lock alive

        if _plat.system() != "Linux":
            return

        # Method 1: systemd D-Bus inhibitor (preferred, no root needed)
        try:
            import subprocess, struct

            # Use gdbus to call TakeInhibitorLock and get back the fd number
            result = subprocess.run(
                ["gdbus", "call", "--system",
                 "--dest", "org.freedesktop.login1",
                 "--object-path", "/org/freedesktop/login1",
                 "--method", "org.freedesktop.login1.Manager.Inhibit",
                 "sleep:idle:handle-lid-switch",
                 "GeoNSRepair",
                 "Overnight biomedical annotation run",
                 "block"],
                capture_output=True, text=True, timeout=5)

            if result.returncode == 0:
                self._log("🔒 Sleep inhibitor acquired (systemd D-Bus) — "
                          "system will not sleep during run")
                # gdbus returns the fd as a unix fd  keep process alive
                # The inhibitor is held as long as this process lives
                return
        except Exception:
            pass

        # Method 2: direct dbus-python (if installed)
        try:
            import dbus
            bus  = dbus.SystemBus()
            mgr  = bus.get_object("org.freedesktop.login1",
                                   "/org/freedesktop/login1")
            iface = dbus.Interface(mgr, "org.freedesktop.login1.Manager")
            fd = iface.Inhibit(
                "sleep:idle:handle-lid-switch",
                "GeoNSRepair",
                "Overnight biomedical annotation run",
                "block")
            # Store the UnixFd object  keeping it in memory holds the lock
            self._inhibit_fd = fd
            self._log("🔒 Sleep inhibitor acquired (dbus-python) — "
                      "system will not sleep during run")
            return
        except Exception:
            pass

        # Method 3: /sys/power/wake_lock (kernel interface, may need root)
        try:
            wl = "/sys/power/wake_lock"
            if os.path.exists(wl):
                with open(wl, "w") as f:
                    f.write("GeoNSRepair")
                self._log("🔒 Sleep inhibitor acquired (/sys/power/wake_lock)")
                return
        except Exception:
            pass

        # Method 4: xdg-screensaver / xset as last resort (X11 only)
        try:
            import subprocess
            subprocess.Popen(["xset", "s", "off", "-dpms"],
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
            self._log("⚠️  Sleep: disabled screensaver via xset "
                      "(screen sleep may still occur)")
            return
        except Exception:
            pass

        self._log("⚠️  Could not acquire sleep inhibitor — "
                  "run: 'systemd-inhibit python3 llm_extractor.py' "
                  "to prevent sleep manually")

    def _release_sleep(self):
        """Release the inhibitor lock when the run finishes."""
        try:
            if self._inhibit_fd is not None:
                self._inhibit_fd.close()
                self._inhibit_fd = None
        except Exception:
            pass
        try:
            wl = "/sys/power/wake_lock"
            wu = "/sys/power/wake_unlock"
            if os.path.exists(wu):
                with open(wu, "w") as f:
                    f.write("GeoNSRepair")
        except Exception:
            pass

    @staticmethod
    def _read_cpu_temp() -> float:
        """Read highest CPU core temperature in °C. Returns 0.0 if unavailable."""
        try:
            temps = psutil.sensors_temperatures()
            if not temps:
                return 0.0
            # Check common sensor names: coretemp (Intel), k10temp (AMD), cpu_thermal (ARM)
            for name in ("coretemp", "k10temp", "cpu_thermal", "zenpower", "acpitz"):
                if name in temps:
                    return max(s.current for s in temps[name] if s.current > 0)
            # Fallback: highest reading across all sensors
            all_readings = [s.current for entries in temps.values()
                            for s in entries if s.current > 0]
            return max(all_readings) if all_readings else 0.0
        except Exception:
            return 0.0

    @staticmethod
    def _read_gpu_temp() -> float:
        """Read GPU temperature in °C via nvidia-smi. Returns 0.0 if unavailable."""
        try:
            out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=temperature.gpu",
                 "--format=csv,noheader,nounits"],
                stderr=subprocess.DEVNULL, text=True, timeout=3)
            vals = [int(v.strip()) for v in out.strip().splitlines() if v.strip()]
            return float(max(vals)) if vals else 0.0
        except Exception:
            return 0.0

    def _loop(self):
        total_ram = psutil.virtual_memory().total / 1e6
        self._prevent_sleep()   # prevent OS sleep for overnight runs
        # Prime the CPU % sampler (first call always returns 0)
        try:
            psutil.cpu_percent(interval=None)
        except Exception:
            pass
        while not self._stop.is_set():
            try:
                # Hot-reload thresholds from GUI config file periodically
                self._config_tick += 1
                if self._config_tick >= self._CONFIG_RELOAD_EVERY:
                    self._config_tick = 0
                    self._reload_thresholds()

                vm      = psutil.virtual_memory()
                ram_pct = vm.percent; ram_mb = vm.used / 1e6
                cpu_pct = psutil.cpu_percent(interval=None)
                vu, vt, vpct = _get_vram_usage(); has_gpu = vt > 0
                cpu_temp = self._read_cpu_temp()
                gpu_temp = self._read_gpu_temp() if has_gpu else 0.0
                cpm   = self.calls_per_min()
                cur_w = getattr(self, "_target_parallel", 0)
                max_w = getattr(self, "_max_workers", cur_w)
                state = "running" if self._gate.is_set() else f"⏸ PAUSED ({self._reason or '?'})"
                wk_str = f"W:{cur_w}/{max_w}" if cur_w > 0 else ""

                # ── Status line with thermal info ──
                temp_str = ""
                if cpu_temp > 0:
                    temp_str += f"CPU:{cpu_temp:.0f}°C "
                if gpu_temp > 0:
                    temp_str += f"GPU:{gpu_temp:.0f}°C "
                if has_gpu:
                    self._stat(f"RAM {ram_mb:.0f}/{total_ram:.0f} MB ({ram_pct:.0f}%) | "
                               f"CPU {cpu_pct:.0f}% | {temp_str}| "
                               f"VRAM {vu:,}/{vt:,} MB ({vpct:.0f}%) | "
                               f"LLM/min:{cpm} | {wk_str} | {state}")
                else:
                    self._stat(f"RAM {ram_mb:.0f}/{total_ram:.0f} MB ({ram_pct:.0f}%) | "
                               f"CPU {cpu_pct:.0f}% | {temp_str}| "
                               f"LLM/min:{cpm} | {wk_str} | {state}")

                # --- FLUID WORKER SCALING (never pause for CPU/RAM, scale down instead) ---
                now_t = time.time()
                adj_fn = getattr(self, "_adjust_concurrency", None)
                current_workers = getattr(self, "_target_parallel", 0)
                cooldown_ok = (now_t - self._last_scale_time) >= self.SCALE_COOLDOWN_S

                # 1) THERMAL — hard pause (hardware safety, non-negotiable)
                if cpu_temp >= self.CPU_TEMP_PAUSE_C and cpu_temp > 0 and self._gate.is_set():
                    self._pause("THERMAL", f"CPU temp {cpu_temp:.0f}°C >= {self.CPU_TEMP_PAUSE_C:.0f}°C")
                elif gpu_temp >= self.GPU_TEMP_PAUSE_C and gpu_temp > 0 and self._gate.is_set():
                    self._pause("THERMAL", f"GPU temp {gpu_temp:.0f}°C >= {self.GPU_TEMP_PAUSE_C:.0f}°C")

                # 2) EXTREME RAM — hard pause (OOM risk)
                elif ram_pct >= self.RAM_PAUSE_PCT and self._gate.is_set():
                    self._pause("RAM", f"RAM at {ram_pct:.0f}% — OOM risk")

                # 3) Resume from hard pause if conditions are safe
                elif not self._gate.is_set():
                    cpu_cool = cpu_temp < self.CPU_TEMP_RESUME_C or cpu_temp == 0
                    gpu_cool = gpu_temp < self.GPU_TEMP_RESUME_C or gpu_temp == 0
                    rok = ram_pct < self.RAM_RESUME_PCT
                    if rok and cpu_cool and gpu_cool:
                        self._resume(f"RAM {ram_pct:.0f}% CPU {cpu_pct:.0f}%")

                # 4) FLUID SCALING — scale down workers when resources are high
                elif cooldown_ok and adj_fn and current_workers > self.MIN_WORKERS:
                    pressure = max(ram_pct >= self.RAM_HIGH_PCT,
                                   cpu_pct >= self.CPU_HIGH_PCT)
                    if pressure:
                        new_n = max(self.MIN_WORKERS,
                                    int(current_workers * self.SCALE_DOWN_FACTOR))
                        if new_n < current_workers:
                            adj_fn(new_n)
                            self._last_scale_time = now_t
                            self._log(f"  ⬇️  Workers: {current_workers} → {new_n} "
                                      f"(RAM {ram_pct:.0f}% CPU {cpu_pct:.0f}%)")

                    # 5) FLUID SCALING — scale up workers when resources are low
                    elif ram_pct < self.RAM_LOW_PCT and cpu_pct < self.CPU_LOW_PCT:
                        max_w = getattr(self, "_max_workers", 210)
                        if current_workers < max_w:
                            new_n = min(max_w,
                                        current_workers + self.SCALE_UP_STEP)
                            if new_n > current_workers:
                                adj_fn(new_n)
                                self._last_scale_time = now_t
                                self._log(f"  ⬆️  Workers: {current_workers} → {new_n} "
                                          f"(RAM {ram_pct:.0f}% CPU {cpu_pct:.0f}%)")
            except Exception:
                pass
            self._stop.wait(self.CHECK_INTERVAL)


# 
#  MAIN PIPELINE
# 



def _build_viz_report(run_dir, res_df, collapse_df, _cols, gse_meta, NS, log_fn):
    import json as _j, os as _os
    if res_df is None or res_df.empty:
        # Try reading the final NS_repaired.csv as fallback
        _ns_path = _os.path.join(run_dir, "NS_repaired.csv")
        if _os.path.isfile(_ns_path):
            try:
                res_df = __import__("pandas").read_csv(_ns_path, dtype=str).fillna("")
                log_fn("  [VIZ] res_df was empty — loaded from NS_repaired.csv")
            except Exception as _re:
                log_fn(f"  [VIZ] No data and could not read NS_repaired.csv: {_re}")
                return
        else:
            log_fn("  [VIZ] No data — NS_repaired.csv not found either. "
                   "This means no samples were resolved in this run.")
            return
    if res_df is None or res_df.empty:
        return

    total = len(res_df)
    gse_ids = res_df["series_id"].dropna().unique().tolist()

    field_stats = {}
    for col in _cols:
        if col not in res_df.columns: continue
        orig = col + "_original"
        was_ns = res_df[orig] == NS if orig in res_df.columns else res_df[col] == NS
        field_stats[col] = {
            "resolved": int((was_ns & (res_df[col] != NS)).sum()),
            "still_ns": int((was_ns & (res_df[col] == NS)).sum()),
        }

    rule_data = {}
    if collapse_df is not None and not collapse_df.empty and "collapse_rule" in collapse_df.columns:
        for col in _cols:
            sub = collapse_df[collapse_df["column"] == col] if "column" in collapse_df.columns else collapse_df
            vc = sub["collapse_rule"].value_counts().head(8)
            rule_data[col] = {"labels": vc.index.tolist(), "values": vc.values.tolist()}

    top_labels = {}
    for col in _cols:
        if col in res_df.columns:
            vc = res_df[col][res_df[col] != NS].value_counts().head(12)
            top_labels[col] = {"labels": [x[:40] for x in vc.index.tolist()], "values": vc.values.tolist()}

    timing_labels, timing_values = [], []
    if "elapsed_s" in res_df.columns and "sample_num" in res_df.columns:
        tdf = res_df[["sample_num","elapsed_s"]].dropna().sort_values("sample_num")
        prev = 0.0
        for _, row in tdf.iterrows():
            dt = round(float(row["elapsed_s"]) - prev, 2)
            prev = float(row["elapsed_s"])
            if dt > 0:
                timing_labels.append(int(row["sample_num"]))
                timing_values.append(dt)
    avg_t = round(sum(timing_values)/len(timing_values), 2) if timing_values else 0

    gse_labels, gse_totals = [], []
    gse_resolved = {col: [] for col in _cols}
    for gse in gse_ids[:40]:
        sub = res_df[res_df["series_id"] == gse]
        gse_labels.append(gse)
        gse_totals.append(len(sub))
        for col in _cols:
            gse_resolved[col].append(int((sub[col] != NS).sum()) if col in sub.columns else 0)

    total_resolved = sum(v["resolved"] for v in field_stats.values())
    collapse_total = len(collapse_df) if collapse_df is not None and not collapse_df.empty else 0

    data = _j.dumps({
        "fields": _cols, "field_stats": field_stats, "rule_data": rule_data,
        "top_labels": top_labels, "gse": {"labels": gse_labels, "totals": gse_totals, "resolved": gse_resolved},
        "timing": {"labels": timing_labels, "values": timing_values, "avg": avg_t},
    })

    # Build section HTML strings safely (no nested f-strings)
    donut_canvases = "".join('<div class="chart-box"><canvas id="donut_' + col + '"></canvas></div>' for col in _cols)
    top_label_sections = "".join(
        '<h2>' + col + ' - top resolved labels</h2><div class="chart-box wide"><canvas id="top_' + col + '"></canvas></div>'
        for col in _cols if col in top_labels and top_labels[col]["labels"]
    )
    rule_canvases = "".join(
        '<div class="chart-box"><canvas id="rules_' + col + '"></canvas></div>'
        for col in _cols if col in rule_data and rule_data.get(col, {}).get("labels")
    )
    rules_section = ('<h2>Collapse rules</h2><div class="chart-row">' + rule_canvases + '</div>') if rule_canvases else ""
    gse_section = ""
    if len(gse_ids) > 1:
        gse_col_canvases = "".join('<div class="chart-box wide"><canvas id="gse_' + col + '"></canvas></div>' for col in _cols)
        gse_section = '<h2>GSE breakdown</h2><div class="chart-box wide"><canvas id="gse_totals"></canvas></div>' + gse_col_canvases

    html = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>GEO NS Repair v2 Report</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#1e1e2e;color:#e0e0f0;font-family:system-ui,sans-serif;padding:24px;line-height:1.5}
h1{color:#a07de0;font-size:20px;font-weight:500;margin-bottom:20px;border-bottom:1px solid #3a3a5e;padding-bottom:10px}
h2{color:#7ebfee;font-size:15px;font-weight:500;margin:28px 0 12px}
.cards{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:24px}
.card{background:#2a2a3e;border-radius:8px;padding:16px 20px;flex:1;min-width:130px}
.card .val{font-size:26px;font-weight:500;color:#a07de0}
.card .lbl{font-size:12px;color:#a0a0c0;margin-top:4px}
.chart-row{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.chart-box{background:#2a2a3e;border-radius:8px;padding:16px;margin-bottom:16px}
.chart-box.wide{grid-column:span 2}
canvas{max-height:300px}
</style></head><body>
<h1>GEO NS Repair v2 - Analysis Report</h1>
<div class="cards">
  <div class="card"><div class="val">TOTAL_VAL</div><div class="lbl">Samples processed</div></div>
  <div class="card"><div class="val">RESOLVED_VAL</div><div class="lbl">NS fields resolved</div></div>
  <div class="card"><div class="val">GSE_VAL</div><div class="lbl">GSE experiments</div></div>
  <div class="card"><div class="val">COLLAPSE_VAL</div><div class="lbl">Collapse events</div></div>
  <div class="card"><div class="val">AVG_VAL s</div><div class="lbl">Avg time / sample</div></div>
</div>
<h2>Resolution summary</h2>
<div class="chart-row">DONUT_CANVASES</div>
<h2>Timing - seconds per sample</h2>
<div class="chart-box wide"><canvas id="timing_chart"></canvas></div>
TOP_LABEL_SECTIONS
RULES_SECTION
GSE_SECTION
<script>
const D = DATA_JSON;
Chart.defaults.color='#a0a0c0';
Chart.defaults.borderColor='#2a2a3e';
const PAL={Tissue:{b:'#7F77DD',bg:'rgba(127,119,221,0.7)'},Condition:{b:'#378ADD',bg:'rgba(55,138,221,0.7)'},Treatment:{b:'#1D9E75',bg:'rgba(29,158,117,0.7)'}};
const p=col=>(PAL[col]||{b:'#888780',bg:'rgba(136,135,128,0.7)'});
D.fields.forEach(col=>{
  const st=D.field_stats[col];if(!st)return;
  new Chart(document.getElementById('donut_'+col),{type:'doughnut',
    data:{labels:['Resolved','Still NS'],datasets:[{data:[st.resolved,st.still_ns],backgroundColor:[p(col).b,'#3a3a4e'],borderWidth:1}]},
    options:{plugins:{legend:{position:'bottom',labels:{font:{size:11}}},title:{display:true,text:col+' ('+( st.resolved+st.still_ns)+' NS rows)',color:'#c0d0ff'}}}
  });
});
if(D.timing.labels.length){
  new Chart(document.getElementById('timing_chart'),{type:'line',
    data:{labels:D.timing.labels,datasets:[{label:'s/sample',data:D.timing.values,borderColor:'#7F77DD',backgroundColor:'rgba(127,119,221,0.1)',pointRadius:2,tension:0.3,fill:true}]},
    options:{plugins:{legend:{display:false},title:{display:true,text:'Avg: '+D.timing.avg+'s/sample',color:'#c0d0ff'}},scales:{x:{ticks:{maxTicksLimit:12}}}}
  });
}
D.fields.forEach(col=>{
  const tl=D.top_labels[col];if(!tl||!tl.labels.length)return;
  const el=document.getElementById('top_'+col);if(!el)return;
  new Chart(el,{type:'bar',
    data:{labels:tl.labels,datasets:[{data:tl.values,backgroundColor:p(col).bg,borderColor:p(col).b,borderWidth:1}]},
    options:{indexAxis:'y',plugins:{legend:{display:false},title:{display:true,text:'Top '+col+' labels',color:'#c0d0ff'}},scales:{y:{ticks:{font:{size:11}}}}}
  });
});
D.fields.forEach(col=>{
  const rl=D.rule_data[col];if(!rl||!rl.labels.length)return;
  const el=document.getElementById('rules_'+col);if(!el)return;
  new Chart(el,{type:'bar',
    data:{labels:rl.labels,datasets:[{data:rl.values,backgroundColor:p(col).bg,borderColor:p(col).b,borderWidth:1}]},
    options:{indexAxis:'y',plugins:{legend:{display:false},title:{display:true,text:col+' collapse rules',color:'#c0d0ff'}},scales:{y:{ticks:{font:{size:11}}}}}
  });
});
if(D.gse.labels.length>1){
  new Chart(document.getElementById('gse_totals'),{type:'bar',
    data:{labels:D.gse.labels,datasets:[{label:'Samples',data:D.gse.totals,backgroundColor:'rgba(136,135,128,0.6)',borderColor:'#888780',borderWidth:1}]},
    options:{plugins:{legend:{display:false},title:{display:true,text:'Samples per GSE',color:'#c0d0ff'}},scales:{x:{ticks:{maxRotation:45,font:{size:10}}}}}
  });
  D.fields.forEach(col=>{
    const el=document.getElementById('gse_'+col);if(!el)return;
    const vals=D.gse.resolved[col];if(!vals)return;
    new Chart(el,{type:'bar',
      data:{labels:D.gse.labels,datasets:[{label:col,data:vals,backgroundColor:p(col).bg,borderColor:p(col).b,borderWidth:1}]},
      options:{plugins:{legend:{display:false},title:{display:true,text:col+' resolved per GSE',color:'#c0d0ff'}},scales:{x:{ticks:{maxRotation:45,font:{size:10}}}}}
    });
  });
}
</script></body></html>"""

    html = (html
        .replace("TOTAL_VAL", f"{total:,}")
        .replace("RESOLVED_VAL", f"{total_resolved:,}")
        .replace("GSE_VAL", f"{len(gse_ids):,}")
        .replace("COLLAPSE_VAL", f"{collapse_total:,}")
        .replace("AVG_VAL", str(avg_t))
        .replace("DONUT_CANVASES", donut_canvases)
        .replace("TOP_LABEL_SECTIONS", top_label_sections)
        .replace("RULES_SECTION", rules_section)
        .replace("GSE_SECTION", gse_section)
        .replace("DATA_JSON", data)
    )

    path = _os.path.join(run_dir, "analysis_report.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    log_fn(" Analysis report    analysis_report.html")


# ══════════════════════════════════════════════════════════════════════════════
#  MULTI-PLATFORM PIPELINE  —  process multiple platforms sequentially
# ══════════════════════════════════════════════════════════════════════════════

def pipeline_multi(config: dict, q: queue.Queue):
    """
    Wrapper that processes multiple platforms sequentially.
    config["platforms"] = [(gpl_id, title, sample_count), ...]
    Each platform reuses the same Ollama server and Memory Agent DB.
    """
    def log(msg):           q.put({"type": "log",      "msg":  msg})
    def prog(pct, label=""): q.put({"type": "progress", "pct":  pct, "label": label})

    platforms = config.get("platforms", [])
    if not platforms:
        log("[ERROR] No platforms selected")
        q.put({"type": "done", "success": False})
        return

    total_platforms = len(platforms)
    total_samples   = sum(p[2] for p in platforms)
    log(f"\n{'='*60}")
    log(f"  MULTI-PLATFORM RUN: {total_platforms} platforms, "
        f"{total_samples:,} total samples")
    log(f"{'='*60}")

    # ETA estimate — Phase 1 (extraction) + Phase 2 (collapse)
    workers = config.get("num_workers") or 8
    p1_s    = total_samples * 0.15 / workers   # ~150ms/sample extraction
    p2_s    = total_samples * 0.20 / workers   # ~200ms/sample collapse
    overhead_s = total_platforms * 120          # DB load + NCBI per platform
    total_s = p1_s + p2_s + overhead_s
    def _fmt_eta(s):
        h, m = int(s // 3600), int((s % 3600) // 60)
        return f"{h}h {m}m" if h > 0 else f"{m}m"
    log(f"  ETA  Phase 1 (extract): ~{_fmt_eta(p1_s)}  |  "
        f"Phase 2 (collapse): ~{_fmt_eta(p2_s)}  |  "
        f"Overhead: ~{_fmt_eta(overhead_s)}")
    log(f"  Total estimated time: ~{_fmt_eta(total_s)} "
        f"({total_samples:,} samples / {workers} workers)")

    # Load GEOmetadb ONCE and share across all platforms
    db_path = config["db_path"]
    log(f"\n🗄  Loading GEOmetadb into RAM (once for all {total_platforms} platforms) …")
    _shared_db_conn = load_db_to_memory(db_path, log)
    log(f"  GEOmetadb loaded — will be reused for all platforms\n")

    completed   = 0
    failed      = []
    all_stats   = []

    for idx, (gpl_id, title, n_samples) in enumerate(platforms, 1):
        log(f"\n{'━'*60}")
        log(f"  [{idx}/{total_platforms}] {gpl_id}: {title[:60]}")
        log(f"  {n_samples:,} samples")
        log(f"{'━'*60}")

        # Build per-platform config — reuse server, model, DB connection
        plat_config = config.copy()
        plat_config["platform"]      = gpl_id
        plat_config["gsm_list_file"] = ""  # force DB-based loading
        plat_config["_multi_mode"]   = True
        plat_config["_multi_idx"]    = idx
        plat_config["_multi_total"]  = total_platforms
        plat_config["_db_mem_conn"]  = _shared_db_conn

        try:
            pipeline(plat_config, q)
            completed += 1
            all_stats.append((gpl_id, n_samples, "OK"))
        except Exception as exc:
            import traceback
            log(f"[ERROR] {gpl_id} failed: {exc}")
            log(traceback.format_exc())
            failed.append(gpl_id)
            all_stats.append((gpl_id, n_samples, f"FAILED: {exc}"))

    # Final summary
    log(f"\n{'='*60}")
    log(f"  MULTI-PLATFORM SUMMARY")
    log(f"{'='*60}")
    log(f"  Completed: {completed}/{total_platforms}")
    if failed:
        log(f"  Failed: {', '.join(failed)}")
    for gpl_id, n, status in all_stats:
        log(f"  {gpl_id:12s}: {n:>8,} samples — {status}")
    log(f"{'='*60}")

    q.put({"type": "done", "success": len(failed) == 0})


def pipeline(config: dict, q: queue.Queue):
    _cols = LABEL_COLS  # overridden to LABEL_COLS_SCRATCH in scratch mode
    def log(msg):           q.put({"type": "log",      "msg":  msg})
    def prog(pct, label=""): q.put({"type": "progress", "pct":  pct, "label": label})

    server_proc = None
    try:
        db_path        = config["db_path"]
        platform_id    = config["platform"]
        model          = config["model"]
        ollama_url     = config["ollama_url"]
        harmonized_dir = config["harmonized_dir"]
        limit          = config["limit"]
        output_dir     = harmonized_dir

        # Unique timestamp for this run  appended to every output/checkpoint
        # so re-runs never overwrite previous results.
        RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")

        #  Structured output folder 
        # Layout:
        #   {harmonized_dir}/
        #     MultiAgentNS_repair_{platform}_results/
        #       {timestamp}/
        #         NS_repaired.csv          final labels for NS rows
        #         full_repaired.csv        full platform with repairs merged
        #         raw_extracted.csv        step-1 only (no context influence)
        #         collapse_report.csv      rows where context changed the raw label
        #         summary.txt              human-readable run stats
        #         checkpoints/             intermediate saves
        #
        # Each platform gets its own named results folder regardless of whether
        # it is a test run (limit) or full run  output is always identifiable.
        # Results saved directly in the platform results folder  no timestamp subdir
        # MultiAgentNS_repair_{platform}_results/
        #   NS_repaired.csv           final labels for NS rows
        #   full_repaired.csv         full platform with repairs merged
        #   collapse_report.csv       every Phase 1.5 change
        #   summary.txt               run statistics
        #  1. Load input data 
        gsm_list_file = config.get("gsm_list_file", "")

        multi_mode = config.get("_multi_mode", False)

        if gsm_list_file and os.path.isfile(gsm_list_file):
            #  Mode B: raw GSM list  annotate from scratch
            # GPL selector is IGNORED  GSMs can be from any platform.
            # Use the GSM list filename as the run identifier.
            gsm_file_stem = os.path.splitext(
                os.path.basename(gsm_list_file))[0].replace(" ", "_")
            platform_id   = f"SCRATCH_{gsm_file_stem}"
            # Resume last incomplete subset, or create a new one
            # Scan ALL existing subset dirs (not just contiguous) via glob
            import glob as _glob
            _existing = sorted(
                _glob.glob(os.path.join(harmonized_dir, "GSM_subset_*_NS_repaired_final_results")))
            _resume_idx = None
            _max_idx = 0
            for _ed in _existing:
                _bn = os.path.basename(_ed)
                try:
                    _idx = int(_bn.split("_")[2])
                except (IndexError, ValueError):
                    continue
                _max_idx = max(_max_idx, _idx)
                # Incomplete = has checkpoint but no final output
                if (os.path.isdir(os.path.join(_ed, "checkpoints"))
                        and not os.path.isfile(os.path.join(_ed, "NS_repaired.csv"))):
                    _resume_idx = _idx
            if _resume_idx is not None:
                _subset_idx = _resume_idx
                log(f"  Resuming incomplete run: GSM_subset_{_subset_idx}")
            else:
                _subset_idx = _max_idx + 1
            run_dir  = os.path.join(harmonized_dir,
                         f"GSM_subset_{_subset_idx}_NS_repaired_final_results")
            ckpt_dir = os.path.join(run_dir, "checkpoints")
            for _d in (run_dir, ckpt_dir):
                os.makedirs(_d, exist_ok=True)
            log(f"\n📁 Results folder: {run_dir}")
            log(f"📂 Loading GSM list from {os.path.basename(gsm_list_file)} …")
            prog(2, "Loading GSM list")
            target = load_gsm_list(gsm_list_file, platform_id)
            if target.empty:
                log("[ERROR] No valid GSM IDs found in file")
                q.put({"type": "done", "success": False}); return
            log(f"  {len(target):,} GSMs loaded — Tissue, Condition, Treatment set to NS")
            log(f"  Mode: ANNOTATE FROM SCRATCH (GPL selector ignored — any platform OK)")
            all_dfs = {platform_id: target}

        elif multi_mode or config.get("_db_platform_mode", False):
            #  Mode C: load platform directly from GEOmetadb
            # Used by multi-platform discovery or when no CSV files exist.
            _cols = LABEL_COLS_SCRATCH
            scratch_mode = True
            run_dir  = os.path.join(harmonized_dir,
                                     f"{platform_id}_NS_repaired_final_results")
            ckpt_dir = os.path.join(run_dir, "checkpoints")
            for _d in (run_dir, ckpt_dir):
                os.makedirs(_d, exist_ok=True)
            if multi_mode:
                _midx = config.get("_multi_idx", "?")
                _mtot = config.get("_multi_total", "?")
                log(f"\n📁 [{_midx}/{_mtot}] Results: {run_dir}")
            else:
                log(f"\n📁 Results folder: {run_dir}")
            log(f"📂 Loading {platform_id} samples from GEOmetadb …")
            prog(2, f"Loading {platform_id} from DB")
            _db_conn = config.get("_db_mem_conn")
            _db_conn_owned = False
            if _db_conn is None:
                _db_conn = load_db_to_memory(db_path, log)
                _db_conn_owned = True
            target = load_platform_from_db(platform_id, _db_conn, log)
            if _db_conn_owned:
                _db_conn.close()
            if target.empty:
                log(f"[ERROR] No samples found for {platform_id} in GEOmetadb")
                if not multi_mode:
                    q.put({"type": "done", "success": False})
                return
            log(f"  {len(target):,} GSMs loaded — annotating from scratch")
            all_dfs = {platform_id: target}

        else:
            #  Mode A: harmonized CSV files (standard mode)
            # Create run_dir here — only for repair mode, not scratch mode
            run_dir  = os.path.join(harmonized_dir,
                                     f"{platform_id}_NS_repaired_final_results")
            ckpt_dir = os.path.join(run_dir, "checkpoints")
            for _d in (run_dir, ckpt_dir):
                os.makedirs(_d, exist_ok=True)
            log(f"\n📁 Results folder: {run_dir}")
            log("📂 Loading harmonized label files …")
            prog(2, "Loading files")
            all_dfs = load_all(harmonized_dir)
            if platform_id not in all_dfs:
                # Try loading from GEOmetadb as fallback
                log(f"  {platform_id} CSV files not found — trying GEOmetadb …")
                _db_conn = config.get("_db_mem_conn")
                _db_conn_owned = False
                if _db_conn is None:
                    _db_conn = load_db_to_memory(db_path, log)
                    _db_conn_owned = True
                target = load_platform_from_db(platform_id, _db_conn, log)
                if _db_conn_owned:
                    _db_conn.close()
                if target.empty:
                    log(f"[ERROR] {platform_id} not found in CSVs or GEOmetadb")
                    q.put({"type": "done", "success": False}); return
                _cols = LABEL_COLS_SCRATCH
                scratch_mode = True
                all_dfs = {platform_id: target}
                log(f"  Mode: ANNOTATE FROM SCRATCH (loaded from GEOmetadb)")
            else:
                target = all_dfs[platform_id].copy()
            for gpl, df in all_dfs.items():
                ns_info = " | ".join(
                    f"{c}:{df[c].astype(str).str.strip().str.lower().isin({'not specified','n/a','none','unknown','na','not available','not applicable','unclear','unspecified','missing','undetermined',''}).sum():,}"
                    for c in _cols)
                log(f"  {gpl}: {len(df):,} GSMs  "
                    f"{df['series_id'].nunique():,} GSEs  NS→ {ns_info}")

        #  2. Scrape NCBI metadata 
        gse_ids = target["series_id"].dropna().unique().tolist()
        log(f"\n🌐 Fetching NCBI GEO metadata ({len(gse_ids):,} experiments) …")
        prog(8, "Scraping NCBI GEO")
        def ncbi_prog(pct): prog(8 + int(pct * 0.12), f"NCBI {pct}%")
        gse_meta = scrape_gse_meta(gse_ids, log, ncbi_prog)

        #  3. Build / load Memory Agent
        # Memory DB + cluster files live in the SCRIPT directory (project root),
        # NOT the output directory. This ensures every run (regardless of output
        # location) uses the same shared vocabulary and memory.
        _script_dir  = os.path.dirname(os.path.abspath(__file__))
        mem_db_path  = os.path.join(_script_dir, MEM_DB_NAME)
        llm_mem_dir  = os.path.join(_script_dir, LLM_MEMORY_DIR)
        # Fallback: if not found in script dir, try harmonized_dir (legacy)
        if not os.path.isfile(mem_db_path) and not os.path.isdir(llm_mem_dir):
            mem_db_path = os.path.join(harmonized_dir, MEM_DB_NAME)
            llm_mem_dir = os.path.join(harmonized_dir, LLM_MEMORY_DIR)
        log(f"\n🧠 Memory Agent — DB: {mem_db_path}")
        log(f"  Cluster files: {llm_mem_dir}")
        prog(18, "Building Memory Agent …")
        mem_agent = MemoryAgent(mem_db_path, ollama_url)
        mem_agent.load_cache_all(log_fn=log)   # fast: loads existing embeddings
        stats = mem_agent.stats()   # read DB state BEFORE deciding whether to rebuild

        if os.path.isdir(llm_mem_dir):
            # SQLite DB is the sole source of truth.
            # Only load from .txt files when DB is empty (first ever run).
            # On all subsequent runs the DB already contains everything —
            # including auto-registered new clusters from previous runs.
            # To force a full reload from .txt (e.g. after manual edits):
            #   delete biomedical_memory.db and restart.
            _db_empty = stats.get("clusters", {}) == {}
            if _db_empty:
                log(f"  DB empty — loading cluster vocabulary from .txt files")
                mem_agent.build_from_clusters(llm_mem_dir, log_fn=log)
                log(f"  DB populated — .txt files no longer needed at runtime")
            else:
                log(f"  DB loaded — skipping .txt files "
                    f"({stats.get('clusters',{})} clusters already in DB)")
                mem_agent.load_cache_all(log_fn=log)
        else:
            # No LLM_memory folder and DB empty — embed from input CSVs
            log(f"  ⚠️  LLM_memory/ not found and DB empty — "
                f"building vocab from input CSVs")
            mem_agent.build(all_dfs, log_fn=log)

        stats = mem_agent.stats()
        log(f"  Clusters : {stats.get('clusters',{})}  "
            f"| Semantic: {stats.get('semantic',{})}  "
            f"| Episodic: {stats.get('episodic',{})}  "
            f"| KG: {stats.get('kg_triples',0):,}")

        #  4. Build one GSEContext per experiment 
        log("\n🧠 Building GSEContext memory for every experiment …")
        prog(20, "Building GSE contexts")
        gse_contexts: Dict[str, GSEContext] = {}
        for gse, grp in target.groupby("series_id"):
            ctx  = GSEContext(str(gse))
            meta = gse_meta.get(str(gse), {})
            ctx.set_meta(meta.get("gse_title",  ""),
                         meta.get("gse_summary", ""),
                         meta.get("gse_design",  ""))
            for _, row in grp.iterrows():
                gsm    = str(row.get("gsm", "")).strip()
                labels = {c: str(row.get(c, NS)).strip() for c in _cols}
                ctx.add_sample(gsm, labels, mem_agent=mem_agent)
            gse_contexts[gse] = ctx

        rich_gses = sum(1 for ctx in gse_contexts.values()
                        if any(ctx.labeled_count(c) > 0 for c in _cols))
        log(f"  → {len(gse_contexts):,} experiments built  |  "
            f"{rich_gses:,} have ≥1 existing label")

        #  _cols: set based on mode BEFORE any target inspection
        scratch_mode = bool(
            (config.get("gsm_list_file") and
             os.path.isfile(config.get("gsm_list_file", ""))) or
            config.get("_multi_mode") or
            config.get("_db_platform_mode"))
        _cols = LABEL_COLS_SCRATCH if scratch_mode else LABEL_COLS

        #  4. Identify NS rows and group by GSE 
        # Only check columns that actually exist in target
        # (scratch mode builds all _cols but defensive here)
        existing_label_cols = [c for c in _cols if c in target.columns]
        for c in _cols:
            if c not in target.columns:
                target[c] = NS
        # Case-insensitive NS detection — CSVs may have "NOT SPECIFIED" (uppercase)
        ns_mask = target[_cols].apply(
            lambda c: c.astype(str).str.strip().str.lower().isin(
                {"not specified", "n/a", "none", "unknown", "na",
                 "not available", "not applicable", "unclear", "unspecified",
                 "missing", "undetermined", ""})
        ).any(axis=1)
        ns_df   = target[ns_mask].copy().reset_index(drop=True)
        # Normalise NS values to title case for consistent downstream handling
        for c in _cols:
            if c in ns_df.columns:
                ns_df.loc[ns_df[c].astype(str).str.strip().str.lower().isin(
                    {"not specified", "n/a", "none", "unknown", "na",
                     "not available", "not applicable", "unclear", "unspecified",
                     "missing", "undetermined", ""}), c] = NS
        if limit:
            ns_df = ns_df.head(limit)
            log(f"\n  ⚙  TEST MODE: first {limit} NS rows")

        n = len(ns_df)
        log(f"\n{''*60}")
        log(f"  Platform : {platform_id}")
        log(f"  Total    : {len(target):,} GSMs")
        log(f"  NS rows  : {n:,}  ({100*n/len(target):.1f}%)")
        log(f"{''*60}")
        for col in _cols:
            cnt = ns_df[col].astype(str).apply(is_ns).sum()
            log(f"    {col:<18}: {cnt:,} NS ({100*cnt/max(n,1):.1f}%)")

        # Group by GSE  each worker gets its own bucket
        ns_by_gse: Dict[str, list] = defaultdict(list)
        for _, row in ns_df.iterrows():
            gse = str(row.get("series_id", "")).strip()
            gsm = str(row.get("gsm", "")).strip()
            gpl = str(row.get("platform", platform_id)).strip()
            current = {c: str(row.get(c, NS)).strip() for c in _cols}
            ns_by_gse[gse].append((gsm, gse, gpl, current, row.to_dict()))

        gse_counts = Counter({g: len(s) for g, s in ns_by_gse.items()})
        log(f"\n  NS samples span {len(ns_by_gse):,} GSE experiments")
        log(f"  Largest NS workloads: "
            + " | ".join(f"{g}:{c}" for g, c in gse_counts.most_common(5)))

        #  5. Load GEOmetadb, fetch raw text
        # Reuse pre-loaded connection if passed via config (avoids reloading ~19 GB per platform)
        mem_conn = config.get("_db_mem_conn")
        if mem_conn is None:
            log(f"\n🗄  Loading GEOmetadb into RAM …")
            prog(28, "Loading GEOmetadb")
            mem_conn = load_db_to_memory(db_path, log)
        else:
            log(f"\n🗄  Reusing GEOmetadb from RAM (already loaded)")
            prog(28, "GEOmetadb (cached)")
        log(f"\n🔬 Fetching raw GEO text for {n:,} samples …")
        prog(32, "Fetching raw GEO text")
        gsm_list_fetch = ns_df["gsm"].tolist()
        raw_map  = fetch_gsm_raw(mem_conn, gsm_list_fetch)

        #  Fill series_id for gsm_list mode (Mode B) 
        # GEOmetadb is still open  look up GSMGSE now.
        if config.get("gsm_list_file") and os.path.isfile(config.get("gsm_list_file","")):
            all_gsms_q = target["gsm"].tolist()
            ph = ",".join("?" * len(all_gsms_q))
            try:
                gse_map_df = pd.read_sql_query(
                    f"SELECT gsm, series_id FROM gsm WHERE gsm IN ({ph})",
                    mem_conn, params=all_gsms_q)
                gse_map = dict(zip(gse_map_df["gsm"], gse_map_df["series_id"]))
                target["series_id"] = target["gsm"].map(gse_map).fillna("UNKNOWN")
                all_dfs[platform_id] = target
                # Rebuild ns_df with correct series_id
                ns_df["series_id"] = ns_df["gsm"].map(gse_map).fillna("UNKNOWN")
                found = (target["series_id"] != "UNKNOWN").sum()
                log(f"  Series IDs: {found:,}/{len(target):,} GSMs matched in GEOmetadb")
                # Rebuild gse_ids + gse_meta now that series_id is populated
                # (scratch mode starts with series_id=""  only known after GEOmetadb)
                new_gse_ids = [g for g in target["series_id"].dropna().unique()
                               if g and g != "UNKNOWN" and g != ""]
                if new_gse_ids:
                    missing_meta = [g for g in new_gse_ids if g not in gse_meta]
                    if missing_meta:
                        log(f"  Fetching NCBI metadata for {len(missing_meta)} GSEs …")
                        extra_meta = scrape_gse_meta(missing_meta, log)
                        gse_meta.update(extra_meta)
                    gse_ids = new_gse_ids
            except Exception as _e:
                log(f"  [WARN] Could not look up series_id: {_e}")

        # Only close if we loaded it ourselves (not shared from pipeline_multi)
        if config.get("_db_mem_conn") is None:
            mem_conn.close()

        #  Rebuild gse_contexts now that series_id is correctly populated 
        # In scratch mode series_id was "" at step 4  now it has real GSE IDs.
        if scratch_mode:
            gse_contexts = {}
            for gse, grp in target.groupby("series_id"):
                if not gse or gse == "UNKNOWN": continue
                ctx  = GSEContext(str(gse))
                meta = gse_meta.get(str(gse), {})
                ctx.set_meta(meta.get("gse_title",  ""),
                             meta.get("gse_summary", ""),
                             meta.get("gse_design",  ""))
                for _, row in grp.iterrows():
                    gsm    = str(row.get("gsm", "")).strip()
                    labels = {c: str(row.get(c, NS)).strip() for c in _cols}
                    ctx.add_sample(gsm, labels, mem_agent=mem_agent)
                gse_contexts[gse] = ctx
            log(f"  Rebuilt {len(gse_contexts):,} GSEContexts with real series IDs")
            # Also rebuild ns_by_gse with correct series_ids
            # (was built before GEOmetadb so all samples had gse="")
            # Build gsmgse map from target (already has updated series_id)
            _gsm_gse_map = dict(zip(target["gsm"], target["series_id"]))
            ns_by_gse = defaultdict(list)
            for _, row in ns_df.iterrows():
                gse_r = str(row.get("series_id", "")).strip()
                if not gse_r or gse_r == "UNKNOWN":
                    gse_r = _gsm_gse_map.get(str(row.get("gsm","")), "")
                gsm_r = str(row.get("gsm", "")).strip()
                gpl_r = str(row.get("platform", platform_id)).strip()
                cur_r = {c: str(row.get(c, NS)).strip() for c in _cols}
                ns_by_gse[gse_r].append((gsm_r, gse_r, gpl_r, cur_r, row.to_dict()))
            log(f"  Rebuilt ns_by_gse: {sum(len(v) for v in ns_by_gse.values()):,} "
                f"samples across {len(ns_by_gse):,} GSEs")

        log(f"   → {len(raw_map):,}/{n:,} records in GEOmetadb  "
            f"(GEOmetadb released)")

        #  5b. NCBI fallback  scrape GSMs missing from GEOmetadb 
        missing_gsms = [g for g in gsm_list_fetch if g not in raw_map
                        or not any(raw_map[g].get(k)
                                   for k in ("gsm_title", "source_name",
                                             "characteristics"))]
        if missing_gsms:
            log(f"\n🌐 {len(missing_gsms):,} GSMs missing from GEOmetadb "
                f"— scraping NCBI GEO directly …")
            prog(33, f"Scraping {len(missing_gsms):,} GSMs from NCBI …")
            def gsm_scrape_prog(pct):
                prog(33 + int(pct * 0.04),
                     f"NCBI GSM scrape: {pct}%")
            scraped = scrape_gsm_raw(missing_gsms, log, gsm_scrape_prog)
            # Merge  scraped fills in what GEOmetadb missed
            raw_map.update(scraped)
            n_now = sum(
                1 for g in gsm_list_fetch
                if g in raw_map and any(raw_map[g].get(k)
                   for k in ("gsm_title","source_name","characteristics"))
            )
            log(f"   → {n_now:,}/{n:,} samples now have usable raw text "
                f"({n - n_now:,} still empty after both sources)")
        else:
            log(f"   → All {n:,} samples found in GEOmetadb")

        #  5c. Probe  log first 3 raw blocks so you can see what LLM receives
        log(f"\n🔎 RAW TEXT PROBE — first 3 NS samples:")
        for probe_gsm in gsm_list_fetch[:3]:
            rec = raw_map.get(probe_gsm, {})
            block = format_raw_block(rec)
            log(f"  {probe_gsm}:")
            for line in block.splitlines():
                log(f"    {line.strip()}")

        #  6. Compute parallel slots  one slot = one GSE worker 
        _ext_gb = MODEL_RAM_GB.get(EXTRACTION_MODEL.strip().lower(), 2.0)
        auto_total, auto_gpu, auto_cpu = compute_ollama_parallel(
            model, extra_vram_gb=_ext_gb)
        # Use user-set workers if provided, otherwise auto-detect
        num_parallel  = config.get("num_workers") or auto_total
        free_gb       = psutil.virtual_memory().available / 1e9
        slot_gb       = MODEL_RAM_GB.get(model.strip().lower(), DEFAULT_MODEL_GB)
        src = "user-set" if config.get("num_workers") else "auto-detected"
        if config.get("num_workers"):
            log(f"\n🧮 RAM: {free_gb:.0f} GB  |  model: {slot_gb:.1f} GB  "
                f"|  GSE workers: {num_parallel} (user-set)")
        else:
            log(f"\n🧮 RAM: {free_gb:.0f} GB  |  model: {slot_gb:.1f} GB  "
                f"|  GSE workers: {num_parallel} "
                f"({auto_gpu} GPU + {auto_cpu} CPU, auto-detected)")

        server_proc = config.get("server_proc")
        if server_proc is not None:
            log("  Restarting Ollama with GPU flags …")
            try: server_proc.terminate(); time.sleep(2)
            except Exception: pass
            # Check free VRAM before starting — warn if too low
            _gpus = detect_gpus()
            if _gpus:
                _free = _gpus[0].get("free_vram_gb", 99)
                if _free < 3.0:
                    log(f"[WARN] Only {_free:.1f}GB VRAM free — another process may be"
                        f" holding GPU memory. Run: sudo fuser -k /dev/nvidia0")
                else:
                    log(f"  Free VRAM: {_free:.1f}GB — OK")
            server_proc = start_ollama_server_blocking(log, num_parallel)
        else:
            gpus = detect_gpus()
            if gpus:
                names = " + ".join(f"{g['name']} ({g['vram_gb']}GB)" for g in gpus)
                log(f"  GPU(s): {names}")
            else:
                log("  No GPU detected — running on CPU")

        # CPU swarm DISABLED — causes system freezing
        global _CPU_OLLAMA_ACTIVE, _cpu_server_proc
        _CPU_OLLAMA_ACTIVE = False
        _cpu_server_proc = None
        log("  CPU swarm disabled — GPU-only mode (prevents freezing)")

        watchdog = Watchdog(
            log_fn  = log,
            stat_fn = lambda s: q.put({"type": "watchdog", "msg": s}),
        ).start()
        watchdog._model = model   # for mid-run VRAM upgrade recalc

        # ── SHARED THROTTLE SEMAPHORE — fluid worker scaling across ALL phases ──
        # The Watchdog scales this up/down based on CPU/RAM pressure.
        # Phase 1, 1b, and 2 all acquire this before making LLM calls.
        _throttle_sem       = threading.Semaphore(num_parallel)
        _throttle_lock      = threading.Lock()
        _throttle_current   = [num_parallel]  # mutable — watchdog updates this

        def _throttle_adjust(new_n: int):
            """Scale the shared throttle semaphore up or down. Thread-safe."""
            with _throttle_lock:
                old_n = _throttle_current[0]
                if new_n == old_n:
                    return
                if new_n > old_n:
                    for _ in range(new_n - old_n):
                        _throttle_sem.release()
                else:
                    drained = 0
                    for _ in range(old_n - new_n):
                        got = _throttle_sem.acquire(blocking=False)
                        if got:
                            drained += 1
                        else:
                            break
                    # Adjust target even if not all permits drained —
                    # workers will naturally finish and not re-acquire
                _throttle_current[0] = new_n

        watchdog._adjust_concurrency = _throttle_adjust
        watchdog._target_parallel    = num_parallel
        watchdog._max_workers        = num_parallel

        log(f"\n🐕 Watchdog started  |  {num_parallel} fluid workers (scales {Watchdog.MIN_WORKERS}-{num_parallel})  |"
            f"  scale down at CPU>{Watchdog.CPU_HIGH_PCT:.0f}% / RAM>{Watchdog.RAM_HIGH_PCT:.0f}%"
            f"  |  hard pause only: thermal {Watchdog.CPU_TEMP_PAUSE_C:.0f}°C / {Watchdog.GPU_TEMP_PAUSE_C:.0f}°C or RAM>{Watchdog.RAM_PAUSE_PCT:.0f}%")


        time.sleep(3)
        # Unload stale models from previous runs to free VRAM
        # Skip if _shared_ollama flag is set (another pipeline is using the same server)
        if not config.get("_shared_ollama"):
            log("  Clearing stale models from VRAM …")
            _unload_all_models(ollama_url, log_fn=log)
        else:
            log("  Shared Ollama — skipping model unload")

        # Pre-load extraction model into VRAM before Phase 1 threads fire
        # Without this the first call loads the model causing others to time out
        # Skip if shared Ollama — model is already loaded by the other pipeline
        if not config.get("_shared_ollama"):
            _preload_ok = False
            for _preload_attempt in range(1, 3):
                try:
                    if _OLLAMA_LIB_OK:
                        _ollama_lib.chat(model=EXTRACTION_MODEL,
                                         messages=[{"role":"user","content":"1"}],
                                         options={"num_predict":1,"num_ctx":512},
                                         keep_alive=-1, stream=False)
                        log(f"  {EXTRACTION_MODEL} loaded into VRAM.")
                        _preload_ok = True
                        break
                except Exception as _wu:
                    _wu_s = str(_wu).lower()
                    if _preload_attempt == 1 and ("out of memory" in _wu_s or "cudamalloc" in _wu_s):
                        log(f"  [WARN] Pre-load OOM — unloading all models and retrying …")
                        _unload_all_models(ollama_url, log_fn=log)
                        time.sleep(3)
                    else:
                        log(f"  [WARN] Pre-load failed: {_wu}")
                        break
            if not _preload_ok:
                log(f"  [WARN] Could not pre-load {EXTRACTION_MODEL} — first inference will be slower")
        else:
            log(f"  Shared Ollama — model already loaded, skipping pre-load")
        gpu_status, gpu_vram = check_ollama_gpu(ollama_url)
        if gpu_status == "gpu":
            log(f"\n✅ Ollama confirmed on GPU ({gpu_vram:.1f} GB VRAM)")
        elif gpu_status == "cpu":
            log(f"\n⚠️  WARNING: Ollama running on CPU — will be slow!")
            log(f"   Fix: CUDA_VISIBLE_DEVICES=0 ollama serve")

        #  6b. SCRATCH MODE: Phase 1 extraction pass (runs after num_parallel set) 
        # In annotate-from-scratch mode ALL samples are NS, so GSEContexts
        # are empty  tool_gse_context would return nothing useful.
        # Solution: run Step 1 extraction on EVERY sample in every GSE first,
        # then seed GSEContext.label_counts with those extracted labels.
        # The collapse agents in Phase 2 then see real sibling labels.
        scratch_mode = bool(
            (config.get("gsm_list_file") and
             os.path.isfile(config.get("gsm_list_file", ""))) or
            config.get("_multi_mode") or
            config.get("_db_platform_mode"))
        # Always run Phase 1 bulk extraction — even in repair mode.
        # This extracts all NS samples with gemma2:2b in parallel FIRST,
        # Phase 2 collapse uses same model as extraction (retrieval + LLM reasoning).
        if True:  # was: if scratch_mode:
            q.put({"type": "show_treatment_bar"})  # unhide Treatment row immediately
            phase1_extracted: Dict[str, Dict[str, str]] = {}  # gsm  {col: label}

            # ── Phase 1 checkpoint resume ──────────────────────────────
            _p1_ckpt_path = os.path.join(run_dir, "checkpoints", "phase1_extracted.json")
            _p1_resumed = False
            if os.path.isfile(_p1_ckpt_path):
                try:
                    with open(_p1_ckpt_path) as _f:
                        phase1_extracted = json.load(_f)
                    log(f"\n✅ Phase 1 checkpoint loaded: {len(phase1_extracted):,} samples "
                        f"from {os.path.basename(_p1_ckpt_path)}")
                    _p1_resumed = True
                except Exception as _e:
                    log(f"  [WARN] Could not load Phase 1 checkpoint: {_e} — re-extracting")
                    phase1_extracted = {}

            # Only extract NS samples — already-labeled samples don't need extraction
            _p1_target = ns_df if not scratch_mode else target
            n_phase1_total = len(_p1_target)

            # Filter out samples already in checkpoint
            if _p1_resumed and phase1_extracted:
                _already_done = set(phase1_extracted.keys())
                _p1_target_new = _p1_target[~_p1_target["gsm"].isin(_already_done)]
                n_skipped = n_phase1_total - len(_p1_target_new)
                log(f"  Skipping {n_skipped:,} already-extracted samples, "
                    f"{len(_p1_target_new):,} remaining")
                _p1_target = _p1_target_new

            n_phase1 = len(_p1_target)
            _mode_str = "SCRATCH" if scratch_mode else "REPAIR"
            if n_phase1 == 0:
                log(f"\n✅ {_mode_str} MODE — Phase 1: all {n_phase1_total:,} samples "
                    f"already extracted (checkpoint complete)")
            else:
                log(f"\n🔬 {_mode_str} MODE — Phase 1: extracting {n_phase1:,} samples …")
            log(f"  ({num_parallel} threads — Ollama GPU)")
            prog(22, "Phase 1: raw extraction …")

            # Pre-build one GSEWorker per GSE  reused for all samples in that GSE
            _p1_workers: Dict[str, GSEWorker] = {}
            for gse_ in gse_contexts:
                _p1_workers[gse_] = GSEWorker(
                    gse_, gse_contexts[gse_], model, ollama_url,
                    watchdog, mem_agent=mem_agent, platform=platform_id)
            log(f"  Phase 1 workers ready — {len(_p1_workers)} GSEs: "
                f"{list(_p1_workers.keys())[:5]}")

            def _extract_one_gsm(args):
                """Phase 1: 3 independent per-label LLM agents extract in parallel."""
                gsm_, gse_, raw_ = args
                worker_ = _p1_workers.get(gse_) or GSEWorker(
                    gse_, GSEContext(gse_), model, ollama_url,
                    watchdog, mem_agent=mem_agent, platform=platform_id)
                raw_block = format_sample_for_extraction(raw_)
                if raw_block == "(no metadata)":
                    log(f"  [P1 WARN] {gsm_}: no metadata")
                # Common metadata fields
                _title = str(raw_.get("gsm_title","")).strip()[:80]
                _source = str(raw_.get("source_name","")).strip()[:80]
                _char = str(raw_.get("characteristics","")).replace("\t"," ").strip()[:300]
                _treat = str(raw_.get("treatment_protocol","")).replace("\t"," ").strip()[:200]
                _desc = str(raw_.get("description","")).replace("\t"," ").strip()[:200]
                # GSE experiment context for THIS sample's experiment
                _gse_info = gse_meta.get(gse_, {})
                _gse_ctx = ""
                if _gse_info.get("title"):
                    _gse_ctx += f"Experiment: {_gse_info['title'][:120]}\n"
                if _gse_info.get("summary"):
                    _gse_ctx += f"Summary: {_gse_info['summary'][:250]}\n"

                def _call_one_label(col_):
                    """One LLM call for one label — system prompt (KV-cached) + user metadata."""
                    # System prompt = agent definition (cached by Ollama across calls)
                    sys_prompt_ = _PER_LABEL_SYSTEM_PROMPTS[col_]
                    # User prompt = only this sample's metadata
                    user_ = (_EXTRACT_USER_TEMPLATE
                        .replace("{TITLE}", _title)
                        .replace("{SOURCE}", _source)
                        .replace("{CHAR}", _char))
                    if _desc:
                        user_ += f"\nDescription: {_desc}"
                    if _gse_ctx:
                        user_ += f"\n{_gse_ctx}"
                    text_ = ""
                    for _attempt in range(3):
                        text_ = worker_._llm_with_model(
                            user_, model=EXTRACTION_MODEL,
                            max_tokens=60, system=sys_prompt_)
                        if text_:
                            break
                        time.sleep(3 * (_attempt + 1))
                    return col_, _parse_single_label(text_)

                # Run 3 label agents in parallel (threads — Ollama handles concurrency)
                from concurrent.futures import ThreadPoolExecutor as _TPE_inner
                result = {c: NS for c in _cols}
                with _TPE_inner(max_workers=3, thread_name_prefix="P1L") as _ex:
                    futs = {_ex.submit(_call_one_label, c): c for c in _cols}
                    for f in futs:
                        try:
                            col_r, val_r = f.result()
                            result[col_r] = val_r
                        except Exception as _e:
                            log(f"  [P1 WARN] {gsm_}/{futs[f]}: {_e}")
                log(f"  [P1 RAW] {gsm_}: " +
                    " | ".join(f"{c}={result.get(c,NS)[:30]}" for c in _cols))
                return gsm_, result

            # Build task list — only NS samples in repair mode, all in scratch
            phase1_tasks = []
            if n_phase1 > 0:
              for _, row in _p1_target.iterrows():
                gsm_ = str(row.get("gsm", "")).strip()
                gse_ = str(row.get("series_id", "")).strip()
                raw_ = raw_map.get(gsm_, {})
                phase1_tasks.append((gsm_, gse_, raw_))

            # Run extractions in parallel with per-sample progress + ETA
            from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _ac
            import time as _time
            done_p1 = 0
            t_p1_start = _time.time()
            _p1_max = num_parallel  # define before conditional so Phase 1b can use it
            def _extract_one_gsm_throttled(args):
                """Phase 1 extraction gated by shared throttle semaphore."""
                _throttle_sem.acquire()
                try:
                    return _extract_one_gsm(args)
                finally:
                    _throttle_sem.release()

            if n_phase1 > 0:
             # Phase 1 workers — gated by shared throttle semaphore (fluid scaling)
             with _TPE(max_workers=_p1_max,
                      thread_name_prefix="Phase1") as ex1:
                fut_p1 = {ex1.submit(_extract_one_gsm_throttled, t): t[0]
                          for t in phase1_tasks}
                for fut in _ac(fut_p1):
                    gsm_r = fut_p1[fut]
                    try:
                        gsm_r, extracted = fut.result()
                        phase1_extracted[gsm_r] = extracted
                    except Exception as _p1e:
                        phase1_extracted[gsm_r] = {c: NS for c in _cols}
                        log(f"  [P1 warn] {gsm_r}: {_p1e}")
                    done_p1 += 1
                    _p1_vals = " | ".join(
                        c + "=" + str(phase1_extracted.get(gsm_r, {}).get(c, NS))[:20]
                        for c in _cols)
                    log(f"  P1 {done_p1}/{n_phase1}: {gsm_r} — {_p1_vals}")
                    # Update progress every sample with ETA
                    elapsed_p1 = _time.time() - t_p1_start
                    rate_p1    = done_p1 / elapsed_p1 if elapsed_p1 > 0 else 0
                    rem_p1     = n_phase1 - done_p1
                    eta_p1     = int(rem_p1 / rate_p1) if rate_p1 > 0 else 0
                    eta_str    = str(timedelta(seconds=eta_p1)) if eta_p1 > 0 else ""
                    pct = 22 + int(8 * done_p1 / max(n_phase1, 1))
                    ms_p1 = (elapsed_p1 / done_p1 * 1000) if done_p1 > 0 else 0
                    lat_p1 = f"{ms_p1:.0f}ms" if ms_p1 < 1000 else f"{ms_p1/1000:.1f}s"
                    label = f"Phase 1: {done_p1}/{n_phase1}  {lat_p1}/sample"
                    if eta_str: label += f"  ETA {eta_str}"
                    prog(pct, label)
                    q.put({"type": "stat", "key": "phase1_tick", "val": done_p1})
                    # Periodic Phase 1 checkpoint every 5000 samples
                    if done_p1 % 5000 == 0:
                        _p1_ckpt_periodic = os.path.join(run_dir, "checkpoints", "phase1_extracted.json")
                        try:
                            with open(_p1_ckpt_periodic, "w") as _f:
                                json.dump(phase1_extracted, _f)
                        except Exception:
                            pass

            # Save Phase 1 checkpoint
            _p1_ckpt = os.path.join(run_dir, "checkpoints", "phase1_extracted.json")
            try:
                with open(_p1_ckpt, "w") as _f:
                    json.dump(phase1_extracted, _f)
                log(f"  Phase 1 checkpoint saved: {_p1_ckpt}")
            except Exception as _ce:
                log(f"  [WARN] Could not save Phase 1 checkpoint: {_ce}")
            log(f"  Phase 1 complete — {len(phase1_extracted):,} GSMs extracted")

            # Save Phase 1-only snapshot (before Phase 1b mutates the dict)
            try:
                import copy as _copy_mod
                _p1_only_snap = _copy_mod.deepcopy(phase1_extracted)
                _p1_only_path = os.path.join(run_dir, "checkpoints", "phase1_only_extracted.json")
                with open(_p1_only_path, "w") as _f:
                    json.dump(_p1_only_snap, _f)
                log(f"  Phase 1-only snapshot saved: {_p1_only_path}")
            except Exception as _ce:
                log(f"  [WARN] Could not save Phase 1-only snapshot: {_ce}")

            #  Phase 1b: NS inference via GSEInferencer (KV cache reuse)
            # GSE context sent as SYSTEM prompt → Ollama caches KV tensors
            # → ~40% latency reduction vs old approach (context in user msg)
            _ns_after_p1 = [(gsm_, labs_) for gsm_, labs_ in phase1_extracted.items()
                            if any(is_ns(labs_.get(c, NS)) for c in _cols)]
            if _ns_after_p1:
                log(f"\n🔍 Phase 1b: GSEInferencer — NS inference from GSE context "
                    f"({len(_ns_after_p1):,} samples with NS fields) …")
                log(f"  GSE context as SYSTEM prompt → Ollama KV cache reuse")
                prog(30, "Phase 1b: GSE context inference …")
                _p1b_done = [0]
                _t_p1b = _time.time()
                _p1b_lock = threading.Lock()

                # Build GSM→GSE map
                _p1b_source = ns_df if not scratch_mode else target
                gsm_to_gse_map = dict(zip(_p1b_source["gsm"].astype(str),
                                          _p1b_source["series_id"].astype(str)))

                # Create one GSEInferencer per GSE (system prompt cached per GSE)
                _p1b_inferencers: Dict[str, GSEInferencer] = {}
                for gse_ in set(gsm_to_gse_map.values()):
                    _gi = gse_meta.get(gse_, {})
                    if _gi.get("gse_title") or _gi.get("title"):
                        _p1b_inferencers[gse_] = GSEInferencer(
                            gse_, _gi, ollama_url, watchdog=watchdog, log_fn=log)
                log(f"  {len(_p1b_inferencers):,} GSEInferencers created")

                def _infer_ns_v2(args):
                    gsm_, current_labels = args
                    ns_fields = [c for c in _cols if is_ns(current_labels.get(c, NS))]
                    if not ns_fields:
                        return gsm_, current_labels
                    gse_ = gsm_to_gse_map.get(gsm_, "")
                    inferencer = _p1b_inferencers.get(gse_)
                    if not inferencer:
                        return gsm_, current_labels
                    raw_ = raw_map.get(gsm_, {})
                    return gsm_, inferencer.infer_sample(gsm_, raw_, current_labels, _cols)

                def _infer_ns_v2_throttled(args):
                    """Phase 1b inference gated by shared throttle semaphore."""
                    _throttle_sem.acquire()
                    try:
                        return _infer_ns_v2(args)
                    finally:
                        _throttle_sem.release()

                # Phase 1b checkpoint: save every 2000 samples so restarts
                # don't redo all Phase 1b work (resolved labels persist in
                # phase1_extracted → fewer NS on next _ns_after_p1 scan).
                _P1B_CKPT_EVERY = 2000

                with _TPE(max_workers=_p1_max,
                          thread_name_prefix="P1b") as ex1b:
                    fut_p1b = {ex1b.submit(_infer_ns_v2_throttled, item): item[0]
                               for item in _ns_after_p1}
                    for fut in _ac(fut_p1b):
                        try:
                            gsm_r, updated_labs = fut.result()
                            with _p1b_lock:
                                phase1_extracted[gsm_r] = updated_labs
                        except Exception:
                            pass
                        _p1b_done[0] += 1
                        if _p1b_done[0] % 100 == 0 or _p1b_done[0] == len(_ns_after_p1):
                            _el = _time.time() - _t_p1b
                            _rt = _p1b_done[0] / _el if _el > 0 else 0
                            _eta_1b = int((len(_ns_after_p1) - _p1b_done[0]) / _rt) if _rt > 0 else 0
                            log(f"  P1b {_p1b_done[0]:,}/{len(_ns_after_p1):,} | "
                                f"{1000*_el/_p1b_done[0]:.0f}ms/sample | "
                                f"ETA {_eta_1b}s")
                        # Periodic Phase 1b checkpoint — save every _P1B_CKPT_EVERY
                        if _p1b_done[0] > 0 and _p1b_done[0] % _P1B_CKPT_EVERY == 0:
                            try:
                                with _p1b_lock:
                                    _p1b_snap = dict(phase1_extracted)
                                with open(_p1_ckpt, "w") as _f:
                                    json.dump(_p1b_snap, _f)
                                log(f"  P1b checkpoint saved ({_p1b_done[0]:,} samples)")
                            except Exception as _ce:
                                log(f"  [WARN] P1b checkpoint failed: {_ce}")

                # Count improvement
                _ns_after_p1b = sum(1 for labs in phase1_extracted.values()
                                    if any(is_ns(labs.get(c, NS)) for c in _cols))
                _resolved_p1b = len(_ns_after_p1) - _ns_after_p1b
                log(f"  Phase 1b complete — {_resolved_p1b:,} additional fields resolved from GSE context")
                for c in _cols:
                    _ns_c = sum(1 for labs in phase1_extracted.values() if is_ns(labs.get(c, NS)))
                    log(f"    {c}: {_ns_c:,} still NS")

                # Save checkpoint after P1b
                try:
                    with open(_p1_ckpt, "w") as _f:
                        json.dump(phase1_extracted, _f)
                except Exception:
                    pass

            # ── Skip Phase 2 if requested ──
            if config.get("skip_phase2", False):
                log(f"\n✅ Phase 1 + 1b complete — skipping Phase 2 (skip_phase2=True)")
                log(f"  Phase 1 checkpoint: {_p1_ckpt}")
                _gsm2gse = dict(zip(target["gsm"], target["series_id"]))
                import json as _json2
                _p1_only = os.path.join(run_dir, "checkpoints", "phase1_only_extracted.json")
                _p1_combined = os.path.join(run_dir, "checkpoints", "phase1_extracted.json")
                for src, name in [(_p1_only, "labels_raw.csv"), (_p1_combined, "labels_phase1b.csv")]:
                    if os.path.isfile(src):
                        with open(src) as _f:
                            _data = _json2.load(_f)
                        _rows = [{"gsm": g, "series_id": _gsm2gse.get(g, ""),
                                  **{c: _data.get(g, {}).get(c, NS) for c in _cols}} for g in _data]
                        pd.DataFrame(_rows).to_csv(os.path.join(run_dir, name), index=False)
                        log(f"  Saved {name} ({len(_rows):,} rows)")
                q.put({"type": "done", "success": True})
                return

            # Phase 2 uses same model as extraction — no model swap needed.
            # BioLORD-2023 embeddings run on CPU (sentence-transformers), no VRAM.
            log(f"  Phase 2 collapse: {model} + BioLORD-2023 retrieval (CPU)")

            # Seed GSEContexts with Phase 1 extracted labels as siblings
            # Only seed GSMs that actually belong to this GSE.
            # For Tissue/Condition: normalise via cluster_lookup (vocabulary exists).
            # For Treatment: keep raw extracted label  no vocabulary, raw IS the output.
            # Build GSMGSE map for fast lookup
            gsm_to_gse = dict(zip(target["gsm"], target["series_id"]))
            seeded = 0
            for gsm_, labels in phase1_extracted.items():
                gse_of_gsm = gsm_to_gse.get(gsm_, "")
                ctx_ = gse_contexts.get(gse_of_gsm)
                if ctx_ is None:
                    continue
                for col in _cols:
                    val = labels.get(col, NS)
                    if not is_ns(val):
                        # All fields: normalise casing via cluster_lookup
                        cased = mem_agent.cluster_lookup(col, val) if mem_agent else None
                        val = cased if cased else val
                        ctx_.label_counts[col][val] += 1
                        seeded += 1

            log(f"  Seeded {seeded:,} extracted labels into GSE sibling contexts")
            log(f"  Phase 2: collapse agents now have real sibling context …")

        #  7. Resume from live CSV checkpoint if available
        results = []
        fstats  = {c: {"fixed": 0, "ns": 0} for c in _cols}
        total_f = 0
        _p2_already_done = set()

        # Check for existing live CSV from a previous interrupted run
        _p2_live_path = os.path.join(run_dir, "NS_repaired_live.csv")
        if os.path.isfile(_p2_live_path):
            try:
                _prev_df = pd.read_csv(_p2_live_path)
                _p2_already_done = set(_prev_df["gsm"].astype(str).tolist())
                log(f"\n✅ Phase 2 checkpoint loaded: {len(_p2_already_done):,} samples "
                    f"already repaired in NS_repaired_live.csv")
                # Rebuild fstats from previous results
                for c in _cols:
                    _before = f"{c}_before"
                    _after  = f"{c}_after"
                    if _before in _prev_df.columns and _after in _prev_df.columns:
                        _was_ns = _prev_df[_before].astype(str).str.strip().str.lower().isin(
                            {"not specified", "n/a", "none", "unknown", ""})
                        _now_ok = ~_prev_df[_after].astype(str).str.strip().str.lower().isin(
                            {"not specified", "n/a", "none", "unknown", ""})
                        fstats[c]["fixed"] = int((_was_ns & _now_ok).sum())
                        fstats[c]["ns"]    = int((_was_ns & ~_now_ok).sum())
                total_f = sum(fstats[c]["fixed"] for c in _cols)
            except Exception as _e:
                log(f"  [WARN] Could not load Phase 2 checkpoint: {_e} — starting fresh")
                _p2_already_done = set()

        # Filter out already-done samples from ns_by_gse
        ns_by_gse_rem = {}
        for gse, samples in ns_by_gse.items():
            remaining = [s for s in samples if s[0] not in _p2_already_done]
            if remaining:
                ns_by_gse_rem[gse] = remaining

        n_rem_gses    = len(ns_by_gse_rem)
        n_rem_samples = sum(len(s) for s in ns_by_gse_rem.values())
        n_total_samples = sum(len(s) for s in ns_by_gse.values())

        if _p2_already_done:
            log(f"\n🔧 Repair: {n_rem_samples:,} remaining samples in "
                f"{n_rem_gses:,} GSEs  (resumed — {len(_p2_already_done):,} already done)")
        else:
            log(f"\n🔧 Repair: {n_rem_samples:,} samples in "
                f"{n_rem_gses:,} GSEs  (fresh run)")



        #  8. Launch parallel GSE workers 
        t0           = time.time()
        res_lock     = threading.Lock()
        stop_evt     = threading.Event()
        sample_num   = 0
        gse_done     = 0
        live_fixed   = {c: 0 for c in _cols}
        live_still_ns= {c: 0 for c in _cols}
        total_to_do  = n_rem_samples

        # Live results CSV  written incrementally so nothing is lost on crash
        FLUSH_EVERY   = 100   # flush to disk every N samples
        live_csv_path = os.path.join(run_dir, "NS_repaired_live.csv")
        # If resuming, keep existing CSV and append; otherwise start fresh
        if _p2_already_done:
            # Resuming — existing CSV is valid, header already written
            _flush_header_written = [True]
            _flush_state = [True, len(_p2_already_done)]
            log(f"  Appending to existing {os.path.basename(live_csv_path)} "
                f"({len(_p2_already_done):,} rows already on disk)")
        else:
            if os.path.isfile(live_csv_path):
                try: os.remove(live_csv_path)
                except Exception: pass
            _flush_header_written = [False]
            _flush_state = [False, 0]

        def _flush_results():
            """
            Append unsaved rows to NS_repaired_live.csv then TRIM them from RAM.
            results[] only holds rows not yet flushed — never the full run history.
            This prevents unbounded RAM growth on large platforms (GPL570 = 46k rows).
            """
            if not results:
                return
            df_chunk = pd.DataFrame(results)
            df_chunk.to_csv(live_csv_path,
                            mode="a",
                            header=not _flush_state[0],
                            index=False)
            _flush_state[0] = True
            _flush_state[1] += len(results)
            results.clear()   #  free RAM  rows are now safely on disk

        def _sample_callback(gsm, current, updated, row_dict):
            """Called by each GSEWorker after every individual sample.
            Accumulates results AND updates GUI counters — both happen per sample."""
            nonlocal sample_num, total_f
            with res_lock:
                sample_num += 1

                #  Build result row 
                nfc = 0
                try:
                    audit_map = json.loads(updated.get("_audit", "{}"))
                except Exception:
                    audit_map = {}

                r = {"gsm":       gsm,
                     "series_id": row_dict.get("series_id", ""),
                     "platform":  platform_id,
                     "Gene":      row_dict.get("Gene", "N/A")}
                for col in _cols:
                    fdata = audit_map.get(col, {})
                    orig  = current.get(col, NS)
                    final = updated.get(col, orig)
                    # Clean output: before / after only
                    # Normalise case — "BLOOD" and "Blood" are the same label
                    if final and final != NS and not is_ns(final):
                        final = final.strip().title()
                    r[f"{col}_before"] = orig
                    r[f"{col}_after"]  = final
                    if orig == NS:
                        if final != NS:
                            live_fixed[col]      += 1
                            fstats[col]["fixed"] += 1
                            nfc += 1
                        else:
                            live_still_ns[col]  += 1
                            fstats[col]["ns"]   += 1
                r["fields_fixed"] = nfc
                results.append(r)
                total_f += nfc

                # Flush to disk every FLUSH_EVERY samples so nothing is lost on crash
                if sample_num % FLUSH_EVERY == 0:
                    try:
                        _flush_results()
                    except Exception:
                        pass   # never let a flush error kill the worker

                #  GUI progress update 
                elapsed    = time.time() - t0
                spd        = sample_num / elapsed if elapsed > 0 else 0
                ms_sample  = (elapsed / sample_num * 1000) if sample_num > 0 else 0
                remaining  = total_to_do - sample_num
                eta_s      = int(remaining / spd) if spd > 0 else 0
                eta_str    = str(timedelta(seconds=eta_s))
                pct        = 35 + int(60 * sample_num / total_to_do) \
                             if total_to_do else 95
                total_fixed = sum(live_fixed.values())
                total_ns    = sum(live_still_ns.values())
                # Latency string: show ms if fast, seconds if slow
                if ms_sample < 1000:
                    lat_str = f"{ms_sample:.0f} ms/sample"
                else:
                    lat_str = f"{ms_sample/1000:.1f} s/sample"

                q.put({"type": "progress", "pct": pct,
                       "label": f"{sample_num:,}/{n:,} samples  |  "
                                f"resolved: {total_fixed:,}  |  "
                                f"still NS: {total_ns:,}  |  "
                                f"{lat_str}  |  ETA: {eta_str}"})
                q.put({"type": "stats_live",
                       "sample_num":  sample_num,
                       "total":       n,
                       "fixed":       total_fixed,
                       "still_ns":    total_ns,
                       "gse_done":    gse_done,
                       "gse_total":   n_rem_gses,
                       "speed":       spd,
                       "latency_ms":  ms_sample,
                       "eta":         eta_str,
                       "scratch_mode": scratch_mode,
                       "per_col":     {c: {"fixed": live_fixed[c],
                                           "ns":    live_still_ns[c]}
                                       for c in _cols}})

        log(f"\n▶ n_rem_samples={n_rem_samples}  n_rem_gses={n_rem_gses}  "
            f"ns_by_gse keys={list(ns_by_gse.keys())[:5]}")
        if n_rem_samples > 0:
            gse_list = list(ns_by_gse_rem.items())

            #  8a. ETA estimate (no separate preview  go straight to swarm) 
            avg_s_est = 5.0   # conservative estimate per sample
            eta_total_s = (n_rem_samples / max(num_parallel, 1)) * avg_s_est
            eta_h = int(eta_total_s // 3600)
            eta_m = int((eta_total_s % 3600) // 60)
            log(f"  ETA estimate: {eta_h}h {eta_m}m "
                f"({n_rem_samples:,} samples / {num_parallel} workers)")

                        #  Swarm dispatch: per-GSE CollapseWorkers with working memory
            # Each GSE processed as a unit — samples sequential within GSE,
            # GSEs parallel across thread pool. Working memory accumulates
            # resolutions within each experiment.
            _cw = CollapseWorker(model, ollama_url, mem_agent,
                                 watchdog=watchdog, log_fn=log)
            for _col in _cols:
                _cw.load_cluster_vectors(_col)

            _total_samples = sum(len(samples) for _, samples in gse_list)
            log(f"  CollapseWorker created — per-GSE working memory + retrieval + LLM")
            log(f"  {len(gse_list):,} GSEs, {_total_samples:,} samples to collapse")

            # Process one GSE at a time. Samples within each GSE are sequential
            # so working memory accumulates. Global episodic grows across GSEs.

            def _collapse_one_gse(gse_task):
                gse_id, samples_in_gse = gse_task
                if stop_evt.is_set():
                    return 0

                # Each thread gets its own worker with shared episodic but
                # independent working memory
                # Use the shared _cw (episodic is thread-safe dict reads;
                # working memory is per-GSE and cleared between GSEs)
                gse_ctx = gse_contexts.get(gse_id, GSEContext(gse_id))

                # Begin GSE — initialize working memory with experiment context
                _cw.begin_gse(gse_id, gse_ctx)

                count = 0
                for (gsm, gse_, gpl, current, row_dict) in samples_in_gse:
                    if stop_evt.is_set():
                        break
                    try:
                        raw = raw_map.get(gsm, {})
                        pre = phase1_extracted.get(gsm)
                        raw_extracted = pre if pre else {c: current.get(c, NS) for c in _cols}

                        updated = dict(current)
                        audit_map = {}
                        for col in _cols:
                            raw_label = raw_extracted.get(col, current.get(col, NS))
                            final, collapsed, rule, audit = _cw.collapse_field(
                                gsm=gsm, col=col, raw_label=raw_label,
                                gse_ctx=gse_ctx, raw=raw, platform=platform_id)
                            if final and not is_ns(final):
                                final = final.strip().title()
                            updated[col] = final
                            audit_map[col] = audit

                        updated["_audit"] = json.dumps(audit_map)
                        updated["_agents"] = "CollapseWorker"
                        _sample_callback(gsm, current, updated, row_dict)
                        count += 1
                    except Exception as _sample_exc:
                        log(f"  [WARN] {gsm} collapse failed: {_sample_exc}")
                        # Fall back: keep raw extracted labels
                        _sample_callback(gsm, current, dict(current), row_dict)
                        count += 1

                # End GSE — promote working memory to global episodic
                _cw.end_gse()
                return count

            # Process GSEs: sequential within each GSE (for working memory),
            # but we process ONE GSE at a time to keep working memory coherent.
            # The speedup comes from episodic cache growing — later GSEs
            # resolve most labels without LLM calls.
            log(f"  Processing GSEs sequentially (working memory per GSE)...")
            _gse_done = [0]
            _samples_done = [0]
            _t0_phase2 = time.time()

            for gse_id, samples_in_gse in gse_list:
                if stop_evt.is_set():
                    break
                n = _collapse_one_gse((gse_id, samples_in_gse))
                _gse_done[0] += 1
                _samples_done[0] += n

                # Progress update every 10 GSEs or 500 samples
                if _gse_done[0] % 10 == 0 or _samples_done[0] % 500 < n:
                    elapsed = time.time() - _t0_phase2
                    rate = elapsed / max(_samples_done[0], 1)
                    remaining = _total_samples - _samples_done[0]
                    eta = rate * remaining
                    wm_size = len(_cw._working_mem)
                    ep_size = len(_cw._episodic)
                    pct = 60 + 40 * _samples_done[0] / max(_total_samples, 1)
                    prog(pct, f"Phase 2: {_samples_done[0]:,}/{_total_samples:,} "
                         f"| {_gse_done[0]}/{len(gse_list)} GSEs "
                         f"| episodic={ep_size:,} "
                         f"| {rate*1000:.0f}ms/sample "
                         f"| ETA {eta/60:.0f}min")
                    log(f"  [{pct:.0f}%] {_samples_done[0]:,}/{_total_samples:,} samples "
                        f"| {_gse_done[0]}/{len(gse_list)} GSEs "
                        f"| episodic={ep_size:,} "
                        f"| {rate*1000:.0f}ms/sample "
                        f"| ETA {_fmt_eta(eta)}")

                    with res_lock:
                        # Log progress every 100 samples
                        if sample_num % 100 == 0 or sample_num == total_to_do:
                            elapsed  = time.time() - t0
                            spd      = sample_num / elapsed if elapsed > 0 else 0
                            rem      = total_to_do - sample_num
                            eta_str  = str(timedelta(seconds=int(rem/spd))) \
                                       if spd > 0 else "?"
                            ms_s = (elapsed / sample_num * 1000) if sample_num > 0 else 0
                            lat_s = f"{ms_s:.0f}ms" if ms_s < 1000 else f"{ms_s/1000:.1f}s"
                            log(f"\n  [{sample_num:>7,}/{total_to_do:,}] samples  "
                                f"fixed:{total_f:,}  "
                                f"{lat_s}/sample  ETA:{eta_str}")
                            for col in _cols:
                                f_ = fstats[col]["fixed"]; s_ = fstats[col]["ns"]
                                tot = f_ + s_
                                pct_c = 100 * f_ / tot if tot else 0
                                bar   = "█" * int(pct_c / 5) + "░" * (20 - int(pct_c / 5))
                                log(f"    {col:<18} [{bar}] {pct_c:5.1f}%  "
                                    f"resolved {f_:,}/{tot:,}  still NS {s_:,}")

                        # Checkpoint every CKPT_EVERY resolved NS samples
                        if sample_num > 0 and sample_num % CKPT_EVERY == 0:
                            try:
                                _flush_results()
                                import shutil as _shutil
                                ckpt_path = os.path.join(
                                    ckpt_dir, f"ckpt_{sample_num}.csv")
                                _shutil.copy2(live_csv_path, ckpt_path)
                                log(f"   Checkpoint ({sample_num:,} samples)  "
                                    f"{os.path.basename(ckpt_path)}")
                            except Exception as _ce:
                                log(f"  [WARN] Checkpoint failed: {_ce}")

            stop_evt.set()

        # Final flush  ensure any remaining samples reach disk
        try:
            _flush_results()
            log(f"   Live results flushed  NS_repaired_live.csv  "
                f"({_flush_state[1]:,} rows on disk)")
        except Exception as e:
            log(f"  [WARN] Final flush failed: {e}")

        watchdog.stop()
        # Shutdown CPU Ollama swarm
        if _cpu_server_proc:
            try:
                _cpu_server_proc.terminate()
                log("  🖥️  CPU Ollama stopped")
            except Exception:
                pass
            _cpu_server_proc = None
            _CPU_OLLAMA_ACTIVE = False
        log("\n Watchdog stopped.")

        #  9. Summary 
        log(f"\n{'─'*60}")
        log(f"   REPAIR SUMMARY  {platform_id}")
        log(f"{'─'*60}")
        for col in _cols:
            f_ = fstats[col]["fixed"]; s_ = fstats[col]["ns"]
            orig = f_ + s_
            pct  = 100 * f_ / orig if orig else 0
            log(f"  {col:<18}: fixed {f_:,}/{orig:,} ({pct:.1f}%)  still NS: {s_:,}")
        log(f"{'─'*60}")

        #  10. Save structured outputs — CLEAN SEPARATE FILES
        prog(98, "Saving ")
        # results[] was cleared after each flush to save RAM.
        # All rows are in NS_repaired_live.csv  read from there.
        try:
            _flush_results()   # flush any remaining unflushed rows
        except Exception:
            pass
        if os.path.isfile(live_csv_path):
            try:
                res_df = pd.read_csv(live_csv_path, dtype=str).fillna("")
                if "gsm" in res_df.columns:
                    res_df = res_df[res_df["gsm"] != "gsm"].reset_index(drop=True)
            except Exception as _e:
                log(f"[WARN] Could not read live CSV: {_e}")
                res_df = pd.DataFrame()
        else:
            res_df = pd.DataFrame()
        if "gsm" not in res_df.columns and not res_df.empty:
            log("[WARN] res_df has rows but no gsm column  dropping")
            res_df = pd.DataFrame()

        # Delete checkpoint CSV files
        try:
            import glob as _glob
            ckpt_files = _glob.glob(os.path.join(ckpt_dir, "ckpt_*.csv"))
            for f in ckpt_files:
                os.remove(f)
            if ckpt_files:
                log(f"    Deleted {len(ckpt_files)} checkpoint file(s)")
        except Exception as _e:
            log(f"  [WARN] Could not delete checkpoints: {_e}")

        # ── Build clean DataFrames from the live audit data ──
        # The live CSV has: gsm, series_id, platform, Gene,
        #   Tissue_before, Tissue_after, Condition_before, Condition_after,
        #   Treatment_before, Treatment_after, fields_fixed
        #
        # We produce 3 SEPARATE clean files:
        #   1. labels_before.csv   — gsm, gse, Tissue, Condition, Treatment (before repair)
        #   2. labels_final.csv    — gsm, gse, Tissue, Condition, Treatment (after collapse)
        #   3. labels_raw.csv      — gsm, gse, Tissue, Condition, Treatment (Phase 1 raw = _after)
        #
        # Plus the full audit file and collapse report.

        _out_cols = ["gsm", "series_id"] + list(_cols)

        if not res_df.empty:
            # --- labels_before.csv  (input state — all NS in scratch mode) ---
            before_df = res_df[["gsm", "series_id"]].copy()
            for c in _cols:
                before_df[c] = res_df.get(f"{c}_before", NS)
            before_path = os.path.join(run_dir, "labels_before.csv")
            before_df.to_csv(before_path, index=False)
            log(f"\n labels_before.csv      {len(before_df):,} rows  (input labels)")

            # --- labels_final.csv  (after all collapse/repair phases) ---
            final_df = res_df[["gsm", "series_id"]].copy()
            for c in _cols:
                final_df[c] = res_df.get(f"{c}_after", NS)
            final_path = os.path.join(run_dir, "labels_final.csv")
            final_df.to_csv(final_path, index=False)
            n_resolved = {c: (final_df[c] != NS).sum() for c in _cols}
            log(f" labels_final.csv      {len(final_df):,} rows  "
                f"(resolved: {', '.join(f'{c}={n_resolved[c]}' for c in _cols)})")

            # --- labels_raw.csv / labels_phase1.csv  (Phase 1 extraction only) ---
            import json as _json
            _p1_only_path = os.path.join(run_dir, "checkpoints", "phase1_only_extracted.json")
            _ckpt_path = os.path.join(run_dir, "checkpoints", "phase1_extracted.json")

            # Phase 1 only (before Phase 1b inference)
            _p1_src = _p1_only_path if os.path.isfile(_p1_only_path) else _ckpt_path
            if os.path.isfile(_p1_src):
                with open(_p1_src) as _f:
                    _p1_data = _json.load(_f)
                raw_df = res_df[["gsm", "series_id"]].copy()
                for c in _cols:
                    raw_df[c] = raw_df["gsm"].map(lambda g, _c=c: _p1_data.get(g, {}).get(_c, NS))
                _tag = "Phase 1 only" if _p1_src == _p1_only_path else "Phase 1+1b (no P1-only snapshot)"
                log(f" labels_raw.csv        {len(raw_df):,} rows  ({_tag})")
            else:
                raw_df = final_df.copy()
                log(f" labels_raw.csv        {len(raw_df):,} rows  (no checkpoint — copy of final)")
            raw_path = os.path.join(run_dir, "labels_raw.csv")
            raw_df.to_csv(raw_path, index=False)

            # Phase 1b (after NS inference, before collapse)
            if os.path.isfile(_ckpt_path):
                with open(_ckpt_path) as _f:
                    _p1b_data = _json.load(_f)
                p1b_df = res_df[["gsm", "series_id"]].copy()
                for c in _cols:
                    p1b_df[c] = p1b_df["gsm"].map(lambda g, _c=c: _p1b_data.get(g, {}).get(_c, NS))
                p1b_path = os.path.join(run_dir, "labels_phase1b.csv")
                p1b_df.to_csv(p1b_path, index=False)
                log(f" labels_phase1b.csv    {len(p1b_df):,} rows  (Phase 1 + 1b, before collapse)")

            # --- full_repaired.csv  (complete platform with labels merged) ---
            full_df = target.copy()
            _tmp = res_df.dropna(subset=["gsm"]).set_index("gsm")
            for col in _cols:
                if col not in full_df.columns:
                    full_df[col] = NS
                _after_col = f"{col}_after"
                if _after_col in _tmp.columns:
                    _map = _tmp[_after_col].to_dict()
                    full_df[col] = full_df["gsm"].map(_map).fillna(full_df[col])
            full_path = os.path.join(run_dir, "full_repaired.csv")
            full_df.to_csv(full_path, index=False)
            log(f" full_repaired.csv     {len(full_df):,} rows  (full platform)")

            # --- NS_repaired.csv  (audit trail: before + after + fields_fixed) ---
            audit_cols = ["gsm", "series_id"] + \
                sum([[f"{c}_before", f"{c}_after"] for c in _cols], []) + \
                ["fields_fixed"]
            ns_path = os.path.join(run_dir, "NS_repaired.csv")
            res_df[[c for c in audit_cols if c in res_df.columns]].to_csv(
                ns_path, index=False)
            log(f" NS_repaired.csv      {len(res_df):,} rows  (audit trail)")

        else:
            log("[WARN] No results to save")
            for fname in ("labels_before.csv", "labels_final.csv",
                          "labels_raw.csv", "full_repaired.csv", "NS_repaired.csv"):
                pd.DataFrame(columns=_out_cols).to_csv(
                    os.path.join(run_dir, fname), index=False)

        # ── Novel label check ──
        log(f"\n Checking for novel labels (not in original vocabulary) ")
        orig_vocab = {
            col: set(v for v in target[col] if v and v != NS)
            if col in target.columns else set()
            for col in _cols}

        novel_rows = []
        for _, row in (res_df.iterrows() if not res_df.empty else iter([])):
            for col in _cols:
                before_val = str(row.get(f"{col}_before", NS)).strip()
                after_val  = str(row.get(f"{col}_after", NS)).strip()
                if not is_ns(before_val):
                    continue
                if is_ns(after_val):
                    continue
                if after_val not in orig_vocab[col]:
                    novel_rows.append({
                        "gsm":            row.get("gsm", ""),
                        "series_id":      row.get("series_id", ""),
                        "field":          col,
                        "resolved_label": after_val,
                    })

        if novel_rows:
            novel_df   = pd.DataFrame(novel_rows)
            novel_path = os.path.join(harmonized_dir, "novel_labels.csv")
            novel_df.to_csv(novel_path, index=False)
            log(f"    {len(novel_rows):,} resolved label(s) are NOT in the original vocabulary:")
            for col in _cols:
                sub = novel_df[novel_df["field"] == col]
                if sub.empty:
                    continue
                log(f"  {col}: {sub['resolved_label'].nunique():,} unique novel label(s) "
                    f"across {len(sub):,} sample(s)")
                for lbl, cnt in sub["resolved_label"].value_counts().head(10).items():
                    log(f"    [{cnt:>4}]  {lbl}")
            log(f"   Saved to novel_labels.csv for review")
        else:
            log(f"   All resolved labels are within the original vocabulary  no novel labels.")

        #  collapse_report.csv  rows where context match changed the raw label
        collapse_rows = []
        gse_title_map = {g: (gse_meta.get(g) or {}).get("gse_title", "") for g in gse_ids}
        for _, row in (res_df.iterrows() if not res_df.empty else iter([])):
            for col in _cols:
                raw_v = row.get(f"{col}_raw", row.get(f"{col}_before", ""))
                final = row.get(col, row.get(f"{col}_after", NS))
                # Normalise: bool from live run, or string "True"/"False" if somehow mixed
                coll  = bool(row.get(f"{col}_collapsed", False)) \
                        if not isinstance(row.get(f"{col}_collapsed"), str) \
                        else str(row.get(f"{col}_collapsed")).strip().lower() == "true"
                ctx_lb = row.get(f"{col}_ctx_labels", "")
                rule   = row.get(f"{col}_collapse_rule", "")
                if coll and final and final != NS:
                    gse = row.get("series_id", "")
                    collapse_rows.append({
                        "gsm":            row.get("gsm", ""),
                        "series_id":      gse,
                        "gse_title":      gse_title_map.get(gse, ""),
                        "column":         col,
                        "raw_extracted":  raw_v,
                        "collapsed_to":   final,
                        "collapse_rule":  rule,
                        "context_labels": ctx_lb,
                    })
        collapse_df   = pd.DataFrame(collapse_rows)
        collapse_path = os.path.join(run_dir, "collapse_report.csv")
        collapse_df.to_csv(collapse_path, index=False)
        n_collapsed = len(collapse_df)
        def _count(rule):
            return int((collapse_df["collapse_rule"] == rule).sum()) \
                   if not collapse_df.empty else 0
        def _count_prefix(pfx):
            return int(collapse_df["collapse_rule"].str.startswith(pfx).sum()) \
                   if not collapse_df.empty else 0
        n_gse_ctx   = _count("gse_dominant") + _count_prefix("gse_")
        n_semantic  = _count_prefix("semantic_direct") + _count_prefix("semantic_llm")
        n_cluster   = _count("direct_cluster_map") + _count_prefix("cluster")
        n_episodic  = _count_prefix("episodic")
        n_kg        = _count_prefix("kg_")
        n_agent     = _count_prefix("agent")
        n_det       = n_collapsed - n_gse_ctx - n_semantic - n_cluster - n_episodic - n_kg - n_agent
        log(f" Collapse report    collapse_report.csv  "
            f"({n_collapsed:,} collapses: {n_gse_ctx:,} GSE-ctx / "
            f"{n_semantic:,} semantic / {n_cluster:,} cluster / "
            f"{n_agent:,} agent / {n_det:,} other)")

        # -b  unique_outside_clusters_labels.txt 
        # Labels resolved from NS but not matching any cluster in LLM_memory.
        # Expected to be very rare  cluster files cover all 4 human platforms.
        outside_mask = (
            collapse_df["collapse_rule"].str.endswith("+gate_rejected", na=False)
            if not collapse_df.empty else pd.Series([], dtype=bool)
        )
        if outside_mask.any():
            out_df      = collapse_df[outside_mask].copy()
            out_path    = os.path.join(harmonized_dir, "unique_outside_clusters_labels.txt")

            lines_out   = [
                "=" * 72,
                "  Unique Labels Outside LLM_memory Clusters",
                f"  Platform : {platform_id}",
                f"  Run      : {RUN_TS}",
                f"  Total    : {len(out_df):,} sample(s)",
                "=" * 72,
                "",
            ]
            # Group by col + resolved label for a clean summary
            for col in _cols:
                sub = out_df[out_df["column"] == col] if "column" in out_df.columns                       else out_df[out_df.get("col", out_df.columns[0]) == col]
                if sub.empty:
                    continue
                lines_out.append(f"  {col}  ({len(sub):,} samples)")
                lines_out.append("  " + "-" * 60)
                for lbl, grp in sub.groupby("collapsed_to"):
                    gses = grp["series_id"].nunique()
                    lines_out.append(
                        f"    {lbl:<50}  "
                        f"{len(grp):>5} sample(s)  "
                        f"{gses} GSE(s)")
                lines_out.append("")

            with open(out_path, "w", encoding="utf-8") as fh:
                fh.write("\n".join(lines_out) + "\n")



            n_outside = len(out_df)
            log(f"    Outside-cluster labels  {os.path.basename(out_path)}  "
                f"({n_outside:,} samples, "
                f"{out_df['collapsed_to'].nunique() if 'collapsed_to' in out_df.columns else '?'} unique)")
        else:
            log(f"   All resolved labels match a cluster in LLM_memory")

        #  new_clusters_report.csv  all clusters created during this run
        new_cluster_entries = mem_agent.get_new_cluster_log()
        n_new_clusters = len(new_cluster_entries)
        if new_cluster_entries:
            import pandas as _pd_nc
            nc_df = _pd_nc.DataFrame(new_cluster_entries)
            nc_path = os.path.join(run_dir, "new_clusters_report.csv")
            nc_df.to_csv(nc_path, index=False)
            log(f"  New clusters created   : {n_new_clusters:,} "
                f"(saved to new_clusters_report.csv)")
            # Also write a human-readable summary per column
            nc_txt_path = os.path.join(run_dir, "new_clusters_report.txt")
            nc_lines = [
                "=" * 62,
                "  New Clusters Created This Run",
                f"  Platform  : {platform_id}",
                f"  Total     : {n_new_clusters:,}",
                "=" * 62, "",
            ]
            for col in _cols:
                col_entries = [e for e in new_cluster_entries if e["col"]==col]
                if not col_entries: continue
                nc_lines.append(f"  {col}  ({len(col_entries)} new clusters)")
                nc_lines.append("  " + "-" * 58)
                for e in col_entries:
                    nc_lines.append(
                        f"    {e['cluster_name']:<50}  "
                        f"from: {e['raw_label']:<40}  "
                        f"{e['ts']}")
                nc_lines.append("")
            nc_lines += [
                "Note: these clusters were added to biomedical_memory.db",
                "      and embedded into biomedical_memory.db.",
                "      They are immediately active for future runs.",
            ]
            with open(nc_txt_path, "w", encoding="utf-8") as fh:
                fh.write("\n".join(nc_lines) + "\n")
            log(f"  New clusters detail    : new_clusters_report.txt")
        else:
            log(f"  No new clusters created — all labels matched existing vocabulary")
            n_new_clusters = 0

        #  summary.txt  human-readable run statistics
        summary_lines = [
            "=" * 62,
            f"  GEO NS Repair  v2    Run Summary",
            f"  Platform  : {platform_id}",
            f"  Timestamp : {RUN_TS}",
            f"  Model     : {model}",
            "=" * 62,
            f"  Total GSMs in platform : {len(target):,}",
            f"  NS rows processed      : {n:,}",
            f"  GSE experiments        : {len(ns_by_gse):,}",
            "",
        ]
        for col in _cols:
            f_ = fstats[col]["fixed"]; s_ = fstats[col]["ns"]
            tot = f_ + s_
            pct = 100 * f_ / tot if tot else 0
            summary_lines += [
                f"  {col}",
                f"    Resolved   : {f_:,} / {tot:,}  ({pct:.1f}%)",
                f"    Still NS   : {s_:,}",
            ]
        mem_stats = mem_agent.stats()
        summary_lines += [
            "",
            f"  Memory Agent DB    : {MEM_DB_NAME}",
            f"  Semantic labels    : {mem_stats.get('semantic',{})}",
            f"  Episodic log       : {mem_stats.get('episodic',{})}",
            f"  KG triples         : {mem_stats.get('kg_triples',0):,}",
            "",
            f"  Phase 1.5 collapses total   : {n_collapsed:,}",
            f"    GSE context -> cluster   : {n_gse_ctx:,}",
            f"    Cluster map direct       : {n_cluster:,}",
            f"    Semantic LLM -> cluster  : {n_semantic:,}",
            f"    Episodic memory          : {n_episodic:,}",
            f"    Knowledge graph          : {n_kg:,}",
            f"    Deterministic (rules)    : {n_det:,}",
            "",
            "Output files",
            f"  NS_repaired.csv       full audit for every NS row",
            f"  full_repaired.csv     complete platform with repairs merged",
            f"  raw_extracted.csv     step-1 labels only (no context influence)",
            f"  collapse_report.csv   every Phase 1.5 label change (rule tagged)",
            f"  unique_outside_clusters_labels.txt   labels outside LLM_memory (input folder)",
            f"  new_clusters_report.csv   new clusters created during this run",
            f"  new_clusters_report.txt   human-readable new cluster summary",
            f"  checkpoints/          deleted after final CSV written",
            "",
            f"  New clusters added to LLM_memory : {n_new_clusters:,}",
            f"  biomedical_memory.db updated     : yes (live during run)",
            f"  LLM_memory/*.txt                 : not modified (DB is sole source of truth)",
            "=" * 62,
        ]
        summary_path = os.path.join(run_dir, "summary.txt")
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("\n".join(summary_lines) + "\n")
        log(f" Summary            summary.txt")
        log(f"\n All outputs in:  {run_dir}")
        log(f"\n  NS fields recovered    : {total_f:,}")
        log(f"  Phase 1.5 collapses    : {n_collapsed:,}  "
            f"({n_gse_ctx:,} GSE-ctx / {n_cluster:,} cluster / "
            f"{n_semantic:,} semantic / {n_det:,} det.)")

        # Generate HTML visualization report
        try:
            _build_viz_report(run_dir, res_df, collapse_df, _cols, gse_meta, NS, log)
        except Exception as _viz_e:
            log(f"  [WARN] Visualization skipped: {_viz_e}")
        prog(100, "Done!")
        log("\nAll done ")
        if not config.get("_multi_mode"):
            q.put({"type": "done", "success": True, "run_dir": run_dir})

    except Exception as exc:
        import traceback
        log(f"\n[ERROR] {exc}\n{traceback.format_exc()}")
        if not config.get("_multi_mode"):
            q.put({"type": "done", "success": False})
        else:
            raise   # re-raise so pipeline_multi catches it
    finally:
        # Don't kill Ollama in multi-mode — next platform needs it
        if not config.get("_multi_mode"):
            sp = config.get("server_proc") or server_proc
            if sp:
                try: os.killpg(os.getpgid(sp.pid), signal.SIGTERM)
                except Exception:
                    try: sp.terminate()
                    except Exception: pass



# 
#  GUI
# 
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("GEO NS Repair  v2    GSE-Context-Aware Raw Extraction")
        self.configure(bg=BG)
        self.resizable(True, True)
        self.minsize(960, 760)
        self._q           = queue.Queue()
        self._running     = False
        self._server_proc = None
        self._thread      = None
        self._build_ui()
        self._check_env_async()
        self.after(200, self._poll_queue)
        self.after(800, lambda: self._detect_workers(silent=True))  # auto-scan on startup
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _on_close(self):
        """Clean up Ollama process then exit."""
        # Stop any running pipeline
        if self._running:
            try: self._q.put({"type": "stop"})
            except Exception: pass
        # Kill the Ollama process this program started
        _kill_ollama()
        self.destroy()

    #  UI Construction 
    def _build_ui(self):
        # Header
        hdr = tk.Frame(self, bg=ACCENT, pady=10)
        hdr.pack(fill=tk.X)
        tk.Label(hdr, text="GEO NS Repair  v2", bg=ACCENT, fg="white").pack()
        tk.Label(hdr,
                 text="GSE-context-aware extraction  -  "
                      "RAG vocab index (all platforms)  -  "
                      "MemGPT rolling memory  -  GPU-accelerated", bg=ACCENT, fg="#d0c0ff").pack()

        # Main layout
        main = tk.Frame(self, bg=BG)
        main.pack(fill=tk.BOTH, expand=True, padx=14, pady=10)
        main.columnconfigure(0, weight=0, minsize=360)
        main.columnconfigure(1, weight=1)
        main.rowconfigure(0, weight=1)

        left  = tk.Frame(main, bg=BG); left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        right = tk.Frame(main, bg=BG); right.grid(row=0, column=1, sticky="nsew")
        right.rowconfigure(1, weight=1); right.columnconfigure(0, weight=1)

        def card(parent, title):
            outer = tk.Frame(parent, bg=BG2, bd=1, relief="groove")
            outer.pack(fill=tk.X, pady=(0, 8))
            outer.columnconfigure(0, weight=1)
            tk.Label(outer, text=f" {title} ",
                     bg=BG2, fg=ACCENT2, anchor="w").grid(row=0, column=0, sticky="w", padx=4, pady=(4,0))
            f = tk.Frame(outer, bg=BG2, padx=10, pady=8)
            f.grid(row=1, column=0, sticky="ew")
            return f

        #  Data card 
        ca1 = card(left, "Data")
        tk.Label(ca1, text="Harmonized Labels Folder  (GEOmetadb must be in the same folder)",
                 bg=BG2, fg=FG2).grid(row=0, column=0, sticky="w")
        self._var_dir = tk.StringVar(value=SCRIPT_DIR)
        tk.Entry(ca1, textvariable=self._var_dir, bg=BG3, fg=FG, insertbackground=FG,
                 relief="flat", width=28
                 ).grid(row=1, column=0, sticky="ew", pady=(2, 4))
        tk.Button(ca1, text="Browse...", command=self._browse_dir,
                  bg=BG3, fg=FG, relief="flat", cursor="hand2",
                  activebackground=ACCENT, activeforeground="white"
                  ).grid(row=1, column=1, padx=(6, 0))
        ca1.columnconfigure(0, weight=1)
        self._lbl_db_status = tk.Label(ca1, text="GEOmetadb: scanning...",
                                       bg=BG2, fg=FG2)
        self._lbl_db_status.grid(row=2, column=0, columnspan=2, sticky="w")

        #  Platform card  — Species & Platform discovery
        ca2 = card(left, "  Species & Platforms")
        # Top row: legacy GPL radio buttons for quick access
        self._lbl_platform = tk.Label(ca2,
            text="Quick select (legacy):", bg=BG2, fg=FG2)
        self._lbl_platform.pack(anchor="w")
        self._var_platform = tk.StringVar(value="GPL6947")
        self._gpl_frame = tk.Frame(ca2, bg=BG2)
        self._gpl_frame.pack(fill=tk.X, pady=2)
        bf = self._gpl_frame
        for gpl in ALL_GPLS:
            tk.Radiobutton(bf, text=gpl, variable=self._var_platform, value=gpl,
                           bg=BG2, fg=FG, selectcolor=ACCENT,
                           activebackground=BG2, activeforeground=FG).pack(side=tk.LEFT, padx=6)

        # Separator
        ttk.Separator(ca2, orient="horizontal").pack(fill=tk.X, pady=6)

        # Species discovery section
        sp_frame = tk.Frame(ca2, bg=BG2)
        sp_frame.pack(fill=tk.X)
        sp_frame.columnconfigure(1, weight=1)
        tk.Label(sp_frame, text="Species:", bg=BG2, fg=FG2).grid(
            row=0, column=0, sticky="w")
        self._var_species = tk.StringVar(value="Homo sapiens")
        species_cb = ttk.Combobox(sp_frame, textvariable=self._var_species,
                                   values=SPECIES_LIST, width=25, state="normal")
        species_cb.grid(row=0, column=1, sticky="ew", padx=(6, 0))

        # Technology filter
        tk.Label(sp_frame, text="Technology:", bg=BG2, fg=FG2).grid(
            row=1, column=0, sticky="w", pady=(4, 0))
        self._var_tech_filter = tk.StringVar(value="Expression Microarray")
        tech_cb = ttk.Combobox(sp_frame, textvariable=self._var_tech_filter,
                                values=list(TECHNOLOGY_FILTERS.keys()),
                                width=25, state="readonly")
        tech_cb.grid(row=1, column=1, sticky="ew", padx=(6, 0), pady=(4, 0))

        # Min samples + Discover button row
        ctrl_frame = tk.Frame(ca2, bg=BG2)
        ctrl_frame.pack(fill=tk.X, pady=(4, 0))
        tk.Label(ctrl_frame, text="Min samples:", bg=BG2, fg=FG2).pack(side=tk.LEFT)
        self._var_min_samples = tk.StringVar(value=str(MIN_SAMPLES_DEFAULT))
        tk.Spinbox(ctrl_frame, from_=10, to=100000, width=7,
                   textvariable=self._var_min_samples,
                   bg=BG3, fg=FG, insertbackground=FG, buttonbackground=BG3,
                   relief="flat").pack(side=tk.LEFT, padx=(4, 8))
        tk.Button(ctrl_frame, text="Discover Platforms",
                  command=self._discover_platforms,
                  bg=ACCENT, fg="white", relief="flat", cursor="hand2",
                  activebackground="#9b7cd4", activeforeground="white",
                  padx=8).pack(side=tk.LEFT)
        self._lbl_discover_status = tk.Label(ctrl_frame, text="", bg=BG2, fg=FG2)
        self._lbl_discover_status.pack(side=tk.LEFT, padx=(8, 0))

        # Platform list (Treeview) with Technology column
        tree_frame = tk.Frame(ca2, bg=BG2)
        tree_frame.pack(fill=tk.X, pady=(4, 0))
        tree_frame.columnconfigure(0, weight=1)

        style = ttk.Style()
        style.configure("Dark.Treeview",
                        background=BG3, foreground=FG, fieldbackground=BG3,
                        rowheight=20)
        style.configure("Dark.Treeview.Heading",
                        background=BG2, foreground=ACCENT2)

        self._platform_tree = ttk.Treeview(
            tree_frame, columns=("gpl", "samples", "technology", "title"),
            show="headings", height=6, style="Dark.Treeview",
            selectmode="extended")
        self._platform_tree.heading("gpl", text="GPL ID")
        self._platform_tree.heading("samples", text="Samples")
        self._platform_tree.heading("technology", text="Technology")
        self._platform_tree.heading("title", text="Title")
        self._platform_tree.column("gpl", width=80, minwidth=70)
        self._platform_tree.column("samples", width=65, minwidth=55)
        self._platform_tree.column("technology", width=90, minwidth=70)
        self._platform_tree.column("title", width=160, minwidth=80)

        # Scrollbar for treeview
        tree_scroll = ttk.Scrollbar(tree_frame, orient="vertical",
                                     command=self._platform_tree.yview)
        self._platform_tree.configure(yscrollcommand=tree_scroll.set)
        self._platform_tree.grid(row=0, column=0, sticky="ew")
        tree_scroll.grid(row=0, column=1, sticky="ns")

        # Select all / none buttons + summary
        sel_frame = tk.Frame(ca2, bg=BG2)
        sel_frame.pack(fill=tk.X, pady=(4, 0))
        tk.Button(sel_frame, text="Select All",
                  command=self._select_all_platforms,
                  bg=BG3, fg=FG, relief="flat", padx=4, cursor="hand2"
                  ).pack(side=tk.LEFT)
        tk.Button(sel_frame, text="Select None",
                  command=self._select_no_platforms,
                  bg=BG3, fg=FG, relief="flat", padx=4, cursor="hand2"
                  ).pack(side=tk.LEFT, padx=(4, 0))
        self._lbl_platform_summary = tk.Label(sel_frame, text="", bg=BG2, fg=FG2)
        self._lbl_platform_summary.pack(side=tk.LEFT, padx=(8, 0))

        # Store discovered platforms data
        self._discovered_platforms = []

        self._lbl_files = tk.Label(ca2, text="", bg=BG2, fg=FG2, justify=tk.LEFT, wraplength=300)
        self._lbl_files.pack(anchor="w", pady=(4, 0))
        self._var_platform.trace_add("write", lambda *_: self._check_files())
        self._var_dir.trace_add("write",      lambda *_: self._check_files())

        #  Options card 
        ca4 = card(left, "Options")
        g4  = tk.Frame(ca4, bg=BG2); g4.pack(fill=tk.X); g4.columnconfigure(1, weight=1)

        # Row 0  test limit
        tk.Label(g4, text="Test limit (rows):", bg=BG2, fg=FG2).grid(row=0, column=0, sticky="w")
        self._var_limit = tk.StringVar(value="")
        tk.Entry(g4, textvariable=self._var_limit, bg=BG3, fg=FG, insertbackground=FG,
                 relief="flat", width=10
                 ).grid(row=0, column=1, sticky="w", padx=(8, 0))
        tk.Label(g4, text="  (blank = all NS rows)", bg=BG2, fg=FG2).grid(row=0, column=2, sticky="w")

        # Row 1  input mode
        tk.Label(g4, text="Input mode:", bg=BG2, fg=FG2).grid(row=1, column=0, sticky="w", pady=(6, 0))
        mode_frame = tk.Frame(g4, bg=BG2)
        mode_frame.grid(row=1, column=1, columnspan=2, sticky="w",
                        padx=(8, 0), pady=(6, 0))
        self._var_mode = tk.StringVar(value="repair")
        tk.Radiobutton(mode_frame, text="Repair NS labels",
                       variable=self._var_mode, value="repair",
                       bg=BG2, fg=FG, selectcolor=BG3, activebackground=BG2,
                       command=self._on_mode_change).pack(side=tk.LEFT)
        tk.Radiobutton(mode_frame, text="Annotate from scratch (GSM list)",
                       variable=self._var_mode, value="gsm_list",
                       bg=BG2, fg=FG, selectcolor=BG3, activebackground=BG2,
                       command=self._on_mode_change).pack(side=tk.LEFT, padx=(12, 0))

        # Row 2  GSM list file (visible only in gsm_list mode)
        self._gsm_file_frame = tk.Frame(g4, bg=BG2)
        self._gsm_file_frame.grid(row=2, column=0, columnspan=3,
                                   sticky="ew", pady=(4, 0))
        self._gsm_file_frame.columnconfigure(1, weight=1)
        tk.Label(self._gsm_file_frame, text="GSM list file:",
                 bg=BG2, fg=FG2).grid(
                 row=0, column=0, sticky="w")
        self._var_gsm_file = tk.StringVar(value="")
        tk.Entry(self._gsm_file_frame, textvariable=self._var_gsm_file,
                 bg=BG3, fg=FG, insertbackground=FG, relief="flat").grid(
                 row=0, column=1, sticky="ew", padx=(8, 4))
        tk.Button(self._gsm_file_frame, text="Browse",
                  bg=BG3, fg=FG, relief="flat",
                  command=self._browse_gsm_file).grid(row=0, column=2)
        self._gsm_file_frame.grid_remove()   # hidden by default

        # Row 3  parallel GSE workers
        tk.Label(g4, text="Parallel GSE workers:", bg=BG2, fg=FG2).grid(row=3, column=0, sticky="w", pady=(6, 0))
        worker_frame = tk.Frame(g4, bg=BG2)
        worker_frame.grid(row=3, column=1, columnspan=2, sticky="w", padx=(8, 0), pady=(6, 0))
        self._var_workers = tk.StringVar(value="auto")
        tk.Spinbox(worker_frame, from_=1, to=32, width=5,
                   textvariable=self._var_workers,
                   bg=BG3, fg=FG, insertbackground=FG, buttonbackground=BG3,
                   relief="flat"
                   ).pack(side=tk.LEFT)
        tk.Button(worker_frame, text="auto-detect",
                  command=self._detect_workers,
                  bg=BG3, fg=ACCENT2, relief="flat", cursor="hand2",
                  activebackground=ACCENT, activeforeground="white",
                  padx=6
                  ).pack(side=tk.LEFT, padx=(6, 0))
        self._lbl_workers_hint = tk.Label(worker_frame, text="", bg=BG2, fg=FG2)
        self._lbl_workers_hint.pack(side=tk.LEFT, padx=(8, 0))

        # Row 2  skip install
        self._var_skip = tk.BooleanVar(value=False)
        tk.Checkbutton(g4, text="Skip Ollama/model auto-install",
                       variable=self._var_skip, bg=BG2, fg=FG, selectcolor=ACCENT,
                       activebackground=BG2
                       ).grid(row=2, column=0, columnspan=3, sticky="w", pady=(6, 0))

        # Row 3  skip Phase 2 (extract only)
        self._var_skip_phase2 = tk.BooleanVar(value=False)
        tk.Checkbutton(g4, text="Skip Phase 2 collapse (extract Phase 1+1b only)",
                       variable=self._var_skip_phase2, bg=BG2, fg=FG, selectcolor=ACCENT,
                       activebackground=BG2
                       ).grid(row=3, column=0, columnspan=3, sticky="w", pady=(2, 0))

        #  Model & Ollama card 
        c3 = card(left, "Model & Ollama")
        g3 = tk.Frame(c3, bg=BG2); g3.pack(fill=tk.X); g3.columnconfigure(1, weight=1)
        tk.Label(g3, text="Model:", bg=BG2, fg=FG2).grid(row=0, column=0, sticky="w", pady=2)
        # Model name (Ollama)
        tk.Label(g3, text="Model:", bg=BG2, fg=FG2).grid(row=0, column=0, sticky="w")
        self._var_model = tk.StringVar(value=DEFAULT_MODEL)
        self._var_backend = tk.StringVar(value="ollama")  # always ollama
        self._var_gguf = tk.StringVar(value="")  # unused
        mdl_entry = tk.Entry(g3, textvariable=self._var_model,
                             bg=BG3, fg=FG, insertbackground=FG,
                             relief="flat")
        mdl_entry.grid(row=0, column=1, sticky="ew", padx=(8, 0))
        self._var_model.trace_add(
            "write",
            lambda *_: self.after(600,
                lambda: self._detect_workers(silent=True)))
        tk.Label(g3, text="Ollama URL:", bg=BG2, fg=FG2).grid(row=1, column=0, sticky="w", pady=2)
        self._var_url = tk.StringVar(value=DEFAULT_URL)
        tk.Entry(g3, textvariable=self._var_url, bg=BG3, fg=FG, insertbackground=FG,
                 relief="flat").grid(row=1, column=1, sticky="ew",
                                                            padx=(8, 0))
        badge_row = tk.Frame(c3, bg=BG2); badge_row.pack(fill=tk.X, pady=(6, 0))
        self._lbl_ollama = tk.Label(badge_row, text="Ollama: checking...",
                                    bg=BG2, fg=WARNING)
        self._lbl_ollama.pack(side=tk.LEFT, padx=(0, 10))
        self._lbl_model  = tk.Label(badge_row, text="Model: checking...",
                                    bg=BG2, fg=WARNING)
        self._lbl_model.pack(side=tk.LEFT)
        self._lbl_watchdog_top = tk.Label(badge_row, text="",
                                          bg=BG2, fg=FG2)
        self._lbl_watchdog_top.pack(side=tk.RIGHT)

        #  Start / Stop 
        self._btn_start = tk.Button(
            left, text="START - Raw Extraction Pipeline",
            command=self._start, bg=SUCCESS, fg="white", relief="flat", cursor="hand2", pady=8,
            activebackground="#3a9c5e", activeforeground="white")
        self._btn_start.pack(fill=tk.X, pady=(4, 0))

        self._btn_stop = tk.Button(
            left, text="STOP",
            command=self._stop, bg=ERROR, fg="white", relief="flat", cursor="hand2", pady=6,
            activebackground="#c04040", activeforeground="white", state=tk.DISABLED)
        self._btn_stop.pack(fill=tk.X, pady=(4, 0))

        #  Novel label validation card 
        sep = tk.Frame(left, bg=BG3, height=2)
        sep.pack(fill=tk.X, pady=(10, 0))

        c_val = card(left, "Novel Label Validation")
        tk.Label(c_val,
                 text="Checks if any resolved NS labels fall outside the\n"
                      "original harmonized vocabulary. Uses the folder and\n"
                      "platform selected above  picks the most recent run.",
                 bg=BG2, fg=FG2,
                 justify=tk.LEFT).pack(anchor="w", pady=(0, 6))

        self._btn_validate = tk.Button(
            c_val, text="RUN VALIDATION",
            command=self._start_validation,
            bg="#1a6b3c", fg="white", relief="flat", cursor="hand2", pady=7,
            activebackground="#145530", activeforeground="white")
        self._btn_validate.pack(fill=tk.X)

        #  Evaluation card 
        sep2 = tk.Frame(left, bg=BG3, height=2)
        sep2.pack(fill=tk.X, pady=(10, 0))

        c_eval = card(left, "  Evaluation (Judge LLM + Human)")
        tk.Label(c_eval,
                 text="Evaluate NS resolution quality from a completed run.\n"
                      "Judge LLM: 1000 shuffled samples  TP/FP/FN/TN.\n"
                      "Human: 1000 samples GUI. Publication-ready report.",
                 bg=BG2, fg=FG2,
                 justify=tk.LEFT).pack(anchor="w", pady=(0, 4))

        eval_n_row = tk.Frame(c_eval, bg=BG2); eval_n_row.pack(fill=tk.X, pady=(0,4))
        tk.Label(eval_n_row, text="Judge N:", bg=BG2, fg=FG2).pack(side=tk.LEFT)
        self._var_judge_n = tk.StringVar(value="1000")
        tk.Entry(eval_n_row, textvariable=self._var_judge_n,
                 width=6, bg=BG3, fg=FG, insertbackground=FG,
                 relief="flat").pack(side=tk.LEFT, padx=(4,12))
        tk.Label(eval_n_row, text="Human N:", bg=BG2, fg=FG2).pack(side=tk.LEFT)
        self._var_human_n = tk.StringVar(value="1000")
        tk.Entry(eval_n_row, textvariable=self._var_human_n,
                 width=6, bg=BG3, fg=FG, insertbackground=FG,
                 relief="flat").pack(side=tk.LEFT, padx=(4,0))

        eval_btn_row = tk.Frame(c_eval, bg=BG2); eval_btn_row.pack(fill=tk.X)
        self._btn_eval_judge = tk.Button(
            eval_btn_row, text="Judge LLM",
            command=self._start_eval_judge,
            bg="#1a4a7a", fg="white", relief="flat", cursor="hand2", pady=5,
            activebackground="#0f3560", activeforeground="white")
        self._btn_eval_judge.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0,3))
        self._btn_eval_human = tk.Button(
            eval_btn_row, text="Human GUI",
            command=self._start_eval_human,
            bg="#4a1a7a", fg="white", relief="flat", cursor="hand2", pady=5,
            activebackground="#35116a", activeforeground="white")
        self._btn_eval_human.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(3,0))

        #  Progress + Log 
        prog_outer = tk.Frame(right, bg=BG)
        prog_outer.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        prog_outer.columnconfigure(0, weight=1)
        self._prog_var = tk.IntVar(value=0)
        style = ttk.Style(); style.theme_use("default")
        style.configure("custom.Horizontal.TProgressbar",
                        troughcolor=BG3, background=ACCENT, thickness=18, borderwidth=0)
        self._pbar = ttk.Progressbar(prog_outer, variable=self._prog_var, maximum=100,
                                     length=400, style="custom.Horizontal.TProgressbar")
        self._pbar.grid(row=0, column=0, columnspan=2, sticky="ew")

        # Primary status line (samples / ETA)
        self._lbl_prog = tk.Label(prog_outer, text="Ready",
                                  bg=BG, fg=FG2)
        self._lbl_prog.grid(row=1, column=0, columnspan=2, sticky="w", pady=(2, 0))

        # Per-column live stats
        self._lbl_tissue_stat = tk.Label(
            prog_outer, text="Tissue   --", bg=BG, fg=FG2)
        self._lbl_tissue_stat.grid(row=2, column=0, sticky="w")

        self._lbl_cond_stat = tk.Label(
            prog_outer, text="Condition --", bg=BG, fg=FG2)
        self._lbl_cond_stat.grid(row=3, column=0, sticky="w")

        # Treatment stat (scratch mode only  hidden in repair mode)
        self._lbl_treat_stat = tk.Label(
            prog_outer, text="Treatment --", bg=BG, fg=FG2)
        self._lbl_treat_stat.grid(row=4, column=0, sticky="w")
        self._lbl_treat_stat.grid_remove()  # hidden until scratch mode runs

        # Latency / speed / ETA detail row
        self._lbl_latency = tk.Label(
            prog_outer, text="", bg=BG, fg=ACCENT2)
        self._lbl_latency.grid(row=5, column=0, sticky="w")

        # GSE progress line
        self._lbl_gse_prog = tk.Label(
            prog_outer, text="", bg=BG, fg=FG2)
        self._lbl_gse_prog.grid(row=6, column=0, sticky="w")

        # Watchdog
        self._lbl_watchdog = tk.Label(prog_outer, text="Watchdog: idle",
                                      bg=BG, fg=FG2)
        self._lbl_watchdog.grid(row=7, column=0, sticky="w")

        log_frame = tk.Frame(right, bg=BG2, bd=1, relief="groove")
        log_frame.grid(row=1, column=0, sticky="nsew")
        log_frame.rowconfigure(1, weight=1); log_frame.columnconfigure(0, weight=1)
        tk.Label(log_frame, text=" Log ",
                 bg=BG2, fg=ACCENT2, anchor="w").grid(row=0, column=0, sticky="w", padx=4, pady=(4,0))
        self._log_widget = scrolledtext.ScrolledText(
            log_frame, bg="#0d0d1a", fg="#c0d0ff", insertbackground=FG,
            relief="flat", state=tk.DISABLED, wrap=tk.WORD)
        self._log_widget.grid(row=1, column=0, sticky="nsew", padx=2, pady=2)
        self._log_widget.tag_config("ok",   foreground=SUCCESS)
        self._log_widget.tag_config("warn", foreground=WARNING)
        self._log_widget.tag_config("err",  foreground=ERROR)
        self._log_widget.tag_config("head", foreground=ACCENT2)
        self._log_widget.tag_config("dim",  foreground=FG2)

        footer = tk.Frame(self, bg=BG3, pady=4); footer.pack(fill=tk.X, side=tk.BOTTOM)
        tk.Label(footer,
                 text="Outputs: {platform}_NS_repaired.csv  &  {platform}_full_repaired.csv"
                      "   (raw + context-matched labels, no artificial normalization)",
                 bg=BG3, fg=FG2).pack()

        self._check_files()

    #  Event handlers 
    def _log_msg(self, msg, tag=None):
        self._log_widget.config(state=tk.NORMAL)
        ts   = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}\n"
        if tag:
            self._log_widget.insert(tk.END, line, tag)
        else:
            low = msg.lower()
            if any(w in low for w in ("", "done", "ready", "saved", "", "fixed")):
                self._log_widget.insert(tk.END, line, "ok")
            elif any(w in low for w in ("[error]", "[warn]", "failed", "")):
                self._log_widget.insert(tk.END, line, "err")
            elif any(w in low for w in ("", "", "", "platform", "total", "milestone")):
                self._log_widget.insert(tk.END, line, "head")
            else:
                self._log_widget.insert(tk.END, line)
        self._log_widget.see(tk.END)
        self._log_widget.config(state=tk.DISABLED)

    def _detect_workers(self, silent: bool = False):
        """
        Scan all hardware resources and compute optimal worker count.
        Called automatically on startup (silent=True) and by the button.
        Shows a detailed breakdown popup when called manually.
        """
        def _run():
            model    = self._var_model.get().strip()
            total, gpu_w, cpu_w = compute_ollama_parallel(model)
            slot_gb  = MODEL_RAM_GB.get(model.lower(), DEFAULT_MODEL_GB)

            # Gather raw hardware info
            gpus      = detect_gpus()
            cpu_count = os.cpu_count() or 0
            free_ram  = psutil.virtual_memory().available / 1e9
            total_ram = psutil.virtual_memory().total    / 1e9

            # Build hint line for the small label
            if gpus:
                gpu_names = " + ".join(
                    f"{g['name']} ({g['vram_gb']:.0f} GB)"
                    for g in gpus)
                hint = (f"{gpu_w} GPU + {cpu_w} CPU  |  "
                        f"{gpu_names}  |  RAM {free_ram:.0f}/{total_ram:.0f} GB  |  Ollama")
            else:
                hint = (f"CPU only  |  {cpu_w} workers  |  "
                        f"RAM {free_ram:.0f}/{total_ram:.0f} GB  |  "
                        f"{cpu_count} cores  |  Ollama")

            # Must update GUI from main thread
            def _update_gui():
                self._var_workers.set(str(total))
                self._lbl_workers_hint.config(
                    text=f" {total} workers  ({hint})", fg=SUCCESS)
            self.after(0, _update_gui)

            if not silent:
                # Show detailed popup in main thread
                lines = [
                    f"Model          : {model}",
                    f"Model VRAM     : {slot_gb:.1f} GB per instance",
                    "",
                ]
                if gpus:
                    for g in gpus:
                        usable = max(0, g["vram_gb"] - 1.0)
                        kv_per = max(0.3, slot_gb * 0.15)
                        g_workers = max(1, int(usable / slot_gb))
                        lines.append(
                            f"GPU {g['id']}          : {g['name']}")
                        lines.append(
                            f"  VRAM         : {g['vram_gb']:.1f} GB total  "
                            f"({g['free_vram_gb']:.1f} GB currently free)")
                        lines.append(
                            f"  GPU workers  : {g_workers}")
                        lines.append(
                            f"  Backend      : Ollama (GPU)")
                else:
                    lines.append("GPU            : none detected")

                lines += [
                    "",
                    f"CPU cores      : {cpu_count}",
                    f"Usable cores   : {max(1, cpu_count - 2)}"
                    f"  (2 reserved for OS)",
                    f"CPU workers    : {cpu_w}"
                    f"  (~2 threads per worker)",
                    "",
                    f"RAM total      : {total_ram:.0f} GB",
                    f"RAM free       : {free_ram:.0f} GB",
                    f"RAM after GPU  : {free_ram - gpu_w * slot_gb:.0f} GB",
                    "",
                    "" * 44,
                    f"  GPU workers  : {gpu_w}",
                    f"  CPU workers  : {cpu_w}",
                    f"  TOTAL        : {total}  (GPU first, CPU overflow)",
                    "" * 44,
                ]

                def _popup():
                    dlg = tk.Toplevel(self)
                    dlg.title("Hardware scan  worker allocation")
                    dlg.geometry("460x420")
                    dlg.resizable(False, False)
                    try:
                        sw = dlg.winfo_screenwidth()
                        sh = dlg.winfo_screenheight()
                        dlg.geometry(
                            f"460x420+{(sw-460)//2}+{(sh-420)//2}")
                    except Exception:
                        pass
                    dlg.configure(bg=BG)

                    hdr = tk.Frame(dlg, bg=ACCENT, pady=8)
                    hdr.pack(fill=tk.X)
                    tk.Label(hdr, text="Resource scan",
                             bg=ACCENT, fg="white").pack(side=tk.LEFT, padx=12)
                    tk.Label(hdr,
                             text=f"{total} workers auto-set",
                             bg=ACCENT, fg="#FFD54F").pack(
                                 side=tk.RIGHT, padx=12)

                    txt = tk.Text(dlg,
                                  bg=BG2, fg=FG, relief="flat",
                                  padx=12, pady=10, wrap=tk.NONE)
                    txt.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
                    txt.tag_configure("bold",
                                      foreground=ACCENT2)
                    txt.tag_configure("total",
                                      foreground=SUCCESS)
                    for line in lines:
                        if line.startswith("  TOTAL"):
                            txt.insert(tk.END, line + "\n", "total")
                        elif line.startswith(""):
                            txt.insert(tk.END, line + "\n", "bold")
                        else:
                            txt.insert(tk.END, line + "\n")
                    txt.config(state=tk.DISABLED)

                    tk.Button(dlg, text="OK",
                              command=dlg.destroy,
                              bg=ACCENT, fg="white",
                              relief="flat", cursor="hand2",
                              pady=6, padx=30).pack(pady=(0, 10))

                self.after(0, _popup)

        threading.Thread(target=_run, daemon=True).start()


    def _start_validation(self):
        input_dir  = self._var_dir.get().strip()
        platform   = self._var_platform.get().strip()
        # Look for both naming conventions
        run_dir = os.path.join(input_dir, f"{platform}_NS_repaired_final_results")
        if not os.path.isdir(run_dir):
            # Try scratch subset dirs
            _candidates = sorted([
                d for d in os.listdir(input_dir)
                if d.startswith("GSM_subset_") and d.endswith("_NS_repaired_final_results")
                and os.path.isdir(os.path.join(input_dir, d))],
                reverse=True)  # newest = highest index
            if _candidates:
                run_dir = os.path.join(input_dir, _candidates[0])

        if not os.path.isdir(run_dir):
            messagebox.showerror("Not found",
                f"{platform}_NS_repaired_final_results not found in:\n{input_dir}\n\n"
                "Run the pipeline first."); return

        # Find most recent run folder for this platform
        candidates = sorted(
            [d for d in os.listdir(run_dir)
             if os.path.isfile(os.path.join(run_dir, d, "NS_repaired.csv"))],
            reverse=True   # lexicographic desc = most recent timestamp first
        )
        if not candidates:
            messagebox.showerror("Not found",
                f"No completed run found for {platform} in:\n{run_dir}"); return

        # run_dir already points to results folder directly
        self._btn_validate.config(state=tk.DISABLED)
        self._log_msg(f" Novel label validation  {platform}  {candidates[0]}", "head")
        threading.Thread(target=self._run_validation,
                         args=(input_dir, platform, run_dir), daemon=True).start()

    def _run_validation(self, input_dir: str, platform: str, run_dir: str):
        q = self._q
        def log(msg): q.put({"type": "log", "msg": msg})

        try:
            log(f"\n{'─'*60}")
            log("   Novel Label Validation")
            log(f"{'─'*60}")

            # Load original harmonized vocabulary for this platform only
            all_dfs = load_all(input_dir)
            if not all_dfs:
                log("  [ERROR] No harmonized CSV files found in input folder."); return
            if platform not in all_dfs:
                log(f"  [ERROR] No file found for platform {platform} in {input_dir}"); return
            orig_df    = all_dfs[platform]
            orig_vocab = {col: set(v for v in orig_df[col] if v and v != NS)
                          for col in _cols if col in orig_df.columns}
            for col, vocab in orig_vocab.items():
                log(f"  Original vocabulary  {col}: {len(vocab):,} unique labels")

            # Load NS_repaired.csv
            ns_path = os.path.join(run_dir, "NS_repaired.csv")
            res_df  = pd.read_csv(ns_path, dtype=str).fillna(NS)
            log(f"  Loaded {len(res_df):,} repaired NS rows from NS_repaired.csv")

            # Check for novel labels
            novel_rows = []
            for _, row in res_df.iterrows():
                for col in _cols:
                    if col not in orig_vocab:
                        continue
                    original_val = str(row.get(f"{col}_original", NS)).strip()
                    final_val    = str(row.get(col, NS)).strip()
                    if not is_ns(original_val):
                        continue
                    if is_ns(final_val):
                        continue
                    if final_val not in orig_vocab[col]:
                        novel_rows.append({
                            "gsm":            row.get("gsm", ""),
                            "series_id":      row.get("series_id", ""),
                            "field":          col,
                            "resolved_label": final_val,
                            "raw_label":      row.get(f"{col}_raw", ""),
                            "collapsed":      row.get(f"{col}_collapsed", False),
                        })

            if novel_rows:
                novel_df   = pd.DataFrame(novel_rows)
                novel_path = os.path.join(input_dir, "novel_labels.csv")
                novel_df.to_csv(novel_path, index=False)
                log(f"\n    {len(novel_rows):,} resolved label(s) NOT in original vocabulary:")
                for col in _cols:
                    sub = novel_df[novel_df["field"] == col]
                    if sub.empty: continue
                    log(f"\n  {col}: {sub['resolved_label'].nunique():,} unique novel label(s) "
                        f"across {len(sub):,} sample(s)")
                    for lbl, cnt in sub["resolved_label"].value_counts().head(15).items():
                        log(f"    [{cnt:>4}]  {lbl}")
                log(f"\n   Saved to novel_labels.csv")
                messagebox.showwarning("Novel Labels Found",
                    f"{len(novel_rows):,} resolved label(s) are outside the original vocabulary.\n"
                    f"Saved to:\n{novel_path}\n\nReview these  the LLM may have hallucinated.")
            else:
                log("   All resolved labels are within the original vocabulary.")
                messagebox.showinfo("Validation Passed",
                    "All resolved labels are within the original vocabulary.\nNo novel labels found.")

        except Exception as exc:
            import traceback
            log(f"\n[ERROR] Validation: {exc}\n{traceback.format_exc()}")
        finally:
            q.put({"type": "validate_done"})


    # 
    #  EVALUATION ENGINE  Judge LLM + Human GUI
    #  Completely separate from the repair pipeline.
    #  Reads NS_repaired.csv from the most recent run for the selected platform.
    # 

    EVAL_JUDGE_WORKERS = 6

    def _find_latest_run(self) -> str:
        """Return path to the most recent results folder, or '' if not found."""
        d   = self._var_dir.get().strip()
        gpl = self._var_platform.get().strip()
        # Repair mode: GPL570_NS_repaired_final_results
        root = os.path.join(d, f"{gpl}_NS_repaired_final_results")
        if os.path.isdir(root) and os.path.isfile(
                os.path.join(root, "NS_repaired.csv")):
            return root
        # Scratch mode: GSM_subset_{N}_NS_repaired_final_results  pick highest N
        if os.path.isdir(d):
            candidates = sorted([
                os.path.join(d, x) for x in os.listdir(d)
                if x.startswith("GSM_subset_") and
                   x.endswith("_NS_repaired_final_results") and
                   os.path.isfile(os.path.join(d, x, "NS_repaired.csv"))],
                key=lambda p: int(p.split("_subset_")[1].split("_NS")[0])
                    if "_subset_" in p else 0, reverse=True)
            if candidates:
                return candidates[0]
        return ""

    def _load_eval_samples(self, run_dir: str, n: int) -> "pd.DataFrame":
        """
        Load NS_repaired.csv, keep rows that were originally NS and got resolved,
        then shuffle and take n samples proportionally across platforms.
        """
        path = os.path.join(run_dir, "NS_repaired.csv")
        df   = pd.read_csv(path, dtype=str).fillna(NS)

        rows = []
        for col in _cols:
            orig_col  = f"{col}_original"
            final_col = col
            if orig_col not in df.columns:
                continue
            sub = df[(df[orig_col] == NS) & (df[final_col] != NS)].copy()
            sub["field"]           = col
            sub["extracted_value"] = sub[final_col]
            sub["gsm"]             = sub.get("gsm", sub.index.astype(str))
            sub["series_id"]       = sub.get("series_id", "")
            sub["gpl"]             = sub.get("platform", "")
            rows.append(sub[["gsm", "series_id", "gpl", "field",
                              "extracted_value"]])

        if not rows:
            return pd.DataFrame()

        combined = pd.concat(rows, ignore_index=True)
        combined = combined.sample(frac=1, random_state=42).reset_index(drop=True)
        return combined.head(n)

    #  Judge LLM 

    def _start_eval_judge(self):
        run_dir = self._find_latest_run()
        if not run_dir:
            messagebox.showerror("Not found",
                "No completed run found. Run the pipeline first."); return
        try:
            n = int(self._var_judge_n.get().strip())
        except ValueError:
            n = 1000
        self._btn_eval_judge.config(state=tk.DISABLED)
        self._btn_eval_human.config(state=tk.DISABLED)
        self._log_msg(f" Judge LLM evaluation  {n} samples from {os.path.basename(run_dir)}", "head")
        threading.Thread(target=self._run_eval_judge,
                         args=(run_dir, n), daemon=True).start()

    def _run_eval_judge(self, run_dir: str, n: int):
        q   = self._q
        def log(msg): q.put({"type": "log", "msg": msg})
        def prog(pct, lbl=""): q.put({"type": "progress", "pct": pct, "label": lbl})

        try:
            log(f"\n{'─'*60}")
            log(f"   Loading {n} resolved NS samples ")
            samples = self._load_eval_samples(run_dir, n)
            if samples.empty:
                log("  [ERROR] No resolved NS samples found in NS_repaired.csv"); return
            log(f"  Loaded {len(samples):,} samples  "
                f"({samples['field'].value_counts().to_dict()})")
            prog(5, "Judge: evaluating ")

            model      = self._var_model.get().strip()
            ollama_url = self._var_url.get().strip()
            total      = len(samples)
            verdicts   = ["ERROR"] * total
            done = tp = fp = fn = tn = 0
            t0   = time.time()

            def judge_one(args):
                idx, row = args
                prompt = (
                    f"Evaluate this GEO biomedical metadata NS resolution.\n\n"
                    f"SAMPLE: {row.get('gsm','?')}  |  Platform: {row.get('gpl','?')}\n"
                    f"Field: {row['field']}\n"
                    f"Resolved value: \"{row['extracted_value']}\"\n\n"
                    f"RULES:\n"
                    f"- TP = correctly resolved, specific canonical cluster name\n"
                    f"- FP = wrong, hallucinated, too generic, or not a valid cluster\n"
                    f"- FN = should have resolved but result is Not Specified\n"
                    f"- TN = correctly stayed Not Specified (info absent)\n\n"
                    f"Reply ONLY: TP, FP, FN, or TN"
                )
                try:
                    url     = ollama_url.rstrip("/") + "/api/chat"
                    payload = {"model": model, "stream": False,
                               "options": {"temperature": 0.0,
                                           "num_predict": 10, "num_ctx": 512},
                               "messages": [{"role": "user", "content": prompt}]}
                    resp    = requests.post(url, json=payload, timeout=60)
                    resp.raise_for_status()
                    raw     = resp.json()["message"]["content"].strip().upper()
                    v       = next((x for x in ["TP","TN","FP","FN"] if x in raw), "TP")
                except Exception:
                    v = "ERROR"
                return idx, v

            tasks = list(enumerate(samples.to_dict("records")))
            from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _ac
            with _TPE(max_workers=self.EVAL_JUDGE_WORKERS) as ex:
                fmap = {ex.submit(judge_one, t): t[0] for t in tasks}
                for fut in _ac(fmap):
                    i, v = fut.result()
                    verdicts[i] = v
                    done += 1
                    if v=="TP": tp+=1
                    elif v=="FP": fp+=1
                    elif v=="FN": fn+=1
                    elif v=="TN": tn+=1
                    pct = 5 + int(90 * done / total)
                    prog(pct, f"Judge {done}/{total}  TP:{tp} FP:{fp} FN:{fn} TN:{tn}")

            result            = samples.copy().reset_index(drop=True)
            result["verdict"] = verdicts

            # Save results
            out_path = os.path.join(self._var_dir.get().strip(),
                                    f"eval_judge_{os.path.basename(run_dir)}.csv")
            result.to_csv(out_path, index=False)
            log(f"\n  Saved  {os.path.basename(out_path)}")

            # Generate report
            report = self._make_eval_report(result, "Judge LLM", run_dir)
            log(report)
            prog(100, "Judge evaluation complete")

        except Exception as exc:
            import traceback
            log(f"\n[ERROR] Evaluation: {exc}\n{traceback.format_exc()}")
        finally:
            q.put({"type": "eval_done"})

    #  Human GUI evaluation 

    def _start_eval_human(self):
        run_dir = self._find_latest_run()
        if not run_dir:
            messagebox.showerror("Not found",
                "No completed run found. Run the pipeline first."); return
        try:
            n = int(self._var_human_n.get().strip())
        except ValueError:
            n = 200
        self._btn_eval_judge.config(state=tk.DISABLED)
        self._btn_eval_human.config(state=tk.DISABLED)
        self._log_msg(f" Human evaluation  {n} samples", "head")
        threading.Thread(target=self._run_eval_human,
                         args=(run_dir, n), daemon=True).start()

    def _run_eval_human(self, run_dir: str, n: int):
        q   = self._q
        def log(msg): q.put({"type": "log", "msg": msg})

        try:
            samples = self._load_eval_samples(run_dir, n)
            if samples.empty:
                log("  [ERROR] No resolved NS samples found"); return
            log(f"\n  Loaded {len(samples):,} samples for human evaluation")

            # Open DB for GSE context display
            db_path = self._find_db(self._var_dir.get().strip())
            conn    = None
            if db_path:
                try:
                    import gzip, shutil
                    if db_path.endswith(".gz"):
                        tmp = db_path.replace(".gz", ".tmp.sqlite")
                        if not os.path.exists(tmp):
                            with gzip.open(db_path, "rb") as fi, open(tmp, "wb") as fo:
                                shutil.copyfileobj(fi, fo)
                        conn = sqlite3.connect(tmp)
                    else:
                        conn = sqlite3.connect(db_path)
                except Exception:
                    conn = None

            # Run GUI in main thread via after()
            result_holder = [None]

            def open_gui():
                result_holder[0] = self._human_eval_gui(samples, conn, run_dir)
                if conn:
                    conn.close()

            self.after(0, open_gui)
            # Wait for GUI to finish
            while result_holder[0] is None:
                time.sleep(0.3)

            result = result_holder[0]
            if result is not None and not result.empty:
                out_path = os.path.join(self._var_dir.get().strip(),
                                        f"eval_human_{os.path.basename(run_dir)}.csv")
                result.to_csv(out_path, index=False)
                log(f"\n  Saved  {os.path.basename(out_path)}")
                report = self._make_eval_report(result, "Human", run_dir)
                log(report)
            else:
                log("  Human evaluation cancelled or no results.")

        except Exception as exc:
            import traceback
            log(f"\n[ERROR] Human eval: {exc}\n{traceback.format_exc()}")
        finally:
            q.put({"type": "eval_done"})

    def _human_eval_gui(self, samples: "pd.DataFrame",
                        conn, run_dir: str) -> "pd.DataFrame":
        """Human evaluation GUI  keyboard-driven TP/FP/FN/TN rating."""
        items   = list(samples.iterrows())
        total   = len(items)
        results = []
        idx     = [0]
        done    = [False]

        root = tk.Toplevel(self)
        root.title(f"Human Evaluation  ({total} samples)")
        root.geometry("1200x800")
        root.grab_set()

        # Header
        hdr = tk.Frame(root, bg=ACCENT, pady=8)
        hdr.pack(fill=tk.X)
        tk.Label(hdr, text="GEO NS Repair  Human Evaluation", bg=ACCENT, fg="white").pack(side=tk.LEFT, padx=12)
        prog_lbl = tk.Label(hdr, text="",
                             bg=ACCENT, fg="#FFD54F")
        prog_lbl.pack(side=tk.LEFT, padx=8)

        # Split pane
        pw = ttk.PanedWindow(root, orient=tk.HORIZONTAL)
        pw.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        lf  = ttk.Frame(pw, padding=4); pw.add(lf, weight=3)
        txt = tk.Text(lf, wrap=tk.WORD, bg=BG2, fg=FG)
        sb  = ttk.Scrollbar(lf, orient="vertical", command=txt.yview)
        txt.config(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y); txt.pack(fill=tk.BOTH, expand=True)
        txt.tag_configure("h1", foreground=ACCENT2)
        txt.tag_configure("val", foreground="#e8a030",
                          background=BG3)

        rf   = ttk.Frame(pw, padding=10); pw.add(rf, weight=1)
        flbl = tk.Label(rf, text="", fg=ACCENT2); flbl.pack(pady=(4,2))
        plbl = tk.Label(rf, text="", fg="#e8a030",
                        wraplength=300, bg=BG3, padx=8, pady=6, relief=tk.GROOVE)
        plbl.pack(fill=tk.X, pady=(0,10))

        vvar = tk.StringVar()
        btns = {}
        for label, v, c in [("1  TP  Correct resolution", "TP", SUCCESS),
                              ("2  FP  Wrong / hallucinated", "FP", ERROR),
                              ("3  FN  Should have resolved", "FN", WARNING),
                              ("4  TN  Correctly stayed NS", "TN", ACCENT2)]:
            b = tk.Button(rf, text=label, width=28,
                          pady=8, cursor="hand2", bg=BG3, fg=FG, anchor="w",
                          command=lambda x=v: _pick(x))
            b.pack(fill=tk.X, pady=2)
            btns[v] = (b, c)

        ttk.Label(rf, text="Note (optional):").pack(anchor=tk.W, pady=(8,2))
        note_e = ttk.Entry(rf, width=32); note_e.pack(fill=tk.X)

        nav = tk.Frame(rf, bg=BG); nav.pack(fill=tk.X, pady=10)
        tk.Button(nav, text="Submit & Next", bg=SUCCESS, fg="white", pady=4, cursor="hand2",
                  command=lambda: _submit()).pack(side=tk.LEFT, padx=3)
        tk.Button(nav, text="Skip", padx=6,
                  command=lambda: _skip()).pack(side=tk.LEFT, padx=3)
        tk.Button(nav, text="Finish", padx=6,
                  command=lambda: _finish()).pack(side=tk.RIGHT, padx=3)
        ttk.Label(rf, text="Keys: 1=TP  2=FP  3=FN  4=TN  Enter=Submit", foreground=FG2).pack()

        def _pick(v):
            vvar.set(v)
            for k, (b, c) in btns.items():
                b.config(bg=c if k==v else BG3, fg="white" if k==v else FG)

        def _submit():
            v = vvar.get()
            if not v:
                messagebox.showwarning("Select verdict", "Pick TP/FP/FN/TN first.",
                                       parent=root); return
            _, row = items[idx[0]]
            results.append({**row.to_dict(), "verdict": v,
                             "note": note_e.get().strip()})
            idx[0] += 1
            if idx[0] >= total:
                _finish(); return
            _show(idx[0])

        def _skip():
            idx[0] += 1
            if idx[0] >= total: _finish(); return
            _show(idx[0])

        def _finish():
            done[0] = True
            root.grab_release(); root.destroy()

        def _show(i):
            _, row = items[i]
            pred   = str(row.get("extracted_value", ""))
            prog_lbl.config(text=f"{i+1}/{total}  ({(i+1)/total*100:.0f}%)")
            flbl.config(text=row["field"])
            plbl.config(text=pred)
            vvar.set("")
            note_e.delete(0, tk.END)
            for k, (b, c) in btns.items():
                b.config(bg=BG3, fg=FG)

            txt.config(state=tk.NORMAL); txt.delete("1.0", tk.END)
            txt.insert(tk.END, f"SAMPLE: {row.get('gsm','?')}  |  {row.get('gpl','?')}\n", "h1")
            txt.insert(tk.END, f"Field  : {row['field']}\n")
            txt.insert(tk.END, "Resolved: ", "h1")
            txt.insert(tk.END, f"{pred}\n\n", "val")

            gse = str(row.get("series_id", "")).strip()
            if gse and gse != "nan" and conn:
                try:
                    gr = pd.read_sql_query(
                        "SELECT title, summary, overall_design FROM gse WHERE gse=?",
                        conn, params=[gse])
                    if not gr.empty:
                        g = gr.iloc[0]
                        txt.insert(tk.END, f"GSE: {gse}\n", "h1")
                        txt.insert(tk.END, f"Title: {g.get('title','?')}\n")
                        s = str(g.get("summary",""))[:600]
                        if s and s != "None":
                            txt.insert(tk.END, f"\nSummary:\n{s}\n")
                except Exception:
                    pass
            txt.config(state=tk.DISABLED)

        root.bind("1", lambda e: _pick("TP"))
        root.bind("2", lambda e: _pick("FP"))
        root.bind("3", lambda e: _pick("FN"))
        root.bind("4", lambda e: _pick("TN"))
        root.bind("<Return>", lambda e: _submit())
        root.protocol("WM_DELETE_WINDOW", _finish)

        _show(0)
        # Wait for window to close
        root.wait_window()
        return pd.DataFrame(results) if results else pd.DataFrame()

    #  Report generation 

    def _make_eval_report(self, df: "pd.DataFrame",
                          evaluator: str, run_dir: str) -> str:
        """
        Generate a publication-ready evaluation report.
        Returns the report as a string and saves it to the input dir.
        """
        from datetime import datetime as _dt

        def _metrics(sub):
            v   = sub["verdict"].value_counts() if "verdict" in sub.columns else {}
            tp  = int(v.get("TP", 0)); fp = int(v.get("FP", 0))
            fn  = int(v.get("FN", 0)); tn = int(v.get("TN", 0))
            n   = tp + fp + fn + tn
            pre = tp / (tp+fp) if tp+fp else 0
            rec = tp / (tp+fn) if tp+fn else 0
            f1  = 2*pre*rec / (pre+rec) if pre+rec else 0
            acc = (tp+tn) / n if n else 0
            return tp, fp, fn, tn, n, pre, rec, f1, acc

        L = [
            "=" * 68,
            f"  GEO NS Repair  Evaluation Report  [{evaluator}]",
            f"  Platform  : {self._var_platform.get()}",
            f"  Run       : {os.path.basename(run_dir)}",
            f"  Generated : {_dt.now().strftime('%Y-%m-%d %H:%M')}",
            f"  Evaluator : {evaluator}",
            "=" * 68,
        ]

        # Overall metrics
        tp,fp,fn,tn,n,pre,rec,f1,acc = _metrics(df)
        L += [
            "",
            f"  Overall  (n={n})",
            f"  {'─'*50}",
            f"  TP={tp}  FP={fp}  FN={fn}  TN={tn}",
            f"  Precision={pre:.4f}  Recall={rec:.4f}  "
            f"F1={f1:.4f}  Accuracy={acc:.4f}",
        ]

        # Per-field breakdown
        if "field" in df.columns:
            L += ["", f"  Per-field",
                  f"  {'Field':<22} {'n':>5} {'TP':>5} {'FP':>5} "
                  f"{'FN':>5} {'TN':>5} {'Prec':>7} {'Rec':>7} "
                  f"{'F1':>7} {'Acc':>7}",
                  f"  {'─'*72}"]
            for fld in sorted(df["field"].unique()):
                r = df[df["field"] == fld]
                tp,fp,fn,tn,nf,pre,rec,f1,acc = _metrics(r)
                L.append(f"  {fld:<22} {nf:>5} {tp:>5} {fp:>5} "
                         f"{fn:>5} {tn:>5} {pre:>7.4f} {rec:>7.4f} "
                         f"{f1:>7.4f} {acc:>7.4f}")

        # Per-platform breakdown
        if "gpl" in df.columns and df["gpl"].notna().any():
            L += ["", f"  Per-platform",
                  f"  {'Platform':<14} {'n':>5} {'TP':>5} {'FP':>5} "
                  f"{'FN':>5} {'TN':>5} {'Acc':>7}",
                  f"  {'─'*50}"]
            for gpl in sorted(df["gpl"].dropna().unique()):
                r = df[df["gpl"] == gpl]
                tp,fp,fn,tn,ng,pre,rec,f1,acc = _metrics(r)
                L.append(f"  {gpl:<14} {ng:>5} {tp:>5} {fp:>5} "
                         f"{fn:>5} {tn:>5} {acc:>7.4f}")

        L.append("\n" + "=" * 68)
        report = "\n".join(L)

        # Save
        out_dir  = self._var_dir.get().strip()
        rpt_path = os.path.join(out_dir,
                                f"eval_report_{evaluator.lower().replace(' ','_')}"
                                f"_{os.path.basename(run_dir)}.txt")
        try:
            with open(rpt_path, "w", encoding="utf-8") as f:
                f.write(report + "\n")
            self._q.put({"type": "log",
                         "msg": f"  Report  {os.path.basename(rpt_path)}"})
        except Exception:
            pass
        return report


    def _browse_dir(self):
        d = filedialog.askdirectory(title="Select Harmonized Labels Folder",
                                    initialdir=self._var_dir.get() or os.path.expanduser("~"))
        if d: self._var_dir.set(d)

    def _find_db(self, folder: str) -> str:
        """Return path to GEOmetadb in folder, or '' if not found."""
        for fname in os.listdir(folder) if os.path.isdir(folder) else []:
            if fname.lower().endswith(".sqlite.gz") or fname.lower().endswith(".sqlite"):
                if "geo" in fname.lower() or "meta" in fname.lower():
                    return os.path.join(folder, fname)
        # Second pass  any sqlite file
        for fname in os.listdir(folder) if os.path.isdir(folder) else []:
            if fname.lower().endswith(".sqlite.gz") or fname.lower().endswith(".sqlite"):
                return os.path.join(folder, fname)
        return ""

    def _on_mode_change(self):
        """Show/hide GSM list file picker and GPL selector based on mode."""
        if self._var_mode.get() == "gsm_list":
            # Scratch mode  show GSM file picker, hide GPL selector
            self._gsm_file_frame.grid()
            if hasattr(self, "_gpl_frame"):
                self._gpl_frame.pack_forget()
            if hasattr(self, "_lbl_platform"):
                self._lbl_platform.pack_forget()
            # Clear multi-platform selection in GSM list mode
            if hasattr(self, "_platform_tree"):
                self._platform_tree.selection_set()
            if hasattr(self, "_lbl_files"):
                self._lbl_files.config(
                    text="GPL selector hidden  GSMs can be from any platform")
        else:
            # Repair mode  hide GSM file picker, show GPL selector
            self._gsm_file_frame.grid_remove()
            if hasattr(self, "_lbl_platform"):
                self._lbl_platform.pack(anchor="w", before=self._gpl_frame)
            if hasattr(self, "_gpl_frame"):
                self._gpl_frame.pack(fill=tk.X, pady=4)
            self._check_files()

    def _browse_gsm_file(self):
        path = filedialog.askopenfilename(
            title="Select GSM list file",
            filetypes=[("Text files", "*.txt"), ("CSV files", "*.csv"),
                       ("All files", "*.*")])
        if path:
            self._var_gsm_file.set(path)

    # ── Platform Discovery ─────────────────────────────────────────────────
    def _discover_platforms(self):
        """Query GEOmetadb for all expression platforms of the selected species."""
        d = self._var_dir.get()
        db_path = self._find_db(d)
        if not db_path:
            messagebox.showerror("GEOmetadb Not Found",
                "Could not find GEOmetadb in the selected folder.\n"
                "Place GEOmetadb.sqlite.gz in the same folder.")
            return
        species  = self._var_species.get().strip()
        if not species:
            messagebox.showwarning("Species", "Please enter a species name.")
            return
        min_str  = self._var_min_samples.get().strip()
        min_samp = int(min_str) if min_str.isdigit() else MIN_SAMPLES_DEFAULT

        self._lbl_discover_status.config(text="Loading DB...", fg=WARNING)
        self.update_idletasks()

        tech_mode = self._var_tech_filter.get()

        def _run():
            try:
                conn = load_db_to_memory(db_path)
                platforms = discover_platforms(conn, species, min_samp,
                                              tech_mode=tech_mode)

                # Also count total samples per platform for resource estimation
                workers_str = self._var_workers.get().strip()
                workers = int(workers_str) if workers_str.isdigit() else 8
                total_s = sum(p["sample_count"] for p in platforms)
                eta_s   = total_s * 0.2 / workers
                eta_h   = int(eta_s // 3600)
                eta_m   = int((eta_s % 3600) // 60)

                conn.close()
                self._discovered_platforms = platforms
                # Update GUI on main thread
                self.after(0, lambda: self._populate_platform_tree(
                    platforms, eta_h, eta_m, workers))
            except Exception as e:
                self.after(0, lambda: self._lbl_discover_status.config(
                    text=f"Error: {e}", fg=ERROR))

        threading.Thread(target=_run, daemon=True).start()

    def _populate_platform_tree(self, platforms, eta_h, eta_m, workers):
        """Fill the Treeview with discovered platforms."""
        tree = self._platform_tree
        # Clear existing
        for item in tree.get_children():
            tree.delete(item)
        # Shorten technology names for display
        _tech_short = {
            "in situ oligonucleotide": "Microarray",
            "oligonucleotide beads":   "BeadChip",
            "spotted DNA/cDNA":        "cDNA array",
            "spotted oligonucleotide": "Spotted oligo",
            "high-throughput sequencing": "Sequencing",
            "SAGE":                    "SAGE",
            "MPSS":                    "MPSS",
        }
        # Insert platforms with technology column
        for p in platforms:
            tech_display = _tech_short.get(p["technology"],
                                           p["technology"][:15] if p["technology"] else "?")
            tree.insert("", "end", iid=p["gpl"],
                         values=(p["gpl"], f'{p["sample_count"]:,}',
                                 tech_display, p["title"][:70]))
        # Select all by default
        self._select_all_platforms()
        self._lbl_discover_status.config(
            text=f"{len(platforms)} platforms found", fg=SUCCESS)

    def _select_all_platforms(self):
        tree = self._platform_tree
        all_items = tree.get_children()
        tree.selection_set(all_items)
        self._update_platform_summary()

    def _select_no_platforms(self):
        tree = self._platform_tree
        tree.selection_set()
        self._lbl_platform_summary.config(text="")

    def _eta_str(self, total_samples, workers):
        """Compute Phase 1, Phase 2, and total ETA strings."""
        # Phase 1 (extraction): ~150ms/sample with parallel workers
        p1_s = total_samples * 0.15 / workers
        # Phase 2 (collapse/repair): ~200ms/sample with parallel workers
        p2_s = total_samples * 0.20 / workers
        # DB load + NCBI scrape + memory build overhead: ~2min per platform
        n_plat = max(1, len(self._platform_tree.selection()))
        overhead_s = n_plat * 120
        total_s = p1_s + p2_s + overhead_s

        def _fmt(s):
            h, m = int(s // 3600), int((s % 3600) // 60)
            if h > 0:
                return f"{h}h {m}m"
            return f"{m}m"

        return _fmt(p1_s), _fmt(p2_s), _fmt(total_s)

    def _update_platform_summary(self):
        """Update the summary label based on current selection."""
        tree = self._platform_tree
        selected = tree.selection()
        if not selected or not self._discovered_platforms:
            self._lbl_platform_summary.config(text="")
            return
        # Sum samples for selected platforms
        sel_set = set(selected)
        total = sum(p["sample_count"] for p in self._discovered_platforms
                    if p["gpl"] in sel_set)
        workers_str = self._var_workers.get().strip()
        workers = int(workers_str) if workers_str.isdigit() else 8
        p1_eta, p2_eta, total_eta = self._eta_str(total, workers)
        self._lbl_platform_summary.config(
            text=f"{len(selected)} platforms | {total:,} samples | "
                 f"P1:{p1_eta} P2:{p2_eta} Total:~{total_eta} ({workers}w)")

    def _get_selected_platforms(self):
        """Return list of (gpl, title, sample_count) for selected platforms."""
        sel = set(self._platform_tree.selection())
        return [(p["gpl"], p["title"], p["sample_count"])
                for p in self._discovered_platforms if p["gpl"] in sel]

    def _check_files(self):
        d   = self._var_dir.get()
        gpl = self._var_platform.get()
        tp  = os.path.join(d, f"matrix_tissue_{gpl}.csv")
        cp  = os.path.join(d, f"matrix_condition_annotated_{gpl}.csv.gz")
        self._lbl_files.config(
            text=(f"{'OK' if os.path.exists(tp) else 'MISSING'} matrix_tissue_{gpl}.csv\n"
                  f"{'OK' if os.path.exists(cp) else 'MISSING'} "
                  f"matrix_condition_annotated_{gpl}.csv.gz\n"
                  + ("OK " if os.path.exists(os.path.join(d, f"matrix_treatment_{gpl}.csv"))
                     else "-- ") + f"matrix_treatment_{gpl}.csv (optional)\n"                  + ("OK " if os.path.exists(os.path.join(d, "LLM_memory", "Treatments_clusters.txt"))
                     else "-- ") + "LLM_memory/Treatments_clusters.txt (optional)"
                  ))
        db = self._find_db(d)
        if db:
            self._lbl_db_status.config(
                text=f"OK GEOmetadb: {os.path.basename(db)}", fg=SUCCESS)
        else:
            self._lbl_db_status.config(
                text="NOT FOUND: GEOmetadb in this folder  (.sqlite or .sqlite.gz)",
                fg=ERROR)

    def _check_env_async(self):
        def _check():
            ok = ollama_server_ok()
            self._lbl_ollama.config(
                text=f"Ollama: {'OK running' if ok else ('NOT running (will start on Run)' if ollama_binary_exists() else '⚠ not installed (will install)')}",
                fg=SUCCESS if ok else WARNING)
            if ok:
                mdl  = self._var_model.get()
                m_ok = model_available(mdl)
                self._lbl_model.config(
                    text=f"Model: {'OK ready' if m_ok else 'NOT pulled (will pull)'}",
                    fg=SUCCESS if m_ok else WARNING)
            else:
                self._lbl_model.config(text="Model: (check after Ollama starts)", fg=FG2)
            gpus = detect_gpus()
            if gpus:
                names = " | ".join(f"{g['name']} {g['vram_gb']}GB" for g in gpus)
                self._lbl_watchdog_top.config(text=f"GPU: {names}", fg=SUCCESS)
        threading.Thread(target=_check, daemon=True).start()

    def _start(self):
        d   = self._var_dir.get()
        gpl = self._var_platform.get()
        mode = self._var_mode.get()

        # Auto-find GEOmetadb in the same folder
        db_path = self._find_db(d)
        if not db_path:
            messagebox.showerror("GEOmetadb Not Found",
                f"Could not find a .sqlite or .sqlite.gz file in:\n{d}\n\n"
                "Please place GEOmetadb in the same folder as the harmonized CSVs.")
            return

        limit_str   = self._var_limit.get().strip()
        limit       = int(limit_str) if limit_str.isdigit() else None
        workers_str = self._var_workers.get().strip()
        workers     = int(workers_str) if workers_str.isdigit() else None   # None = auto

        # Check if multi-platform discovery mode is active
        selected_platforms = self._get_selected_platforms()
        is_multi = len(selected_platforms) > 0 and mode != "gsm_list"

        if is_multi:
            # Multi-platform mode — process all selected platforms
            total_samples = sum(p[2] for p in selected_platforms)
            if not messagebox.askyesno(
                "Multi-Platform Run",
                f"Process {len(selected_platforms)} platforms "
                f"({total_samples:,} total samples) sequentially?\n\n"
                + "\n".join(f"  {p[0]}: {p[2]:,} samples"
                            for p in selected_platforms[:10])
                + (f"\n  ... and {len(selected_platforms)-10} more"
                   if len(selected_platforms) > 10 else "")):
                return
            self._running = True
            self._btn_start.config(state=tk.DISABLED)
            self._btn_stop.config(state=tk.NORMAL)
            self._prog_var.set(0)
            self._lbl_prog.config(text="Starting multi-platform run ")
            self._log_msg(
                f"  Starting multi-platform run: "
                f"{len(selected_platforms)} platforms, "
                f"{total_samples:,} samples ", "head")
            config = {
                "db_path":          db_path,
                "platform":         selected_platforms[0][0],  # first platform
                "platforms":        selected_platforms,
                "model":            self._var_model.get().strip(),
                "ollama_url":       self._var_url.get().strip(),
                "harmonized_dir":   d,
                "limit":            limit,
                "num_workers":      workers,
                "skip_install":     self._var_skip.get(),
                "skip_phase2":      self._var_skip_phase2.get(),
                "gsm_list_file":    "",
            }
            self._thread = threading.Thread(
                target=self._run_with_setup_multi, args=(config,), daemon=True)
            self._thread.start()
        else:
            # Single platform mode (legacy or GSM list)
            if mode != "gsm_list":
                tp = os.path.join(d, f"matrix_tissue_{gpl}.csv")
                cp = os.path.join(d, f"matrix_condition_annotated_{gpl}.csv.gz")
                missing = [os.path.basename(p) for p in (tp, cp)
                           if not os.path.exists(p)]
                if missing:
                    # Check if we can load from DB instead
                    if not messagebox.askyesno(
                        "CSV Files Not Found",
                        f"Harmonized CSV files for {gpl} not found.\n\n"
                        f"Load {gpl} directly from GEOmetadb and annotate "
                        f"from scratch?"):
                        return

            self._running = True
            self._btn_start.config(state=tk.DISABLED)
            self._btn_stop.config(state=tk.NORMAL)
            self._prog_var.set(0)
            self._lbl_prog.config(text="Starting ")
            self._log_msg(f"  Starting pipeline for {gpl} ", "head")

            config = {
                "db_path":        db_path,
                "platform":       gpl,
                "model":          self._var_model.get().strip(),
                "ollama_url":     self._var_url.get().strip(),
                "harmonized_dir": d,
                "limit":          limit,
                "num_workers":    workers,
                "skip_install":   self._var_skip.get(),
                "skip_phase2":    self._var_skip_phase2.get(),
                "gsm_list_file":  self._var_gsm_file.get().strip()
                                  if mode == "gsm_list" else "",
            }
            self._thread = threading.Thread(
                target=self._run_with_setup, args=(config,), daemon=True)
            self._thread.start()

    def _run_with_setup(self, config):
        q = self._q
        def log(msg):           q.put({"type": "log",      "msg":  msg})
        def prog(pct, label=""): q.put({"type": "progress", "pct":  pct, "label": label})

        server_proc = None
        skip = config.get("skip_install", False)
        try:
            if not skip:
                if not ollama_binary_exists():
                    prog(1, "Installing Ollama ")
                    ok = install_ollama_blocking(log)
                    if not ok: q.put({"type": "done", "success": False}); return
                if not ollama_server_ok(config["ollama_url"]):
                    prog(2, "Starting Ollama server ")
                    num_parallel, _gw, _cw = compute_ollama_parallel(config["model"])
                    server_proc  = start_ollama_server_blocking(log, num_parallel)
                    if server_proc is None:
                        q.put({"type": "done", "success": False}); return
                else:
                    # Ollama is running but may not have the right num_parallel.
                    # Restart it with the correct setting.
                    num_parallel_pre, _, _ = compute_ollama_parallel(config["model"])
                    num_p = config.get("num_workers") or num_parallel_pre
                    _kill_ollama(log)
                    log(f" Restarting Ollama with OLLAMA_NUM_PARALLEL={num_p} ")
                    server_proc = start_ollama_server_blocking(log, num_p)
                    if server_proc is None:
                        log("  Could not restart Ollama  using existing server")
                        log(" Ollama server already running.")
                mdl = config["model"]
                if not model_available(mdl, config["ollama_url"]):
                    prog(3, f"Pulling {mdl} ")
                    def pull_prog(pct): q.put({"type": "progress", "pct": pct,
                                               "label": f"Pulling {mdl} {pct}%"})
                    ok = pull_model_blocking(mdl, log, pull_prog)
                    if not ok: q.put({"type": "done", "success": False}); return
                else:
                    log(f" Model '{mdl}' ready.")
            else:
                log("  Skipping install check.")
                if not ollama_server_ok(config["ollama_url"]):
                    log(f"[ERROR] Ollama not reachable at {config['ollama_url']}")
                    q.put({"type": "done", "success": False}); return

            # Auto-pull gemma2:2b extraction model if not available
            if not model_available(EXTRACTION_MODEL, config["ollama_url"]):
                log(f"Pulling extraction model {EXTRACTION_MODEL} ...")
                pull_model_blocking(EXTRACTION_MODEL, log)
                log(f"{EXTRACTION_MODEL} ready.")
            # Auto-pull gemma2:2b before pipeline starts
            if not model_available(EXTRACTION_MODEL, config["ollama_url"]):
                log(f"Pulling {EXTRACTION_MODEL} ...")
                pull_model_blocking(EXTRACTION_MODEL, log)
                log(f"{EXTRACTION_MODEL} ready.")
            else:
                log(f"{EXTRACTION_MODEL} ready (already pulled).")
            config["server_proc"] = server_proc
            pipeline(config, q)

        except Exception as exc:
            import traceback
            log(f"[ERROR] {exc}\n{traceback.format_exc()}")
            q.put({"type": "done", "success": False})
        finally:
            _kill_ollama()

    def _run_with_setup_multi(self, config):
        """Same as _run_with_setup but calls pipeline_multi instead of pipeline."""
        q = self._q
        def log(msg):           q.put({"type": "log",      "msg":  msg})
        def prog(pct, label=""): q.put({"type": "progress", "pct":  pct, "label": label})

        server_proc = None
        skip = config.get("skip_install", False)
        try:
            if not skip:
                if not ollama_binary_exists():
                    prog(1, "Installing Ollama ")
                    ok = install_ollama_blocking(log)
                    if not ok: q.put({"type": "done", "success": False}); return
                if not ollama_server_ok(config["ollama_url"]):
                    prog(2, "Starting Ollama server ")
                    num_parallel, _gw, _cw = compute_ollama_parallel(config["model"])
                    server_proc = start_ollama_server_blocking(log, num_parallel)
                    if server_proc is None:
                        q.put({"type": "done", "success": False}); return
                else:
                    num_parallel_pre, _, _ = compute_ollama_parallel(config["model"])
                    num_p = config.get("num_workers") or num_parallel_pre
                    _kill_ollama(log)
                    log(f" Restarting Ollama with OLLAMA_NUM_PARALLEL={num_p} ")
                    server_proc = start_ollama_server_blocking(log, num_p)
                    if server_proc is None:
                        log("  Could not restart Ollama  using existing server")
                mdl = config["model"]
                if not model_available(mdl, config["ollama_url"]):
                    prog(3, f"Pulling {mdl} ")
                    def pull_prog(pct): q.put({"type": "progress", "pct": pct,
                                               "label": f"Pulling {mdl} {pct}%"})
                    ok = pull_model_blocking(mdl, log, pull_prog)
                    if not ok: q.put({"type": "done", "success": False}); return
                else:
                    log(f" Model '{mdl}' ready.")
            else:
                log("  Skipping install check.")
                if not ollama_server_ok(config["ollama_url"]):
                    log(f"[ERROR] Ollama not reachable at {config['ollama_url']}")
                    q.put({"type": "done", "success": False}); return

            if not model_available(EXTRACTION_MODEL, config["ollama_url"]):
                log(f"Pulling extraction model {EXTRACTION_MODEL} ...")
                pull_model_blocking(EXTRACTION_MODEL, log)
                log(f"{EXTRACTION_MODEL} ready.")
            else:
                log(f"{EXTRACTION_MODEL} ready (already pulled).")

            config["server_proc"] = server_proc
            # Call multi-platform pipeline instead of single
            pipeline_multi(config, q)

        except Exception as exc:
            import traceback
            log(f"[ERROR] {exc}\n{traceback.format_exc()}")
            q.put({"type": "done", "success": False})
        finally:
            _kill_ollama()

    def _stop(self):
        self._running = False
        self._log_msg("  Stop requested  will finish current GSE batch ", "warn")
        self._btn_stop.config(state=tk.DISABLED)



    def _finish_dialog(self, run_dir: str):
        """Show the final completion messagebox."""
        messagebox.showinfo("Pipeline Complete",
            f"Platform {self._var_platform.get()} done!\n\n"
            f"Run folder:\n{run_dir}\n\n"
            f"    NS_repaired.csv       full audit (raw + final + collapsed)\n"
            f"    full_repaired.csv     complete platform merged\n"
            f"    novel_labels.csv      resolved labels outside original vocab   (in input folder)\n"
            f"    unique_outside_clusters_labels.txt  labels not in LLM_memory   (in input folder)\n"
            f"    raw_extracted.csv     step-1 labels, no context\n"
            f"    collapse_report.csv        every Phase 1.5 change\n"
            f"    new_clusters_report.csv    new clusters added to vocabulary\n"
            f"    new_clusters_report.txt    human-readable new cluster list\n"
            f"    summary.txt                run statistics\n"
            f"    checkpoints/               incremental saves\n\n"
            f"biomedical_memory.db (sole source of truth — .txt files not modified)\n"
            f"updated live during run with all new clusters.")

    def _poll_queue(self):
        try:
            while True:
                item = self._q.get_nowait()
                t    = item.get("type")
                if t == "log":
                    self._log_msg(item["msg"])
                elif t == "progress":
                    self._prog_var.set(item.get("pct", 0))
                    self._lbl_prog.config(text=item.get("label", ""))
                elif t == "show_treatment_bar":
                    self._lbl_treat_stat.config(text="Treatment [----------]   0.0%  resolved: 0  still NS: ?")
                    self._lbl_treat_stat.grid()
                elif t == "stats_live":
                    pc = item.get("per_col", {})
                    col_widgets = [
                        ("Tissue",    self._lbl_tissue_stat),
                        ("Condition", self._lbl_cond_stat),
                        ("Treatment", self._lbl_treat_stat),
                    ]
                    _scratch = bool(item.get("scratch_mode", False))
                    for col, lbl in col_widgets:
                        d = pc.get(col, {})
                        f_ = d.get("fixed", 0); s_ = d.get("ns", 0)
                        tot = f_ + s_
                        # Always show Treatment in scratch mode
                        # Hide only if col has no data and not scratch mode
                        if tot == 0:
                            if col == "Treatment" and _scratch:
                                lbl.grid()  # keep visible, just show zeros
                                lbl.config(
                                    text=f"{col:<10} [----------]   0.0%  resolved: 0  still NS: ?",
                                    fg=FG2)
                            else:
                                lbl.grid_remove()
                            continue
                        lbl.grid()
                        pct = 100 * f_ / tot if tot else 0
                        bar = "" * int(pct / 10) + "" * (10 - int(pct / 10))
                        lbl.config(
                            text=f"{col:<10} [{bar}] {pct:5.1f}%  "
                                 f"resolved: {f_:,}  still NS: {s_:,}",
                            fg=SUCCESS if pct >= 80 else (WARNING if pct >= 40 else FG2))
                    # Latency + speed + ETA
                    ms  = item.get("latency_ms", 0)
                    spd = item.get("speed", 0)
                    eta = item.get("eta", "?")
                    lat_str = f"{ms:.0f} ms/sample" if ms < 1000 else f"{ms/1000:.1f} s/sample"
                    self._lbl_latency.config(
                        text=f"Latency: {lat_str}   |   {spd:.2f} samples/s   |   ETA: {eta}")
                    self._lbl_gse_prog.config(
                        text=f"GSEs: {item.get('gse_done',0):,}/{item.get('gse_total',0):,}  "
                             f"samples: {item.get('sample_num',0):,}/{item.get('total',0):,}  "
                             f"fixed: {item.get('fixed',0):,}  still NS: {item.get('still_ns',0):,}")
                
                elif t == "show_preview_dialog":
                    # Legacy  no longer used (auto-proceed replaced dialog)
                    pass
                elif t == "preview_summary":
                    n_res = item.get("n_resolved", 0)
                    n_tot = item.get("n_total", 0)
                    eta_h = item.get("eta_h", 0)
                    eta_m = item.get("eta_m", 0)
                    self._log(f"  Preview: {n_res}/{n_tot} resolved  "
                              f"| ETA: {eta_h}h {eta_m}m  | running full repair ")
                elif t == "eval_done":
                    self._btn_eval_judge.config(state=tk.NORMAL)
                    self._btn_eval_human.config(state=tk.NORMAL)
                elif t == "validate_done":
                    self._btn_validate.config(state=tk.NORMAL)
                elif t == "watchdog":
                    self._lbl_watchdog.config(text=f"Watchdog: {item['msg']}")
                elif t == "done":
                    self._running = False
                    self._btn_start.config(state=tk.NORMAL)
                    self._btn_stop.config(state=tk.DISABLED)
                    ok = item.get("success", False)
                    self._lbl_prog.config(
                        text="Completed!" if ok else "Finished with errors")
                    if ok:
                        run_dir = item.get("run_dir", self._var_dir.get())
                        self._finish_dialog(run_dir)
                    else:
                        messagebox.showerror("Failed",
                            "An error occurred. Check the log for details.")
        except queue.Empty:
            pass
        self.after(150, self._poll_queue)


# 
#  ENTRY POINT
# 
if __name__ == "__main__":
    app = App()
    app.mainloop()
