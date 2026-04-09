#!/usr/bin/env python3
"""
LLM-Label-Extractor  —  Modern GUI for GEO Label Extraction Pipeline
Fancy dark-theme interface with fluid worker scaling, per-phase progress,
platform discovery, and real-time resource monitoring.
"""

import os, sys, json, threading, queue, time, re, subprocess
from datetime import timedelta
from collections import Counter

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, font as tkfont

# ── Add parent dir so we can import the backend ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PARENT_DIR)

# ══════════════════════════════════════════════════════════════════════════════
#  THEME CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════
BG          = "#0f0f17"       # deep dark background
BG_CARD     = "#1a1a2e"       # card background
BG_INPUT    = "#16213e"       # input fields
BG_HOVER    = "#1f2b47"       # hover state
ACCENT      = "#7c5cbf"       # purple accent
ACCENT_L    = "#9d7fea"       # lighter accent
GREEN       = "#00d26a"       # success / resolved
GREEN_DIM   = "#1a4731"       # muted green bg
BLUE        = "#0ea5e9"       # info / Phase 1
CYAN        = "#22d3ee"       # Phase 1b
ORANGE      = "#f59e0b"       # warning / Phase 2
RED         = "#ef4444"       # error / rejected
RED_DIM     = "#4c1d1d"       # muted red bg
GRAY        = "#6b7280"       # muted text
TEXT        = "#e2e8f0"       # primary text
TEXT_DIM    = "#94a3b8"       # secondary text
BORDER      = "#2d2d44"       # card borders
WHITE       = "#ffffff"

# Unicode icons
ICO_FOLDER  = "\U0001F4C2"    # 📂
ICO_DNA     = "\U0001F9EC"    # 🧬
ICO_BRAIN   = "\U0001F9E0"    # 🧠
ICO_GEAR    = "\u2699\ufe0f"  # ⚙️
ICO_PLAY    = "\u25B6"        # ▶
ICO_STOP    = "\u23F9"        # ⏹
ICO_CHECK   = "\u2705"        # ✅
ICO_WARN    = "\u26A0"        # ⚠
ICO_FIRE    = "\U0001F525"    # 🔥
ICO_GPU     = "\U0001F4BB"    # 💻
ICO_WORKER  = "\U0001F916"    # 🤖
ICO_CHART   = "\U0001F4CA"    # 📊
ICO_MICRO   = "\U0001F52C"    # 🔬
ICO_ROCKET  = "\U0001F680"    # 🚀
ICO_CLOCK   = "\U0001F552"    # 🕒

# ══════════════════════════════════════════════════════════════════════════════
#  SPECIES / TECHNOLOGY LISTS
# ══════════════════════════════════════════════════════════════════════════════
SPECIES_LIST = [
    "Homo sapiens", "Mus musculus", "Rattus norvegicus",
    "Drosophila melanogaster", "Caenorhabditis elegans",
    "Danio rerio", "Saccharomyces cerevisiae", "Arabidopsis thaliana",
    "Sus scrofa", "Bos taurus", "Gallus gallus",
]

TECHNOLOGY_MAP = {
    "Expression Microarray": {
        "in situ oligonucleotide", "spotted DNA/cDNA",
        "spotted oligonucleotide", "oligonucleotide beads",
    },
    "RNA-Seq": {"high-throughput sequencing"},
    "Methylation Array": {"methylation profiling by array"},
    "SNP/Genotyping": {"SNP genotyping by array", "genotyping by array"},
    "miRNA": {"miRNA profiling by array"},
    "All Technologies": set(),  # no filter
}

SEQ_EXCLUDE = re.compile(
    r"sequenc|hiseq|miseq|nextseq|novaseq|ion torrent|solid|pacbio|"
    r"bgiseq|dnbseq|genome analyzer|454 gs|"
    r"cytoscan|snp|genotyp|copy number|cgh|tiling|"
    r"methylat|bisulfite|rrbs|"
    r"chipseq|chip-seq|mirna|microrna|ncrna|lncrna|"
    r"exome|16s|metagenom|"
    r"mapping\\d|mapping array|splicing|"
    r"miRBase|RNAi|shRNA|siRNA",
    re.IGNORECASE
)


# ══════════════════════════════════════════════════════════════════════════════
#  MODERN APP CLASS
# ══════════════════════════════════════════════════════════════════════════════
class ModernApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(f"{ICO_DNA} LLM-Label-Extractor — GEO Label Extraction Pipeline")
        self.configure(bg=BG)
        self.geometry("1400x900")
        self.minsize(1100, 700)

        # State
        self._q = queue.Queue()
        self._running = False
        self._pipeline_thread = None
        self._platforms = []          # discovered platforms
        self._checked = {}            # gpl -> bool (checkbox state)
        self._server_proc = None
        self._db_conn = None
        self._start_time = None       # epoch when pipeline started
        self._last_phase_label = ""   # track last progress label for phase detection

        # Fonts
        self._fn_title = tkfont.Font(family="Segoe UI", size=14, weight="bold")
        self._fn_head  = tkfont.Font(family="Segoe UI", size=11, weight="bold")
        self._fn_body  = tkfont.Font(family="Segoe UI", size=10)
        self._fn_small = tkfont.Font(family="Segoe UI", size=9)
        self._fn_mono  = tkfont.Font(family="Consolas", size=9)
        self._fn_icon  = tkfont.Font(family="Segoe UI Emoji", size=12)

        self._setup_styles()
        self._build_ui()
        self._poll_queue()

    # ── ttk styles ───────────────────────────────────────────────────────
    def _setup_styles(self):
        s = ttk.Style(self)
        s.theme_use("clam")

        s.configure(".", background=BG, foreground=TEXT, font=self._fn_body)
        s.configure("Card.TFrame", background=BG_CARD)
        s.configure("Card.TLabel", background=BG_CARD, foreground=TEXT)
        s.configure("CardDim.TLabel", background=BG_CARD, foreground=TEXT_DIM)
        s.configure("Title.TLabel", background=BG, foreground=WHITE, font=self._fn_title)
        s.configure("Head.TLabel", background=BG_CARD, foreground=ACCENT_L, font=self._fn_head)
        s.configure("Green.TLabel", background=BG_CARD, foreground=GREEN)
        s.configure("Orange.TLabel", background=BG_CARD, foreground=ORANGE)
        s.configure("Red.TLabel", background=BG_CARD, foreground=RED)
        s.configure("Blue.TLabel", background=BG_CARD, foreground=BLUE)

        # Buttons
        s.configure("Accent.TButton", background=ACCENT, foreground=WHITE,
                     font=self._fn_head, padding=(20, 10))
        s.map("Accent.TButton",
              background=[("active", ACCENT_L), ("disabled", GRAY)])
        s.configure("Stop.TButton", background=RED, foreground=WHITE,
                     font=self._fn_head, padding=(20, 10))
        s.map("Stop.TButton", background=[("active", "#dc2626")])
        s.configure("Outline.TButton", background=BG_CARD, foreground=ACCENT_L,
                     padding=(12, 6))
        s.map("Outline.TButton", background=[("active", BG_HOVER)])

        # Progress bars
        s.configure("Blue.Horizontal.TProgressbar",
                     troughcolor=BG_INPUT, background=BLUE, thickness=18)
        s.configure("Cyan.Horizontal.TProgressbar",
                     troughcolor=BG_INPUT, background=CYAN, thickness=18)
        s.configure("Orange.Horizontal.TProgressbar",
                     troughcolor=BG_INPUT, background=ORANGE, thickness=18)
        s.configure("Green.Horizontal.TProgressbar",
                     troughcolor=BG_INPUT, background=GREEN, thickness=14)
        s.configure("Red.Horizontal.TProgressbar",
                     troughcolor=BG_INPUT, background=RED, thickness=14)

        # Combobox
        s.configure("TCombobox", fieldbackground=BG_INPUT, foreground=TEXT,
                     selectbackground=ACCENT, selectforeground=WHITE)
        s.map("TCombobox", fieldbackground=[("readonly", BG_INPUT)])

        # Spinbox
        s.configure("TSpinbox", fieldbackground=BG_INPUT, foreground=TEXT)

        # Treeview
        s.configure("Treeview", background=BG_INPUT, foreground=TEXT,
                     fieldbackground=BG_INPUT, rowheight=24, font=self._fn_small)
        s.configure("Treeview.Heading", background=BG_CARD, foreground=ACCENT_L,
                     font=self._fn_small)
        s.map("Treeview", background=[("selected", ACCENT)],
              foreground=[("selected", WHITE)])

        # Checkbutton
        s.configure("TCheckbutton", background=BG_CARD, foreground=TEXT)

        # Notebook
        s.configure("TNotebook", background=BG, borderwidth=0)
        s.configure("TNotebook.Tab", background=BG_CARD, foreground=TEXT_DIM,
                     padding=(16, 8), font=self._fn_body)
        s.map("TNotebook.Tab",
              background=[("selected", ACCENT)],
              foreground=[("selected", WHITE)])

    # ── Build UI ─────────────────────────────────────────────────────────
    def _build_ui(self):
        # Title bar
        title_frame = tk.Frame(self, bg=BG)
        title_frame.pack(fill="x", padx=12, pady=(12, 4))
        tk.Label(title_frame, text=f"{ICO_DNA} LLM-Label-Extractor", bg=BG, fg=ACCENT_L,
                 font=tkfont.Font(family="Segoe UI", size=20, weight="bold")).pack(side="left")
        tk.Label(title_frame, text="  GEO Label Extraction Pipeline", bg=BG,
                 fg=TEXT_DIM, font=self._fn_head).pack(side="left", padx=(5, 0))

        # Main paned window
        pw = tk.PanedWindow(self, orient="horizontal", bg=BG, sashwidth=6,
                            sashrelief="flat")
        pw.pack(fill="both", expand=True, padx=12, pady=(8, 12))

        # LEFT PANEL — Config
        left = tk.Frame(pw, bg=BG, width=460)
        pw.add(left, minsize=380)
        self._build_left_panel(left)

        # RIGHT PANEL — Progress + Log
        right = tk.Frame(pw, bg=BG)
        pw.add(right, minsize=500)
        self._build_right_panel(right)

    # ── LEFT PANEL ───────────────────────────────────────────────────────
    def _build_left_panel(self, parent):
        canvas = tk.Canvas(parent, bg=BG, highlightthickness=0)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scroll_frame = tk.Frame(canvas, bg=BG)

        scroll_frame.bind("<Configure>",
                          lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        self._left_canvas_win = canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Stretch inner frame to canvas width (eliminates right-side gaps)
        def _on_canvas_resize(event):
            canvas.itemconfig(self._left_canvas_win, width=event.width)
        canvas.bind("<Configure>", _on_canvas_resize)

        # Bind mousewheel
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # ── Card 1: Data Source ──
        c1 = self._card(scroll_frame, f"{ICO_FOLDER} Data Source")
        # GEOmetadb path
        tk.Label(c1, text="GEOmetadb path:", bg=BG_CARD, fg=TEXT_DIM,
                 font=self._fn_small).pack(anchor="w", padx=10, pady=(5, 0))
        db_frame = tk.Frame(c1, bg=BG_CARD)
        db_frame.pack(fill="x", padx=10, pady=2)
        self._db_var = tk.StringVar(value=os.path.join(PARENT_DIR, "GEOmetadb.sqlite"))
        tk.Entry(db_frame, textvariable=self._db_var, bg=BG_INPUT, fg=TEXT,
                 insertbackground=TEXT, font=self._fn_small,
                 relief="flat", bd=5).pack(side="left", fill="x", expand=True)
        ttk.Button(db_frame, text="Browse", style="Outline.TButton",
                   command=self._browse_db).pack(side="right", padx=(5, 0))

        # OR load CSV
        tk.Label(c1, text="— OR load your own CSV / GSM list —", bg=BG_CARD,
                 fg=GRAY, font=self._fn_small).pack(pady=5)
        csv_frame = tk.Frame(c1, bg=BG_CARD)
        csv_frame.pack(fill="x", padx=10, pady=(0, 8))
        self._csv_var = tk.StringVar()
        tk.Entry(csv_frame, textvariable=self._csv_var, bg=BG_INPUT, fg=TEXT,
                 insertbackground=TEXT, font=self._fn_small,
                 relief="flat", bd=5).pack(side="left", fill="x", expand=True)
        ttk.Button(csv_frame, text="Load CSV", style="Outline.TButton",
                   command=self._browse_csv).pack(side="right", padx=(5, 0))

        # ── Card 2: Platform Discovery ──
        c2 = self._card(scroll_frame, f"{ICO_MICRO} Platform Discovery")

        filter_frame = tk.Frame(c2, bg=BG_CARD)
        filter_frame.pack(fill="x", padx=10, pady=5)

        # Species
        tk.Label(filter_frame, text="Species:", bg=BG_CARD, fg=TEXT_DIM,
                 font=self._fn_small, width=12, anchor="w").grid(row=0, column=0, sticky="w", padx=(0, 4))
        self._species_var = tk.StringVar(value="Homo sapiens")
        species_cb = ttk.Combobox(filter_frame, textvariable=self._species_var,
                                   values=SPECIES_LIST, state="readonly")
        species_cb.grid(row=0, column=1, padx=0, pady=2, sticky="ew")

        # Technology
        tk.Label(filter_frame, text="Technology:", bg=BG_CARD, fg=TEXT_DIM,
                 font=self._fn_small, width=12, anchor="w").grid(row=1, column=0, sticky="w", padx=(0, 4))
        self._tech_var = tk.StringVar(value="Expression Microarray")
        tech_cb = ttk.Combobox(filter_frame, textvariable=self._tech_var,
                                values=list(TECHNOLOGY_MAP.keys()),
                                state="readonly")
        tech_cb.grid(row=1, column=1, padx=0, pady=2, sticky="ew")

        # Min samples
        tk.Label(filter_frame, text="Min samples:", bg=BG_CARD, fg=TEXT_DIM,
                 font=self._fn_small, width=12, anchor="w").grid(row=2, column=0, sticky="w", padx=(0, 4))
        self._min_var = tk.IntVar(value=5)
        ttk.Spinbox(filter_frame, from_=1, to=10000, textvariable=self._min_var,
                     width=8).grid(row=2, column=1, padx=0, pady=2, sticky="w")

        filter_frame.columnconfigure(1, weight=1)

        # Discover button
        ttk.Button(c2, text=f"{ICO_MICRO} Discover Platforms", style="Outline.TButton",
                   command=self._discover_platforms).pack(pady=(5, 3), padx=10, anchor="w")

        # Platform tree with checkboxes
        tree_frame = tk.Frame(c2, bg=BG_CARD)
        tree_frame.pack(fill="both", expand=True, padx=10, pady=(0, 8))

        cols = ("gpl", "samples", "title")
        self._tree = ttk.Treeview(tree_frame, columns=cols, show="headings",
                                   height=10, selectmode="extended")
        self._tree.heading("gpl", text="GPL ID")
        self._tree.heading("samples", text="Samples")
        self._tree.heading("title", text="Platform Title")
        self._tree.column("gpl", width=90, minwidth=80)
        self._tree.column("samples", width=70, minwidth=60)
        self._tree.column("title", width=280, minwidth=200)

        tree_scroll = ttk.Scrollbar(tree_frame, orient="vertical",
                                     command=self._tree.yview)
        self._tree.configure(yscrollcommand=tree_scroll.set)
        self._tree.pack(side="left", fill="both", expand=True)
        tree_scroll.pack(side="right", fill="y")

        # Select all / none
        sel_frame = tk.Frame(c2, bg=BG_CARD)
        sel_frame.pack(fill="x", padx=10, pady=(0, 8))
        ttk.Button(sel_frame, text="Select All", style="Outline.TButton",
                   command=self._select_all).pack(side="left", padx=(0, 5))
        ttk.Button(sel_frame, text="Select None", style="Outline.TButton",
                   command=self._select_none).pack(side="left")
        self._plat_count_var = tk.StringVar(value="No platforms loaded")
        tk.Label(sel_frame, textvariable=self._plat_count_var, bg=BG_CARD,
                 fg=TEXT_DIM, font=self._fn_small).pack(side="right")

        # ── Card 3: Model & Workers ──
        c3 = self._card(scroll_frame, f"{ICO_BRAIN} Model & Workers")

        opt_frame = tk.Frame(c3, bg=BG_CARD)
        opt_frame.pack(fill="x", padx=10, pady=5)

        # Model
        tk.Label(opt_frame, text="LLM Model:", bg=BG_CARD, fg=TEXT_DIM,
                 font=self._fn_small, width=12, anchor="w").grid(row=0, column=0, sticky="w", padx=(0, 4))
        self._model_var = tk.StringVar(value="gemma4:e2b")
        model_cb = ttk.Combobox(opt_frame, textvariable=self._model_var,
                                 values=["gemma4:e2b", "gemma2:2b", "gemma2:9b",
                                         "gemma3:4b", "llama3.2:3b", "phi3:mini"])
        model_cb.grid(row=0, column=1, padx=0, pady=2, sticky="ew")

        # Ollama URL
        tk.Label(opt_frame, text="Ollama URL:", bg=BG_CARD, fg=TEXT_DIM,
                 font=self._fn_small, width=12, anchor="w").grid(row=1, column=0, sticky="w", padx=(0, 4))
        self._url_var = tk.StringVar(value="http://localhost:11434")
        tk.Entry(opt_frame, textvariable=self._url_var, bg=BG_INPUT, fg=TEXT,
                 insertbackground=TEXT, font=self._fn_small, relief="flat",
                 bd=5).grid(row=1, column=1, padx=0, pady=2, sticky="ew")

        # Workers
        tk.Label(opt_frame, text="Workers:", bg=BG_CARD, fg=TEXT_DIM,
                 font=self._fn_small, width=12, anchor="w").grid(row=2, column=0, sticky="w", padx=(0, 4))

        wk_frame = tk.Frame(opt_frame, bg=BG_CARD)
        wk_frame.grid(row=2, column=1, padx=0, pady=2, sticky="ew")

        self._auto_workers = tk.BooleanVar(value=True)
        ttk.Checkbutton(wk_frame, text="Auto (fluid)",
                        variable=self._auto_workers,
                        command=self._toggle_auto_workers).pack(side="left")
        self._workers_var = tk.IntVar(value=210)
        self._workers_spin = ttk.Spinbox(wk_frame, from_=1, to=500,
                                          textvariable=self._workers_var,
                                          width=6, state="disabled")
        self._workers_spin.pack(side="left", padx=(10, 0))

        # Auto-detect button
        ttk.Button(opt_frame, text=f"{ICO_GPU} Detect Resources",
                   style="Outline.TButton",
                   command=self._detect_resources).grid(row=3, column=0,
                   columnspan=2, pady=(8, 5), padx=0, sticky="ew")

        opt_frame.columnconfigure(1, weight=1)

        self._resources_var = tk.StringVar(value="Click 'Detect Resources' to auto-configure")
        tk.Label(c3, textvariable=self._resources_var, bg=BG_CARD, fg=CYAN,
                 font=self._fn_small, wraplength=400).pack(padx=10, pady=(0, 8),
                 anchor="w")

        # ── Card 4: Output ──
        c4 = self._card(scroll_frame, f"{ICO_CHART} Output")
        tk.Label(c4, text="Output directory:", bg=BG_CARD, fg=TEXT_DIM,
                 font=self._fn_small).pack(anchor="w", padx=10, pady=(5, 0))
        out_frame = tk.Frame(c4, bg=BG_CARD)
        out_frame.pack(fill="x", padx=10, pady=(2, 8))
        self._out_var = tk.StringVar(value=PARENT_DIR)
        tk.Entry(out_frame, textvariable=self._out_var, bg=BG_INPUT, fg=TEXT,
                 insertbackground=TEXT, font=self._fn_small,
                 relief="flat", bd=5).pack(side="left", fill="x", expand=True)
        ttk.Button(out_frame, text="Browse", style="Outline.TButton",
                   command=self._browse_output).pack(side="right", padx=(5, 0))

        # Sample limit
        lim_frame = tk.Frame(c4, bg=BG_CARD)
        lim_frame.pack(fill="x", padx=10, pady=(0, 8))
        tk.Label(lim_frame, text="Sample limit (0=all):", bg=BG_CARD,
                 fg=TEXT_DIM, font=self._fn_small).pack(side="left")
        self._limit_var = tk.IntVar(value=0)
        ttk.Spinbox(lim_frame, from_=0, to=999999, textvariable=self._limit_var,
                     width=8).pack(side="left", padx=5)

        # ── START / STOP ──
        btn_frame = tk.Frame(scroll_frame, bg=BG)
        btn_frame.pack(fill="x", pady=(12, 16), padx=8)

        # Center the buttons
        btn_inner = tk.Frame(btn_frame, bg=BG)
        btn_inner.pack(anchor="center")

        self._start_btn = ttk.Button(btn_inner, text=f" {ICO_ROCKET} START PIPELINE ",
                                      style="Accent.TButton",
                                      command=self._start_pipeline)
        self._start_btn.pack(side="left", padx=(0, 12))

        self._stop_btn = ttk.Button(btn_inner, text=f" {ICO_STOP} STOP ",
                                     style="Stop.TButton",
                                     command=self._stop_pipeline, state="disabled")
        self._stop_btn.pack(side="left")

    # ── RIGHT PANEL ──────────────────────────────────────────────────────
    def _build_right_panel(self, parent):
        # Notebook: Progress | Log
        nb = ttk.Notebook(parent)
        nb.pack(fill="both", expand=True)

        # Tab 1: Progress
        prog_tab = tk.Frame(nb, bg=BG)
        nb.add(prog_tab, text=f" {ICO_CHART} Progress ")
        self._build_progress_tab(prog_tab)

        # Tab 2: Log
        log_tab = tk.Frame(nb, bg=BG)
        nb.add(log_tab, text=f" {ICO_CLOCK} Log ")
        self._build_log_tab(log_tab)

    def _build_progress_tab(self, parent):
        # Scrollable progress tab
        prog_canvas = tk.Canvas(parent, bg=BG, highlightthickness=0)
        prog_scroll = ttk.Scrollbar(parent, orient="vertical",
                                     command=prog_canvas.yview)
        prog_inner = tk.Frame(prog_canvas, bg=BG)
        prog_inner.bind("<Configure>",
                        lambda e: prog_canvas.configure(
                            scrollregion=prog_canvas.bbox("all")))
        self._prog_canvas_win = prog_canvas.create_window((0, 0), window=prog_inner, anchor="nw")
        prog_canvas.configure(yscrollcommand=prog_scroll.set)
        prog_canvas.pack(side="left", fill="both", expand=True)
        prog_scroll.pack(side="right", fill="y")

        # Stretch inner frame to canvas width
        def _on_prog_canvas_resize(event):
            prog_canvas.itemconfig(self._prog_canvas_win, width=event.width)
        prog_canvas.bind("<Configure>", _on_prog_canvas_resize)

        # ══════════════════════════════════════════════════════════════════
        #  HERO: Big overall progress — always visible at the top
        # ══════════════════════════════════════════════════════════════════
        hero_card = self._card(prog_inner, f"{ICO_ROCKET} Overall Progress")

        # ── Big percentage + ETA row ──
        hero_top = tk.Frame(hero_card, bg=BG_CARD)
        hero_top.pack(fill="x", padx=12, pady=(6, 2))

        self._hero_pct_var = tk.StringVar(value="0%")
        tk.Label(hero_top, textvariable=self._hero_pct_var, bg=BG_CARD,
                 fg=ACCENT_L,
                 font=tkfont.Font(family="Consolas", size=36, weight="bold")
                 ).pack(side="left")

        hero_right = tk.Frame(hero_top, bg=BG_CARD)
        hero_right.pack(side="right", anchor="ne")
        self._hero_eta_var = tk.StringVar(value="ETA: --:--:--")
        tk.Label(hero_right, textvariable=self._hero_eta_var, bg=BG_CARD,
                 fg=GREEN,
                 font=tkfont.Font(family="Consolas", size=16, weight="bold")
                 ).pack(anchor="e")
        self._hero_elapsed_var = tk.StringVar(value="Elapsed: 0:00:00")
        tk.Label(hero_right, textvariable=self._hero_elapsed_var, bg=BG_CARD,
                 fg=TEXT_DIM, font=self._fn_body).pack(anchor="e")

        # ── Big progress bar ──
        self._setup_styles()   # ensure styles exist
        ttk.Style(self).configure("Hero.Horizontal.TProgressbar",
                                   troughcolor=BG_INPUT, background=ACCENT_L,
                                   thickness=28)
        self._hero_bar = ttk.Progressbar(
            hero_card, maximum=100, style="Hero.Horizontal.TProgressbar")
        self._hero_bar.pack(fill="x", padx=12, pady=(2, 4))

        # ── Sample counter + speed row ──
        hero_stats = tk.Frame(hero_card, bg=BG_CARD)
        hero_stats.pack(fill="x", padx=12, pady=(0, 4))

        self._hero_samples_var = tk.StringVar(value="0 / 0 samples")
        tk.Label(hero_stats, textvariable=self._hero_samples_var, bg=BG_CARD,
                 fg=TEXT, font=self._fn_head).pack(side="left")

        self._hero_speed_var = tk.StringVar(value="")
        tk.Label(hero_stats, textvariable=self._hero_speed_var, bg=BG_CARD,
                 fg=CYAN, font=self._fn_body).pack(side="right")

        # ── Current activity line ──
        self._hero_activity_var = tk.StringVar(
            value="Ready — configure and click START")
        tk.Label(hero_card, textvariable=self._hero_activity_var, bg=BG_CARD,
                 fg=ORANGE, font=self._fn_mono, anchor="w",
                 wraplength=700).pack(fill="x", padx=12, pady=(0, 4))

        # ── Resolved / Still NS counters ──
        cnt_row = tk.Frame(hero_card, bg=BG_CARD)
        cnt_row.pack(fill="x", padx=12, pady=(0, 8))

        self._hero_resolved_var = tk.StringVar(value="Resolved: 0")
        tk.Label(cnt_row, textvariable=self._hero_resolved_var, bg=BG_CARD,
                 fg=GREEN, font=self._fn_head).pack(side="left", padx=(0, 20))

        self._hero_ns_var = tk.StringVar(value="Still NS: 0")
        tk.Label(cnt_row, textvariable=self._hero_ns_var, bg=BG_CARD,
                 fg=RED, font=self._fn_head).pack(side="left", padx=(0, 20))

        self._hero_gse_var = tk.StringVar(value="")
        tk.Label(cnt_row, textvariable=self._hero_gse_var, bg=BG_CARD,
                 fg=TEXT_DIM, font=self._fn_small).pack(side="right")

        # ══════════════════════════════════════════════════════════════════
        #  Resource Monitor (compact)
        # ══════════════════════════════════════════════════════════════════
        res_card = self._card(prog_inner, f"{ICO_GPU} System Resources")

        gauges = tk.Frame(res_card, bg=BG_CARD)
        gauges.pack(fill="x", padx=10, pady=8)

        self._gauge_data = {}
        for i, (name, color) in enumerate([
            ("CPU", BLUE), ("RAM", GREEN), ("VRAM", ORANGE), ("TEMP", RED)
        ]):
            tk.Label(gauges, text=f" {name}", bg=BG_CARD, fg=TEXT_DIM,
                     font=self._fn_small, width=6, anchor="w").grid(row=i, column=0, sticky="w")
            canvas = tk.Canvas(gauges, height=14, bg=BG_INPUT,
                               highlightthickness=0, bd=0)
            canvas.grid(row=i, column=1, padx=5, pady=2, sticky="ew")
            lbl = tk.Label(gauges, text="—", bg=BG_CARD, fg=TEXT,
                           font=self._fn_small, width=18, anchor="w")
            lbl.grid(row=i, column=2, sticky="w")
            self._gauge_data[name] = {"canvas": canvas, "label": lbl, "color": color}

        gauges.columnconfigure(1, weight=1)

        # Worker count
        wk_frame = tk.Frame(res_card, bg=BG_CARD)
        wk_frame.pack(fill="x", padx=10, pady=(0, 8))
        tk.Label(wk_frame, text=f"{ICO_WORKER} Workers:", bg=BG_CARD, fg=TEXT_DIM,
                 font=self._fn_small).pack(side="left")
        self._workers_live_var = tk.StringVar(value="— / —")
        tk.Label(wk_frame, textvariable=self._workers_live_var, bg=BG_CARD,
                 fg=CYAN, font=self._fn_head).pack(side="left", padx=10)
        self._llm_rate_var = tk.StringVar(value="")
        tk.Label(wk_frame, textvariable=self._llm_rate_var, bg=BG_CARD,
                 fg=TEXT_DIM, font=self._fn_small).pack(side="left")

        # ══════════════════════════════════════════════════════════════════
        #  Throttle Thresholds (live — affects the running pipeline!)
        # ══════════════════════════════════════════════════════════════════
        thr_card = self._card(prog_inner, f"{ICO_GEAR} Throttle Thresholds (live)")

        self._threshold_vars = {}
        thr_cfg = self._load_threshold_config()

        thr_grid = tk.Frame(thr_card, bg=BG_CARD)
        thr_grid.pack(fill="x", padx=10, pady=6)

        for i, (key, label, default, color) in enumerate([
            ("CPU_HIGH_PCT",   "CPU scale-down %",   90, BLUE),
            ("CPU_PAUSE_PCT",  "CPU hard-pause %",   95, RED),
            ("VRAM_PAUSE_PCT", "VRAM pause %",       90, ORANGE),
            ("RAM_HIGH_PCT",   "RAM scale-down %",   92, GREEN),
        ]):
            tk.Label(thr_grid, text=label, bg=BG_CARD, fg=color,
                     font=self._fn_small, anchor="w", width=16).grid(
                row=i, column=0, sticky="w", padx=(0, 4), pady=2)

            val = thr_cfg.get(key, default)
            var = tk.IntVar(value=int(val))
            self._threshold_vars[key] = var

            scale = tk.Scale(thr_grid, from_=50, to=100, orient="horizontal",
                             variable=var, bg=BG_CARD, fg=TEXT,
                             troughcolor=BG_INPUT, highlightthickness=0,
                             font=self._fn_small,
                             showvalue=True, sliderlength=18,
                             command=lambda v, k=key: self._on_threshold_change(k))
            scale.grid(row=i, column=1, padx=4, pady=2, sticky="ew")

            pct_lbl = tk.Label(thr_grid, text=f"{int(val)}%", bg=BG_CARD,
                               fg=TEXT, font=self._fn_small, width=5)
            pct_lbl.grid(row=i, column=2, sticky="w")
            self._threshold_vars[f"{key}_lbl"] = pct_lbl

        thr_grid.columnconfigure(1, weight=1)

        # ── Live worker count slider ──
        wk_live_frame = tk.Frame(thr_card, bg=BG_CARD)
        wk_live_frame.pack(fill="x", padx=10, pady=(6, 2))

        tk.Label(wk_live_frame, text=f"{ICO_WORKER} Max workers (live):",
                 bg=BG_CARD, fg=CYAN, font=self._fn_small).pack(side="left")

        self._live_workers_var = tk.IntVar(value=int(thr_cfg.get("MAX_WORKERS", 80)))
        self._live_workers_scale = tk.Scale(
            wk_live_frame, from_=4, to=200, orient="horizontal",
            variable=self._live_workers_var, bg=BG_CARD, fg=TEXT,
            troughcolor=BG_INPUT, highlightthickness=0,
            font=self._fn_small, showvalue=True, sliderlength=18,
            command=lambda v: self._on_live_workers_change())
        self._live_workers_scale.pack(side="left", fill="x", expand=True, padx=(8, 4))

        self._live_workers_lbl = tk.Label(wk_live_frame,
                                           text=f"{self._live_workers_var.get()}",
                                           bg=BG_CARD, fg=CYAN, font=self._fn_head, width=4)
        self._live_workers_lbl.pack(side="left")

        self._thr_status_var = tk.StringVar(
            value="Drag sliders to throttle the running pipeline in real-time")
        tk.Label(thr_card, textvariable=self._thr_status_var, bg=BG_CARD,
                 fg=TEXT_DIM, font=self._fn_small, wraplength=500
                 ).pack(padx=10, pady=(0, 8), anchor="w")

        # ══════════════════════════════════════════════════════════════════
        #  Per-Phase Progress
        # ══════════════════════════════════════════════════════════════════
        phase_card = self._card(prog_inner, f"{ICO_GEAR} Pipeline Phases")

        self._phase_widgets = {}
        for phase, label, color, style in [
            ("P1",  "Phase 1 — Raw Extraction",             BLUE,   "Blue"),
            ("P1b", "Phase 1b — NS Inference (KV-cached)",  CYAN,   "Cyan"),
            ("P2",  "Phase 2 — Label Collapse",             ORANGE, "Orange"),
        ]:
            pf = tk.Frame(phase_card, bg=BG_CARD)
            pf.pack(fill="x", padx=10, pady=4)

            status_lbl = tk.Label(pf, text="⏸", bg=BG_CARD, fg=GRAY,
                                   font=self._fn_body)
            status_lbl.pack(side="left", padx=(0, 5))
            tk.Label(pf, text=label, bg=BG_CARD, fg=color,
                     font=self._fn_body).pack(side="left")

            eta_lbl = tk.Label(pf, text="", bg=BG_CARD, fg=TEXT_DIM,
                               font=self._fn_small)
            eta_lbl.pack(side="right")

            bar = ttk.Progressbar(phase_card, maximum=100,
                                   style=f"{style}.Horizontal.TProgressbar")
            bar.pack(fill="x", padx=10, pady=(0, 2))

            detail_lbl = tk.Label(phase_card, text="", bg=BG_CARD, fg=TEXT_DIM,
                                   font=self._fn_small, anchor="w")
            detail_lbl.pack(fill="x", padx=12, pady=(0, 6))

            self._phase_widgets[phase] = {
                "bar": bar, "status": status_lbl,
                "eta": eta_lbl, "detail": detail_lbl,
            }

        # ══════════════════════════════════════════════════════════════════
        #  Per-Field Resolution (Tissue / Condition / Treatment)
        # ══════════════════════════════════════════════════════════════════
        field_card = self._card(prog_inner, f"{ICO_DNA} Label Resolution")
        self._field_widgets = {}
        for col, color in [("Tissue", BLUE), ("Condition", ORANGE),
                           ("Treatment", GREEN)]:
            ff = tk.Frame(field_card, bg=BG_CARD)
            ff.pack(fill="x", padx=10, pady=3)
            tk.Label(ff, text=col, bg=BG_CARD, fg=color, font=self._fn_body,
                     width=12, anchor="w").pack(side="left")
            bar = ttk.Progressbar(ff, maximum=100,
                                   style="Green.Horizontal.TProgressbar")
            bar.pack(side="left", fill="x", expand=True, padx=5)
            stat_lbl = tk.Label(ff, text="—", bg=BG_CARD, fg=TEXT,
                                font=self._fn_small, width=30, anchor="e")
            stat_lbl.pack(side="right")
            self._field_widgets[col] = {"bar": bar, "label": stat_lbl}

        # ══════════════════════════════════════════════════════════════════
        #  Overall Status (bottom bar)
        # ══════════════════════════════════════════════════════════════════
        status_frame = tk.Frame(prog_inner, bg=BG)
        status_frame.pack(fill="x", padx=8, pady=10)
        self._overall_var = tk.StringVar(value="Ready — configure and click START")
        tk.Label(status_frame, textvariable=self._overall_var, bg=BG, fg=TEXT,
                 font=self._fn_head).pack(anchor="w")
        self._elapsed_var = tk.StringVar(value="")
        tk.Label(status_frame, textvariable=self._elapsed_var, bg=BG, fg=TEXT_DIM,
                 font=self._fn_small).pack(anchor="w")

    def _build_log_tab(self, parent):
        log_frame = tk.Frame(parent, bg=BG)
        log_frame.pack(fill="both", expand=True, padx=8, pady=8)

        self._log_text = tk.Text(log_frame, bg=BG_INPUT, fg=TEXT,
                                  font=self._fn_mono, wrap="word",
                                  insertbackground=TEXT, relief="flat", bd=8,
                                  state="disabled")
        log_scroll = ttk.Scrollbar(log_frame, command=self._log_text.yview)
        self._log_text.configure(yscrollcommand=log_scroll.set)
        self._log_text.pack(side="left", fill="both", expand=True)
        log_scroll.pack(side="right", fill="y")

        # Color tags
        self._log_text.tag_configure("ok", foreground=GREEN)
        self._log_text.tag_configure("warn", foreground=ORANGE)
        self._log_text.tag_configure("err", foreground=RED)
        self._log_text.tag_configure("head", foreground=ACCENT_L)
        self._log_text.tag_configure("dim", foreground=TEXT_DIM)

    # ── Helpers ──────────────────────────────────────────────────────────
    def _card(self, parent, title):
        """Create a styled card frame with title."""
        outer = tk.Frame(parent, bg=BG)
        outer.pack(fill="x", padx=8, pady=4)

        card = tk.Frame(outer, bg=BG_CARD, highlightbackground=BORDER,
                        highlightthickness=1, bd=0)
        card.pack(fill="both", expand=True)

        tk.Label(card, text=f" {title}", bg=BG_CARD, fg=ACCENT_L,
                 font=self._fn_head, anchor="w").pack(fill="x", padx=10, pady=(8, 2))

        sep = tk.Frame(card, bg=BORDER, height=1)
        sep.pack(fill="x", padx=10, pady=(2, 4))

        return card

    # ── Threshold config I/O ─────────────────────────────────────────────
    _THRESHOLD_PATH = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        ".watchdog_thresholds.json")

    def _load_threshold_config(self):
        try:
            with open(self._THRESHOLD_PATH, "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_threshold_config(self, cfg):
        tmp = self._THRESHOLD_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(cfg, f, indent=2)
        os.replace(tmp, self._THRESHOLD_PATH)

    def _on_threshold_change(self, key):
        """Called when user drags a threshold slider — writes to config file
        so the running pipeline's Watchdog picks it up within ~15 seconds."""
        cfg = self._load_threshold_config()
        for k, var in self._threshold_vars.items():
            if k.endswith("_lbl"):
                continue
            cfg[k] = var.get()
        # Also derive resume thresholds (10% below pause)
        if "CPU_PAUSE_PCT" in cfg:
            cfg["CPU_RESUME_PCT"] = max(50, cfg["CPU_PAUSE_PCT"] - 10)
        if "VRAM_PAUSE_PCT" in cfg:
            cfg["VRAM_RESUME_PCT"] = max(50, cfg["VRAM_PAUSE_PCT"] - 10)
        if "CPU_HIGH_PCT" in cfg:
            cfg["CPU_LOW_PCT"] = max(40, cfg["CPU_HIGH_PCT"] - 15)
        if "RAM_HIGH_PCT" in cfg:
            cfg["RAM_LOW_PCT"] = max(50, cfg["RAM_HIGH_PCT"] - 12)
        self._save_threshold_config(cfg)
        # Update labels
        for k, var in self._threshold_vars.items():
            if k.endswith("_lbl"):
                continue
            lbl_w = self._threshold_vars.get(f"{k}_lbl")
            if lbl_w:
                lbl_w.config(text=f"{var.get()}%")
        self._thr_status_var.set(
            f"Saved! Pipeline will apply within ~15s  "
            f"(CPU:{cfg.get('CPU_HIGH_PCT',90)}%/"
            f"{cfg.get('CPU_PAUSE_PCT',95)}%  "
            f"VRAM:{cfg.get('VRAM_PAUSE_PCT',90)}%  "
            f"RAM:{cfg.get('RAM_HIGH_PCT',92)}%)")

    def _on_live_workers_change(self):
        """Called when user drags the live worker slider — writes MAX_WORKERS
        to config file so the running pipeline's Watchdog adjusts within ~15s."""
        new_val = self._live_workers_var.get()
        cfg = self._load_threshold_config()
        cfg["MAX_WORKERS"] = new_val
        self._save_threshold_config(cfg)
        self._live_workers_lbl.config(text=f"{new_val}")
        self._thr_status_var.set(
            f"Workers → {new_val}  (pipeline will adjust within ~15s)")

    def _update_gauge(self, name, pct, text):
        """Update a resource gauge bar."""
        g = self._gauge_data.get(name)
        if not g:
            return
        c = g["canvas"]
        w = c.winfo_width() or 200
        h = c.winfo_height() or 14
        c.delete("all")
        fill_w = max(1, int(w * min(pct, 100) / 100))
        color = g["color"]
        if pct >= 90:
            color = RED
        elif pct >= 75:
            color = ORANGE
        c.create_rectangle(0, 0, fill_w, h, fill=color, outline="")
        g["label"].config(text=text)

    def _log(self, msg, tag=None):
        """Append to log widget."""
        self._log_text.configure(state="normal")
        if tag:
            self._log_text.insert("end", msg + "\n", tag)
        else:
            self._log_text.insert("end", msg + "\n")
        self._log_text.see("end")
        self._log_text.configure(state="disabled")

    # ── Callbacks ────────────────────────────────────────────────────────
    def _browse_db(self):
        path = filedialog.askopenfilename(
            title="Select GEOmetadb",
            filetypes=[("SQLite", "*.sqlite *.sqlite.gz"), ("All", "*.*")])
        if path:
            self._db_var.set(path)

    def _browse_csv(self):
        path = filedialog.askopenfilename(
            title="Select CSV or GSM list",
            filetypes=[("CSV", "*.csv *.txt *.tsv"), ("All", "*.*")])
        if path:
            self._csv_var.set(path)

    def _browse_output(self):
        path = filedialog.askdirectory(title="Select output directory")
        if path:
            self._out_var.set(path)

    def _toggle_auto_workers(self):
        if self._auto_workers.get():
            self._workers_spin.configure(state="disabled")
        else:
            self._workers_spin.configure(state="normal")

    def _select_all(self):
        for item in self._tree.get_children():
            self._tree.selection_add(item)
        self._update_plat_count()

    def _select_none(self):
        self._tree.selection_remove(*self._tree.get_children())
        self._update_plat_count()

    def _update_plat_count(self):
        sel = len(self._tree.selection())
        total = len(self._tree.get_children())
        samples = sum(int(self._tree.item(i, "values")[1])
                      for i in self._tree.selection())
        self._plat_count_var.set(
            f"{sel}/{total} platforms selected ({samples:,} samples)")

    def _detect_resources(self):
        """Auto-detect GPU, RAM, CPU and propose worker count."""
        try:
            import llm_extractor as G
            total, gpu_w, cpu_w = G.compute_ollama_parallel(self._model_var.get())
            gpus = G.detect_gpus()
            import psutil
            ram = psutil.virtual_memory()
            cpu_count = os.cpu_count() or 4

            gpu_str = "No GPU"
            if gpus:
                g = gpus[0]
                gpu_str = f"{g['name']} ({g['vram_gb']:.0f} GB VRAM)"

            info = (f"{ICO_GPU} GPU: {gpu_str}\n"
                    f"{ICO_WORKER} Recommended: {total} workers "
                    f"({gpu_w} GPU + {cpu_w} CPU)\n"
                    f"RAM: {ram.total / 1e9:.0f} GB  |  "
                    f"CPU: {cpu_count} cores")

            self._resources_var.set(info)
            self._workers_var.set(total)
            self._log(info, "ok")
        except Exception as e:
            self._resources_var.set(f"Error: {e}")
            self._log(f"Resource detection failed: {e}", "err")

    def _discover_platforms(self):
        """Load GEOmetadb and populate platform tree."""
        db_path = self._db_var.get()
        if not os.path.isfile(db_path):
            messagebox.showerror("Error", f"GEOmetadb not found: {db_path}")
            return

        self._log(f"Loading GEOmetadb: {os.path.basename(db_path)}...", "head")
        self._tree.delete(*self._tree.get_children())

        def _discover():
            try:
                import sqlite3 as _sql
                species = self._species_var.get()
                tech_name = self._tech_var.get()
                min_n = self._min_var.get()
                tech_set = TECHNOLOGY_MAP.get(tech_name, set())

                self._q.put({"type": "log", "msg": "Querying GEOmetadb on disk..."})
                conn = _sql.connect(db_path)
                cur = conn.cursor()
                if tech_set:
                    tech_list = ",".join(f"'{t}'" for t in tech_set)
                    cur.execute(f"""
                        SELECT g.gpl, g.title, g.technology, COUNT(s.gsm)
                        FROM gpl g JOIN gsm s ON s.gpl = g.gpl
                        WHERE g.organism = ? AND g.technology IN ({tech_list})
                        GROUP BY g.gpl HAVING COUNT(s.gsm) >= ?
                        ORDER BY COUNT(s.gsm) DESC
                    """, (species, min_n))
                else:
                    cur.execute("""
                        SELECT g.gpl, g.title, g.technology, COUNT(s.gsm)
                        FROM gpl g JOIN gsm s ON s.gpl = g.gpl
                        WHERE g.organism = ?
                        GROUP BY g.gpl HAVING COUNT(s.gsm) >= ?
                        ORDER BY COUNT(s.gsm) DESC
                    """, (species, min_n))

                platforms = []
                for row in cur.fetchall():
                    title = row[1] or ""
                    if tech_name == "Expression Microarray" and SEQ_EXCLUDE.search(title):
                        continue
                    platforms.append({
                        "gpl": row[0], "title": title,
                        "technology": row[2] or "", "samples": row[3]
                    })
                conn.close()

                self._q.put({"type": "platforms_loaded", "platforms": platforms})
            except Exception as e:
                self._q.put({"type": "log", "msg": f"Error: {e}", "tag": "err"})

        threading.Thread(target=_discover, daemon=True).start()

    def _start_pipeline(self):
        """Launch the pipeline in a background thread."""
        if self._running:
            return

        csv_path = self._csv_var.get().strip()
        selected = self._tree.selection()

        if not csv_path and not selected:
            messagebox.showwarning("No input",
                                   "Select platforms from the tree or load a CSV file.")
            return

        self._running = True
        self._start_time = time.time()
        self._start_btn.configure(state="disabled")
        self._stop_btn.configure(state="normal")
        self._overall_var.set(f"{ICO_ROCKET} Pipeline running...")
        self._hero_activity_var.set("Starting pipeline...")
        self._tick_elapsed()

        def _run():
            try:
                import llm_extractor as G

                db_path = self._db_var.get()
                model = self._model_var.get()
                url = self._url_var.get()
                out_dir = self._out_var.get()
                limit = self._limit_var.get() or None
                workers = None if self._auto_workers.get() else self._workers_var.get()

                # Set extraction_model to selected model (enables gemma4:e2b per-label agents)
                extraction_model = model

                if csv_path:
                    config = {
                        "db_path": db_path, "platform": "",
                        "model": model, "ollama_url": url,
                        "extraction_model": extraction_model,
                        "harmonized_dir": out_dir, "limit": limit,
                        "num_workers": workers, "skip_install": True,
                        "gsm_list_file": csv_path, "server_proc": None,
                    }
                    G.pipeline(config, self._q)
                else:
                    plats = []
                    for item_id in selected:
                        vals = self._tree.item(item_id, "values")
                        plats.append((vals[0], vals[2], int(vals[1])))

                    if len(plats) == 1:
                        config = {
                            "db_path": db_path, "platform": plats[0][0],
                            "model": model, "ollama_url": url,
                            "extraction_model": extraction_model,
                            "harmonized_dir": out_dir, "limit": limit,
                            "num_workers": workers, "skip_install": True,
                            "gsm_list_file": "", "server_proc": None,
                        }
                        G.pipeline(config, self._q)
                    else:
                        config = {
                            "db_path": db_path, "platform": plats[0][0],
                            "platforms": plats, "model": model,
                            "extraction_model": extraction_model,
                            "ollama_url": url, "harmonized_dir": out_dir,
                            "limit": limit, "num_workers": workers,
                            "skip_install": True, "gsm_list_file": "",
                            "server_proc": None,
                        }
                        G.pipeline_multi(config, self._q)

            except Exception as e:
                self._q.put({"type": "log", "msg": f"PIPELINE ERROR: {e}", "tag": "err"})
            finally:
                self._q.put({"type": "done", "success": True})

        self._pipeline_thread = threading.Thread(target=_run, daemon=True)
        self._pipeline_thread.start()

    def _stop_pipeline(self):
        """Request pipeline stop."""
        self._running = False
        self._start_btn.configure(state="normal")
        self._stop_btn.configure(state="disabled")
        self._overall_var.set(f"{ICO_STOP} Pipeline stopped by user")
        self._log("Pipeline stop requested", "warn")

    # ── Elapsed time ticker ───────────────────────────────────────────────
    def _tick_elapsed(self):
        """Update the elapsed timer every second while running."""
        if self._running and self._start_time:
            elapsed = int(time.time() - self._start_time)
            self._hero_elapsed_var.set(f"Elapsed: {timedelta(seconds=elapsed)}")
            self.after(1000, self._tick_elapsed)

    # ── Queue Polling ────────────────────────────────────────────────────
    def _poll_queue(self):
        """Process messages from the pipeline queue."""
        try:
            while True:
                item = self._q.get_nowait()
                mtype = item.get("type", "")

                if mtype == "log":
                    tag = item.get("tag")
                    msg = item.get("msg", "")
                    if not tag:
                        if "ERROR" in msg or "FAILED" in msg:
                            tag = "err"
                        elif "WARN" in msg:
                            tag = "warn"
                        elif msg.startswith("━") or msg.startswith("═") or msg.startswith("▶"):
                            tag = "head"
                    self._log(msg, tag)
                    # Update hero activity with meaningful log lines
                    if msg and not msg.startswith(" ") and len(msg) > 5:
                        short = msg[:120]
                        self._hero_activity_var.set(short)

                elif mtype == "progress":
                    pct = item.get("pct", 0)
                    label = item.get("label", "")
                    phase = item.get("phase")
                    self._last_phase_label = label

                    # ── Update hero bar with raw pipeline pct ──
                    self._hero_bar["value"] = min(pct, 100)
                    self._hero_pct_var.set(f"{min(pct, 100):.0f}%")
                    if label:
                        self._hero_activity_var.set(label[:120])

                    # ── Route to phase bars ──
                    # Detect phase from pct ranges + label text
                    if phase and phase in self._phase_widgets:
                        target = phase
                    elif "Phase 1b" in label or "P1b" in label:
                        target = "P1b"
                    elif "Phase 1" in label or pct <= 22:
                        target = "P1"
                    elif "Phase 2" in label or "Collapse" in label:
                        target = "P2"
                    elif pct <= 34:
                        # pct 22-34 = Phase 1b range
                        target = "P1b"
                    else:
                        # pct 35+ = main NS repair loop
                        target = "P2"

                    w = self._phase_widgets[target]
                    # Normalize pct into phase-local 0-100 range
                    if target == "P1":
                        local_pct = min(pct / 22 * 100, 100) if pct <= 22 else 100
                    elif target == "P1b":
                        local_pct = min((pct - 22) / 12 * 100, 100) if pct > 22 else 0
                    else:  # P2
                        local_pct = min((pct - 34) / 64 * 100, 100) if pct > 34 else 0
                    w["bar"]["value"] = max(0, min(local_pct, 100))
                    w["detail"].config(text=label)
                    colors = {"P1": BLUE, "P1b": CYAN, "P2": ORANGE}
                    w["status"].config(text="▶", fg=colors.get(target, GREEN))

                    # Parse ETA from label if present
                    eta_m = re.search(r'ETA:\s*(\S+)', label)
                    if eta_m:
                        w["eta"].config(text=f"ETA: {eta_m.group(1)}")
                        self._hero_eta_var.set(f"ETA: {eta_m.group(1)}")

                    # Parse sample count from label
                    samp_m = re.search(
                        r'(\d[\d,]*)/(\d[\d,]*)\s*samples', label)
                    if samp_m:
                        cur_s = samp_m.group(1)
                        tot_s = samp_m.group(2)
                        self._hero_samples_var.set(
                            f"{cur_s} / {tot_s} samples")

                    # Parse resolved / still NS
                    res_m = re.search(r'resolved:\s*([\d,]+)', label)
                    ns_m = re.search(r'still NS:\s*([\d,]+)', label)
                    if res_m:
                        self._hero_resolved_var.set(
                            f"Resolved: {res_m.group(1)}")
                    if ns_m:
                        self._hero_ns_var.set(
                            f"Still NS: {ns_m.group(1)}")

                    # Parse speed
                    spd_m = re.search(r'(\d+)\s*ms/sample', label)
                    if spd_m:
                        self._hero_speed_var.set(
                            f"{spd_m.group(1)} ms/sample")
                    else:
                        spd_m2 = re.search(
                            r'([\d.]+)\s*s/sample', label)
                        if spd_m2:
                            self._hero_speed_var.set(
                                f"{spd_m2.group(1)} s/sample")

                elif mtype == "watchdog":
                    msg = item.get("msg", "")
                    cpu_m = re.search(r'CPU (\d+)%', msg)
                    ram_m = re.search(r'RAM.*?(\d+)%', msg)
                    vram_m = re.search(r'VRAM.*?(\d+)%', msg)
                    cpu_temp = re.search(r'CPU:(\d+)', msg)
                    gpu_temp = re.search(r'GPU:(\d+)', msg)
                    wk_m = re.search(r'W:(\d+)/(\d+)', msg)
                    llm_m = re.search(r'LLM/min:(\d+)', msg)

                    if cpu_m:
                        self._update_gauge("CPU", int(cpu_m.group(1)),
                                           f"{cpu_m.group(1)}%  " +
                                           (f"{cpu_temp.group(1)}°C"
                                            if cpu_temp else ""))
                    if ram_m:
                        self._update_gauge("RAM", int(ram_m.group(1)),
                                           f"{ram_m.group(1)}%")
                    if vram_m:
                        self._update_gauge("VRAM", int(vram_m.group(1)),
                                           f"{vram_m.group(1)}%  " +
                                           (f"{gpu_temp.group(1)}°C"
                                            if gpu_temp else ""))
                    if cpu_temp or gpu_temp:
                        ct = int(cpu_temp.group(1)) if cpu_temp else 0
                        gt = int(gpu_temp.group(1)) if gpu_temp else 0
                        mx = max(ct, gt)
                        self._update_gauge("TEMP", min(mx, 100),
                                           f"CPU {ct}°C  GPU {gt}°C")
                    if wk_m:
                        cur, mx = wk_m.group(1), wk_m.group(2)
                        self._workers_live_var.set(f"{cur} / {mx}")
                    if llm_m:
                        self._llm_rate_var.set(
                            f"{llm_m.group(1)} LLM/min")

                elif mtype == "stats_live":
                    per_col = item.get("per_col", {})
                    for col, data in per_col.items():
                        w = self._field_widgets.get(col)
                        if not w:
                            continue
                        fixed = data.get("fixed", 0)
                        ns = data.get("ns", 0)
                        total = fixed + ns
                        pct = 100 * fixed / total if total > 0 else 0
                        w["bar"]["value"] = pct
                        w["label"].config(
                            text=f"{fixed:,} / {total:,}  ({pct:.1f}%)")

                    # ── Update hero section from stats_live ──
                    sn = item.get("sample_num", 0)
                    tot = item.get("total", 0)
                    eta = item.get("eta", "")
                    spd = item.get("speed", 0)
                    lat = item.get("latency_ms", 0)
                    fixed_total = item.get("fixed", 0)
                    ns_total = item.get("still_ns", 0)
                    gse_done = item.get("gse_done", 0)
                    gse_total = item.get("gse_total", 0)
                    scratch = item.get("scratch_mode", False)

                    # Hero bar
                    if tot > 0:
                        overall_pct = 35 + int(60 * sn / tot)
                        self._hero_bar["value"] = min(overall_pct, 100)
                        self._hero_pct_var.set(f"{min(overall_pct, 100)}%")

                    self._hero_samples_var.set(
                        f"{sn:,} / {tot:,} samples")
                    self._hero_eta_var.set(f"ETA: {eta}" if eta else "ETA: --:--:--")
                    self._hero_resolved_var.set(f"Resolved: {fixed_total:,}")
                    self._hero_ns_var.set(f"Still NS: {ns_total:,}")

                    lat_str = (f"{lat:.0f} ms/sample"
                               if lat < 1000
                               else f"{lat/1000:.1f} s/sample")
                    self._hero_speed_var.set(
                        f"{spd:.1f} samples/sec  |  {lat_str}")

                    if gse_total > 0:
                        mode_str = "scratch" if scratch else "repair"
                        self._hero_gse_var.set(
                            f"GSEs: {gse_done:,}/{gse_total:,}  [{mode_str}]")

                    # Also update bottom status
                    self._overall_var.set(
                        f"{ICO_ROCKET} {sn:,} / {tot:,} samples  |  "
                        f"ETA: {eta}")
                    self._elapsed_var.set(
                        f"{spd:.1f} samples/sec  |  {lat_str}")

                elif mtype == "platforms_loaded":
                    platforms = item.get("platforms", [])
                    self._tree.delete(*self._tree.get_children())
                    for p in platforms:
                        self._tree.insert("", "end",
                                          values=(p["gpl"], p["samples"],
                                                  p["title"]))
                    self._plat_count_var.set(
                        f"{len(platforms)} platforms found")
                    self._log(
                        f"Discovered {len(platforms)} platforms", "ok")
                    self._select_all()

                elif mtype == "done":
                    ok = item.get("success", False)
                    self._running = False
                    self._start_btn.configure(state="normal")
                    self._stop_btn.configure(state="disabled")

                    # Final elapsed
                    if self._start_time:
                        elapsed = int(time.time() - self._start_time)
                        self._hero_elapsed_var.set(
                            f"Total: {timedelta(seconds=elapsed)}")

                    if ok:
                        self._overall_var.set(
                            f"{ICO_CHECK} Pipeline complete!")
                        self._hero_activity_var.set("Pipeline complete!")
                        self._hero_pct_var.set("100%")
                        self._hero_bar["value"] = 100
                        self._log("Pipeline finished successfully", "ok")
                    else:
                        self._overall_var.set(
                            f"{ICO_WARN} Pipeline finished with errors")
                        self._hero_activity_var.set(
                            "Pipeline finished with errors")
                        self._log(
                            "Pipeline finished with errors", "warn")

                    for phase in self._phase_widgets:
                        self._phase_widgets[phase]["status"].config(
                            text=ICO_CHECK, fg=GREEN)

        except queue.Empty:
            pass

        self.after(150, self._poll_queue)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    app = ModernApp()
    app.mainloop()

if __name__ == "__main__":
    main()
