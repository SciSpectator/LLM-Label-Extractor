#!/usr/bin/env python3
"""Headless runner for llm_extractor pipeline — GSM list mode (GitHub version)."""
import sys, os, queue, threading, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import llm_extractor as ext

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

config = {
    "db_path":        os.path.join(SCRIPT_DIR, "GEOmetadb.sqlite"),
    "platform":       "CUSTOM",
    "model":          ext.DEFAULT_MODEL,       # gemma4:e2b
    "extraction_model": ext.EXTRACTION_MODEL,  # gemma4:e2b — per-label agents with think=false
    "ollama_url":     ext.DEFAULT_URL,
    "harmonized_dir": SCRIPT_DIR,
    "limit":          None,
    "num_workers":    None,   # auto-detect (no cap)
    "skip_install":   False,
    "gsm_list_file":  "",  # set path to a GSM list CSV file
}

q = queue.Queue()

def log(msg):
    q.put({"type": "log", "msg": msg})

def prog(pct, label=""):
    q.put({"type": "progress", "pct": pct, "label": label})

def drain():
    while True:
        try:
            item = q.get(timeout=1)
        except queue.Empty:
            continue
        if item.get("type") == "log":
            print(item["msg"], flush=True)
        elif item.get("type") == "progress":
            lbl = item.get("label", "")
            pct = item.get("pct", 0)
            if lbl:
                print(f"  [{pct:.0f}%] {lbl}", flush=True)
        elif item.get("type") == "done":
            ok = item.get("success", False)
            print(f"\n{'='*60}")
            print(f"  PIPELINE {'COMPLETED' if ok else 'FAILED'}")
            print(f"{'='*60}", flush=True)
            return

drain_thread = threading.Thread(target=drain, daemon=True)
drain_thread.start()

server_proc = None
try:
    if not ext.ollama_binary_exists():
        print("Installing Ollama...")
        ext.install_ollama_blocking(log)

    if not ext.ollama_server_ok(config["ollama_url"]):
        print("Starting Ollama server...")
        num_p, _, _ = ext.compute_ollama_parallel(config["model"])
        server_proc = ext.start_ollama_server_blocking(log, num_p)
    else:
        num_p, _, _ = ext.compute_ollama_parallel(config["model"])
        ext._kill_ollama(log)
        print(f"Restarting Ollama with OLLAMA_NUM_PARALLEL={num_p}...")
        server_proc = ext.start_ollama_server_blocking(log, num_p)

    for mdl in [config["model"], ext.EXTRACTION_MODEL]:
        if not ext.model_available(mdl, config["ollama_url"]):
            print(f"Pulling {mdl}...")
            ext.pull_model_blocking(mdl, log)
        else:
            print(f"{mdl} ready.")

    config["server_proc"] = server_proc

    ext.pipeline(config, q)

except KeyboardInterrupt:
    print("\nInterrupted by user.")
    q.put({"type": "done", "success": False})
except Exception as exc:
    import traceback
    print(f"[ERROR] {exc}\n{traceback.format_exc()}")
    q.put({"type": "done", "success": False})
finally:
    ext._kill_ollama()

drain_thread.join(timeout=10)
