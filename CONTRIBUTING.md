# Contributing to LLM-Label-Extractor

Thank you for your interest in contributing!

## Author

**Mateusz Szczepaniak** ([@SciSpectator](https://github.com/SciSpectator))

## How to Contribute

### Reporting Bugs

1. Check existing [issues](https://github.com/SciSpectator/LLM-Label-Extractor/issues) to avoid duplicates.
2. Use the [Bug Report template](.github/ISSUE_TEMPLATE/bug_report.md) to file a new issue.
3. Include your OS, Python version, Ollama version, and GPU info.
4. Attach relevant log output (from `batch_run.log` or the GUI log panel).

### Suggesting Features

1. Use the [Feature Request template](.github/ISSUE_TEMPLATE/feature_request.md).
2. Describe the problem your feature would solve and the proposed solution.

### Submitting Pull Requests

1. Fork the repository and create a feature branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. Make your changes. Keep commits focused and well-described.
3. Test your changes:
   - Run the GUI (`python run_gui.py`) and verify basic functionality.
   - Run the batch pipeline (`python run_batch_terminal.py`) on at least one platform.
   - Verify that label extraction and collapse produce reasonable output.
4. Push your branch and open a Pull Request against `main`.

## Development Setup

```bash
# Clone your fork
git clone https://github.com/SciSpectator/LLM-Label-Extractor.git
cd LLM-Label-Extractor

# Install dependencies
pip install -r requirements.txt

# Ensure Ollama is running with the required model
ollama pull gemma2:2b
ollama serve
```

## Project Structure

| File / Directory | Purpose |
|---|---|
| `llm_extractor.py` | Core pipeline: extraction, inference, collapse, memory agent |
| `gui_app.py` | Tkinter GUI application |
| `run_gui.py` | GUI launcher |
| `run_batch_terminal.py` | Headless CLI batch runner |
| `monitor.sh` | Bash script to monitor batch progress |
| `LLM_memory/` | Cluster map files (Tissue, Condition, Treatment) |
| `biomedical_memory.db` | 4-tier memory database (auto-created) |

## Code Style

- Follow PEP 8 where practical.
- Use descriptive variable names, especially for biomedical terms.
- Add docstrings to new functions and classes.
- Keep imports at the top of the file, grouped by standard library, third-party, and local.

## Cluster Files

If you add or modify cluster mappings in `LLM_memory/`, ensure:
- One cluster per line, formatted as expected by the memory agent.
- No duplicate cluster names within a category.
- Biologically accurate groupings (do not merge distinct conditions, e.g., HSV and HIV).

## Questions?

Open a [Discussion](https://github.com/SciSpectator/LLM-Label-Extractor/discussions) or file an issue. We are happy to help!
