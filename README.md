# Invoice UOM – Invoice Ingestion & Line-Item Extraction Pipeline

A production-oriented local pipeline that ingests invoice PDFs, extracts structured
line items, normalises UOM / pack quantities, and optionally resolves missing data
via agentic online lookup + Gemini LLM.

## Quick Start

```bash
# 1. Create a virtual environment
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux / macOS
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

3. Set Gemini API key in the .env file
#    Windows PowerShell:
$env:GEMINI_API_KEY = "your-key-here"
#    Linux / macOS:
export GEMINI_API_KEY="your-key-here"
```

## Usage

### Batch-process all PDFs in a folder

```bash
python -m invoice_uom run --input ./input_pdfs --output ./outputs

or 

python -m invoice_uom run --input ./input_pdfs --output ./outputs --force
```

### Watch a folder for new PDFs (continuous)

```bash
python -m invoice_uom watch --input ./input_pdfs --output ./outputs
```

### Output

| Path | Description |
|------|-------------|
| `./outputs/<name>.json` | Structured line items |
| `./outputs/<name>.debug.json` | Intermediate artefacts & confidence evidence |
| `./failed/<name>.error.json` | Error details on failure |

## Running Tests

```bash
python -m pytest tests/ -v
```

## Architecture

```
invoice_uom/
├── config.py            # Constants, alias maps, known suppliers
├── rate_limit.py        # Token-bucket (7 RPM) + daily cap (20 RPD)
├── cache.py             # SQLite cache for lookup results
├── uom_normalize.py     # Regex/rules for UOM + pack parsing
├── supplier_normalize.py# Alias map + fuzzy match
├── scoring.py           # Explainable confidence scoring
├── extract_docling.py   # Docling PDF extraction
├── ocr_paddle.py        # PaddleOCR fallback
├── line_items.py        # Table → line-item parsing
├── llm_client.py        # Gemini client with rate limiting
├── lookup_agent.py      # Agentic online lookup
├── pipeline.py          # Orchestrator
├── watcher.py           # Watchdog folder watcher
└── cli.py               # CLI entry points
```
