"""Central configuration – constants, alias maps, known suppliers, thresholds."""

from __future__ import annotations

import os
from pathlib import Path

# ── directory defaults ──────────────────────────────────────────────────────
BASE_DIR = Path(os.environ.get("INVOICE_UOM_BASE", "."))
INPUT_DIR = BASE_DIR / "input_pdfs"
OUTPUT_DIR = BASE_DIR / "outputs"
FAILED_DIR = BASE_DIR / "failed"
LOG_DIR = BASE_DIR / "logs"
CACHE_DIR = BASE_DIR / ".cache"

# ── UOM alias map → canonical short form ────────────────────────────────────
UOM_ALIASES: dict[str, str] = {
    "EACH": "EA",
    "UNIT": "EA",
    "PC":   "EA",
    "PCS":  "EA",
    "PIECE":"EA",
    "PIECES":"EA",
    "EA":   "EA",

    "CS":   "CS",
    "CASE": "CS",
    "CASES":"CS",

    "BX":   "BX",
    "BOX":  "BX",
    "BOXES":"BX",

    "PK":   "PK",
    "PACK": "PK",
    "PACKS":"PK",
    "PKG":  "PK",
    "PACKAGE":"PK",

    "RL":   "RL",
    "ROLL": "RL",
    "ROLLS":"RL",

    "DZ":   "DZ",
    "DOZEN":"DZ",

    "CT":   "CT",
    "COUNT":"CT",

    "BG":   "BG",
    "BAG":  "BG",
    "BAGS": "BG",

    "TB":   "TB",
    "TUBE": "TB",

    "BT":   "BT",
    "BTL":  "BT",
    "BOTTLE":"BT",

    "GL":   "GL",
    "GAL":  "GL",
    "GALLON":"GL",

    "LB":   "LB",
    "LBS":  "LB",
    "POUND":"LB",

    "OZ":   "OZ",
    "OUNCE":"OZ",

    "SH":   "SH",
    "SHEET":"SH",
    "SHEETS":"SH",
}

# These UOMs are inherently "pack-like" (contain multiple EA)
PACK_UOMS: set[str] = {"CS", "BX", "PK", "RL", "DZ", "CT", "BG", "TB", "BT"}

# These UOMs are already per-base-unit
EACH_UOMS: set[str] = {"EA"}

# ── supplier normalisation ──────────────────────────────────────────────────
KNOWN_SUPPLIERS: list[str] = [
    "Subway Supplies",
    "Sysco",
    "US Foods",
    "Performance Food Group",
    "Gordon Food Service",
    "McLane Company",
    "Ben E. Keith",
    "Shamrock Foods",
    "Reinhart Foodservice",
    "Gala Janitorial Supplies",
    "Interboro Packaging",
    "Imperial Dade",
    "Essendant",
    "S.P. Richards",
    "Fastenal",
    "Grainger",
    "HD Supply",
    "Wesco International",
    "MSC Industrial",
    "Uline",
    "Staples",
    "Office Depot",
    "Magid Glove and Safety Manufacturing Co. LLC",
    "Cintas Corp"
]

SUPPLIER_ALIASES: dict[str, str] = {
    "SYSCO": "Sysco",
    "US FOODS": "US Foods",
    "USFOODS": "US Foods",
    "PFG": "Performance Food Group",
    "GFS": "Gordon Food Service",
    "GORDON FOOD": "Gordon Food Service",
    "MCLANE": "McLane Company",
    "SHAMROCK": "Shamrock Foods",
    "REINHART": "Reinhart Foodservice",
    "GALA": "Gala Janitorial Supplies",
    "GALA JANITORIAL": "Gala Janitorial Supplies",
    "INTERBORO": "Interboro Packaging",
    "IMPERIAL DADE": "Imperial Dade",
    "FASTENAL": "Fastenal",
    "GRAINGER": "Grainger",
    "ULINE": "Uline",
    "STAPLES": "Staples",
    "OFFICE DEPOT": "Office Depot",
    "MSC": "MSC INDUSTRIAL SUPPLY CO.",
    "Magid": "Magid Glove and Safety Manufacturing Co. LLC",
}

# ── LLM / rate-limit constants ──────────────────────────────────────────────
GEMINI_MODEL = "gemini-2.5-flash"
LLM_RPM = 7                     # max requests per minute
LLM_RPD = 20                    # max requests per day
LLM_MAX_RETRIES = 5             # retries on 429 / RESOURCE_EXHAUSTED
LLM_MAX_CALLS_PER_PDF = 3       # budget cap per single PDF run
LLM_TEMPERATURE = 0.0

# ── confidence thresholds ───────────────────────────────────────────────────
CONFIDENCE_THRESHOLD = 0.60     # below this → escalation_flag = True

# ── non-line-item keywords (case-insensitive) ──────────────────────────────
NON_ITEM_KEYWORDS: list[str] = [
    "subtotal", "sub total", "sub-total",
    "total", "grand total",
    "tax", "sales tax", "gst", "vat", "hst",
    "freight", "shipping", "delivery", "handling",
    "discount", "rebate", "credit", "adjustment",
    "round", "rounding", "round-off", "round off",
    "payment", "deposit", "balance due", "amount due",
    "invoice total", "order total", "net total",
    "surcharge", "fuel surcharge", "environmental fee",
]

# ── manifest / idempotency ──────────────────────────────────────────────────
MANIFEST_FILE = CACHE_DIR / "manifest.json"
DAILY_COUNTER_FILE = CACHE_DIR / "daily_llm_counter.json"
CACHE_DB_FILE = CACHE_DIR / "lookup_cache.db"
