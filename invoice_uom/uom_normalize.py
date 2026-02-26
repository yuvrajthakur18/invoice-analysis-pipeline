"""UOM detection, pack-quantity extraction, and normalisation.

All logic is deterministic (regex / rules).  No LLM calls.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from invoice_uom import config

# ── OCR noise clean-up ──────────────────────────────────────────────────────

# Common OCR misreads: digit↔letter
_OCR_FIXES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bC4SE\b", re.I), "CASE"),
    (re.compile(r"\bCA5E\b", re.I), "CASE"),
    (re.compile(r"\bB0X\b",  re.I), "BOX"),
    (re.compile(r"\bP\s*K\s*(\d+)", re.I), r"PK\1"),   # "P K1O" → "PK10"
    (re.compile(r"\bE\s*A\b", re.I), "EA"),              # "E A" → "EA"
    (re.compile(r"\b1O\b"),    "10"),                     # "1O" → "10" (letter O)
    (re.compile(r"\bI(\d)\b"), r"1\1"),                   # "I2" → "12"
    (re.compile(r"\b(\d)O\b"), r"\g<1>0"),                # "2O" → "20"
    (re.compile(r"\b(\d)l\b"), r"\g<1>1"),                # "2l" → "21"
    (re.compile(r"\b(\d)S\b"), r"\g<1>5"),                # "2S" → "25" in numeric ctx
]


def _clean_ocr(text: str) -> str:
    """Apply OCR-noise corrections and normalise whitespace."""
    text = re.sub(r"[^\S\n]+", " ", text)  # collapse whitespace (keep newlines)
    text = text.strip().rstrip(".")
    for pat, repl in _OCR_FIXES:
        text = pat.sub(repl, text)
    return text


# ── pack-quantity patterns ──────────────────────────────────────────────────
# Order matters: more specific patterns first.

@dataclass
class UOMParseResult:
    """Result of parsing UOM / pack info from a text fragment."""

    original_uom: str | None = None           # raw UOM token found
    canonical_uom: str | None = None          # mapped via alias table
    detected_pack_quantity: int | None = None
    evidence_text: str | None = None          # substring that matched
    pack_evidence_text: str | None = None     # substring for pack qty


# Compiled pattern list: each yields (pack_qty_group, uom_group, full_match)
_PACK_PATTERNS: list[tuple[re.Pattern[str], int, int]] = [
    # "25/CS", "100/CASE", "12/BX"
    (re.compile(r"\b(\d+)\s*/\s*([A-Za-z]{2,})\b"), 1, 2),
    # "PK10", "PK 10", "PACK10"
    (re.compile(r"\b(PK|PACK|PKG)\s*(\d+)\b", re.I), 2, 1),
    # "case of 12", "box of 24", "pack of 10"
    (re.compile(r"\b(CASE|BOX|PACK|PACKAGE|PKG)\s+OF\s+(\d+)\b", re.I), 2, 1),
    # "(10 per pack)", "(6 per case)"
    (re.compile(r"\(?\s*(\d+)\s+PER\s+(PACK|CASE|BOX|PACKAGE|PKG|ROLL|BAG)\s*\)?", re.I), 1, 2),
    # "1000 EA", "50 EACH"
    (re.compile(r"\b(\d+)\s+(EA|EACH|UNIT|PC|PCS|PIECE|PIECES)\b", re.I), 1, 2),
    # "CS 12" – UOM followed by qty
    (re.compile(r"\b(CS|CASE|BX|BOX|PK|PACK|PKG)\s+(\d+)\b", re.I), 2, 1),
]

# Standalone UOM token (no pack qty)
_UOM_ONLY_PATTERN = re.compile(
    r"\b("
    + "|".join(sorted(config.UOM_ALIASES.keys(), key=len, reverse=True))
    + r")\b",
    re.I,
)


def parse_uom_and_pack(text: str) -> UOMParseResult:
    """Extract UOM and pack quantity from *text* using regex rules.

    Returns an :class:`UOMParseResult` with whatever could be detected.
    Fields that could not be determined remain ``None``.
    """
    if not text:
        return UOMParseResult()

    cleaned = _clean_ocr(text)
    result = UOMParseResult()

    # Try pack patterns first (they also yield a UOM).
    for pat, qty_grp, uom_grp in _PACK_PATTERNS:
        m = pat.search(cleaned)
        if m:
            try:
                qty = int(m.group(qty_grp))
            except (ValueError, IndexError):
                continue
            raw_uom = m.group(uom_grp).upper()
            canonical = config.UOM_ALIASES.get(raw_uom, raw_uom)
            result.original_uom = raw_uom
            result.canonical_uom = canonical
            result.detected_pack_quantity = qty
            result.evidence_text = m.group(0)
            result.pack_evidence_text = m.group(0)
            return result

    # Fallback: standalone UOM token (no pack qty detected)
    m = _UOM_ONLY_PATTERN.search(cleaned)
    if m:
        raw_uom = m.group(1).upper()
        canonical = config.UOM_ALIASES.get(raw_uom, raw_uom)
        result.original_uom = raw_uom
        result.canonical_uom = canonical
        result.evidence_text = m.group(0)
        # If it's an EA-type UOM, implicit pack qty = 1
        if canonical in config.EACH_UOMS:
            result.detected_pack_quantity = 1
            result.pack_evidence_text = m.group(0)
        return result

    return result


def normalise_uom_code(raw: str) -> str:
    """Map a raw UOM string to its canonical short code via the alias table."""
    return config.UOM_ALIASES.get(raw.upper().strip(), raw.upper().strip())
