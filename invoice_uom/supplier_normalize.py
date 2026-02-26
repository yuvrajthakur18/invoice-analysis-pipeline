"""Supplier-name extraction helpers & normalisation.

Deterministic alias lookup first, optional fuzzy match via rapidfuzz.
"""

from __future__ import annotations

import re
from typing import Any

from invoice_uom import config

try:
    from rapidfuzz import fuzz, process as rf_process  # type: ignore[import-untyped]
    _HAS_RAPIDFUZZ = True
except ImportError:
    _HAS_RAPIDFUZZ = False


def extract_supplier_candidates(text_blocks: list[str], max_blocks: int = 15) -> list[str]:
    """Heuristically pull supplier-name candidates from the first few text blocks.

    1. Actively searches for known anchor keywords (LLC, INC, www., @)
    2. Falls back to cleaning arbitrary header lines.
    """
    candidates: list[str] = []
    
    # Pass 1: Active anchor search (highest confidence)
    for i, block in enumerate(text_blocks):
        if i >= max_blocks:
            break
        line = block.strip()
        # If the block is too long, don't use it for the anchor search to avoid grabbing paragraphs
        if len(line.split()) < 15:
            if re.search(r"\b(LLC|INC\.?|L\.L\.C\.?|LTD\.?|CORP\.?|COMPANY)\b", line, re.I):
                # Clean up trailing garbage if any
                clean_line = re.sub(r"\|.*$", "", line).strip()
                clean_line = re.sub(r"<!--.*-->", "", clean_line).strip()
                if len(clean_line) > 3:
                    candidates.insert(0, clean_line)  # highest priority
        
        # Look for domain names / emails that give away the company (now correctly matches raw domains without www)
        domain_match = re.search(r"\b([\w\-]+)\.(?:com|net|org|co)\b", line, re.I)
        if domain_match:
            domain = domain_match.group(1).replace("-", " ")
            if len(domain) > 2 and domain.lower() not in ("gmail", "yahoo", "hotmail", "invoice", "sales", "info", "orders", "remit", "www"):
                candidates.append(domain.upper())

    # Pass 2: Heuristic header cleaning
    for i, block in enumerate(text_blocks):
        if i >= max_blocks:
            break
        line = block.strip()
        if not line or len(line) < 3:
            continue
        # Skip markdown artifacts
        if line.startswith("<!--") or line.startswith("##") or line.startswith("#"):
            continue
        if line.startswith("|") or line.startswith("---"):
            continue
        # Skip lines that are just images or formatting
        if re.match(r"^[\[\]!<>()]+", line):
            continue
        # Skip lines that look like dates, phone numbers, addresses only
        if re.search(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b", line): # phone
            continue
        if re.match(r"^[\d\s/\-\.]+$", line):
            continue
        # Skip very short single tokens (e.g. "SOLD", "TO:", field labels)
        if len(line) < 5 and ":" not in line:
            continue
        # Skip field labels like "Cust. No.", "Ship To:", "Invoice #"
        if re.match(r"^(cust|ship|bill|sold|remit|invoice|order|date|page|po|job)\b", line, re.I):
            continue
        # Skip lines that are all numeric or very long paragraphs
        if len(line.split()) > 8:
            continue
        # Prefer multi-word candidates (more likely to be company names)
        if line not in candidates:
            candidates.append(line)
            
    return candidates


def normalise_supplier(raw_name: str) -> tuple[str, str]:
    """Return *(raw_name, normalised_name)*.

    Resolution order:
    1. Exact alias lookup (case-insensitive).
    2. Fuzzy match against ``config.KNOWN_SUPPLIERS`` (if rapidfuzz available).
    3. Fall back to *raw_name*.
    """
    key = raw_name.strip().upper()

    # 1. Alias map
    if key in config.SUPPLIER_ALIASES:
        return raw_name, config.SUPPLIER_ALIASES[key]

    # Also try progressively shorter prefixes of the key
    for word_count in range(len(key.split()), 0, -1):
        prefix = " ".join(key.split()[:word_count])
        if prefix in config.SUPPLIER_ALIASES:
            return raw_name, config.SUPPLIER_ALIASES[prefix]

    # 2. Fuzzy match
    if _HAS_RAPIDFUZZ and config.KNOWN_SUPPLIERS:
        # Use token_set_ratio which is much better at finding full words
        # inside longer strings (e.g. "Staples" inside "Remit to Staples Advantage")
        match = rf_process.extractOne(
            raw_name,
            config.KNOWN_SUPPLIERS,
            scorer=fuzz.token_set_ratio,
            score_cutoff=75,  # Lower cutoff since token_set is more robust
        )
        if match is not None:
            return raw_name, match[0]

    # 3. Fallback
    return raw_name, raw_name
