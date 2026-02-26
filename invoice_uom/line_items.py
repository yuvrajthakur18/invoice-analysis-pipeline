"""Line-item extraction from raw tables / text blocks.

Handles:
- Column mapping heuristics (desc/qty/uom/unit_price/amount/sku/mpn)
- Continuation-row merging (rows with description but no qty/price)
- Non-line-item filtering (subtotal, tax, freight, etc.)
- Markdown pipe-table detection in text blocks
- Cell value cleaning (strips markdown noise)
"""

from __future__ import annotations

import logging
import re
from typing import Any, cast

from invoice_uom import config  # type: ignore[import-not-found]

logger = logging.getLogger(__name__)


def _clean_cell_value(text: str) -> str:
    """Strip markdown noise from a cell value."""
    if not text:
        return ""
    # Remove HTML comments
    text = re.sub(r"<!--.*?-->", "", text)
    # Remove markdown header markers
    text = re.sub(r"^\s*#{1,6}\s+", "", text)
    # Remove leading/trailing pipes (leftover from bad splits)
    text = text.strip("| ")
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    # Remove trailing dots that are just noise
    text = text.rstrip(".")
    return text

# ── column-identification keywords ──────────────────────────────────────────
_COL_PATTERNS: dict[str, list[str]] = {
    "description": ["desc", "description", "item", "product", "material", "name", "particulars"],
    "quantity":    ["qty", "quantity", "qnty", "ordered", "shipped", "units"],
    "uom":         ["uom", "um", "unit", "measure", "u/m", "pack"],
    "unit_price":  ["unit price", "unit cost", "price", "rate", "unit", "each"],
    "amount":      ["amount", "total", "ext", "extension", "extended", "line total", "net"],
    "sku":         ["sku", "item #", "item no", "item number", "stock", "catalog", "cat #", "cat no"],
    "mpn":         ["mpn", "mfg", "mfr", "manufacturer", "part", "part #", "part no", "mfg #", "mfr part"],
}


def _identify_columns(header: list[str]) -> dict[str, int]:
    """Map semantic column roles to header indices."""
    mapping: dict[str, int] = {}
    
    # Pre-sort patterns by length descending so "item #" is checked before "item"
    sorted_patterns: list[tuple[str, str]] = []
    for role, keywords in _COL_PATTERNS.items():
        for kw in keywords:
            sorted_patterns.append((kw, role))
    sorted_patterns.sort(key=lambda x: len(x[0]), reverse=True)

    for idx, cell in enumerate(header):
        cell_lower = cell.lower().strip()
        
        # 1. Look for exact matches first
        found = False
        for kw, role in sorted_patterns:
            if role in mapping:
                continue
            if cell_lower == kw:
                mapping[role] = idx
                found = True
                break
                
        # 2. Look for partial matches if no exact match
        if not found:
            for kw, role in sorted_patterns:
                if role in mapping:
                    continue
                # If kw is 'item' and cell is 'item #', don't match 'item' if we already
                # have an 'item #' rule that didn't fire (though it should have fired if present)
                # But more importantly, if cell is "material #", don't map to "material" (description)
                if kw in cell_lower:
                    # Anti-heuristics for "item" or "material" matching a number column
                    if kw in ("item", "material") and ("#" in cell_lower or "no." in cell_lower or "number" in cell_lower):
                        continue
                    mapping[role] = idx
                    break

    return mapping

def _find_header_row(table: list[list[str]], max_rows: int = 6) -> tuple[int, dict[str, int]]:
    """Scan the top rows of a table to find the best header row.
    
    Returns
    -------
    (header_idx, col_map)
    """
    best_idx = 0
    best_map: dict[str, int] = {}
    best_score = -1
    
    import itertools
    for i, row in enumerate(itertools.islice(table, max_rows)):
        col_map = _identify_columns(row)
        
        # We value finding 'description' highly.
        score = len(col_map)
        if "description" in col_map:
            score += 2 # boost description
            
        if score > best_score:
            best_map = col_map
            best_idx = i
            best_score = score
            
    return best_idx, best_map


def _is_non_item(text: str) -> bool:
    """Return True if *text* looks like a subtotal / tax / non-item line."""
    text_lower = text.lower().strip()
    return any(kw in text_lower for kw in config.NON_ITEM_KEYWORDS)


def _parse_number(val: str | None) -> float | None:
    """Attempt to parse a numeric value from a cell string."""
    if not val:
        return None
    # Remove currency symbols and commas
    cleaned = re.sub(r"[^\d.\-]", "", val.strip())
    if not cleaned or cleaned in (".", "-"):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _has_numeric_content(row: list[str], skip_idx: int | None = None) -> bool:
    """Return True if the row has at least one numeric cell (ignoring *skip_idx*)."""
    for i, cell in enumerate(row):
        if i == skip_idx:
            continue
        if _parse_number(cell) is not None:
            return True
    return False


def extract_line_items_from_tables(
    tables: list[list[list[str]]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Parse tables into line items.

    Returns
    -------
    items : list[dict]
        Extracted line items with raw field values.
    debug : dict
        Debug info: column mapping, raw rows used, etc.
    """
    all_items: list[dict[str, Any]] = []
    debug_info: dict[str, Any] = {"tables_processed": 0, "column_mappings": [], "raw_rows": []}

    for table in tables:
        if not table or len(table) < 2:
            continue

        debug_info["tables_processed"] += 1

        # Scan the first few rows to find the most likely header
        header_idx, col_map = _find_header_row(table)
        header = table[header_idx]
        debug_info["column_mappings"].append({"header": header, "mapping": col_map, "header_row_index": header_idx})

        if not col_map.get("description"):
            # No description column found — skip or try heuristic
            logger.debug("No description column identified in table header: %s", header)
            # Try using the first column as description
            col_map.setdefault("description", 0)

        desc_idx = col_map.get("description", 0)
        qty_idx = col_map.get("quantity")
        uom_idx = col_map.get("uom")
        uprice_idx = col_map.get("unit_price")
        amount_idx = col_map.get("amount")
        sku_idx = col_map.get("sku")
        mpn_idx = col_map.get("mpn")

        prev_item: dict[str, Any] | None = None

        import itertools
        for row in itertools.islice(table, header_idx + 1, None):
            debug_info["raw_rows"].append(row)

            if not row or all(not c.strip() for c in row):
                continue

            desc = _clean_cell_value(_safe_get(row, desc_idx) or "")
            if not desc:
                continue

            # Filter non-item rows
            if _is_non_item(desc):
                continue

            qty = _parse_number(_clean_cell_value(_safe_get(row, qty_idx) or ""))
            uom_raw = _clean_cell_value(_safe_get(row, uom_idx) or "") or None
            unit_price = _parse_number(_clean_cell_value(_safe_get(row, uprice_idx) or ""))
            amount = _parse_number(_clean_cell_value(_safe_get(row, amount_idx) or ""))
            sku = _clean_cell_value(_safe_get(row, sku_idx) or "") or None
            mpn = _clean_cell_value(_safe_get(row, mpn_idx) or "") or None

            # Continuation-row logic: row has description but no qty and no price
            if qty is None and unit_price is None and amount is None:
                if prev_item is not None:
                    # Merge description
                    p_item = cast(dict[str, Any], prev_item)
                    desc = str(desc)
                    p_item["item_description"] = f"{str(p_item.get('item_description', ''))} {desc}".strip()
                    if sku and not p_item.get("sku"):
                        p_item["sku"] = sku
                    if mpn and not p_item.get("manufacturer_part_number"):
                        p_item["manufacturer_part_number"] = mpn
                    continue

            item: dict[str, Any] = {
                "item_description": desc,
                "quantity": qty,
                "uom_raw": uom_raw,
                "unit_price": unit_price,
                "amount": amount,
                "sku": sku,
                "manufacturer_part_number": mpn,
            }
            all_items.append(item)
            prev_item = item

    all_items = _merge_orphaned_descriptions(all_items)
    return all_items, debug_info


def extract_line_items_from_text(
    text_blocks: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fallback: parse line items from raw text blocks using heuristics.

    First tries to detect markdown pipe tables within text blocks.
    Then falls back to regex-based line matching.
    """
    all_items: list[dict[str, Any]] = []
    debug_info: dict[str, Any] = {"method": "text_heuristic", "matched_lines": []}

    combined = "\n".join(text_blocks)

    # ── Step 1: detect pipe-delimited tables in text blocks ──────────
    pipe_tables = _detect_pipe_tables_in_text(combined)
    if pipe_tables:
        items, tbl_debug = extract_line_items_from_tables(pipe_tables)
        if items:
            debug_info["method"] = "pipe_table_from_text"
            debug_info["pipe_tables_found"] = len(pipe_tables)
            debug_info.update(tbl_debug)
            return items, debug_info

    # ── Step 2: regex line matching ─────────────────────────────────
    line_pat = re.compile(
        r"^(?P<desc>.{10,}?)\s+"
        r"(?P<qty>\d+(?:\.\d+)?)\s+"
        r"(?:(?P<uom>[A-Za-z]{1,6})\s+)?"
        r"(?P<price>\d+(?:,\d{3})*\.\d{2})"
    )

    prev_item: dict[str, Any] | None = None

    for line in combined.splitlines():
        line = _clean_cell_value(line)
        if not line:
            continue

        if _is_non_item(line):
            continue

        m = line_pat.match(line)  # type: ignore
        if m:
            desc = _clean_cell_value(m.group("desc"))
            qty = _parse_number(m.group("qty"))
            uom = m.group("uom")
            price = _parse_number(m.group("price"))

            debug_info["matched_lines"].append(line)

            item: dict[str, Any] = {
                "item_description": desc,
                "quantity": qty,
                "uom_raw": uom.strip().upper() if uom else None,
                "unit_price": price,
                "amount": None,
                "sku": None,
                "manufacturer_part_number": None,
            }
            all_items.append(item)
            prev_item = item
        else:
            if prev_item and not re.search(r"\d+\.\d{2}", line) and len(line) > 3:
                p_item = cast(dict[str, Any], prev_item)
                p_item["item_description"] = f"{str(p_item.get('item_description', ''))} {line}".strip()
                continue

    all_items = _merge_orphaned_descriptions(all_items)
    return all_items, debug_info


def _detect_pipe_tables_in_text(text: str) -> list[list[list[str]]]:
    """Detect markdown pipe tables embedded in text and parse them."""
    tables: list[list[list[str]]] = []
    current_table_lines: list[str] = []

    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("|") and stripped.endswith("|") and stripped.count("|") >= 3:
            current_table_lines.append(stripped)
        elif _is_pipe_separator(stripped) and current_table_lines:
            current_table_lines.append(stripped)
        else:
            if len(current_table_lines) >= 2:
                table = _parse_pipe_rows(current_table_lines)
                if table and len(table) >= 2:
                    tables.append(table)
            current_table_lines = []

    # Flush last
    if len(current_table_lines) >= 2:
        table = _parse_pipe_rows(current_table_lines)
        if table and len(table) >= 2:
            tables.append(table)

    return tables


def _is_pipe_separator(line: str) -> bool:
    """Check if line is a table separator like |---|---|."""
    stripped = line.strip().strip("|").strip()
    if not stripped:
        return False
    return bool(re.match(r"^[\s\-:|]+$", stripped))


def _parse_pipe_rows(lines: list[str]) -> list[list[str]]:
    """Parse pipe-delimited lines into rows of cleaned cells."""
    rows: list[list[str]] = []
    for line in lines:
        if _is_pipe_separator(line):
            continue
        cells = line.split("|")
        if cells and not cells[0].strip():
            cells.pop(0)
        if cells and not cells[-1].strip():
            cells.pop(-1)
        cleaned = [_clean_cell_value(c) for c in cells]
        if any(cleaned):
            rows.append(cleaned)
    return rows


def _merge_orphaned_descriptions(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Post-processing step to merge multi-line description rows.

    If an item has a description, but NO quantity, NO price, and NO amount,
    it is almost certainly an orphaned description line belonging to the item above it.
    This safely merges the text up and removes the row.
    """
    if not items:
        return items

    merged: list[dict[str, Any]] = []
    for item in items:
        # Check if it's an orphan
        is_orphan = (
            bool(item.get("item_description")) and 
            item.get("quantity") is None and 
            item.get("unit_price") is None and 
            item.get("amount") is None and
            item.get("manufacturer_part_number") is None
        )
        
        if is_orphan and merged:
            # Append description to the active parent above
            merged[-1]["item_description"] = f"{merged[-1]['item_description']} {item['item_description']}".strip()
            
            # If the orphan somehow snagged a SKU or UOM, carry it up
            if item.get("sku") and not merged[-1].get("sku"):
                merged[-1]["sku"] = item["sku"]
            if item.get("uom_raw") and not merged[-1].get("uom_raw"):
                merged[-1]["uom_raw"] = item["uom_raw"]
        else:
            merged.append(item)
            
    return merged


def extract_line_items(extraction: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Main entrypoint: parses line items from PDF extraction dict.

    Returns ``(items, debug_info)``.
    """
    tables = extraction.get("tables", [])
    text_blocks = extraction.get("text_blocks", [])

    items: list[dict[str, Any]] = []
    debug_info: dict[str, Any] = {"method": "unknown"}

    if tables:
        items, tbl_debug = extract_line_items_from_tables(tables)
        if items:
            debug_info["method"] = "tables"
            debug_info.update(tbl_debug)
            return items, debug_info

    if text_blocks:
        items, txt_debug = extract_line_items_from_text(text_blocks)
        if items:
            debug_info["method"] = "text_fallback"
            debug_info.update(txt_debug)
            return items, debug_info

    return [], debug_info


def _safe_get(row: list[str], idx: int | None) -> str | None:
    if idx is None:
        return None
    if idx < 0 or idx >= len(row):
        return None
    val = row[idx]
    return val if val.strip() else None
