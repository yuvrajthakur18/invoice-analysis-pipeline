"""Pipeline orchestrator – single-PDF processing end-to-end.

Flow:
  1) Hash check (idempotency).
  2) Try Docling extraction; fall back to PaddleOCR.
  3) Extract line items.
  4) Per item: parse UOM/pack → compute price → trigger lookup if needed → score.
  5) Atomic write to ``outputs/`` or ``failed/``.
  6) Write debug JSON.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import traceback
from pathlib import Path
from typing import Any, Callable

from invoice_uom import config
from invoice_uom.extract_docling import extract_with_docling
from invoice_uom.line_items import extract_line_items
from invoice_uom.lookup_agent import LookupAgent
from invoice_uom.scoring import compute_confidence, should_escalate
from invoice_uom.supplier_normalize import extract_supplier_candidates, normalise_supplier
from invoice_uom.uom_normalize import normalise_uom_code, parse_uom_and_pack

logger = logging.getLogger(__name__)


# ── manifest helpers ────────────────────────────────────────────────────────

def _file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_manifest() -> dict[str, str]:
    try:
        return json.loads(config.MANIFEST_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_manifest(manifest: dict[str, str]) -> None:
    config.MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = config.MANIFEST_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    tmp.replace(config.MANIFEST_FILE)


# ── price computation ──────────────────────────────────────────────────────

def _compute_price_per_base_unit(
    item: dict[str, Any],
    debug_prices: dict[str, Any],
) -> float | None:
    """Deterministic price-per-EA computation with transparent formula logging."""
    unit_price = item.get("unit_price")
    amount = item.get("amount")
    quantity = item.get("quantity")
    original_uom = item.get("original_uom")
    pack_qty = item.get("detected_pack_quantity")

    candidate_price: float | None = None
    formula = ""

    # Step 1: determine candidate price
    if unit_price is not None:
        candidate_price = unit_price
        formula = f"unit_price={unit_price}"
    elif amount is not None and quantity is not None and quantity > 0:
        candidate_price = amount / quantity
        formula = f"amount({amount})/qty({quantity})"
    else:
        debug_prices["formula"] = "insufficient data"
        debug_prices["result"] = None
        return None

    # Step 2: normalise to EA
    if original_uom:
        canonical = normalise_uom_code(original_uom)
        if canonical in config.EACH_UOMS:
            # Already per-unit
            debug_prices["formula"] = formula
            debug_prices["result"] = round(candidate_price, 6)
            return round(candidate_price, 6)

        if canonical in config.PACK_UOMS:
            if pack_qty is not None and pack_qty > 0:
                base = candidate_price / pack_qty
                debug_prices["formula"] = f"({formula})/{pack_qty}(pack_qty)"
                debug_prices["result"] = round(base, 6)
                return round(base, 6)
            else:
                # Cannot safely convert – pack_qty unknown
                debug_prices["formula"] = f"{formula} — pack UOM but pack_qty unknown"
                debug_prices["result"] = None
                return None

    # No UOM specified → treat as unit price directly (but flag ambiguity)
    debug_prices["formula"] = f"{formula} (no UOM, assume per-unit)"
    debug_prices["result"] = round(candidate_price, 6)
    return round(candidate_price, 6)


# ── main orchestrator ──────────────────────────────────────────────────────

def process_pdf(
    pdf_path: Path,
    output_dir: Path | None = None,
    failed_dir: Path | None = None,
    force: bool = False,
    status_cb: Callable[[str], None] | None = None,
) -> dict[str, Any] | None:
    """Process a single invoice PDF end-to-end.

    Returns the final output dict, or ``None`` if already processed.
    """
    output_dir = output_dir or config.OUTPUT_DIR
    failed_dir = failed_dir or config.FAILED_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)

    pdf_name = pdf_path.stem
    fhash = _file_hash(pdf_path)

    # Idempotency check
    if not force:
        manifest = _load_manifest()
        if manifest.get(pdf_name) == fhash:
            logger.info("Skipping %s (already processed, hash match)", pdf_name)
            return None

    logger.info("Processing %s …", pdf_path.name)
    debug: dict[str, Any] = {"file": pdf_path.name, "stages": {}}

    try:
        def _status(msg: str) -> None:
            if status_cb:
                status_cb(msg)

        _status("Extracting PDF text and tables via Docling (fallback: PaddleOCR)...")
        # ── Stage 1: extraction ──────────────────────────────────────────
        extraction = _extract(pdf_path, debug)

        _status("Parsing tables into structured line items...")
        # ── Stage 2: line items ──────────────────────────────────────────
        raw_items, items_debug = extract_line_items(extraction)
        debug["stages"]["line_items"] = items_debug
        logger.info("Extracted %d raw line item(s)", len(raw_items))

        _status("Detecting supplier name from page headers...")
        # ── Stage 3: supplier detection ──────────────────────────────────
        text_blocks = extraction.get("text_blocks", [])
        supplier_raw, supplier_normalised = _detect_supplier(text_blocks, debug)

        # ── Stage 3.5: QUALITY GATE — LLM fallback ──────────────────────
        supplier_looks_bad = _supplier_looks_bad(supplier_normalised)
        items_look_bad = _items_look_bad(raw_items)
        needs_llm = not raw_items or supplier_looks_bad or items_look_bad

        if needs_llm:
            reason = (
                "no items" if not raw_items
                else "bad supplier" if supplier_looks_bad
                else "low quality items"
            )
            logger.info(
                "Quality gate triggered (%s, items=%d, supplier=%r), trying LLM fallback",
                reason, len(raw_items), supplier_normalised,
            )
            _status(f"Quality gate triggered ({reason}). Attempting LLM Extraction Fallback...")
            llm_result = _try_llm_extraction(extraction, pdf_path.name, debug)
            if llm_result:
                if llm_result.get("line_items"):
                    raw_items = llm_result["line_items"]
                    debug["stages"]["llm_extraction"] = {
                        "triggered": True,
                        "reason": reason,
                        "items_from_llm": len(raw_items),
                    }
                if llm_result.get("supplier_name"):
                    supplier_normalised = llm_result["supplier_name"]
                    supplier_raw = llm_result["supplier_name"]
                    debug["stages"]["supplier"]["llm_override"] = supplier_normalised

        # ── Stage 4: per-item enrichment ─────────────────────────────────
        lookup_agent = LookupAgent()
        lookup_agent.reset_pdf_budget()

        # Deduplicate lookup queries
        lookup_queries_done: dict[str, dict[str, Any]] = {}

        _status(f"Enriching and scoring {len(raw_items)} line items...")
        final_items: list[dict[str, Any]] = []
        for raw in raw_items:
            item = _enrich_item(
                raw, supplier_normalised, lookup_agent,
                lookup_queries_done, debug, status_cb
            )
            final_items.append(item)

        # ── Stage 5: build output ────────────────────────────────────────
        num_escalations = sum(1 for i in final_items if i.get("escalation_flag"))
        output = {
            "file": pdf_path.name,
            "supplier_name": supplier_normalised,
            "line_items": final_items,
            "stats": {
                "num_items": len(final_items),
                "num_escalations": num_escalations,
            },
        }

        # Atomic writes
        _atomic_write(output_dir / f"{pdf_name}.json", output)
        _atomic_write(output_dir / f"{pdf_name}.debug.json", debug)

        # Update manifest
        manifest = _load_manifest()
        manifest[pdf_name] = fhash
        _save_manifest(manifest)

        logger.info(
            "✓ %s → %d items, %d escalations",
            pdf_name, len(final_items), num_escalations,
        )
        return output

    except Exception as exc:
        logger.exception("✗ Failed to process %s", pdf_name)
        error_output = {
            "file": pdf_path.name,
            "stage": debug.get("stages", {}).get("current_stage", "unknown"),
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "partial_debug": debug,
        }
        _atomic_write(failed_dir / f"{pdf_name}.error.json", error_output)
        return None


# ── helpers ─────────────────────────────────────────────────────────────────

def _extract(pdf_path: Path, debug: dict[str, Any]) -> dict[str, Any]:
    """Try Docling, fallback to PaddleOCR."""
    debug["stages"]["current_stage"] = "extraction"

    try:
        result = extract_with_docling(pdf_path)
        debug["stages"]["extraction"] = {
            "method": "docling",
            "num_tables": len(result.get("tables", [])),
            "num_text_blocks": len(result.get("text_blocks", [])),
        }
        # If docling returned no tables AND no text, try paddle
        if not result.get("tables") and not result.get("text_blocks"):
            raise RuntimeError("Docling returned no content")
        return result
    except Exception as exc:
        logger.warning("Docling failed (%s), falling back to PaddleOCR", exc)
        debug["stages"]["extraction_docling_error"] = str(exc)

    try:
        from invoice_uom.ocr_paddle import extract_with_paddle
        result = extract_with_paddle(pdf_path)
        debug["stages"]["extraction"] = {
            "method": "paddleocr",
            "num_tables": len(result.get("tables", [])),
            "num_text_blocks": len(result.get("text_blocks", [])),
        }
        return result
    except Exception as exc:
        debug["stages"]["extraction_paddle_error"] = str(exc)
        raise RuntimeError(f"Both Docling and PaddleOCR failed: {exc}") from exc


def _detect_supplier(
    text_blocks: list[str], debug: dict[str, Any]
) -> tuple[str, str]:
    """Extract & normalise supplier name from header text blocks."""
    debug["stages"]["current_stage"] = "supplier_detection"
    candidates = extract_supplier_candidates(text_blocks)
    debug["stages"]["supplier"] = {"candidates": candidates}

    if candidates:
        raw, normalised = normalise_supplier(candidates[0])
        debug["stages"]["supplier"]["raw"] = raw
        debug["stages"]["supplier"]["normalised"] = normalised
        return raw, normalised

    return "", ""


def _enrich_item(
    raw: dict[str, Any],
    supplier_name: str,
    lookup_agent: LookupAgent,
    lookup_queries_done: dict[str, dict[str, Any]],
    debug: dict[str, Any],
    status_cb: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Enrich a single raw item: UOM parsing, price, lookup, scoring."""
    # Parse UOM from all available text
    uom_text_sources: list[str] = []
    if raw.get("uom_raw"):
        uom_text_sources.append(raw["uom_raw"])
    if raw.get("item_description"):
        uom_text_sources.append(raw["item_description"])

    uom_result = None
    for src in uom_text_sources:
        uom_result = parse_uom_and_pack(src)
        if uom_result.original_uom:
            break

    if uom_result is None:
        from invoice_uom.uom_normalize import UOMParseResult
        uom_result = UOMParseResult()

    original_uom = uom_result.original_uom
    pack_qty = uom_result.detected_pack_quantity
    canonical_uom = uom_result.canonical_uom

    # Build partially-enriched item
    item: dict[str, Any] = {
        "supplier_name": supplier_name,
        "item_description": raw.get("item_description", ""),
        "manufacturer_part_number": raw.get("manufacturer_part_number"),
        "original_uom": original_uom,
        "detected_pack_quantity": pack_qty,
        "canonical_base_uom": "EA",
        "quantity": raw.get("quantity"),
        "unit_price": raw.get("unit_price"),
        "amount": raw.get("amount"),
    }

    # Agentic lookup if needed
    evidence: dict[str, Any] = {}
    llm_evidence: dict[str, Any] = {"llm_call_used": False, "llm_call_reason": None,
                                     "llm_call_status": "not_needed", "llm_call_attempts": 0}
    lookup_sources: list[dict[str, str]] = []

    needs_lookup = (
        (canonical_uom not in config.EACH_UOMS and pack_qty is None)
        or (canonical_uom is None)
        or (pack_qty is None and raw.get("quantity") is not None)
    ) and bool(raw.get("sku") or raw.get("manufacturer_part_number") or len(raw.get("item_description", "")) > 5)

    if needs_lookup:
        query_key = LookupAgent._build_query(
            raw.get("item_description", ""),
            raw.get("sku"),
            raw.get("manufacturer_part_number"),
        )
        if query_key and query_key in lookup_queries_done:
            # Reuse previous lookup result
            lr = lookup_queries_done[query_key]
            if lr.get("pack_qty") is not None and pack_qty is None:
                pack_qty = lr["pack_qty"]
                item["detected_pack_quantity"] = pack_qty
            if lr.get("uom") is not None and original_uom is None:
                original_uom = lr["uom"]
                item["original_uom"] = original_uom
            lookup_sources = lr.get("lookup_sources", [])
            llm_evidence = lr.get("llm_result", llm_evidence)
            evidence["lookup_match"] = bool(lr.get("pack_qty") or lr.get("uom"))
        elif query_key:
            if status_cb:
                status_cb(f"Agentic Lookup via DuckDuckGo for: '{query_key}'...")
            lr = lookup_agent.resolve(
                raw.get("item_description", ""),
                raw.get("sku"),
                raw.get("manufacturer_part_number"),
            )
            lookup_queries_done[query_key] = lr
            if lr.get("pack_qty") is not None and pack_qty is None:
                pack_qty = lr["pack_qty"]
                item["detected_pack_quantity"] = pack_qty
            if lr.get("uom") is not None and original_uom is None:
                original_uom = lr["uom"]
                item["original_uom"] = original_uom
            lookup_sources = lr.get("lookup_sources", [])
            llm_evidence = lr.get("llm_result", llm_evidence)
            evidence["lookup_match"] = bool(lr.get("pack_qty") or lr.get("uom"))

    # Price computation
    price_debug: dict[str, Any] = {}
    price = _compute_price_per_base_unit(item, price_debug)
    item["price_per_base_unit"] = price

    # Confidence scoring
    score, breakdown = compute_confidence(item, evidence)
    escalate = should_escalate(score, item)

    item["confidence_score"] = score
    item["escalation_flag"] = escalate

    # Build evidence sub-dict
    item["evidence"] = {
        "uom_evidence_text": uom_result.evidence_text,
        "pack_evidence_text": uom_result.pack_evidence_text,
        "lookup_sources": lookup_sources,
        **llm_evidence,
    }

    # Remove internal-only fields
    item.pop("quantity", None)
    item.pop("unit_price", None)
    item.pop("amount", None)

    # Add price debug to debug artifact
    debug.setdefault("stages", {}).setdefault("price_computations", []).append(price_debug)
    debug.setdefault("stages", {}).setdefault("confidence_breakdowns", []).append(breakdown)

    return item


def _atomic_write(path: Path, data: Any) -> None:
    """Write JSON *data* to *path* atomically (write to temp, then rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=str(path.parent), suffix=".tmp", prefix=".tmp_"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        # On Windows, rename fails if target exists
        if path.exists():
            path.unlink()
        Path(tmp_path).rename(path)
    except Exception:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass
        raise


def _supplier_looks_bad(supplier: str) -> bool:
    """Return True if the supplier name is clearly wrong (markdown artifacts, etc.)."""
    if not supplier or len(supplier) < 3:
        return True
    bad_patterns = ["<!--", "##", "|", "---", "**", "image", "Invoice"]
    return any(p in supplier for p in bad_patterns)


def _items_look_bad(items: list[dict[str, Any]]) -> bool:
    """Return True if most extracted items look like garbage.

    Heuristics:
    - Most descriptions are too short (codes only, no real descriptions)
    - Most items have no UOM
    - Descriptions contain obvious noise patterns
    """
    if not items:
        return True

    short_desc_count: int = 0
    no_uom_count: int = 0
    noise_count: int = 0

    for item in items:
        desc = item.get("item_description", "")
        if len(desc) < 10:
            short_desc_count = short_desc_count + 1  # type: ignore
        if not item.get("uom_raw"):
            no_uom_count = no_uom_count + 1  # type: ignore
        # Check for noise patterns in description
        if desc.startswith("_") or desc.startswith("10.") or "____" in desc:
            noise_count = noise_count + 1  # type: ignore

    total = len(items)
    # If >60% of items have short descs AND no UOM, it's bad
    if short_desc_count > total * 0.6 and no_uom_count > total * 0.6:
        return True
    # If any item is clearly noise
    if noise_count > 0:
        return True
    return False


def _try_llm_extraction(
    extraction: dict[str, Any],
    pdf_name: str,
    debug: dict[str, Any],
) -> dict[str, Any] | None:
    """Attempt LLM-based extraction from raw text blocks."""
    try:
        from invoice_uom.llm_extract import extract_with_llm
    except ImportError:
        logger.warning("llm_extract module not available")
        return None

    # Build raw text from all text blocks + any table content
    text_parts: list[str] = []
    
    # Prioritise the semantic HTML / formatting from Docling for the LLM
    structured = extraction.get("structured_tables", [])
    if structured:
        for t in structured:
            text_parts.append(str(t))
    else:
        # Fallback to flattening the 2D arrays
        for table in extraction.get("tables", []):
            for row in table:
                text_parts.append(" | ".join(str(c) for c in row))

    for block in extraction.get("text_blocks", []):
        text_parts.append(block)

    raw_text = "\n".join(text_parts)
    if not raw_text.strip():
        return None

    result = extract_with_llm(raw_text, pdf_name)
    debug["stages"]["llm_fallback"] = {
        "attempted": True,
        "items_returned": len(result.get("line_items", [])),
        "supplier_returned": result.get("supplier_name", ""),
    }
    return result

