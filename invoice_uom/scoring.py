"""Explainable confidence scoring for extracted line items.

Each component adds or deducts from a base score.  The breakdown is
returned alongside the final score for debug output.
"""

from __future__ import annotations

from typing import Any

from invoice_uom import config


# ── score components ────────────────────────────────────────────────────────

BASE_SCORE = 0.50

COMPONENT_WEIGHTS: dict[str, float] = {
    "has_description":          +0.10,
    "has_quantity":              +0.05,
    "has_unit_price":            +0.05,
    "has_amount":                +0.05,
    "uom_explicit_inline":       +0.10,
    "pack_explicit_inline":      +0.10,
    "lookup_evidence_match":     +0.10,
    "supplier_normalised":       +0.05,
    "has_mpn":                   +0.05,
    # deductions
    "conflicting_evidence":      -0.15,
    "ocr_low_confidence":        -0.10,
    "column_ambiguity":          -0.10,
    "missing_uom_pack_for_price":-0.20,
    "price_null":                -0.10,
}


def compute_confidence(
    item: dict[str, Any],
    evidence: dict[str, Any] | None = None,
) -> tuple[float, dict[str, Any]]:
    """Return ``(score, breakdown)`` for a single line item.

    Parameters
    ----------
    item : dict
        Line-item dict with standard fields.
    evidence : dict | None
        Optional evidence dict (lookup results, OCR flags, etc.).

    Returns
    -------
    score : float
        Clamped to [0.0, 1.0].
    breakdown : dict
        Per-component contribution for debug output.
    """
    evidence = evidence or {}
    score = BASE_SCORE
    breakdown: dict[str, float] = {"base": BASE_SCORE}

    def _apply(name: str, condition: bool) -> None:
        nonlocal score
        if condition:
            delta = COMPONENT_WEIGHTS[name]
            score += delta
            breakdown[name] = delta

    # Positive signals
    _apply("has_description",     bool(item.get("item_description")))
    _apply("has_quantity",        item.get("quantity") is not None)
    _apply("has_unit_price",      item.get("unit_price") is not None)
    _apply("has_amount",          item.get("amount") is not None)
    _apply("uom_explicit_inline", item.get("original_uom") is not None)
    _apply("pack_explicit_inline",item.get("detected_pack_quantity") is not None)
    _apply("supplier_normalised", bool(item.get("supplier_name")))
    _apply("has_mpn",            bool(item.get("manufacturer_part_number")))

    # Lookup evidence
    _apply("lookup_evidence_match", bool(evidence.get("lookup_match")))

    # Deductions
    _apply("conflicting_evidence",      bool(evidence.get("conflicting")))
    _apply("ocr_low_confidence",        bool(evidence.get("ocr_low")))
    _apply("column_ambiguity",          bool(evidence.get("column_ambiguity")))

    # Special: if UOM is a pack type but pack_qty is unknown → can't compute price
    uom = item.get("original_uom")
    if uom:
        from invoice_uom.uom_normalize import normalise_uom_code
        canonical = normalise_uom_code(uom)
        if canonical in config.PACK_UOMS and item.get("detected_pack_quantity") is None:
            _apply("missing_uom_pack_for_price", True)

    if item.get("price_per_base_unit") is None:
        _apply("price_null", True)

    score = max(0.0, min(1.0, round(score, 4)))
    return score, breakdown


def should_escalate(score: float, item: dict[str, Any]) -> bool:
    """Return ``True`` if the item should be escalated for human review."""
    if score < config.CONFIDENCE_THRESHOLD:
        return True
    # Escalate if critical fields missing that prevent price computation
    uom = item.get("original_uom")
    if uom:
        from invoice_uom.uom_normalize import normalise_uom_code
        canonical = normalise_uom_code(uom)
        if canonical in config.PACK_UOMS and item.get("detected_pack_quantity") is None:
            return True
    return False
