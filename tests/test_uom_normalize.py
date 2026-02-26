"""Tests for invoice_uom.uom_normalize – ≥15 cases including OCR noise."""

from __future__ import annotations

import pytest

from invoice_uom.uom_normalize import parse_uom_and_pack, normalise_uom_code, _clean_ocr


# ── OCR clean-up tests ──────────────────────────────────────────────────────

class TestCleanOCR:
    def test_collapse_whitespace(self):
        assert _clean_ocr("  hello   world  ") == "hello world"

    def test_trailing_dot(self):
        assert _clean_ocr("EA.") == "EA"

    def test_pk_space_fix(self):
        assert "PK" in _clean_ocr("P K10")

    def test_case_ocr_misread(self):
        assert _clean_ocr("C4SE") == "CASE"

    def test_letter_o_to_zero(self):
        # "1O" should become "10"
        cleaned = _clean_ocr("1O")
        assert "10" in cleaned


# ── parse_uom_and_pack parametrised tests ───────────────────────────────────

@pytest.mark.parametrize(
    "text, expected_uom, expected_pack",
    [
        # Clean pack expressions
        ("25/CS",            "CS",   25),
        ("100/CASE",         "CS",   100),
        ("12/BX",            "BX",   12),
        ("PK10",             "PK",   10),
        ("PACK 6",           "PK",   6),
        ("case of 12",       "CS",   12),
        ("box of 24",        "BX",   24),
        ("(10 per pack)",    "PK",   10),
        ("(6 per case)",     "CS",   6),
        ("1000 EA",          "EA",   1000),
        ("50 EACH",          "EA",   50),
        ("CS 12",            "CS",   12),

        # Standalone UOM (no pack qty)
        ("EA",               "EA",   1),      # EA implies pack_qty=1
        ("ROLL",             "RL",   None),   # pack UOM, qty unknown

        # OCR noisy strings
        ("P K10",            "PK",   10),     # space in PK
        ("25/CS",            "CS",   25),     # clean
        ("EA.",              "EA",   1),      # trailing dot
        ("C4SE",             "CS",   None),   # C4SE → CASE (standalone, no qty pattern)

        # Edge cases
        ("",                 None,   None),
        ("some description", None,   None),
    ],
    ids=[
        "25/CS", "100/CASE", "12/BX", "PK10", "PACK6",
        "case_of_12", "box_of_24", "10_per_pack", "6_per_case",
        "1000_EA", "50_EACH", "CS_12",
        "standalone_EA", "standalone_ROLL",
        "ocr_P_K10", "clean_25_CS", "ocr_EA_dot", "ocr_C4SE",
        "empty", "no_uom",
    ],
)
def test_parse_uom_and_pack(text: str, expected_uom: str | None, expected_pack: int | None):
    result = parse_uom_and_pack(text)

    if expected_uom is None:
        assert result.canonical_uom is None, f"Expected no UOM for '{text}', got {result.canonical_uom}"
    else:
        assert result.canonical_uom == expected_uom, (
            f"For '{text}': expected canonical_uom={expected_uom}, got {result.canonical_uom}"
        )

    if expected_pack is None:
        assert result.detected_pack_quantity is None, (
            f"Expected no pack qty for '{text}', got {result.detected_pack_quantity}"
        )
    else:
        assert result.detected_pack_quantity == expected_pack, (
            f"For '{text}': expected pack_qty={expected_pack}, got {result.detected_pack_quantity}"
        )


# ── normalise_uom_code ──────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("EA",    "EA"),
        ("EACH",  "EA"),
        ("each",  "EA"),
        ("CS",    "CS"),
        ("CASE",  "CS"),
        ("BX",    "BX"),
        ("BOX",   "BX"),
        ("PK",    "PK"),
        ("PACK",  "PK"),
        ("ROLL",  "RL"),
        ("DOZEN", "DZ"),
        ("LBS",   "LB"),
        ("GAL",   "GL"),
        ("UNKNOWN", "UNKNOWN"),  # passthrough
    ],
)
def test_normalise_uom_code(raw: str, expected: str):
    assert normalise_uom_code(raw) == expected


# ── evidence text ────────────────────────────────────────────────────────────

def test_evidence_text_populated():
    result = parse_uom_and_pack("25/CS")
    assert result.evidence_text is not None
    assert "25" in result.evidence_text


def test_pack_evidence_text():
    result = parse_uom_and_pack("case of 12")
    assert result.pack_evidence_text is not None
    assert "12" in result.pack_evidence_text
