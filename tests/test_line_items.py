"""Tests for invoice_uom.line_items – filtering & continuation merging."""

from __future__ import annotations

import pytest

from invoice_uom.line_items import (
    extract_line_items_from_tables,
    extract_line_items_from_text,
    _is_non_item,
)


# ── non-item filtering ──────────────────────────────────────────────────────

class TestNonItemFilter:
    @pytest.mark.parametrize(
        "text",
        [
            "Subtotal",
            "SUBTOTAL",
            "Sub Total",
            "Total",
            "Grand Total",
            "Sales Tax",
            "GST",
            "Freight",
            "Shipping",
            "Discount",
            "Round Off",
            "Payment",
            "Balance Due",
            "Invoice Total",
        ],
    )
    def test_non_items_detected(self, text: str):
        assert _is_non_item(text), f"'{text}' should be flagged as non-item"

    @pytest.mark.parametrize(
        "text",
        [
            "Nitrile Gloves Large",
            "Paper Towels 25/CS",
            "Cleaning Solution 1 GAL",
            "Widget A-100",
        ],
    )
    def test_real_items_pass(self, text: str):
        assert not _is_non_item(text), f"'{text}' should NOT be flagged as non-item"


# ── table extraction with continuation rows ─────────────────────────────────

class TestContinuationMerging:
    def test_continuation_row_merged(self):
        table = [
            ["Description", "Qty", "UOM", "Unit Price", "Amount"],
            ["Nitrile Gloves Large", "5", "CS", "24.99", "124.95"],
            ["Blue, Powder-Free", "", "", "", ""],          # continuation row
            ["Paper Towels", "10", "EA", "3.50", "35.00"],
        ]
        items, debug = extract_line_items_from_tables([table])
        assert len(items) == 2
        assert "Blue, Powder-Free" in items[0]["item_description"]
        assert items[1]["item_description"] == "Paper Towels"

    def test_non_items_filtered_from_table(self):
        table = [
            ["Description", "Qty", "Unit Price", "Amount"],
            ["Widget A", "2", "10.00", "20.00"],
            ["Subtotal", "", "", "20.00"],
            ["Tax", "", "", "1.60"],
            ["Total", "", "", "21.60"],
        ]
        items, debug = extract_line_items_from_tables([table])
        assert len(items) == 1
        assert items[0]["item_description"] == "Widget A"

    def test_empty_table_handled(self):
        items, debug = extract_line_items_from_tables([[]])
        assert items == []

    def test_single_row_table_no_crash(self):
        items, debug = extract_line_items_from_tables([
            [["Header1", "Header2"]]
        ])
        assert items == []


# ── text-heuristic extraction ───────────────────────────────────────────────

class TestTextHeuristic:
    def test_basic_text_line_parsing(self):
        text_blocks = [
            "Invoice #12345",
            "Widget Alpha Model X-200A    2    EA    15.99    31.98",
            "Gadget Beta ZZ-100B          1         29.95    29.95",
            "Subtotal                                        61.93",
        ]
        items, debug = extract_line_items_from_text(text_blocks)
        # Should parse at least Widget Alpha
        assert len(items) >= 1
        assert "Widget" in items[0]["item_description"]

    def test_subtotal_filtered_in_text(self):
        text_blocks = [
            "Widget Alpha Model X-200A    2    EA    15.99    31.98",
            "Subtotal                                        31.98",
        ]
        items, debug = extract_line_items_from_text(text_blocks)
        descs = [i["item_description"] for i in items]
        assert not any("Subtotal" in d for d in descs)


# ── column mapping ──────────────────────────────────────────────────────────

class TestColumnMapping:
    def test_various_header_names(self):
        table = [
            ["Particulars", "Qnty", "U/M", "Rate", "Extension"],
            ["Mop Head Industrial", "3", "EA", "8.50", "25.50"],
        ]
        items, debug = extract_line_items_from_tables([table])
        assert len(items) == 1
        assert items[0]["item_description"] == "Mop Head Industrial"
        assert items[0]["quantity"] == 3.0
        assert items[0]["uom_raw"] == "EA"

    def test_sku_and_mpn_extracted(self):
        table = [
            ["SKU", "Description", "Qty", "MFG #", "Price", "Amount"],
            ["SK-001", "Bolt 1/4-20", "100", "MFR-B14", "0.10", "10.00"],
        ]
        items, debug = extract_line_items_from_tables([table])
        assert len(items) == 1
        assert items[0]["sku"] == "SK-001"
        assert items[0]["manufacturer_part_number"] == "MFR-B14"
