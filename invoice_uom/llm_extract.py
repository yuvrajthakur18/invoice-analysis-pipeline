"""LLM-assisted extraction fallback.

Used when deterministic extraction yields 0 line items or clearly
garbage output.  Sends raw text to Gemini and asks for structured
extraction of supplier name + line items.
"""

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

# Load .env if python-dotenv is available
try:
    from dotenv import load_dotenv  # type: ignore[import-untyped]
    # Look for .env in the project root
    _env_path = Path(__file__).resolve().parent.parent / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass

logger = logging.getLogger(__name__)

_EXTRACT_PROMPT = """You are an elite invoice data extraction expert. Your job is to extract highly precise line-item data and supplier information from messy, OCR-extracted invoice text (which may contain HTML tables or raw Markdown).

Your output MUST be a valid JSON object matching this exact schema:

{
  "supplier_name": "Full legal company name of the supplier/vendor (not the buyer).",
  "line_items": [
    {
      "item_description": "Clear, human-readable product description. If the description spans multiple lines in the text, merge them into one string.",
      "manufacturer_part_number": "MPN or manufacturer part # if explicitly visible, else null",
      "sku": "Supplier's SKU/item/catalog number if visible, else null",
      "quantity": <number or null>,
      "uom_raw": "Unit of measure exactly as written (e.g. CS, EA, PR, BX, RL, PK)",
      "unit_price": <number or null>,
      "amount": <number or null>
    }
  ]
}

## Critical Rules:
1. Extract ONLY actual product line items. STRICTLY IGNORE subtotals, tax, freight, shipping, discounts, and page footers.
2. If the text contains semantic HTML tables (<table>), use the semantic bindings.
3. If a product description drops to a second line without a price/qty, you MUST merge it into the previous item's description.
4. For `uom_raw`, extract the literal abbreviation.

## Few-Shot Example:
INPUT TEXT:
INVOICE # 99281   DATE: 10/12/23
Remit To:
Grainger Industrial Supply
Dept 82828
Chicago IL

QTY   SKU     DESCRIPTION         UOM   PRICE   TOTAL
2     12X88   Heavy Duty Drill    EA    45.00   90.00
              18v Lithium
1     99A12   Safety Goggles      PR    12.50   12.50

OUTPUT JSON:
{
  "supplier_name": "Grainger Industrial Supply",
  "line_items": [
    {"item_description": "Heavy Duty Drill 18v Lithium", "sku": "12X88", "manufacturer_part_number": null, "quantity": 2, "uom_raw": "EA", "unit_price": 45.0, "amount": 90.0},
    {"item_description": "Safety Goggles", "sku": "99A12", "manufacturer_part_number": null, "quantity": 1, "uom_raw": "PR", "unit_price": 12.5, "amount": 12.5}
  ]
}

## REAL EXTRACTION TASK:
RAW INVOICE TEXT:
"""

_MAX_TEXT_LENGTH = 12000  # Increased token limit since prompt is longer and we support HTML


def extract_with_llm(
    raw_text: str,
    pdf_name: str,
) -> dict[str, Any]:
    """Send raw text to Gemini for structured extraction.

    Returns
    -------
    dict with keys: supplier_name, line_items, llm_extraction_used
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        logger.warning("GEMINI_API_KEY not set, skipping LLM extraction fallback")
        return {"supplier_name": "", "line_items": [], "llm_extraction_used": False}

    # Truncate text if too long
    text = raw_text[:_MAX_TEXT_LENGTH] if len(raw_text) > _MAX_TEXT_LENGTH else raw_text

    prompt = _EXTRACT_PROMPT + text

    try:
        import time
        from google import genai  # type: ignore[import-untyped]

        client = genai.Client(api_key=api_key)

        from pydantic import BaseModel, Field  # type: ignore[import-not-found, import-untyped]

        class LineItem(BaseModel):
            item_description: str
            manufacturer_part_number: str | None
            sku: str | None
            quantity: float | None
            uom_raw: str | None
            unit_price: float | None
            amount: float | None

        class InvoiceData(BaseModel):
            supplier_name: str
            line_items: list[LineItem]

        # Retry with backoff for rate limit errors
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = client.models.generate_content(
                    model="gemini-2.0-flash",
                    contents=prompt,
                    config=genai.types.GenerateContentConfig(
                        temperature=0.1,
                        max_output_tokens=8192,
                        response_mime_type="application/json",
                        response_schema=InvoiceData,
                    ),
                )
                break  # Success
            except Exception as api_err:
                err_str = str(api_err)
                if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                    wait_time = 30 * (2 ** attempt)  # 30s, 60s, 120s
                    logger.info(
                        "Rate limited (attempt %d/%d), waiting %ds before retry",
                        attempt + 1, max_retries, wait_time,
                    )
                    time.sleep(wait_time)
                    if attempt == max_retries - 1:
                        raise
                else:
                    raise

        response_text = response.text or ""

        try:
            with open("gemini_raw_input.txt", "w", encoding="utf-8") as f:
                f.write(prompt)
            with open("gemini_raw_output_post.json", "w", encoding="utf-8") as f:
                f.write(response_text)
        except Exception:
            pass

        # Strip markdown code fencing if present
        response_text = re.sub(r"^```(?:json)?\s*\n?", "", response_text.strip())
        response_text = re.sub(r"\n?```\s*$", "", response_text.strip())

        data = json.loads(response_text)

        supplier = data.get("supplier_name", "") or ""
        items = data.get("line_items", []) or []

        # Validate items structure
        validated_items: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            desc = item.get("item_description", "")
            if not desc or len(str(desc)) < 3:
                continue
            validated_items.append({
                "item_description": str(desc).strip(),
                "manufacturer_part_number": item.get("manufacturer_part_number"),
                "sku": item.get("sku"),
                "quantity": _to_float(item.get("quantity")),
                "uom_raw": item.get("uom_raw"),
                "unit_price": _to_float(item.get("unit_price")),
                "amount": _to_float(item.get("amount")),
            })

        logger.info(
            "LLM extraction for %s: supplier=%r, %d items",
            pdf_name, supplier, len(validated_items),
        )

        return {
            "supplier_name": supplier,
            "line_items": validated_items,
            "llm_extraction_used": True,
        }

    except json.JSONDecodeError as exc:
        logger.warning("LLM returned invalid JSON for %s: %s", pdf_name, exc)
        return {"supplier_name": "", "line_items": [], "llm_extraction_used": False}
    except Exception as exc:
        logger.error("LLM extraction failed for %s: %s: %s", pdf_name, type(exc).__name__, exc)
        try:
            with open("gemini_api_error.txt", "w", encoding="utf-8") as f:
                f.write(f"PROMPT:\\n{prompt}\\n\\nERROR: {type(exc).__name__}: {exc}")
        except Exception:
            pass
        return {"supplier_name": "", "line_items": [], "llm_extraction_used": False}


def _to_float(val: Any) -> float | None:
    """Convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
