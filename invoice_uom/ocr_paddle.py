"""OCR fallback via PaddleOCR PP-Structure for scanned / noisy PDFs.

Returns the same shape as extract_docling so the downstream pipeline
can treat them interchangeably.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def extract_with_paddle(pdf_path: Path) -> dict[str, Any]:
    """Extract tables and text blocks from *pdf_path* using PaddleOCR.

    Returns
    -------
    dict
        ``{"tables": [...], "text_blocks": [...], "method": "paddleocr"}``
    """
    try:
        from paddleocr import PPStructure  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "PaddleOCR is not installed.  Install with: pip install paddleocr paddlepaddle"
        ) from exc

    logger.info("Extracting with PaddleOCR: %s", pdf_path.name)

    engine = PPStructure(show_log=False, recovery=True, lang="en")

    # PaddleOCR works on images; convert PDF pages to images first.
    images = _pdf_to_images(pdf_path)

    tables: list[list[list[str]]] = []
    text_blocks: list[str] = []

    for page_idx, img_path in enumerate(images):
        import cv2  # type: ignore[import-untyped]
        img = cv2.imread(str(img_path))
        if img is None:
            logger.warning("Could not read image for page %d", page_idx)
            continue

        result = engine(img)
        for block in result:
            block_type = block.get("type", "")
            if block_type == "table":
                html = block.get("res", {}).get("html", "")
                if html:
                    parsed = _parse_html_table(html)
                    if parsed:
                        tables.append(parsed)
            elif block_type == "text":
                text = block.get("res", {}).get("text", "")
                if text and text.strip():
                    text_blocks.append(text.strip())
            elif block_type == "figure":
                # Figures may contain embedded text via OCR
                ocr_res = block.get("res", [])
                if isinstance(ocr_res, list):
                    for item in ocr_res:
                        if isinstance(item, dict):
                            t = item.get("text", "")
                            if t:
                                text_blocks.append(t.strip())

    logger.info(
        "PaddleOCR extracted %d table(s), %d text block(s)",
        len(tables), len(text_blocks),
    )
    return {"tables": tables, "text_blocks": text_blocks, "method": "paddleocr"}


def _pdf_to_images(pdf_path: Path) -> list[Path]:
    """Convert PDF pages to PNG images in a temp directory."""
    try:
        import fitz  # PyMuPDF  # type: ignore[import-untyped]
    except ImportError:
        # Fallback: try pdf2image
        try:
            from pdf2image import convert_from_path  # type: ignore[import-untyped]
            tmp_dir = Path(tempfile.mkdtemp(prefix="invoice_ocr_"))
            images = convert_from_path(str(pdf_path), dpi=300)
            paths: list[Path] = []
            for i, img in enumerate(images):
                p = tmp_dir / f"page_{i}.png"
                img.save(str(p), "PNG")
                paths.append(p)
            return paths
        except ImportError:
            logger.error("Neither PyMuPDF nor pdf2image is available for PDF→image conversion")
            return []

    tmp_dir = Path(tempfile.mkdtemp(prefix="invoice_ocr_"))
    doc = fitz.open(str(pdf_path))
    paths: list[Path] = []
    for i, page in enumerate(doc):
        mat = fitz.Matrix(3.0, 3.0)  # 300 DPI
        pix = page.get_pixmap(matrix=mat)
        p = tmp_dir / f"page_{i}.png"
        pix.save(str(p))
        paths.append(p)
    doc.close()
    return paths


def _parse_html_table(html: str) -> list[list[str]]:
    """Parse an HTML table string into a list of rows."""
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-untyped]
    except ImportError:
        return []

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    rows: list[list[str]] = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if any(cells):
            rows.append(cells)
    return rows
