"""PDF extraction via the *docling* library.

Returns a dict with ``tables`` (list of list-of-lists) and ``text_blocks``
(list of strings), suitable for downstream line-item parsing.

Key fix: Docling is now configured to export tables directly into Semantic HTML strings. This completely eliminates alignment issues when passing the data to the LLM.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── markdown noise patterns ─────────────────────────────────────────────────
_MD_NOISE = re.compile(
    r"<!--.*?-->|"           # HTML comments
    r"^\s*#{1,6}\s+|"       # markdown headers
    r"^\s*\*{3,}\s*$|"      # horizontal rules (***)
    r"^\s*-{3,}\s*$|"       # horizontal rules (---)
    r"^\s*={3,}\s*$",        # horizontal rules (===)
    re.MULTILINE,
)


def extract_with_docling(pdf_path: Path) -> dict[str, Any]:
    """Extract tables and text blocks from *pdf_path* using docling.

    Returns
    -------
    dict
        ``{"tables": [...], "text_blocks": [...], "method": "docling"}``
    """
    try:
        from docling.document_converter import DocumentConverter  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "docling is not installed.  Install it with: pip install docling"
        ) from exc

    logger.info("Extracting with docling: %s", pdf_path.name)

    converter = DocumentConverter()
    result = converter.convert(str(pdf_path))
    doc = result.document

    tables: list[list[list[str]]] = []
    structured_tables: list[str] = []
    text_blocks: list[str] = []

    # Walk document items
    for item in doc.iterate_items():
        # Pyre narrowing
        item_obj: Any = item[1] if isinstance(item, tuple) and len(item) > 1 else (item[0] if isinstance(item, tuple) else item)
        type_name = type(item_obj).__name__

        if type_name == "TableItem":
            try:
                table_data = _extract_table(item_obj)
                if table_data:
                    tables.append(table_data)
                
                # Also save the semantic HTML specifically for the LLM Fallback pipeline
                html_table = item_obj.export_to_html()
                if html_table:
                    structured_tables.append(str(html_table))
            except Exception:
                logger.debug("Failed to parse a TableItem, skipping", exc_info=True)

        elif type_name == "TextItem":
            text = getattr(item_obj, "text", None) or str(item_obj)
            text = text.strip()
            if text:
                text_blocks.append(text)

    # If no tables found, try export_to_markdown and parse pipe tables from it
    if not tables:
        try:
            md = doc.export_to_markdown()
            if md:
                md_tables, md_text_blocks = _parse_markdown_content(md)
                tables.extend(md_tables)
                # Only use md text blocks if we didn't get any from iterate_items
                if not text_blocks:
                    text_blocks.extend(md_text_blocks)
                elif md_tables:
                    # We found tables in markdown, also add non-table text
                    text_blocks.extend(md_text_blocks)
        except Exception:
            logger.debug("Markdown export fallback failed", exc_info=True)

    logger.info(
        "docling extracted %d table(s), %d text block(s)",
        len(tables), len(text_blocks),
    )
    return {
        "tables": tables,
        "structured_tables": structured_tables,
        "text_blocks": text_blocks,
        "method": "docling"
    }


def _extract_table(table_item: Any) -> list[list[str]]:
    """Convert a docling TableItem to a list of rows (list of cell strings)."""
    rows: list[list[str]] = []

    # Try export_to_dataframe first
    try:
        df = table_item.export_to_dataframe()
        header = [str(c) for c in df.columns.tolist()]
        rows.append(header)
        for _, row in df.iterrows():
            rows.append([str(v) for v in row.tolist()])
        return rows
    except Exception:
        pass

    # Try table_cells / grid
    try:
        grid = table_item.data.grid if hasattr(table_item, "data") else None
        if grid:
            for row_cells in grid:
                rows.append([str(getattr(c, "text", c)) for c in row_cells])
            return rows
    except Exception:
        pass

    # Markdown table fallback
    try:
        md = table_item.export_to_markdown()
        return _parse_pipe_table(md)
    except Exception:
        pass

    return rows


def _parse_markdown_content(md_text: str) -> tuple[list[list[list[str]]], list[str]]:
    """Parse full markdown export: extract pipe tables AND remaining text blocks.

    Returns (tables, text_blocks).
    """
    tables: list[list[list[str]]] = []
    text_blocks: list[str] = []

    lines = md_text.splitlines()
    i = 0
    current_table_lines: list[str] = []
    non_table_lines: list[str] = []

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Detect pipe-table rows: lines with at least 2 pipe characters
        if stripped.startswith("|") and stripped.endswith("|") and stripped.count("|") >= 2:
            current_table_lines.append(stripped)
        elif _is_separator_line(stripped):
            # Separator row like |---|---|---| or just ---|---
            if current_table_lines:
                current_table_lines.append(stripped)
            # else skip stray separators
        else:
            # Non-table line — flush any accumulated table
            if current_table_lines:
                table = _parse_pipe_table("\n".join(current_table_lines))
                if table and len(table) >= 2:
                    tables.append(table)
                current_table_lines = []

            # Clean the text line
            cleaned = _clean_md_line(stripped)
            if cleaned:
                non_table_lines.append(cleaned)
        i += 1

    # Flush final table if any
    if current_table_lines:
        table = _parse_pipe_table("\n".join(current_table_lines))
        if table and len(table) >= 2:
            tables.append(table)

    # Consolidate adjacent non-table lines into blocks
    if non_table_lines:
        current_block: list[str] = []
        for ln in non_table_lines:
            if ln:
                current_block.append(ln)
            elif current_block:
                text_blocks.append(" ".join(current_block))
                current_block = []
        if current_block:
            text_blocks.append(" ".join(current_block))

    return tables, text_blocks


def _parse_pipe_table(md_text: str) -> list[list[str]]:
    """Parse a pipe-delimited markdown table into rows of cleaned cells."""
    rows: list[list[str]] = []
    for line in md_text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        # Skip separator rows like |---|---|---|
        if _is_separator_line(line):
            continue
        # Split on pipes and clean
        if "|" in line:
            cells = line.split("|")
            # Remove empty leading/trailing from split on "|col1|col2|"
            if cells and cells[0].strip() == "":
                cells.pop(0)
            if cells and cells[-1].strip() == "":
                cells.pop()
            cleaned = [_clean_cell(c) for c in cells]
            if any(cleaned):
                rows.append(cleaned)
    return rows


def _is_separator_line(line: str) -> bool:
    """Check if a line is a markdown table separator (e.g. |---|---|)."""
    stripped = line.strip().strip("|").strip()
    if not stripped:
        return False
    # Separator lines contain only dashes, colons, pipes, and spaces
    return bool(re.match(r"^[\s\-:|]+$", stripped))


def _clean_cell(text: str) -> str:
    """Clean a single table cell value."""
    text = text.strip()
    # Remove markdown artifacts
    text = re.sub(r"<!--.*?-->", "", text)
    text = re.sub(r"^\s*#{1,6}\s+", "", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _clean_md_line(line: str) -> str:
    """Clean a non-table markdown line."""
    if not line:
        return ""
    # Remove HTML comments
    line = re.sub(r"<!--.*?-->", "", line)
    # Remove markdown header markers
    line = re.sub(r"^\s*#{1,6}\s+", "", line)
    # Remove horizontal rules
    if re.match(r"^\s*[\*\-=]{3,}\s*$", line):
        return ""
    return line.strip()
