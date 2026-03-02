from pathlib import Path
from invoice_uom.llm_extract import extract_with_llm
import json

with open("plumber_layout.txt", "r", encoding="utf-8") as f:
    raw_text = f.read()

pdf_name = "2700-APUSBWAY_31594_GALAG172312_6fe93_page_1.pdf"
res = extract_with_llm(raw_text, pdf_name)
with open("llm_plumber_result.json", "w", encoding="utf-8") as f:
    json.dump(res, f, indent=2)

print("LLM extracted items:", len(res.get("line_items", [])))
