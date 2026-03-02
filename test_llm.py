from pathlib import Path
from invoice_uom.llm_extract import extract_with_llm
import fitz
import json

pdf_path = Path('input_pdfs/2700-APUSBWAY_31594_GALAG172312_6fe93_page_1.pdf')
doc = fitz.open(pdf_path)
page = doc[0]
raw_text = page.get_text("text")

res = extract_with_llm(raw_text, pdf_path.name)
with open("llm_fitz_result.json", "w", encoding="utf-8") as f:
    json.dump(res, f, indent=2)
print("LLM Check successful:", len(res.get("line_items", [])))
