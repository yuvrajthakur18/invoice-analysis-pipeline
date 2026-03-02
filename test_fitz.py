from pathlib import Path
import fitz
import json

pdf_path = Path('input_pdfs/2700-APUSBWAY_31594_GALAG172312_6fe93_page_1.pdf')
doc = fitz.open(pdf_path)
page = doc[0]
text = page.get_text("text")
with open("fitz_text.txt", "w", encoding="utf-8") as f:
    f.write(text)
text_blocks = page.get_text("blocks")
with open("fitz_blocks.json", "w", encoding="utf-8") as f:
    json.dump(text_blocks, f, indent=2)
