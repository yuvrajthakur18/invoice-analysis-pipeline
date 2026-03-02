from pathlib import Path
from invoice_uom.ocr_paddle import extract_with_paddle
import json

pdf_path = Path('input_pdfs/2700-APUSBWAY_31594_GALAG172312_6fe93_page_1.pdf')
result = extract_with_paddle(pdf_path)
with open('raw_paddle.json', 'w', encoding='utf-8') as f:
    json.dump(result, f, indent=2)
