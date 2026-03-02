from pathlib import Path
from invoice_uom.pipeline import _extract
import json

pdf_path = Path('input_pdfs/2700-APUSBWAY_31594_GALAG172312_6fe93_page_1.pdf')
debug = {"stages": {}}
result = _extract(pdf_path, debug)
with open('raw_extraction2.json', 'w', encoding='utf-8') as f:
    json.dump(result, f, indent=2)
