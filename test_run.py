import logging
from pathlib import Path
from invoice_uom.pipeline import process_pdf

logging.basicConfig(level=logging.INFO)

pdf_path = Path("input_pdfs/2700-APUSBWAY_31594_GALAG172312_6fe93_page_1.pdf")
out_dir = Path("outputs")
failed_dir = Path("failed")

result = process_pdf(pdf_path, out_dir, failed_dir, force=True)
print("\nExtraction Success!")
if result:
    print(f"Supplier: {result.get('supplier_name')}")
    print(f"Items Extract: {len(result.get('line_items', []))}")
else:
    print("Process returned None")
