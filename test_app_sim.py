import logging
import tempfile
import shutil
from pathlib import Path
from invoice_uom.pipeline import process_pdf

logging.basicConfig(level=logging.INFO)

src = Path("input_pdfs/2700-APUSBWAY_31594_GALAG172312_6fe93_page_1.pdf")
with tempfile.TemporaryDirectory() as tmp_dir:
    tmp_path = Path(tmp_dir) / src.name
    shutil.copy(src, tmp_path)
    
    out_dir = Path(tmp_dir) / "out"
    failed_dir = Path(tmp_dir) / "failed"
    
    result = process_pdf(tmp_path, out_dir, failed_dir, force=True)
    if result:
        print("Success Items:", len(result.get("line_items", [])))
    else:
        print("Failed to process") 
