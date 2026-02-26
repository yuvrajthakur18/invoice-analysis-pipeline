"""Streamlit web UI for the Invoice Analysis Pipeline.

Upload invoice PDFs → watch live progress → view results in accordions → download JSON.
"""

import io
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Invoice Analysis Pipeline",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Load API key from Streamlit secrets or .env ──────────────────────────────
def _load_api_key() -> None:
    """Inject GEMINI_API_KEY into the environment from Streamlit secrets or .env."""
    if os.environ.get("GEMINI_API_KEY"):
        return
    try:
        key = st.secrets.get("GEMINI_API_KEY", "")
        if key:
            os.environ["GEMINI_API_KEY"] = key
            return
    except Exception:
        pass
    try:
        from dotenv import load_dotenv  # type: ignore[import-untyped]
        load_dotenv()
    except ImportError:
        pass

_load_api_key()


# ── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    html, body, .stApp { font-family: 'Inter', sans-serif; }
    .block-container { padding-top: 1.5rem; max-width: 1100px; }

    /* Header */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2.2rem 2.5rem;
        border-radius: 16px;
        margin-bottom: 1.8rem;
        color: white;
        box-shadow: 0 8px 32px rgba(102, 126, 234, 0.3);
    }
    .main-header h1 { margin: 0; font-size: 2rem; font-weight: 700; }
    .main-header p  { margin: 0.4rem 0 0 0; opacity: 0.9; font-size: 1rem; }

    /* Supplier card */
    .supplier-card {
        background: linear-gradient(135deg, #f8f9ff 0%, #e8ecff 100%);
        padding: 1rem 1.5rem;
        border-radius: 12px;
        margin-bottom: 1rem;
        border-left: 5px solid #667eea;
    }
    .supplier-card .label { font-size: 0.8rem; color: #718096; text-transform: uppercase; letter-spacing: 0.5px; }
    .supplier-card .name  { font-size: 1.3rem; font-weight: 700; color: #4c51bf; margin-top: 0.2rem; }

    /* Metric cards */
    .metric-card {
        background: white;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 1.2rem 1rem;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }
    .metric-card .value { font-size: 2rem; font-weight: 700; color: #667eea; }
    .metric-card .label { font-size: 0.8rem; color: #718096; margin-top: 0.2rem; }
    .metric-card .value.green  { color: #22c55e; }
    .metric-card .value.yellow { color: #f59e0b; }
    .metric-card .value.red    { color: #ef4444; }

    /* Step indicator */
    .step-row {
        display: flex;
        align-items: center;
        gap: 0.6rem;
        padding: 0.5rem 0;
        font-size: 0.95rem;
    }
    .step-icon { font-size: 1.2rem; }
    .step-done   { color: #22c55e; }
    .step-active { color: #667eea; font-weight: 600; }
    .step-wait   { color: #cbd5e1; }

    /* Footer */
    .footer { text-align: center; padding: 2rem 0 1rem; color: #a0aec0; font-size: 0.82rem; }
</style>
""", unsafe_allow_html=True)


# ── Header ───────────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <h1>📄 Invoice Analysis Pipeline</h1>
    <p>Upload invoice PDFs to extract structured line items, detect suppliers, and normalise UOM data.</p>
</div>
""", unsafe_allow_html=True)


# ── File uploader ────────────────────────────────────────────────────────────
uploaded_files = st.file_uploader(
    "📎 Upload Invoice PDFs",
    type=["pdf"],
    accept_multiple_files=True,
    help="Upload one or more invoice PDF files for analysis.",
)


# ── Processing function ─────────────────────────────────────────────────────
def run_pipeline(uploaded_file: Any) -> dict[str, Any] | None:
    """Process an uploaded PDF through the full pipeline with visible progress."""
    from invoice_uom.pipeline import process_pdf

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / uploaded_file.name
        tmp_path.write_bytes(uploaded_file.getvalue())
        output_dir = Path(tmp_dir) / "out"
        failed_dir = Path(tmp_dir) / "failed"

        # Progress placeholders
        progress_bar = st.progress(0, text="Initialising...")
        step_container = st.container()

        steps_done: list[str] = []
        total_steps = 5  # extraction, parsing, supplier, llm/enrich, output

        def _status_cb(msg: str) -> None:
            steps_done.append(msg)
            pct = min(int(len(steps_done) / total_steps * 100), 95)
            progress_bar.progress(pct, text=msg)

            # Render step list
            with step_container:
                step_container.empty()
                html = ""
                for i, s in enumerate(steps_done):
                    html += f'<div class="step-row"><span class="step-icon step-done">✅</span> {s}</div>'
                st.markdown(html, unsafe_allow_html=True)

        try:
            result = process_pdf(
                pdf_path=tmp_path,
                output_dir=output_dir,
                failed_dir=failed_dir,
                force=True,
                status_cb=_status_cb,
            )
            progress_bar.progress(100, text="✅ Processing complete!")
            time.sleep(0.3)
            progress_bar.empty()
            return result
        except Exception as exc:
            progress_bar.progress(100, text="❌ Error occurred")
            st.error(f"Pipeline error: {exc}")
            return None


def render_results(result: dict[str, Any], filename: str) -> None:
    """Render extraction results with accordions and download buttons."""
    supplier = result.get("supplier_name", "Unknown")
    items = result.get("line_items", [])
    stats = result.get("stats", {})
    num_items = stats.get("num_items", len(items))
    num_escalations = stats.get("num_escalations", 0)

    # ── Supplier ──────────────────────────────────────────────────────
    st.markdown(f"""
    <div class="supplier-card">
        <div class="label">Detected Supplier</div>
        <div class="name">🏢 {supplier}</div>
    </div>
    """, unsafe_allow_html=True)

    # ── Metrics row ───────────────────────────────────────────────────
    avg_conf = 0.0
    if items:
        scores = [i.get("confidence_score", 0) for i in items if isinstance(i.get("confidence_score"), (int, float))]
        avg_conf = sum(scores) / len(scores) if scores else 0
    conf_class = "green" if avg_conf >= 0.7 else "yellow" if avg_conf >= 0.4 else "red"

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f'<div class="metric-card"><div class="value">{num_items}</div><div class="label">Line Items</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="metric-card"><div class="value yellow">{num_escalations}</div><div class="label">Need Review</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="metric-card"><div class="value {conf_class}">{avg_conf:.0%}</div><div class="label">Avg Confidence</div></div>', unsafe_allow_html=True)

    st.markdown("")

    # ── Line items accordion ──────────────────────────────────────────
    if items:
        with st.expander(f"📋 **Line Items Table** ({num_items} items)", expanded=True):
            rows = []
            for i, item in enumerate(items, 1):
                conf = item.get("confidence_score", 0)
                if isinstance(conf, (int, float)):
                    conf_str = f"{conf:.0%}"
                else:
                    conf_str = str(conf)
                rows.append({
                    "#": i,
                    "Description": item.get("item_description", ""),
                    "SKU": item.get("sku") or "—",
                    "MPN": item.get("manufacturer_part_number") or "—",
                    "Qty": item.get("quantity") if item.get("quantity") is not None else "—",
                    "UOM": item.get("uom_raw") or item.get("original_uom") or "—",
                    "Unit Price": f"${item['unit_price']:.2f}" if item.get("unit_price") is not None else "—",
                    "Amount": f"${item['amount']:.2f}" if item.get("amount") is not None else "—",
                    "Confidence": conf_str,
                })

            df = pd.DataFrame(rows)
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                height=min(400, 40 + len(rows) * 35),
                column_config={
                    "#": st.column_config.NumberColumn(width="small"),
                    "Description": st.column_config.TextColumn(width="large"),
                },
            )

        # ── Raw JSON accordion ────────────────────────────────────────
        with st.expander("🔍 **Raw JSON Output** (click to expand)"):
            st.json(result, expanded=False)

        # ── Download buttons ──────────────────────────────────────────
        st.markdown("#### 📥 Download")
        dl1, dl2, _ = st.columns([1, 1, 4])
        with dl1:
            csv_data = df.to_csv(index=False)
            st.download_button(
                "⬇️ CSV",
                data=csv_data,
                file_name=f"{Path(filename).stem}_results.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with dl2:
            json_str = json.dumps(result, indent=2, default=str)
            st.download_button(
                "⬇️ JSON",
                data=json_str,
                file_name=f"{Path(filename).stem}_results.json",
                mime="application/json",
                use_container_width=True,
            )
    else:
        st.warning(
            "⚠️ **No line items were extracted.** This can happen if:\n"
            "- The PDF layout is unusual and the parser couldn't find tables\n"
            "- The Gemini API quota is exhausted (429 error)\n\n"
            "Check the Raw JSON below for diagnostic details."
        )
        with st.expander("🔍 **Raw JSON Output** (diagnostics)"):
            st.json(result, expanded=True)

        json_str = json.dumps(result, indent=2, default=str)
        st.download_button(
            "⬇️ Download JSON",
            data=json_str,
            file_name=f"{Path(filename).stem}_results.json",
            mime="application/json",
        )


# ── Main flow ────────────────────────────────────────────────────────────────
if uploaded_files:
    st.markdown("---")
    st.markdown("## 📊 Results")

    for uploaded_file in uploaded_files:
        st.markdown(f"### 📄 `{uploaded_file.name}`")

        result = run_pipeline(uploaded_file)

        if result is not None:
            render_results(result, uploaded_file.name)
        else:
            st.error(f"Processing failed for **{uploaded_file.name}**. Check logs for details.")

        st.markdown("---")

    st.success(f"🎉 All {len(uploaded_files)} file(s) processed!")

else:
    # ── Landing page ──────────────────────────────────────────────────
    st.markdown("---")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("### 1️⃣ Upload")
        st.markdown("Drop one or more invoice PDFs into the upload zone above.")
    with c2:
        st.markdown("### 2️⃣ Extract")
        st.markdown("The pipeline extracts tables, identifies suppliers, and parses line items using AI.")
    with c3:
        st.markdown("### 3️⃣ Download")
        st.markdown("View results in a table and export as CSV or JSON.")

    st.markdown("---")
    st.info("💡 **Tip:** The pipeline uses Docling + PaddleOCR for table extraction and Gemini AI for complex invoices.")


# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown('<div class="footer">Invoice Analysis Pipeline • Powered by Docling, PaddleOCR & Gemini AI</div>', unsafe_allow_html=True)
