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
def _load_default_api_key() -> str:
    """Get the default GEMINI_API_KEY from Streamlit secrets or .env."""
    # 1) Already in environment
    key = os.environ.get("GEMINI_API_KEY", "")
    if key:
        return key
    # 2) Streamlit secrets (used on Streamlit Cloud)
    try:
        key = st.secrets.get("GEMINI_API_KEY", "")
        if key:
            return key
    except Exception:
        pass
    # 3) .env file (local dev)
    try:
        from dotenv import load_dotenv  # type: ignore[import-untyped]
        load_dotenv()
        key = os.environ.get("GEMINI_API_KEY", "")
        if key:
            return key
    except ImportError:
        pass
    return ""

_default_key = _load_default_api_key()

# ── Sidebar: optional user API key override ──────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Settings")
    st.markdown("---")
    st.markdown("**Gemini API Key**")
    if _default_key:
        st.success("✅ Default API key is configured.")
        st.markdown("_If you encounter rate limits, you can provide your own key below._")
    else:
        st.warning("⚠️ No default API key found. Please enter one below.")

    user_key = st.text_input(
        "Your Gemini API Key (optional)",
        type="password",
        placeholder="Paste your key here...",
        help="Get a free key at https://aistudio.google.com/apikey",
    )
    if user_key:
        st.info("🔑 Using your custom API key for this session.")

# Set the active key (user override takes priority)
_active_key = user_key.strip() if user_key else _default_key
if _active_key:
    os.environ["GEMINI_API_KEY"] = _active_key


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

    /* Scroll anchor */
    .scroll-anchor { scroll-margin-top: 2rem; }

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
def run_pipeline(uploaded_file: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """Process an uploaded PDF. Returns (result, log_messages)."""
    from invoice_uom.pipeline import process_pdf

    log_messages: list[str] = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / uploaded_file.name
        tmp_path.write_bytes(uploaded_file.getvalue())
        output_dir = Path(tmp_dir) / "out"
        failed_dir = Path(tmp_dir) / "failed"

        # Progress bar
        progress_bar = st.progress(0, text="Initialising pipeline...")
        total_steps = 5

        def _status_cb(msg: str) -> None:
            log_messages.append(msg)
            pct = min(int(len(log_messages) / total_steps * 100), 95)
            progress_bar.progress(pct, text=msg)

        try:
            result = process_pdf(
                pdf_path=tmp_path,
                output_dir=output_dir,
                failed_dir=failed_dir,
                force=True,
                status_cb=_status_cb,
            )
            progress_bar.progress(100, text="✅ Processing complete!")
            time.sleep(0.5)
            progress_bar.empty()
            return result, log_messages
        except Exception as exc:
            progress_bar.progress(100, text="❌ Error occurred")
            log_messages.append(f"ERROR: {exc}")
            return None, log_messages


def render_results(result: dict[str, Any], filename: str, log_messages: list[str]) -> None:
    """Render extraction results: supplier → stats → table → JSON → downloads → logs."""
    supplier = result.get("supplier_name", "Unknown")
    items = result.get("line_items", [])
    stats = result.get("stats", {})
    num_items = stats.get("num_items", len(items))
    num_escalations = stats.get("num_escalations", 0)

    # ── Scroll anchor ─────────────────────────────────────────────────
    st.markdown(f'<div class="scroll-anchor" id="results-{filename}"></div>', unsafe_allow_html=True)

    # ── Supplier card ─────────────────────────────────────────────────
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

    # ── Line items table ──────────────────────────────────────────────
    if items:
        with st.expander(f"📋 **Line Items Table** ({num_items} items)", expanded=True):
            rows = []
            for i, item in enumerate(items, 1):
                conf = item.get("confidence_score", 0)
                escalation = item.get("escalation_flag", False)
                row = {
                    "#": i,
                    "Description": item.get("item_description", ""),
                    "MPN": item.get("manufacturer_part_number") or "—",
                    "Original UOM": item.get("original_uom") or "—",
                    "Pack Qty": str(item.get("detected_pack_quantity")) if item.get("detected_pack_quantity") is not None else "—",
                    "Base UOM": item.get("canonical_base_uom") or "—",
                    "Price/Base Unit": f"${item['price_per_base_unit']:.4f}" if item.get("price_per_base_unit") is not None else "—",
                    "Confidence": f"{conf:.0%}" if isinstance(conf, (int, float)) else str(conf),
                    "Escalation": "🚩 Yes" if escalation else "✅ No",
                }
                rows.append(row)

            df = pd.DataFrame(rows)
            st.dataframe(
                df,
                width=None,
                hide_index=True,
                height=min(500, 40 + len(rows) * 35),
                column_config={
                    "#": st.column_config.NumberColumn(width="small"),
                    "Description": st.column_config.TextColumn(width="large"),
                    "Confidence": st.column_config.TextColumn(width="small"),
                    "Escalation": st.column_config.TextColumn(width="small"),
                },
            )

        # ── Escalated items evidence ──────────────────────────────────
        escalated_items = [item for item in items if item.get("escalation_flag")]
        if escalated_items:
            with st.expander(f"🚩 **Escalated Items Evidence** ({len(escalated_items)} items)", expanded=False):
                for item in escalated_items:
                    desc = item.get("item_description", "Unknown")
                    evidence = item.get("evidence", {})
                    st.markdown(f"**{desc}**")
                    ev_data = {
                        "UOM Evidence": evidence.get("uom_evidence_text") or "—",
                        "Pack Evidence": evidence.get("pack_evidence_text") or "—",
                        "LLM Used": "Yes" if evidence.get("llm_call_used") else "No",
                        "LLM Status": evidence.get("llm_call_status") or "—",
                        "LLM Reason": evidence.get("llm_call_reason") or "—",
                        "LLM Attempts": evidence.get("llm_call_attempts", 0),
                    }
                    sources = evidence.get("lookup_sources", [])
                    if sources:
                        ev_data["Lookup Sources"] = ", ".join(
                            s.get("url", "") for s in sources if isinstance(s, dict)
                        ) or "—"
                    st.json(ev_data, expanded=True)
                    st.markdown("---")

        # ── Raw JSON output ───────────────────────────────────────────
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
            )
        with dl2:
            json_str = json.dumps(result, indent=2, default=str)
            st.download_button(
                "⬇️ JSON",
                data=json_str,
                file_name=f"{Path(filename).stem}_results.json",
                mime="application/json",
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

    # ── Processing logs (collapsible, at the bottom) ──────────────────
    if log_messages:
        with st.expander(f"📝 **Processing Logs** ({len(log_messages)} entries)", expanded=False):
            # Show last 5 by default, all available inside
            display_logs = list(log_messages[-5:]) if len(log_messages) > 5 else log_messages
            for log in display_logs:
                st.markdown(f"✅ {log}")
            if len(log_messages) > 5:
                st.markdown(f"_...and {len(log_messages) - 5} earlier entries_")
                with st.expander("Show all logs"):
                    for log in log_messages:
                        st.markdown(f"• {log}")

    # ── Auto-scroll to results ────────────────────────────────────────
    safe_id = filename.replace(".", "_").replace(" ", "_")
    st.markdown(
        f"""
        <script>
            const el = document.getElementById('results-{filename}');
            if (el) {{ el.scrollIntoView({{ behavior: 'smooth', block: 'start' }}); }}
        </script>
        """,
        unsafe_allow_html=True,
    )


# ── Main flow ────────────────────────────────────────────────────────────────
if uploaded_files:
    st.markdown("---")
    st.markdown("## 📊 Results")

    for uploaded_file in uploaded_files:
        st.markdown(f"### 📄 `{uploaded_file.name}`")

        result, log_messages = run_pipeline(uploaded_file)

        if result is not None:
            render_results(result, uploaded_file.name, log_messages)
        else:
            st.error(f"Processing failed for **{uploaded_file.name}**.")
            # Still show logs for failed files
            if log_messages:
                with st.expander(f"📝 **Processing Logs** ({len(log_messages)} entries)", expanded=True):
                    for log in log_messages:
                        st.markdown(f"• {log}")

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
