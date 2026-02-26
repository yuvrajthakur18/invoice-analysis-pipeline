"""Agentic online lookup for missing UOM / pack quantity.

Trigger conditions (all must be met):
  1) ``original_uom`` is None  OR  ``detected_pack_quantity`` is None
     AND  ``price_per_base_unit`` cannot be safely computed.
  2) We have a query handle (SKU/MPN or strong description).

Lookup flow:
  1) Build normalised query string.
  2) Online search (top 3 results).
  3) Fetch pages → extract snippets containing pack/UOM patterns.
  4) Regex-first extraction from snippets.
  5) LLM (Gemini) only if regex inconclusive but snippets exist.
  6) Always record ``lookup_sources``.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import quote_plus

from invoice_uom import config
from invoice_uom.cache import LookupCache, LookupResult
from invoice_uom.llm_client import LLMCallResult, resolve_uom_with_llm
from invoice_uom.uom_normalize import parse_uom_and_pack

logger = logging.getLogger(__name__)

# ── snippet extraction patterns ─────────────────────────────────────────────
_PACK_SNIPPET_RE = re.compile(
    r"(?:\d+\s*/\s*(?:CS|CASE|BX|BOX|PK|PACK|PKG|EA|EACH|UNIT|ROLL|BAG|CT|DZ))"
    r"|(?:(?:PK|PACK|PKG)\s*\d+)"
    r"|(?:(?:CASE|BOX|PACK|PACKAGE|PKG)\s+OF\s+\d+)"
    r"|(?:\d+\s+PER\s+(?:PACK|CASE|BOX|PACKAGE|PKG|ROLL|BAG))"
    r"|(?:\d+\s+(?:EA|EACH|UNIT|PC|PCS))",
    re.I,
)

_SESSION = None


def _get_session():
    """Lazy-init a requests.Session."""
    global _SESSION
    if _SESSION is None:
        import requests  # type: ignore[import-untyped]  # lazy import
        _SESSION = requests.Session()
        _SESSION.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; InvoiceUOM/0.1; +internal-use-only)",
            "Accept": "text/html",
        })
    return _SESSION


class LookupAgent:
    """Agentic lookup resolver with caching and LLM budget tracking."""

    def __init__(self, cache: LookupCache | None = None) -> None:
        self._cache = cache or LookupCache()
        self._pdf_llm_calls = 0

    def reset_pdf_budget(self) -> None:
        """Reset the per-PDF LLM call counter (call at the start of each PDF)."""
        self._pdf_llm_calls = 0

    @property
    def pdf_llm_budget_remaining(self) -> int:
        return max(0, config.LLM_MAX_CALLS_PER_PDF - self._pdf_llm_calls)

    def resolve(
        self,
        description: str,
        sku: str | None = None,
        mpn: str | None = None,
    ) -> dict[str, Any]:
        """Attempt to resolve UOM + pack quantity for one product.

        Returns a dict with keys:
          pack_qty, uom, lookup_sources, llm_result (LLMCallResult-like dict)
        """
        query = self._build_query(description, sku, mpn)
        if not query:
            return self._empty_result("no usable query handle")

        # 1) Check cache
        cached = self._cache.get(query)
        if cached is not None:
            logger.info("Cache hit for: %s", query[:60])
            return {
                "pack_qty": cached.pack_qty,
                "uom": cached.uom,
                "lookup_sources": [
                    {"url": u, "snippet": s.get("snippet", "")}
                    for u, s in zip(cached.source_urls, cached.evidence_snippets)
                ],
                "llm_result": LLMCallResult(status="not_needed", reason="cache hit").to_evidence_dict(),
            }

        # 2) Online search
        urls = self._search(query)
        if not urls:
            self._cache.put(LookupResult(query=query))
            return self._empty_result("no search results")

        # 3) Fetch pages & extract snippets
        snippets = self._fetch_snippets(urls)
        if not snippets:
            self._cache.put(LookupResult(query=query, source_urls=urls))
            return self._empty_result("no relevant snippets found")

        # 4) Regex-first extraction
        regex_result = self._regex_extract(snippets)
        if regex_result["pack_qty"] is not None or regex_result["uom"] is not None:
            # Cache and return
            self._cache.put(LookupResult(
                query=query,
                pack_qty=regex_result["pack_qty"],
                uom=regex_result["uom"],
                evidence_snippets=[{"url": s["url"], "snippet": s["snippet"]} for s in snippets],
                source_urls=[s["url"] for s in snippets],
                llm_used=False,
            ))
            return {
                **regex_result,
                "lookup_sources": snippets,
                "llm_result": LLMCallResult(status="not_needed", reason="regex extraction successful").to_evidence_dict(),
            }

        # 5) LLM if budget allows
        if self._pdf_llm_calls >= config.LLM_MAX_CALLS_PER_PDF:
            self._cache.put(LookupResult(
                query=query,
                source_urls=[s["url"] for s in snippets],
                evidence_snippets=[{"url": s["url"], "snippet": s["snippet"]} for s in snippets],
            ))
            return {
                "pack_qty": None,
                "uom": None,
                "lookup_sources": snippets,
                "llm_result": LLMCallResult(
                    status="skipped_rate_limit",
                    reason="per-PDF LLM budget exhausted",
                ).to_evidence_dict(),
            }

        self._pdf_llm_calls += 1
        llm_result = resolve_uom_with_llm(description, snippets, mpn)

        pack_qty = None
        uom = None
        if llm_result.status == "success" and llm_result.data:
            conf = llm_result.data.get("confidence", "none")
            if conf in ("high", "medium"):
                pack_qty = llm_result.data.get("pack_quantity")
                uom = llm_result.data.get("uom")

        self._cache.put(LookupResult(
            query=query,
            pack_qty=pack_qty,
            uom=uom,
            evidence_snippets=[{"url": s["url"], "snippet": s["snippet"]} for s in snippets],
            source_urls=[s["url"] for s in snippets],
            llm_used=True,
        ))

        return {
            "pack_qty": pack_qty,
            "uom": uom,
            "lookup_sources": snippets,
            "llm_result": llm_result.to_evidence_dict(),
        }

    # ── internal helpers ────────────────────────────────────────────────

    @staticmethod
    def _build_query(
        description: str, sku: str | None, mpn: str | None
    ) -> str:
        """Build a normalised search query string."""
        if mpn and mpn.strip():
            return mpn.strip()
        if sku and sku.strip():
            return sku.strip()
        # Clean description: remove special chars, collapse whitespace
        cleaned = re.sub(r"[^\w\s\-/]", " ", description)
        cleaned = " ".join(cleaned.split())
        if len(cleaned) < 5:
            return ""
        # Truncate very long descriptions
        return " ".join(cleaned.split()[:10])

    @staticmethod
    def _search(query: str, max_results: int = 3) -> list[str]:
        """Simple DuckDuckGo-HTML search → return top URLs."""
        try:
            from bs4 import BeautifulSoup  # type: ignore[import-untyped]
            session = _get_session()
            url = f"https://html.duckduckgo.com/html/?q={quote_plus(query + ' pack size UOM')}"
            resp = session.get(url, timeout=10)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            urls: list[str] = []
            for a in soup.select("a.result__a"):
                href = a.get("href", "")
                if href and href.startswith("http"):
                    urls.append(href)
                if len(urls) >= max_results:
                    break
            return urls
        except Exception as exc:
            logger.warning("Search failed: %s", exc)
            return []

    @staticmethod
    def _fetch_snippets(urls: list[str]) -> list[dict[str, str]]:
        """Fetch pages and extract text snippets containing UOM/pack patterns."""
        from bs4 import BeautifulSoup  # type: ignore[import-untyped]
        session = _get_session()
        snippets: list[dict[str, str]] = []
        for url in urls:
            try:
                resp = session.get(url, timeout=8)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
                # Remove scripts, styles
                for tag in soup(["script", "style", "nav", "footer", "header"]):
                    tag.decompose()
                text = soup.get_text(separator=" ", strip=True)
                # Find sentences containing pack/UOM patterns
                matches = _PACK_SNIPPET_RE.findall(text)
                if matches:
                    # Grab context around first match
                    for m in matches[:3]:
                        idx = text.find(m)
                        start = max(0, idx - 100)
                        end = min(len(text), idx + len(m) + 150)
                        snippet = text[start:end].strip()
                        snippets.append({"url": url, "snippet": snippet})
                    break  # Got good snippets from this page
            except Exception as exc:
                logger.debug("Fetch failed for %s: %s", url, exc)
        return snippets[:5]

    @staticmethod
    def _regex_extract(snippets: list[dict[str, str]]) -> dict[str, Any]:
        """Try regex extraction from snippets without LLM."""
        for s in snippets:
            result = parse_uom_and_pack(s["snippet"])
            if result.detected_pack_quantity is not None and result.canonical_uom:
                return {
                    "pack_qty": result.detected_pack_quantity,
                    "uom": result.canonical_uom,
                }
        return {"pack_qty": None, "uom": None}

    @staticmethod
    def _empty_result(reason: str) -> dict[str, Any]:
        return {
            "pack_qty": None,
            "uom": None,
            "lookup_sources": [],
            "llm_result": LLMCallResult(status="not_needed", reason=reason).to_evidence_dict(),
        }
