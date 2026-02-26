"""Gemini LLM client – strict JSON output, integrated rate limiting.

Uses the ``google-genai`` SDK with ``gemini-2.5-flash``.
All outputs are validated against a minimal JSON schema.
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from typing import Any

from invoice_uom import config
from invoice_uom.rate_limit import get_limiter

logger = logging.getLogger(__name__)

# ── expected output schema for UOM/pack resolution ──────────────────────────
_UOM_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "uom": {"type": ["string", "null"]},
        "pack_quantity": {"type": ["integer", "null"]},
        "evidence_text": {"type": ["string", "null"]},
        "confidence": {"type": "string", "enum": ["high", "medium", "low", "none"]},
    },
    "required": ["uom", "pack_quantity", "evidence_text", "confidence"],
}


class LLMCallResult:
    """Outcome of a single LLM call attempt."""

    def __init__(
        self,
        status: str = "not_needed",
        data: dict[str, Any] | None = None,
        reason: str | None = None,
        attempts: int = 0,
    ) -> None:
        self.status = status       # success | skipped_rate_limit | failed_429 | failed_other | not_needed
        self.data = data or {}
        self.reason = reason
        self.attempts = attempts

    def to_evidence_dict(self) -> dict[str, Any]:
        return {
            "llm_call_used": self.status == "success",
            "llm_call_reason": self.reason,
            "llm_call_status": self.status,
            "llm_call_attempts": self.attempts,
        }


def resolve_uom_with_llm(
    description: str,
    snippets: list[dict[str, str]],
    mpn: str | None = None,
) -> LLMCallResult:
    """Ask Gemini to extract UOM/pack info from evidence snippets.

    Parameters
    ----------
    description : str
        Cleaned item description.
    snippets : list[dict]
        Evidence snippets ``[{"url": ..., "snippet": ...}, ...]``.
    mpn : str | None
        Manufacturer part number if available.

    Returns
    -------
    LLMCallResult
    """
    if not snippets:
        return LLMCallResult(status="not_needed", reason="no snippets provided")

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return LLMCallResult(status="failed_other", reason="GEMINI_API_KEY not set")

    # Rate-limit check
    limiter = get_limiter()
    if limiter.daily_remaining <= 0:
        return LLMCallResult(status="skipped_rate_limit", reason="daily LLM budget exhausted")

    # Build a concise prompt
    snippet_text = "\n---\n".join(
        f"Source: {s['url']}\n{s['snippet'][:500]}" for s in snippets[:3]
    )
    prompt = (
        "You are a product-data extraction assistant.\n"
        f"Product description: {description}\n"
    )
    if mpn:
        prompt += f"Manufacturer Part Number: {mpn}\n"
    prompt += (
        "\nBelow are snippet(s) from product/supplier pages. "
        "Extract the unit-of-measure (UOM) and pack quantity ONLY if they are "
        "explicitly stated in the snippets. Do NOT guess.\n"
        "If the evidence is ambiguous or conflicting, set confidence to 'none' "
        "and return nulls.\n\n"
        f"Snippets:\n{snippet_text}\n\n"
        "Respond with ONLY this JSON (no markdown, no extra text):\n"
        '{"uom": <string or null>, "pack_quantity": <integer or null>, '
        '"evidence_text": <exact quote from snippet or null>, '
        '"confidence": "high"|"medium"|"low"|"none"}'
    )

    # Retry loop with exponential backoff
    max_retries = config.LLM_MAX_RETRIES
    for attempt in range(1, max_retries + 1):
        if not limiter.acquire(timeout=30):
            return LLMCallResult(
                status="skipped_rate_limit",
                reason="rate limit not available within timeout",
                attempts=attempt,
            )
        try:
            data = _call_gemini(api_key, prompt)
            return LLMCallResult(status="success", data=data, attempts=attempt)
        except _RateLimitError as exc:
            wait = _backoff(attempt, exc.retry_after)
            logger.warning("Gemini 429 (attempt %d/%d), backing off %.1fs", attempt, max_retries, wait)
            time.sleep(wait)
        except Exception as exc:
            logger.error("Gemini call failed (attempt %d): %s", attempt, exc)
            return LLMCallResult(
                status="failed_other",
                reason=str(exc),
                attempts=attempt,
            )

    return LLMCallResult(
        status="failed_429",
        reason=f"exhausted {max_retries} retries on rate limit",
        attempts=max_retries,
    )


class _RateLimitError(Exception):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _call_gemini(api_key: str, prompt: str) -> dict[str, Any]:
    """Make a single Gemini API call and parse the JSON response."""
    try:
        from google import genai  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError("google-genai is not installed") from exc

    client = genai.Client(api_key=api_key)

    try:
        response = client.models.generate_content(
            model=config.GEMINI_MODEL,
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                temperature=config.LLM_TEMPERATURE,
                max_output_tokens=256,
            ),
        )
    except Exception as exc:
        exc_str = str(exc).lower()
        if "429" in exc_str or "resource_exhausted" in exc_str:
            retry_after = None
            # Try to parse Retry-After
            import re as _re
            m = _re.search(r"retry.after[:\s]+(\d+)", exc_str)
            if m:
                retry_after = float(m.group(1))
            raise _RateLimitError(str(exc), retry_after) from exc
        raise

    text = response.text.strip()

    # Strip markdown fences if present
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

    data: dict[str, Any] = json.loads(text)
    return data


def _backoff(attempt: int, retry_after: float | None = None) -> float:
    """Exponential backoff with jitter."""
    if retry_after and retry_after > 0:
        return retry_after + random.uniform(0, 1)
    return min(60, (2 ** attempt) + random.uniform(0, 1))
