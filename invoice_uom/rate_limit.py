"""Shared rate limiter – token bucket (7 RPM) + persisted daily counter (20 RPD).

Thread-safe.  All LLM call-sites must call ``limiter.acquire()`` before making a
request.  Returns ``True`` if the call is allowed, ``False`` if the daily budget
is exhausted.  Blocks (with back-off) if the per-minute bucket is empty.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import date
from pathlib import Path

from invoice_uom import config


class RateLimiter:
    """Token-bucket (per-minute) + daily cap, with disk-persisted counter."""

    def __init__(
        self,
        rpm: int = config.LLM_RPM,
        rpd: int = config.LLM_RPD,
        counter_file: Path = config.DAILY_COUNTER_FILE,
    ) -> None:
        self._rpm = rpm
        self._rpd = rpd
        self._counter_file = counter_file

        # Token bucket state
        self._tokens = float(rpm)
        self._max_tokens = float(rpm)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

        # Daily counter (persisted)
        self._today: str = ""
        self._daily_count: int = 0
        self._load_daily_counter()

    # ── public API ──────────────────────────────────────────────────────
    def acquire(self, timeout: float = 120.0) -> bool:
        """Block until a token is available or *timeout* seconds elapse.

        Returns ``True`` if the request is allowed.  Returns ``False`` if
        the daily cap has been reached.
        """
        with self._lock:
            self._rotate_day_if_needed()
            if self._daily_count >= self._rpd:
                return False

        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    self._daily_count += 1
                    self._persist_daily_counter()
                    return True
            # Wait before retrying
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(2.0, remaining))

    @property
    def daily_remaining(self) -> int:
        with self._lock:
            self._rotate_day_if_needed()
            return max(0, self._rpd - self._daily_count)

    # ── internals ───────────────────────────────────────────────────────
    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._max_tokens, self._tokens + elapsed * (self._rpm / 60.0))
        self._last_refill = now

    def _rotate_day_if_needed(self) -> None:
        today = date.today().isoformat()
        if today != self._today:
            self._today = today
            self._daily_count = 0
            self._persist_daily_counter()

    def _load_daily_counter(self) -> None:
        try:
            data = json.loads(self._counter_file.read_text(encoding="utf-8"))
            stored_date = data.get("date", "")
            if stored_date == date.today().isoformat():
                self._daily_count = int(data.get("count", 0))
                self._today = stored_date
            else:
                self._today = date.today().isoformat()
                self._daily_count = 0
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            self._today = date.today().isoformat()
            self._daily_count = 0

    def _persist_daily_counter(self) -> None:
        self._counter_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._counter_file.with_suffix(".tmp")
        tmp.write_text(
            json.dumps({"date": self._today, "count": self._daily_count}),
            encoding="utf-8",
        )
        tmp.replace(self._counter_file)


# Module-level singleton – import this everywhere.
_limiter: RateLimiter | None = None
_limiter_lock = threading.Lock()


def get_limiter() -> RateLimiter:
    """Return the global *RateLimiter* singleton (created on first call)."""
    global _limiter
    if _limiter is None:
        with _limiter_lock:
            if _limiter is None:
                _limiter = RateLimiter()
    return _limiter
