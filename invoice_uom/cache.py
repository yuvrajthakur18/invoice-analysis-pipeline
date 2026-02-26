"""SQLite-backed cache for agentic-lookup results.

Keyed on a *normalised query string* (lower-cased, whitespace-collapsed).
Stores resolved pack_qty, uom, evidence snippets, source URLs, and timestamp.
Thread-safe (SQLite handles its own locking in WAL mode).
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from invoice_uom import config


@dataclass
class LookupResult:
    """Cached outcome of a single agentic lookup."""

    query: str
    pack_qty: int | None = None
    uom: str | None = None
    evidence_snippets: list[dict[str, str]] = field(default_factory=list)
    source_urls: list[str] = field(default_factory=list)
    llm_used: bool = False
    timestamp: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "query": self.query,
            "pack_qty": self.pack_qty,
            "uom": self.uom,
            "evidence_snippets": self.evidence_snippets,
            "source_urls": self.source_urls,
            "llm_used": self.llm_used,
            "timestamp": self.timestamp,
        }


_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS lookup_cache (
    query_key    TEXT PRIMARY KEY,
    pack_qty     INTEGER,
    uom          TEXT,
    evidence     TEXT,      -- JSON array of {url, snippet}
    source_urls  TEXT,      -- JSON array of strings
    llm_used     INTEGER DEFAULT 0,
    ts           REAL
);
"""


class LookupCache:
    """Thread-safe SQLite cache for lookup results."""

    def __init__(self, db_path: Path = config.CACHE_DB_FILE) -> None:
        self._db_path = db_path
        self._local = threading.local()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        # Ensure table exists via a temporary connection.
        with self._connect() as conn:
            conn.execute(_CREATE_SQL)

    # ── public API ─────────────────────────────────────────────────────
    def get(self, query: str) -> LookupResult | None:
        key = self._normalise(query)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT pack_qty, uom, evidence, source_urls, llm_used, ts "
                "FROM lookup_cache WHERE query_key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        return LookupResult(
            query=query,
            pack_qty=row[0],
            uom=row[1],
            evidence_snippets=json.loads(row[2]) if row[2] else [],
            source_urls=json.loads(row[3]) if row[3] else [],
            llm_used=bool(row[4]),
            timestamp=row[5] or 0.0,
        )

    def put(self, result: LookupResult) -> None:
        key = self._normalise(result.query)
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO lookup_cache "
                "(query_key, pack_qty, uom, evidence, source_urls, llm_used, ts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    key,
                    result.pack_qty,
                    result.uom,
                    json.dumps(result.evidence_snippets),
                    json.dumps(result.source_urls),
                    int(result.llm_used),
                    time.time(),
                ),
            )

    # ── internals ──────────────────────────────────────────────────────
    def _connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self._db_path), timeout=10)
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return conn

    @staticmethod
    def _normalise(query: str) -> str:
        return " ".join(query.lower().split())
