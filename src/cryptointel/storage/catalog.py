"""SQLite catalog: ingestion lineage and the data-quality report (relational metadata)."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from cryptointel.domain.models import utc_now

SCHEMA = """
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id            TEXT PRIMARY KEY,
    provider          TEXT NOT NULL,
    dataset           TEXT NOT NULL,
    market            TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    interval          TEXT,
    requested_start   TEXT,
    effective_start   TEXT,
    requested_end     TEXT,
    status            TEXT NOT NULL,  -- running | success | success_with_rejections | up_to_date | failed
    rows_fetched      INTEGER NOT NULL DEFAULT 0,
    rows_written      INTEGER NOT NULL DEFAULT 0,
    rows_rejected     INTEGER NOT NULL DEFAULT 0,
    rows_dropped      INTEGER NOT NULL DEFAULT 0,  -- excluded for non-quality reasons (open candles)
    quality_score     REAL,
    raw_path          TEXT,
    error             TEXT,
    started_at        TEXT NOT NULL,
    finished_at       TEXT
);
CREATE INDEX IF NOT EXISTS ix_runs_series ON ingestion_runs (dataset, symbol, market, interval, started_at);

-- One row per (series, check) per validation run, passing checks included.
CREATE TABLE IF NOT EXISTS dq_report (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp  TEXT NOT NULL,
    run_id         TEXT,             -- ingestion run id; NULL for full-history audits
    scope          TEXT NOT NULL,    -- ingest | audit
    dataset        TEXT NOT NULL,
    provider       TEXT NOT NULL,
    market         TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    timeframe      TEXT,
    check_name     TEXT NOT NULL,
    severity       TEXT NOT NULL,    -- info | warning | error
    status         TEXT NOT NULL,    -- pass | info | warn | fail
    failed_rows    INTEGER NOT NULL,
    total_rows     INTEGER NOT NULL,
    message        TEXT,
    sample_json    TEXT
);
CREATE INDEX IF NOT EXISTS ix_dq_series ON dq_report (dataset, symbol, market, timeframe, run_timestamp);
CREATE INDEX IF NOT EXISTS ix_dq_run ON dq_report (run_id);
"""

_REPORT_FIELDS = ["run_timestamp", "run_id", "scope", "dataset", "provider", "market", "symbol", "timeframe",
                  "check_name", "severity", "status", "failed_rows", "total_rows", "message"]


class Catalog:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            self._migrate(conn)
            conn.executescript(SCHEMA)

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        """Bring catalogs created by earlier versions up to date (idempotent)."""
        cols = {r[1] for r in conn.execute("PRAGMA table_info(ingestion_runs)")}
        if "rows_quarantined" in cols:
            conn.execute("ALTER TABLE ingestion_runs RENAME COLUMN rows_quarantined TO rows_rejected")
            conn.execute("UPDATE ingestion_runs SET status='success_with_rejections' "
                         "WHERE status='success_with_quarantine'")

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def start_run(self, *, run_id: str, provider: str, dataset: str, market: str, symbol: str,
                  interval: str | None, requested_start: str, effective_start: str, requested_end: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO ingestion_runs (run_id, provider, dataset, market, symbol, interval,
                   requested_start, effective_start, requested_end, status, started_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?)""",
                (run_id, provider, dataset, market, symbol, interval,
                 requested_start, effective_start, requested_end, utc_now().isoformat()),
            )

    def finish_run(self, run_id: str, *, status: str, rows_fetched: int = 0, rows_written: int = 0,
                   rows_rejected: int = 0, rows_dropped: int = 0, quality_score: float | None = None,
                   raw_path: str | None = None, error: str | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE ingestion_runs SET status=?, rows_fetched=?, rows_written=?, rows_rejected=?,
                   rows_dropped=?, quality_score=?, raw_path=?, error=?, finished_at=? WHERE run_id=?""",
                (status, rows_fetched, rows_written, rows_rejected, rows_dropped, quality_score,
                 raw_path, error, utc_now().isoformat(), run_id),
            )

    def record_dq_report(self, rows: list[dict]) -> None:
        if not rows:
            return
        values = [tuple(r[f] for f in _REPORT_FIELDS) + (json.dumps(r.get("sample", [])),) for r in rows]
        with self._connect() as conn:
            conn.executemany(
                f"INSERT INTO dq_report ({', '.join(_REPORT_FIELDS)}, sample_json) "
                f"VALUES ({', '.join('?' * (len(_REPORT_FIELDS) + 1))})",
                values,
            )

    def recent_runs(self, limit: int = 50) -> list[dict]:
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT ?", (limit,))
            return [dict(r) for r in cur.fetchall()]

    def dq_report(self, *, run_id: str | None = None, scope: str | None = None, symbol: str | None = None,
                  statuses: list[str] | None = None, min_severity: str | None = None, limit: int = 500) -> list[dict]:
        clauses, params = [], []
        if run_id:
            clauses.append("run_id = ?"); params.append(run_id)
        if scope:
            clauses.append("scope = ?"); params.append(scope)
        if symbol:
            clauses.append("symbol = ?"); params.append(symbol)
        if statuses:
            clauses.append(f"status IN ({', '.join('?' * len(statuses))})"); params.extend(statuses)
        if min_severity == "warning":
            clauses.append("severity IN ('warning', 'error')")
        elif min_severity == "error":
            clauses.append("severity = 'error'")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            cur = conn.execute(f"SELECT * FROM dq_report {where} ORDER BY id DESC LIMIT ?", (*params, limit))
            out = []
            for r in cur.fetchall():
                row = dict(r)
                row["sample"] = json.loads(row.pop("sample_json") or "[]")
                out.append(row)
            return out

    def issues(self, **filters) -> list[dict]:
        """Report rows whose check did not pass."""
        return self.dq_report(statuses=["info", "warn", "fail"], **filters)
