"""SQLite catalog: ingestion lineage and data-quality history (relational metadata)."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator

from cryptointel.domain.models import utc_now
from cryptointel.validation.validator import QualityIssue

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
    status            TEXT NOT NULL,  -- running | success | success_with_quarantine | up_to_date | failed
    rows_fetched      INTEGER NOT NULL DEFAULT 0,
    rows_written      INTEGER NOT NULL DEFAULT 0,
    rows_quarantined  INTEGER NOT NULL DEFAULT 0,
    rows_dropped      INTEGER NOT NULL DEFAULT 0,
    quality_score     REAL,
    raw_path          TEXT,
    error             TEXT,
    started_at        TEXT NOT NULL,
    finished_at       TEXT
);
CREATE INDEX IF NOT EXISTS ix_runs_series ON ingestion_runs (dataset, symbol, market, interval, started_at);

CREATE TABLE IF NOT EXISTS data_quality_issues (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id         TEXT,             -- NULL for full-history audits
    scope          TEXT NOT NULL,    -- ingest | audit
    provider       TEXT NOT NULL,
    dataset        TEXT NOT NULL,
    market         TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    interval       TEXT,
    check_name     TEXT NOT NULL,
    severity       TEXT NOT NULL,    -- info | warning | error
    affected_rows  INTEGER NOT NULL,
    message        TEXT,
    sample_json    TEXT,
    created_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_dq_series ON data_quality_issues (dataset, symbol, market, interval, created_at);
"""


class Catalog:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(SCHEMA)

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
                   rows_quarantined: int = 0, rows_dropped: int = 0, quality_score: float | None = None,
                   raw_path: str | None = None, error: str | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE ingestion_runs SET status=?, rows_fetched=?, rows_written=?, rows_quarantined=?,
                   rows_dropped=?, quality_score=?, raw_path=?, error=?, finished_at=? WHERE run_id=?""",
                (status, rows_fetched, rows_written, rows_quarantined, rows_dropped, quality_score,
                 raw_path, error, utc_now().isoformat(), run_id),
            )

    def record_issues(self, issues: Iterable[QualityIssue], *, scope: str, provider: str, dataset: str,
                      market: str, symbol: str, interval: str | None, run_id: str | None = None) -> None:
        now = utc_now().isoformat()
        rows = [(run_id, scope, provider, dataset, market, symbol, interval, i.check, i.severity.value,
                 i.affected_rows, i.message, json.dumps(i.sample), now) for i in issues]
        if not rows:
            return
        with self._connect() as conn:
            conn.executemany(
                """INSERT INTO data_quality_issues (run_id, scope, provider, dataset, market, symbol, interval,
                   check_name, severity, affected_rows, message, sample_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )

    def recent_runs(self, limit: int = 50) -> list[dict]:
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT ?", (limit,))
            return [dict(r) for r in cur.fetchall()]

    def issues(self, *, run_id: str | None = None, scope: str | None = None, symbol: str | None = None,
               min_severity: str | None = None, limit: int = 200) -> list[dict]:
        clauses, params = [], []
        if run_id:
            clauses.append("run_id = ?"); params.append(run_id)
        if scope:
            clauses.append("scope = ?"); params.append(scope)
        if symbol:
            clauses.append("symbol = ?"); params.append(symbol)
        if min_severity == "warning":
            clauses.append("severity IN ('warning', 'error')")
        elif min_severity == "error":
            clauses.append("severity = 'error'")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            cur = conn.execute(f"SELECT * FROM data_quality_issues {where} ORDER BY id DESC LIMIT ?", (*params, limit))
            out = []
            for r in cur.fetchall():
                row = dict(r)
                row["sample"] = json.loads(row.pop("sample_json") or "[]")
                out.append(row)
            return out
