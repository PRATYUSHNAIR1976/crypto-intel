"""Read-only local API over the normalized store and catalog.

Run: uvicorn cryptointel.api.main:create_app --factory   (or `cip serve`)
"""

from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

from cryptointel import __version__
from cryptointel.config import Settings, get_settings
from cryptointel.domain.models import Interval, Market, normalize_symbol
from cryptointel.storage.catalog import Catalog
from cryptointel.storage.query import MarketDataRepository
from cryptointel.storage.reader import SourceResolvingReader


def _records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso", date_unit="s"))


def _symbol(value: str) -> str:
    try:
        return normalize_symbol(value)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    repo = MarketDataRepository(settings.processed_dir)
    reader = SourceResolvingReader(repo, settings.source_priority)  # market data is served source-agnostic
    catalog = Catalog(settings.catalog_path)

    app = FastAPI(
        title="Crypto Intel API",
        version=__version__,
        description="Local, read-only market-data API. Research and education only — no trading endpoints.",
    )

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "version": __version__, "data_dir": str(settings.data_dir)}

    @app.get("/api/v1/datasets")
    def datasets() -> list[dict]:
        """What is stored: one row per series with row counts and time range."""
        return _records(repo.summary())

    @app.get("/api/v1/candles")
    def candles(symbol: str, interval: Interval, market: Market = Market.SPOT,
                start: datetime | None = None, end: datetime | None = None,
                limit: int = Query(500, ge=1, le=10_000)) -> list[dict]:
        return _records(reader.candles(_symbol(symbol), interval, market, start, end, limit))

    @app.get("/api/v1/funding")
    def funding(symbol: str, start: datetime | None = None, end: datetime | None = None,
                limit: int = Query(500, ge=1, le=10_000)) -> list[dict]:
        return _records(reader.funding(_symbol(symbol), start, end, limit))

    @app.get("/api/v1/open-interest")
    def open_interest(symbol: str, interval: Interval, start: datetime | None = None,
                      end: datetime | None = None, limit: int = Query(500, ge=1, le=10_000)) -> list[dict]:
        return _records(reader.open_interest(_symbol(symbol), interval, start, end, limit))

    @app.get("/api/v1/ingestion-runs")
    def ingestion_runs(limit: int = Query(50, ge=1, le=1000)) -> list[dict]:
        return catalog.recent_runs(limit)

    @app.get("/api/v1/data-quality")
    def data_quality(run_id: str | None = None, scope: str | None = Query(None, pattern="^(ingest|audit)$"),
                     symbol: str | None = None,
                     status: list[str] | None = Query(None, description="pass | info | warn | fail (repeatable)"),
                     min_severity: str | None = Query(None, pattern="^(warning|error)$"),
                     limit: int = Query(200, ge=1, le=5000)) -> list[dict]:
        """DQ report rows (one per series x check). Defaults to non-passing checks."""
        if status and not set(status) <= {"pass", "info", "warn", "fail"}:
            raise HTTPException(status_code=422, detail="status must be pass, info, warn or fail")
        return catalog.dq_report(run_id=run_id, scope=scope, symbol=_symbol(symbol) if symbol else None,
                                 statuses=status or ["info", "warn", "fail"], min_severity=min_severity, limit=limit)

    return app
