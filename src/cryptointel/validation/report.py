"""DQ report: one row per (series, check), including passing checks."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from cryptointel.validation.validator import ValidationResult

REPORT_COLUMNS = [
    "run_timestamp", "run_id", "scope", "dataset", "provider", "market", "symbol", "timeframe",
    "check_name", "severity", "status", "failed_rows", "total_rows", "message", "sample",
]


def report_rows(result: ValidationResult, *, run_timestamp: datetime, scope: str, dataset: str, provider: str,
                market: str, symbol: str, timeframe: str | None, run_id: str | None = None) -> list[dict]:
    return [{
        "run_timestamp": run_timestamp.isoformat(), "run_id": run_id, "scope": scope, "dataset": dataset,
        "provider": provider, "market": market, "symbol": symbol, "timeframe": timeframe,
        **check.to_dict(),
    } for check in result.checks]


def report_frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=REPORT_COLUMNS)
