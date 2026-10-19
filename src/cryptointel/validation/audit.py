"""Full-history data-quality audit over everything in the processed store.

Ingest-time validation only sees each new batch; the audit re-runs every check
across complete series, so it also catches gaps at batch boundaries. Read-only:
it reports, it never modifies stored data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from cryptointel.domain.models import Dataset, Interval, utc_now
from cryptointel.storage.catalog import Catalog
from cryptointel.storage.parquet_store import ParquetStore
from cryptointel.validation.report import report_rows
from cryptointel.validation.rules import DQConfig
from cryptointel.validation.validator import ValidationResult, validate


@dataclass
class SeriesAudit:
    dataset: str
    provider: str
    market: str
    symbol: str
    interval: str | None
    first_ts: str | None
    last_ts: str | None
    result: ValidationResult


def audit_store(store: ParquetStore, catalog: Catalog | None = None, config: DQConfig | None = None,
                now: datetime | None = None) -> tuple[list[SeriesAudit], list[dict]]:
    """Validate every stored series. Returns per-series audits and DQ report rows."""
    now = now or utc_now()
    audits: list[SeriesAudit] = []
    rows: list[dict] = []
    for dataset in Dataset:
        for path in store.list_series(dataset):
            parts = dict(p.split("=", 1) for p in path.parent.relative_to(store.root / dataset.value).parts)
            interval = parts.get("interval")
            df = store.read(dataset, parts["provider"], parts["market"], parts["symbol"], interval)
            result = validate(df, dataset, Interval(interval) if interval else None, now=now, config=config,
                              check_incomplete=False)
            audits.append(SeriesAudit(
                dataset=dataset.value, provider=parts["provider"], market=parts["market"], symbol=parts["symbol"],
                interval=interval, first_ts=df["ts"].min().isoformat() if len(df) else None,
                last_ts=df["ts"].max().isoformat() if len(df) else None, result=result,
            ))
            rows += report_rows(result, run_timestamp=now, scope="audit", dataset=dataset.value,
                                provider=parts["provider"], market=parts["market"], symbol=parts["symbol"],
                                timeframe=interval)
    if catalog is not None:
        catalog.record_dq_report(rows)
    return audits, rows
