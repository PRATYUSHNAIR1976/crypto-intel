"""Full-history data-quality audit over everything in the normalized store.

Ingest-time validation only sees each new batch; the audit re-runs every check
across complete series, so it also catches gaps at batch boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from cryptointel.domain.models import Dataset, Interval, utc_now
from cryptointel.storage.catalog import Catalog
from cryptointel.storage.parquet_store import ParquetStore
from cryptointel.validation.validator import QualityIssue, validate


@dataclass
class SeriesAudit:
    dataset: str
    provider: str
    market: str
    symbol: str
    interval: str | None
    rows: int
    first_ts: str | None
    last_ts: str | None
    quality_score: float
    issues: list[QualityIssue] = field(default_factory=list)


def audit_store(store: ParquetStore, catalog: Catalog | None = None) -> list[SeriesAudit]:
    now = utc_now()
    results: list[SeriesAudit] = []
    for dataset in Dataset:
        for path in store.list_series(dataset):
            parts = dict(p.split("=", 1) for p in path.parent.relative_to(store.root / dataset.value).parts)
            interval = parts.get("interval")
            df = store.read(dataset, parts["provider"], parts["market"], parts["symbol"], interval)
            result = validate(df, dataset, Interval(interval) if interval else None, now=now, drop_incomplete=False)
            results.append(SeriesAudit(
                dataset=dataset.value, provider=parts["provider"], market=parts["market"], symbol=parts["symbol"],
                interval=interval, rows=len(df),
                first_ts=df["ts"].min().isoformat() if len(df) else None,
                last_ts=df["ts"].max().isoformat() if len(df) else None,
                quality_score=result.quality_score, issues=result.issues,
            ))
            if catalog is not None:
                catalog.record_issues(result.issues, scope="audit", provider=parts["provider"], dataset=dataset.value,
                                      market=parts["market"], symbol=parts["symbol"], interval=interval)
    return results
