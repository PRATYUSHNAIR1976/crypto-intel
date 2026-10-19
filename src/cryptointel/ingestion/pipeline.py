"""Ingestion pipeline: fetch -> raw landing -> validate -> processed + rejected stores -> DQ report.

Incremental by default: each job resumes from the latest stored timestamp.
A failing job is recorded and reported; it never aborts the other jobs.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

from cryptointel.domain.models import Dataset, Interval, Market, utc_now
from cryptointel.providers.base import (
    FetchResult,
    FundingSource,
    MarketDataProvider,
    OHLCVSource,
    OpenInterestSource,
    ProviderError,
)
from cryptointel.storage.catalog import Catalog
from cryptointel.storage.parquet_store import ParquetStore, RejectedStore
from cryptointel.storage.raw import RawStore
from cryptointel.validation.report import report_rows
from cryptointel.validation.rules import DQConfig, default_dq_config
from cryptointel.validation.validator import CheckResult, Status, validate

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestionJob:
    dataset: Dataset
    symbol: str
    market: Market
    interval: Interval | None
    start: datetime
    end: datetime | None = None

    def label(self) -> str:
        return f"{self.dataset}:{self.symbol}:{self.market}" + (f":{self.interval}" if self.interval else "")


@dataclass
class IngestionReport:
    run_id: str
    job: IngestionJob
    status: str
    effective_start: datetime | None = None
    rows_fetched: int = 0
    rows_written: int = 0
    rows_rejected: int = 0
    rows_dropped: int = 0  # excluded for non-quality reasons (still-open candles); also in rejected/
    quality_score: float | None = None
    checks: list[CheckResult] = field(default_factory=list)
    raw_path: str | None = None
    error: str | None = None

    @property
    def issues(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status is not Status.PASS]


class IngestionPipeline:
    """raw/ (as received) -> validate -> processed/ (validated) + rejected/ (with reasons) + DQ report."""

    def __init__(self, provider: MarketDataProvider, raw_store: RawStore, store: ParquetStore,
                 rejected: RejectedStore, catalog: Catalog, *, incremental: bool = True,
                 dq_config: DQConfig | None = None, clock: Callable[[], datetime] = utc_now) -> None:
        self.provider = provider
        self.raw_store = raw_store
        self.store = store
        self.rejected = rejected
        self.catalog = catalog
        self.incremental = incremental
        self.dq_config = dq_config or default_dq_config()
        self._clock = clock

    def _resolve_start(self, job: IngestionJob) -> datetime:
        if not self.incremental:
            return job.start
        interval = job.interval.value if job.interval else None
        latest = self.store.latest_ts(job.dataset, self.provider.name, job.market.value, job.symbol, interval)
        if latest is None:
            return job.start
        step = job.interval.timedelta if job.interval else timedelta(milliseconds=1)
        return max(job.start, (latest + step).to_pydatetime())

    def _fetch(self, job: IngestionJob, start: datetime, end: datetime) -> FetchResult:
        p = self.provider
        if job.dataset is Dataset.OHLCV and isinstance(p, OHLCVSource):
            return p.fetch_ohlcv(job.symbol, job.market, job.interval, start, end)
        if job.dataset is Dataset.FUNDING and isinstance(p, FundingSource):
            return p.fetch_funding_rates(job.symbol, start, end)
        if job.dataset is Dataset.OPEN_INTEREST and isinstance(p, OpenInterestSource):
            return p.fetch_open_interest(job.symbol, job.interval, start, end)
        raise ProviderError(f"{p.name} has no {job.dataset} capability")

    def run(self, job: IngestionJob) -> IngestionReport:
        run_id = uuid.uuid4().hex[:12]
        now = self._clock()
        end = job.end or now
        interval = job.interval.value if job.interval else None
        meta = dict(provider=self.provider.name, dataset=job.dataset.value, market=job.market.value,
                    symbol=job.symbol, interval=interval)

        if not self.provider.supports(job.dataset, job.market, job.interval):
            return IngestionReport(run_id, job, "unsupported",
                                   error=f"{self.provider.name} does not support {job.label()}")

        start = self._resolve_start(job)
        report = IngestionReport(run_id, job, "running", effective_start=start)
        self.catalog.start_run(run_id=run_id, requested_start=job.start.isoformat(),
                               effective_start=start.isoformat(), requested_end=end.isoformat(), **meta)

        # Need at least one full interval to have elapsed for a closed candle to exist.
        min_span = job.interval.timedelta if job.interval else timedelta(0)
        if start + min_span > end:
            report.status = "up_to_date"
            self.catalog.finish_run(run_id, status=report.status)
            return report

        try:
            result = self._fetch(job, start, end)
            report.rows_fetched = len(result.frame)
            report.raw_path = str(self.raw_store.write(run_id=run_id, pages=result.pages, **meta))

            validation = validate(result.frame, job.dataset, job.interval, now=now, config=self.dq_config)
            report.rows_written = self.store.upsert(job.dataset, validation.validated)
            self.rejected.write(job.dataset, validation.rejected, run_id)
            report.rows_rejected = len(validation.rejected)
            report.rows_dropped = validation.excluded_rows
            report.quality_score = validation.quality_score
            report.checks = validation.checks
            report.status = "success_with_rejections" if validation.has_errors else "success"
            self.catalog.record_dq_report(report_rows(
                validation, run_timestamp=now, scope="ingest", run_id=run_id, dataset=meta["dataset"],
                provider=meta["provider"], market=meta["market"], symbol=meta["symbol"], timeframe=interval))
        except Exception as exc:  # recorded, not swallowed: surfaces in catalog, CLI, API and logs
            report.status = "failed"
            report.error = f"{type(exc).__name__}: {exc}"
            logger.error("ingestion failed", exc_info=True, extra={
                "event": "ingest_failed", "run_id": run_id, **meta, "error": report.error})

        self.catalog.finish_run(
            run_id, status=report.status, rows_fetched=report.rows_fetched, rows_written=report.rows_written,
            rows_rejected=report.rows_rejected, rows_dropped=report.rows_dropped,
            quality_score=report.quality_score, raw_path=report.raw_path, error=report.error,
        )
        logger.info("ingestion finished", extra={
            "event": "ingest_finished", "run_id": run_id, **meta, "status": report.status,
            "rows_fetched": report.rows_fetched, "rows_written": report.rows_written,
            "rows_rejected": report.rows_rejected, "rows_dropped": report.rows_dropped,
            "quality_score": report.quality_score,
            "failed_checks": [c.check_name for c in report.issues if c.status is Status.FAIL],
            "warned_checks": [c.check_name for c in report.issues if c.status is Status.WARN],
        })
        return report

    def run_all(self, jobs: list[IngestionJob]) -> list[IngestionReport]:
        reports = []
        for job in jobs:
            logger.info("ingestion started", extra={"event": "ingest_started", "job": job.label(),
                                                    "provider": self.provider.name})
            reports.append(self.run(job))
        return reports
