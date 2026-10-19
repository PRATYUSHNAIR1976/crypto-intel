"""Command-line entry point: ``cip ingest | validate | summary | serve``."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

import pandas as pd

from cryptointel.config import Settings, get_settings
from cryptointel.domain.models import Dataset, Interval, Market, normalize_symbol
from cryptointel.ingestion.pipeline import IngestionJob, IngestionPipeline, IngestionReport
from cryptointel.providers.binance import BinanceProvider
from cryptointel.storage.catalog import Catalog
from cryptointel.storage.parquet_store import ParquetStore, QuarantineStore
from cryptointel.storage.query import MarketDataRepository
from cryptointel.storage.raw import RawStore
from cryptointel.validation.audit import audit_store
from cryptointel.validation.validator import Severity


def _csv(value: str | None) -> list[str] | None:
    return [v.strip() for v in value.split(",") if v.strip()] if value else None


def _parse_date(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def build_jobs(symbols: list[str], markets: list[Market], intervals: list[Interval],
               datasets: list[Dataset], start: datetime) -> list[IngestionJob]:
    jobs: list[IngestionJob] = []
    for symbol in symbols:
        if Dataset.OHLCV in datasets:
            jobs += [IngestionJob(Dataset.OHLCV, symbol, m, i, start) for m in markets for i in intervals]
        if Market.PERP in markets:
            if Dataset.FUNDING in datasets:
                jobs.append(IngestionJob(Dataset.FUNDING, symbol, Market.PERP, None, start))
            if Dataset.OPEN_INTEREST in datasets:
                jobs += [IngestionJob(Dataset.OPEN_INTEREST, symbol, Market.PERP, i, start) for i in intervals]
    return jobs


def _print_reports(reports: list[IngestionReport]) -> None:
    rows = [{
        "job": r.job.label(), "status": r.status, "from": r.effective_start.strftime("%Y-%m-%d %H:%M") if r.effective_start else "",
        "fetched": r.rows_fetched, "new": r.rows_written, "quarantined": r.rows_quarantined,
        "dropped": r.rows_dropped, "score": r.quality_score if r.quality_score is not None else "",
        "warnings": sum(i.severity is Severity.WARNING for i in r.issues),
        "error": (r.error or "")[:80],
    } for r in reports]
    print(pd.DataFrame(rows).to_string(index=False))


def cmd_ingest(args: argparse.Namespace, settings: Settings) -> int:
    symbols = [normalize_symbol(s) for s in (_csv(args.symbols) or settings.symbols)]
    markets = [Market(m) for m in (_csv(args.markets) or settings.markets)]
    intervals = [Interval(i) for i in (_csv(args.intervals) or settings.intervals)]
    datasets = [Dataset(d) for d in (_csv(args.datasets) or [d.value for d in Dataset])]
    start = _parse_date(args.since or settings.history_start)

    provider = BinanceProvider(
        spot_base_url=settings.binance_spot_base_url, futures_base_url=settings.binance_futures_base_url,
        timeout=settings.http_timeout_seconds, request_pause_seconds=settings.request_pause_seconds,
        max_attempts=settings.retry_max_attempts,
    )
    pipeline = IngestionPipeline(
        provider, RawStore(settings.raw_dir), ParquetStore(settings.normalized_dir),
        QuarantineStore(settings.quarantine_dir), Catalog(settings.catalog_path), incremental=not args.full_refresh,
    )
    reports = pipeline.run_all(build_jobs(symbols, markets, intervals, datasets, start))
    _print_reports(reports)
    return 1 if any(r.status == "failed" for r in reports) else 0


def cmd_validate(args: argparse.Namespace, settings: Settings) -> int:
    audits = audit_store(ParquetStore(settings.normalized_dir), Catalog(settings.catalog_path))
    if not audits:
        print("No data stored yet. Run `cip ingest` first.")
        return 0
    summary = pd.DataFrame([{
        "dataset": a.dataset, "symbol": a.symbol, "market": a.market, "interval": a.interval or "-",
        "rows": a.rows, "first": (a.first_ts or "")[:16], "last": (a.last_ts or "")[:16], "score": a.quality_score,
        "errors": sum(i.severity is Severity.ERROR for i in a.issues),
        "warnings": sum(i.severity is Severity.WARNING for i in a.issues),
    } for a in audits])
    print(summary.to_string(index=False))
    findings = [(a, i) for a in audits for i in a.issues if i.severity is not Severity.INFO]
    if findings:
        print("\nFindings:")
        for a, i in findings:
            print(f"  [{i.severity.upper():7}] {a.dataset}:{a.symbol}:{a.market}:{a.interval or '-'} "
                  f"{i.check} ({i.affected_rows} rows) — {i.message} sample={i.sample[:3]}")
    return 1 if any(i.severity is Severity.ERROR for _, i in findings) else 0


def cmd_summary(args: argparse.Namespace, settings: Settings) -> int:
    df = MarketDataRepository(settings.normalized_dir).summary()
    print("No data stored yet." if df.empty else df.to_string(index=False))
    return 0


def cmd_serve(args: argparse.Namespace, settings: Settings) -> int:
    import uvicorn

    uvicorn.run("cryptointel.api.main:create_app", factory=True, host=args.host, port=args.port)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cip", description="Crypto Intel — local research platform (no trading).")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("ingest", help="Fetch market data incrementally into the local store")
    p.add_argument("--symbols", help="e.g. BTC-USDT,ETH-USDT (default from .env)")
    p.add_argument("--markets", help="spot,perp")
    p.add_argument("--intervals", help="15m,1h,4h,1d")
    p.add_argument("--datasets", help="ohlcv,funding,open_interest")
    p.add_argument("--since", help="ISO date, e.g. 2024-01-01")
    p.add_argument("--full-refresh", action="store_true", help="Ignore stored data and re-fetch from --since")
    p.set_defaults(func=cmd_ingest)

    sub.add_parser("validate", help="Audit all stored data for quality issues").set_defaults(func=cmd_validate)
    sub.add_parser("summary", help="Show what is stored").set_defaults(func=cmd_summary)

    p = sub.add_parser("serve", help="Run the local read-only API")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.set_defaults(func=cmd_serve)

    args = parser.parse_args(argv)
    settings = get_settings()
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return args.func(args, settings)


if __name__ == "__main__":
    sys.exit(main())
