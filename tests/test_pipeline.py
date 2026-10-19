from datetime import timedelta

import pandas as pd

from cryptointel.domain.models import Dataset, Interval, Market
from cryptointel.ingestion.pipeline import IngestionJob, IngestionPipeline
from cryptointel.providers.base import FetchResult, MarketDataProvider, OHLCVSource, RawPage
from cryptointel.storage.catalog import Catalog
from cryptointel.storage.parquet_store import ParquetStore, RejectedStore
from cryptointel.storage.raw import RawStore
from tests.conftest import NOW, T0, ohlcv_frame


class FakeProvider(MarketDataProvider, OHLCVSource):
    """OHLCV-only provider serving a fixed in-memory history; records requested windows."""

    name = "binance"

    def __init__(self, history: pd.DataFrame, fail: bool = False):
        self.history = history
        self.fail = fail
        self.requests: list[tuple] = []

    def supports(self, dataset, market, interval=None):
        return dataset is Dataset.OHLCV

    def fetch_ohlcv(self, symbol, market, interval, start, end):
        self.requests.append((start, end))
        if self.fail:
            raise ConnectionError("exchange unreachable")
        rows = self.history[(self.history["ts"] >= start) & (self.history["ts"] <= end)]
        page = RawPage("fake://klines", {"start": str(start)}, 200, NOW, rows.to_dict("records"))
        return FetchResult(Dataset.OHLCV, rows.copy(), [page])


def build(tmp_path, provider, clock=lambda: NOW):
    store = ParquetStore(tmp_path / "processed")
    catalog = Catalog(tmp_path / "catalog.sqlite")
    pipe = IngestionPipeline(provider, RawStore(tmp_path / "raw"), store, RejectedStore(tmp_path / "rejected"),
                             catalog, clock=clock)
    return pipe, store, catalog


JOB = IngestionJob(Dataset.OHLCV, "BTC-USDT", Market.SPOT, Interval.H1, T0)


def test_end_to_end_raw_validated_rejected_are_separate(tmp_path):
    history = ohlcv_frame(24)
    history.loc[5, "close"] = None  # one corrupt candle
    pipe, store, catalog = build(tmp_path, FakeProvider(history))

    report = pipe.run(JOB)

    assert report.status == "success_with_rejections"
    assert (report.rows_fetched, report.rows_written, report.rows_rejected) == (24, 23, 1)
    # raw: everything as received (24 rows incl. the bad one)
    from cryptointel.storage.raw import RawStore as _Raw
    raw_doc = _Raw.read(next((tmp_path / "raw").rglob("*.json.gz")))
    assert len(raw_doc["pages"][0]["payload"]) == 24
    # validated
    assert len(store.read(Dataset.OHLCV, "binance", "spot", "BTC-USDT", "1h")) == 23
    # rejected, with the reason
    rejected = pd.read_parquet(tmp_path / "rejected" / "ohlcv" / f"{report.run_id}.parquet")
    assert list(rejected["dq_reason"]) == ["null_values"] and rejected["close"].isna().all()
    # report: every check recorded, including passes
    run = catalog.recent_runs()[0]
    assert run["run_id"] == report.run_id and run["status"] == "success_with_rejections"
    rows = catalog.dq_report(run_id=report.run_id)
    assert len(rows) == len(report.checks) and {"pass", "fail", "warn"} <= {r["status"] for r in rows}
    null_row = next(r for r in rows if r["check_name"] == "null_values")
    assert (null_row["failed_rows"], null_row["total_rows"], null_row["timeframe"]) == (1, 24, "1h")


def test_incremental_run_resumes_after_latest_stored_candle(tmp_path):
    history = ohlcv_frame(48)
    provider = FakeProvider(history)
    first_clock = T0 + timedelta(hours=24)
    pipe, store, _ = build(tmp_path, provider, clock=lambda: first_clock)
    assert pipe.run(JOB).rows_written == 24  # 24 closed candles at t0+24h

    pipe2, _, _ = build(tmp_path, provider, clock=lambda: T0 + timedelta(hours=48))
    report = pipe2.run(JOB)

    assert provider.requests[-1][0] == T0 + timedelta(hours=24)
    assert report.rows_written == 24
    assert len(store.read(Dataset.OHLCV, "binance", "spot", "BTC-USDT", "1h")) == 48


def test_up_to_date_does_not_call_provider(tmp_path):
    provider = FakeProvider(ohlcv_frame(24))
    clock = lambda: T0 + timedelta(hours=24, minutes=30)  # noqa: E731
    pipe, _, _ = build(tmp_path, provider, clock=clock)
    pipe.run(JOB)
    calls = len(provider.requests)
    assert pipe.run(JOB).status == "up_to_date"
    assert len(provider.requests) == calls


def test_provider_failure_is_recorded_not_raised(tmp_path):
    pipe, _, catalog = build(tmp_path, FakeProvider(ohlcv_frame(5), fail=True))
    report = pipe.run(JOB)
    assert report.status == "failed" and "exchange unreachable" in report.error
    assert catalog.recent_runs()[0]["status"] == "failed"


def test_unsupported_dataset_is_skipped(tmp_path):
    provider = FakeProvider(ohlcv_frame(5))
    assert provider.capabilities() == {Dataset.OHLCV}
    pipe, _, _ = build(tmp_path, provider)
    report = pipe.run(IngestionJob(Dataset.FUNDING, "BTC-USDT", Market.PERP, None, T0))
    assert report.status == "unsupported"


def test_build_jobs_only_creates_supported_combinations():
    from cryptointel.cli import build_jobs
    from cryptointel.providers.binance import BinanceProvider
    from cryptointel.providers.coingecko import CoinGeckoProvider

    all_markets, intervals = list(Market), [Interval.H1, Interval.H4]
    binance = build_jobs(BinanceProvider(), ["BTC-USDT"], all_markets, intervals, list(Dataset), T0)
    assert {(j.dataset, j.market) for j in binance} == {
        (Dataset.OHLCV, Market.SPOT), (Dataset.OHLCV, Market.PERP),
        (Dataset.FUNDING, Market.PERP), (Dataset.OPEN_INTEREST, Market.PERP)}

    coingecko = build_jobs(CoinGeckoProvider(), ["BTC-USDT"], all_markets, intervals, list(Dataset), T0)
    assert [(j.dataset, j.market, j.interval) for j in coingecko] == [(Dataset.OHLCV, Market.INDEX, Interval.H4)]


def test_full_audit_detects_boundary_gap(tmp_path):
    from cryptointel.validation.audit import audit_store

    store = ParquetStore(tmp_path)
    store.upsert(Dataset.OHLCV, ohlcv_frame(48).drop(index=range(10, 13)))
    audits, rows = audit_store(store, now=NOW)
    assert len(audits) == 1 and audits[0].result.input_rows == 45
    missing = audits[0].result.check("missing_timestamps")
    assert missing.failed_rows == 3 and missing.total_rows == 48
    assert audits[0].result.check("timestamp_gaps").failed_rows == 3
    assert {r["scope"] for r in rows} == {"audit"} and "incomplete_candle" not in {r["check_name"] for r in rows}
