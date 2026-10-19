from datetime import datetime, timezone

import pandas as pd

from cryptointel.domain.models import Dataset, Interval
from cryptointel.providers.base import RawPage
from cryptointel.storage.catalog import Catalog
from cryptointel.storage.parquet_store import ParquetStore, QuarantineStore
from cryptointel.storage.query import MarketDataRepository
from cryptointel.storage.raw import RawStore
from cryptointel.validation.validator import QualityIssue, Severity
from tests.conftest import T0, ohlcv_frame


def test_parquet_upsert_merges_and_dedupes(tmp_path):
    store = ParquetStore(tmp_path)
    assert store.upsert(Dataset.OHLCV, ohlcv_frame(10)) == 10

    overlap = ohlcv_frame(15)  # 10 existing + 5 new
    overlap["close"] = overlap["close"] + 1  # revised values must win
    overlap["high"] = overlap["high"] + 1
    assert store.upsert(Dataset.OHLCV, overlap) == 5

    stored = store.read(Dataset.OHLCV, "binance", "spot", "BTC-USDT", "1h")
    assert len(stored) == 15 and stored["ts"].is_monotonic_increasing
    assert stored["close"].iat[0] == overlap["close"].iat[0]
    assert store.latest_ts(Dataset.OHLCV, "binance", "spot", "BTC-USDT", "1h") == stored["ts"].max()
    assert str(stored["ts"].dtype) == "datetime64[ns, UTC]"


def test_parquet_series_are_partitioned(tmp_path):
    store = ParquetStore(tmp_path)
    store.upsert(Dataset.OHLCV, pd.concat([ohlcv_frame(3), ohlcv_frame(3, symbol="ETH-USDT", market="perp")]))
    paths = [p.relative_to(tmp_path).as_posix() for p in store.list_series(Dataset.OHLCV)]
    assert paths == [
        "ohlcv/provider=binance/market=perp/symbol=ETH-USDT/interval=1h/data.parquet",
        "ohlcv/provider=binance/market=spot/symbol=BTC-USDT/interval=1h/data.parquet",
    ]


def test_latest_ts_none_when_empty(tmp_path):
    assert ParquetStore(tmp_path).latest_ts(Dataset.OHLCV, "binance", "spot", "BTC-USDT", "1h") is None


def test_raw_store_roundtrip(tmp_path):
    page = RawPage("https://x/api", {"symbol": "BTCUSDT"}, 200, datetime(2025, 1, 1, tzinfo=timezone.utc), [[1, "2"]])
    path = RawStore(tmp_path).write(run_id="abc", provider="binance", dataset="ohlcv", market="spot",
                                    symbol="BTC-USDT", interval="1h", pages=[page])
    assert path.name == "abc.json.gz"
    doc = RawStore.read(path)
    assert doc["pages"][0]["payload"] == [[1, "2"]] and doc["pages"][0]["params"] == {"symbol": "BTCUSDT"}


def test_quarantine_store(tmp_path):
    q = QuarantineStore(tmp_path)
    assert q.write(Dataset.OHLCV, ohlcv_frame(0), "r1") is None
    path = q.write(Dataset.OHLCV, ohlcv_frame(2).assign(dq_reason="null_close"), "r1")
    assert set(pd.read_parquet(path)["run_id"]) == {"r1"}


def test_catalog_runs_and_issues(tmp_path):
    cat = Catalog(tmp_path / "c.sqlite")
    meta = dict(provider="binance", dataset="ohlcv", market="spot", symbol="BTC-USDT", interval="1h")
    cat.start_run(run_id="r1", requested_start="a", effective_start="b", requested_end="c", **meta)
    cat.finish_run("r1", status="success", rows_fetched=10, rows_written=9, quality_score=0.9)
    cat.record_issues([QualityIssue("null_close", Severity.ERROR, 1, "m", ["2025-01-01T00:00:00+00:00"]),
                       QualityIssue("missing_intervals", Severity.WARNING, 2, "m")],
                      scope="ingest", run_id="r1", **meta)

    run = cat.recent_runs()[0]
    assert run["status"] == "success" and run["rows_written"] == 9 and run["finished_at"]
    assert len(cat.issues(run_id="r1")) == 2
    errors = cat.issues(min_severity="error")
    assert [e["check_name"] for e in errors] == ["null_close"] and errors[0]["sample"] == ["2025-01-01T00:00:00+00:00"]


def test_repository_queries_with_duckdb(tmp_path):
    store = ParquetStore(tmp_path)
    store.upsert(Dataset.OHLCV, pd.concat([ohlcv_frame(48), ohlcv_frame(10, interval=Interval.H4)]))
    repo = MarketDataRepository(tmp_path)

    last5 = repo.candles("BTC-USDT", "spot", "1h", limit=5)
    assert len(last5) == 5 and last5["ts"].is_monotonic_increasing
    assert last5["ts"].iat[-1] == pd.Timestamp(T0) + pd.Timedelta(hours=47)

    window = repo.candles("BTC-USDT", "spot", "1h", start=T0, end=T0 + pd.Timedelta(hours=3))
    assert len(window) == 3

    summary = repo.summary()
    assert set(summary["interval"]) == {"1h", "4h"} and summary["rows"].sum() == 58


def test_repository_empty_store(tmp_path):
    repo = MarketDataRepository(tmp_path)
    assert repo.candles("BTC-USDT", "spot", "1h").empty
    assert repo.summary().empty
