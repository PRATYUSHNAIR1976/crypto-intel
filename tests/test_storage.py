from datetime import datetime, timezone

import pandas as pd

from cryptointel.domain.models import Dataset, Interval
from cryptointel.providers.base import RawPage
from cryptointel.storage.catalog import Catalog
from cryptointel.storage.parquet_store import ParquetStore, RejectedStore
from cryptointel.storage.query import MarketDataRepository
from cryptointel.storage.raw import RawStore
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


def test_rejected_store(tmp_path):
    store = RejectedStore(tmp_path)
    assert store.write(Dataset.OHLCV, ohlcv_frame(0), "r1") is None
    path = store.write(Dataset.OHLCV, ohlcv_frame(2).assign(dq_reason="null_values"), "r1")
    assert path == tmp_path / "ohlcv" / "r1.parquet"
    stored = pd.read_parquet(path)
    assert set(stored["run_id"]) == {"r1"} and set(stored["dq_reason"]) == {"null_values"}


def _report_row(check, status, severity, failed, total=10, **kw):
    return {"run_timestamp": "2025-01-01T00:00:00+00:00", "run_id": "r1", "scope": "ingest", "dataset": "ohlcv",
            "provider": "binance", "market": "spot", "symbol": "BTC-USDT", "timeframe": "1h", "check_name": check,
            "severity": severity, "status": status, "failed_rows": failed, "total_rows": total, "message": "m", **kw}


def test_catalog_runs_and_dq_report(tmp_path):
    cat = Catalog(tmp_path / "c.sqlite")
    meta = dict(provider="binance", dataset="ohlcv", market="spot", symbol="BTC-USDT", interval="1h")
    cat.start_run(run_id="r1", requested_start="a", effective_start="b", requested_end="c", **meta)
    cat.finish_run("r1", status="success_with_rejections", rows_fetched=10, rows_written=9, rows_rejected=1,
                   quality_score=0.9)
    cat.record_dq_report([
        _report_row("null_values", "fail", "error", 1, sample=["2025-01-01T00:00:00+00:00"]),
        _report_row("missing_timestamps", "warn", "warning", 2),
        _report_row("high_below_low", "pass", "error", 0),
    ])

    run = cat.recent_runs()[0]
    assert (run["status"], run["rows_written"], run["rows_rejected"]) == ("success_with_rejections", 9, 1)
    assert len(cat.dq_report(run_id="r1")) == 3  # passes are part of the report
    assert {i["check_name"] for i in cat.issues(run_id="r1")} == {"null_values", "missing_timestamps"}
    errors = cat.issues(min_severity="error")
    assert [e["check_name"] for e in errors] == ["null_values"] and errors[0]["sample"] == ["2025-01-01T00:00:00+00:00"]


def test_catalog_migrates_pre_1b_schema(tmp_path):
    import sqlite3

    path = tmp_path / "old.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE ingestion_runs (run_id TEXT PRIMARY KEY, provider TEXT NOT NULL, dataset TEXT NOT NULL,"
                     " market TEXT NOT NULL, symbol TEXT NOT NULL, interval TEXT, requested_start TEXT,"
                     " effective_start TEXT, requested_end TEXT, status TEXT NOT NULL, rows_fetched INTEGER,"
                     " rows_written INTEGER, rows_quarantined INTEGER, rows_dropped INTEGER, quality_score REAL,"
                     " raw_path TEXT, error TEXT, started_at TEXT NOT NULL, finished_at TEXT)")
        conn.execute("INSERT INTO ingestion_runs (run_id, provider, dataset, market, symbol, status, rows_quarantined,"
                     " started_at) VALUES ('old', 'binance', 'ohlcv', 'spot', 'BTC-USDT', 'success_with_quarantine', 2, 'x')")
    cat = Catalog(path)
    Catalog(path)  # idempotent
    run = cat.recent_runs()[0]
    assert run["rows_rejected"] == 2 and run["status"] == "success_with_rejections"


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
