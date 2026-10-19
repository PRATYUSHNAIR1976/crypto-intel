import pandas as pd
import pytest
from fastapi.testclient import TestClient

from cryptointel.api.main import create_app
from cryptointel.domain.models import Dataset
from cryptointel.domain.schemas import coerce
from cryptointel.storage.catalog import Catalog
from cryptointel.storage.parquet_store import ParquetStore
from tests.conftest import NOW, T0, ohlcv_frame


@pytest.fixture
def client(settings):
    store = ParquetStore(settings.normalized_dir)
    store.upsert(Dataset.OHLCV, ohlcv_frame(48))
    store.upsert(Dataset.FUNDING, coerce(pd.DataFrame({
        "ts": pd.date_range(T0, periods=3, freq="8h", tz="UTC"), "provider": "binance", "market": "perp",
        "symbol": "BTC-USDT", "funding_rate": [0.0001, 0.0002, None], "mark_price": 1.0,
        "ingested_at": pd.Timestamp(NOW),
    }), Dataset.FUNDING))
    Catalog(settings.catalog_path).start_run(run_id="r1", provider="binance", dataset="ohlcv", market="spot",
                                             symbol="BTC-USDT", interval="1h", requested_start="a",
                                             effective_start="a", requested_end="b")
    return TestClient(create_app(settings))


def test_health(client):
    assert client.get("/health").json()["status"] == "ok"


def test_candles(client):
    rows = client.get("/api/v1/candles", params={"symbol": "btc-usdt", "interval": "1h", "limit": 3}).json()
    assert len(rows) == 3
    assert rows[-1]["ts"].startswith("2025-01-02T23:00:00")
    assert {"open", "high", "low", "close", "volume", "taker_buy_volume"} <= rows[0].keys()


def test_candles_validation_errors(client):
    assert client.get("/api/v1/candles", params={"symbol": "BTCUSDT", "interval": "1h"}).status_code == 422
    assert client.get("/api/v1/candles", params={"symbol": "BTC-USDT", "interval": "7m"}).status_code == 422


def test_funding_nan_serialises_as_null(client):
    rows = client.get("/api/v1/funding", params={"symbol": "BTC-USDT"}).json()
    assert [r["funding_rate"] for r in rows] == [0.0001, 0.0002, None]


def test_datasets_and_runs(client):
    datasets = client.get("/api/v1/datasets").json()
    assert {d["dataset"] for d in datasets} == {"ohlcv", "funding"}
    assert client.get("/api/v1/ingestion-runs").json()[0]["run_id"] == "r1"
    assert client.get("/api/v1/data-quality", params={"min_severity": "error"}).json() == []


def test_no_trading_endpoints_exist(client):
    paths = {route.path for route in client.app.routes}
    assert not any(word in p for p in paths for word in ("order", "trade", "execute"))
