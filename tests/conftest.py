from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from cryptointel.config import Settings
from cryptointel.domain.models import Dataset, Interval
from cryptointel.domain.schemas import coerce

T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
T0_MS = int(T0.timestamp() * 1000)
NOW = datetime(2025, 6, 1, tzinfo=timezone.utc)


def binance_klines(start_ms: int, n: int, interval_ms: int, base: float = 100.0) -> list[list]:
    """Rows in Binance /klines wire format (strings for decimals)."""
    rows = []
    for i in range(n):
        o = base + i
        open_time = start_ms + i * interval_ms
        rows.append([
            open_time, f"{o:.2f}", f"{o + 2:.2f}", f"{o - 1:.2f}", f"{o + 1:.2f}", "10.0",
            open_time + interval_ms - 1, "1000.0", 42, "6.0", "600.0", "0",
        ])
    return rows


def ohlcv_frame(n: int = 48, interval: Interval = Interval.H1, start: datetime = T0, symbol: str = "BTC-USDT",
                market: str = "spot", base: float = 100.0) -> pd.DataFrame:
    ts = pd.date_range(start, periods=n, freq=interval.timedelta, tz="UTC")
    close = base + pd.Series(range(n), dtype="float64") * 0.1
    df = pd.DataFrame({
        "ts": ts, "provider": "binance", "market": market, "symbol": symbol, "interval": interval.value,
        "open": close - 0.05, "high": close + 0.5, "low": close - 0.5, "close": close,
        "volume": 10.0, "quote_volume": 1000.0, "trade_count": 5, "taker_buy_volume": 6.0,
        "taker_buy_quote_volume": 600.0, "ingested_at": pd.Timestamp(NOW),
    })
    return coerce(df, Dataset.OHLCV)


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(_env_file=None, data_dir=tmp_path / "data")
