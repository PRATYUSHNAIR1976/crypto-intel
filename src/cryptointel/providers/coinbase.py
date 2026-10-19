"""Coinbase Exchange public API adapter — spot OHLCV (free, no API key).

Behaviour (verified against the live API on 2026-10-19):
  * GET /products/{id}/candles?granularity=&start=&end=  -> at most 300 candles per call,
    NEWEST FIRST, rows ``[time, low, high, open, close, volume]``; ``time`` is the candle
    OPEN time in seconds; start and end are inclusive.
  * Granularities: 60, 300, 900, 3600, 21600, 86400 s. There is no 4h, so 4h candles are
    aggregated here from 1h candles; only complete 4h buckets are emitted.
  * Volume is base-asset volume. No quote volume, trade count or taker split (left null).
  * Candles with no trades are omitted by Coinbase; validation reports them as gaps.
  * USD-stablecoin quotes map to the USD book (Coinbase's USDT books are very thin).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

import httpx
import numpy as np
import pandas as pd

from cryptointel.domain.models import Dataset, Interval, Market, utc_now
from cryptointel.domain.schemas import coerce, empty_frame
from cryptointel.providers.base import FetchResult, MarketDataProvider, OHLCVSource, ProviderError
from cryptointel.providers.http import JsonHttpClient

_NATIVE_GRANULARITY = {Interval.M15: 900, Interval.H1: 3600, Interval.D1: 86400}
_AGGREGATED = {Interval.H4: Interval.H1}  # target interval -> native interval it is built from


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _floor(dt: datetime, interval: Interval) -> datetime:
    ms = int(dt.timestamp() * 1000)
    return datetime.fromtimestamp((ms - ms % interval.ms) / 1000, tz=timezone.utc)


class CoinbaseProvider(MarketDataProvider, OHLCVSource):
    name = "coinbase"

    MAX_CANDLES = 300
    QUOTES = {"USD": "USD", "USDT": "USD", "USDC": "USD"}

    def __init__(
        self,
        client: httpx.Client | None = None,
        base_url: str = "https://api.exchange.coinbase.com",
        timeout: float = 15.0,
        request_pause_seconds: float = 0.0,
        max_attempts: int = 4,
        backoff_seconds: float = 1.0,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._clock = clock
        self._get = JsonHttpClient(
            self.name, client=client, timeout=timeout, request_pause_seconds=request_pause_seconds,
            max_attempts=max_attempts, backoff_seconds=backoff_seconds, clock=clock,
        ).get

    def supports(self, dataset: Dataset, market: Market, interval: Interval | None = None) -> bool:
        return (dataset is Dataset.OHLCV and market is Market.SPOT
                and (interval is None or interval in _NATIVE_GRANULARITY or interval in _AGGREGATED))

    def product_id(self, symbol: str) -> str:
        base, quote = symbol.split("-")
        if quote not in self.QUOTES:
            raise ProviderError(f"coinbase: unsupported quote currency in {symbol}")
        return f"{base}-{self.QUOTES[quote]}"

    def fetch_ohlcv(self, symbol, market, interval, start, end) -> FetchResult:
        if not self.supports(Dataset.OHLCV, market, interval):
            raise ProviderError(f"coinbase serves spot OHLCV at {sorted(i.value for i in [*_NATIVE_GRANULARITY, *_AGGREGATED])}")
        native = _AGGREGATED.get(interval, interval)
        fetch_start = _floor(start, interval)  # whole buckets only when aggregating
        pages, rows = [], []
        step = timedelta(seconds=_NATIVE_GRANULARITY[native])
        cursor = fetch_start
        while cursor <= end:
            window_end = min(cursor + step * (self.MAX_CANDLES - 1), end)
            page = self._get(f"{self._base}/products/{self.product_id(symbol)}/candles", {
                "granularity": _NATIVE_GRANULARITY[native], "start": _iso(cursor), "end": _iso(window_end),
            })
            pages.append(page)
            rows.extend(page.payload or [])
            cursor = window_end + step

        frame = self._to_frame(rows, symbol, native)
        if interval is not native:
            frame = self._aggregate(frame, interval, end)
        frame = frame.loc[frame["ts"] >= pd.Timestamp(start)].reset_index(drop=True)
        return FetchResult(Dataset.OHLCV, frame, pages)

    def _to_frame(self, rows, symbol: str, interval: Interval) -> pd.DataFrame:
        if not rows:
            return empty_frame(Dataset.OHLCV)
        raw = pd.DataFrame([r[:6] for r in rows], columns=["time", "low", "high", "open", "close", "volume"])
        num = lambda col: pd.to_numeric(raw[col], errors="coerce")  # noqa: E731
        frame = pd.DataFrame({
            "ts": pd.to_datetime(num("time"), unit="s", utc=True),
            "provider": self.name,
            "market": Market.SPOT.value,
            "symbol": symbol,
            "interval": interval.value,
            "open": num("open"),
            "high": num("high"),
            "low": num("low"),
            "close": num("close"),
            "volume": num("volume"),
            "quote_volume": np.nan,
            "trade_count": pd.NA,
            "taker_buy_volume": np.nan,
            "taker_buy_quote_volume": np.nan,
            "ingested_at": pd.Timestamp(self._clock()),
        })
        return coerce(frame.sort_values("ts").drop_duplicates("ts", keep="last"), Dataset.OHLCV)

    def _aggregate(self, frame: pd.DataFrame, interval: Interval, end: datetime) -> pd.DataFrame:
        """Build ``interval`` candles from finer ones; drop buckets that end after ``end``."""
        if frame.empty:
            return frame.assign(interval=interval.value)
        frame = frame.assign(bucket=frame["ts"].dt.floor(interval.timedelta))
        agg = frame.groupby("bucket", sort=True).agg(
            open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last"),
            volume=("volume", "sum"), ingested_at=("ingested_at", "max"),
        ).reset_index().rename(columns={"bucket": "ts"})
        agg = agg.loc[agg["ts"] + interval.timedelta <= pd.Timestamp(end)]
        out = agg.assign(
            provider=self.name, market=Market.SPOT.value, symbol=frame["symbol"].iat[0], interval=interval.value,
            quote_volume=np.nan, trade_count=pd.NA, taker_buy_volume=np.nan, taker_buy_quote_volume=np.nan,
        )
        return coerce(out, Dataset.OHLCV)
