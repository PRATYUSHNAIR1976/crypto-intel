"""CoinGecko public API adapter — OHLCV only (free; optional free Demo key).

Free-tier behaviour (verified against the live API on 2026-10-19):
  * GET /coins/{id}/ohlc accepts only days in {1, 7, 14, 30, 90, 180, 365, max} (others -> HTTP 400);
    7, 14 and 30 return 4h candles (1 -> 30m, 90+ -> 4-day).
  * The keyless public tier allows only a few calls per minute (HTTP 429 beyond that).
  * Candle timestamps are the candle CLOSE time; we shift to open time (canonical ``ts``).
  * No per-candle volume: volume columns are null (validation reports ``missing_volume``).
  * Prices are a cross-exchange USD index, so series are stored as ``market='index'`` and
    never mixed with exchange data. USD-stablecoin quotes (USDT/USDC) map to ``usd``.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
from typing import Callable

import httpx
import numpy as np
import pandas as pd

from cryptointel.domain.models import Dataset, Interval, Market, utc_now
from cryptointel.domain.schemas import coerce, empty_frame
from cryptointel.providers.base import FetchResult, MarketDataProvider, OHLCVSource, ProviderError
from cryptointel.providers.http import JsonHttpClient

logger = logging.getLogger(__name__)


class CoinGeckoProvider(MarketDataProvider, OHLCVSource):
    name = "coingecko"

    COIN_IDS = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana"}
    VS_CURRENCIES = {"USD": "usd", "USDT": "usd", "USDC": "usd"}
    INTERVAL = Interval.H4
    DAYS_WITH_4H_CANDLES = (7, 14, 30)  # the only accepted `days` values that yield 4h candles

    def __init__(
        self,
        client: httpx.Client | None = None,
        base_url: str = "https://api.coingecko.com/api/v3",
        api_key: str = "",
        timeout: float = 15.0,
        request_pause_seconds: float = 0.0,
        max_attempts: int = 4,
        backoff_seconds: float = 2.0,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._clock = clock
        headers = {"accept": "application/json"}
        if api_key:
            headers["x-cg-demo-api-key"] = api_key
        self._get = JsonHttpClient(
            self.name, client=client, timeout=timeout, headers=headers,
            request_pause_seconds=request_pause_seconds, max_attempts=max_attempts,
            backoff_seconds=backoff_seconds, clock=clock,
        ).get

    def supports(self, dataset: Dataset, market: Market, interval: Interval | None = None) -> bool:
        return dataset is Dataset.OHLCV and market is Market.INDEX and interval in (None, self.INTERVAL)

    def _ids(self, symbol: str) -> tuple[str, str]:
        base, quote = symbol.split("-")
        if base not in self.COIN_IDS or quote not in self.VS_CURRENCIES:
            raise ProviderError(f"coingecko has no mapping for {symbol}; known bases {sorted(self.COIN_IDS)}")
        return self.COIN_IDS[base], self.VS_CURRENCIES[quote]

    def fetch_ohlcv(self, symbol, market, interval, start, end) -> FetchResult:
        if not self.supports(Dataset.OHLCV, market, interval):
            raise ProviderError(f"coingecko free tier serves OHLCV only for market=index interval={self.INTERVAL}")
        coin, vs = self._ids(symbol)
        lookback_days = math.ceil((self._clock() - start).total_seconds() / 86_400)
        longest = self.DAYS_WITH_4H_CANDLES[-1]
        if lookback_days > longest:
            logger.info("CoinGecko 4h candles cover the last %d days only; earlier data is skipped", longest)
        days = next((d for d in self.DAYS_WITH_4H_CANDLES if d >= lookback_days), longest)

        page = self._get(f"{self._base}/coins/{coin}/ohlc", {"vs_currency": vs, "days": days})
        frame = self._to_frame(page.payload or [], symbol, interval)
        in_window = (frame["ts"] >= pd.Timestamp(start)) & (frame["ts"] <= pd.Timestamp(end))
        return FetchResult(Dataset.OHLCV, frame.loc[in_window].reset_index(drop=True), [page])

    def _to_frame(self, rows, symbol: str, interval: Interval) -> pd.DataFrame:
        if not rows:
            return empty_frame(Dataset.OHLCV)
        raw = pd.DataFrame([r[:5] for r in rows], columns=["close_time", "open", "high", "low", "close"])
        num = lambda col: pd.to_numeric(raw[col], errors="coerce")  # noqa: E731
        frame = pd.DataFrame({
            "ts": pd.to_datetime(num("close_time"), unit="ms", utc=True) - interval.timedelta,
            "provider": self.name,
            "market": Market.INDEX.value,
            "symbol": symbol,
            "interval": interval.value,
            "open": num("open"),
            "high": num("high"),
            "low": num("low"),
            "close": num("close"),
            "volume": np.nan,
            "quote_volume": np.nan,
            "trade_count": pd.NA,
            "taker_buy_volume": np.nan,
            "taker_buy_quote_volume": np.nan,
            "ingested_at": pd.Timestamp(self._clock()),
        })
        return coerce(frame, Dataset.OHLCV)
