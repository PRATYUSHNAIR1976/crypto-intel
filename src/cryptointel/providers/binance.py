"""Binance public market-data adapter (free, no API key).

Endpoints used:
  spot klines        GET {spot}/api/v3/klines
  perp klines        GET {futures}/fapi/v1/klines
  funding history    GET {futures}/fapi/v1/fundingRate
  open interest hist GET {futures}/futures/data/openInterestHist   (last ~30 days only)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Callable

import httpx
import pandas as pd

from cryptointel.domain.models import Dataset, Interval, Market, utc_now
from cryptointel.domain.schemas import coerce, empty_frame
from cryptointel.providers.base import (
    FetchResult,
    FundingSource,
    MarketDataProvider,
    OHLCVSource,
    OpenInterestSource,
)
from cryptointel.providers.http import JsonHttpClient

logger = logging.getLogger(__name__)

_KLINE_FIELDS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume",
]


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class BinanceProvider(MarketDataProvider, OHLCVSource, FundingSource, OpenInterestSource):
    name = "binance"

    _MARKETS = {
        Dataset.OHLCV: {Market.SPOT, Market.PERP},
        Dataset.FUNDING: {Market.PERP},
        Dataset.OPEN_INTEREST: {Market.PERP},
    }

    KLINE_LIMIT = 1000
    FUNDING_LIMIT = 1000
    OI_LIMIT = 500
    # Binance only serves the most recent 30 days of OI history; stay inside it.
    OI_MAX_LOOKBACK = timedelta(days=29)

    def __init__(
        self,
        client: httpx.Client | None = None,
        spot_base_url: str = "https://api.binance.com",
        futures_base_url: str = "https://fapi.binance.com",
        timeout: float = 15.0,
        request_pause_seconds: float = 0.0,
        max_attempts: int = 4,
        backoff_seconds: float = 1.0,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._spot = spot_base_url.rstrip("/")
        self._futures = futures_base_url.rstrip("/")
        self._clock = clock
        self._get = JsonHttpClient(
            self.name, client=client, timeout=timeout, request_pause_seconds=request_pause_seconds,
            max_attempts=max_attempts, backoff_seconds=backoff_seconds, clock=clock,
            fail_fast_messages={
                418: "IP auto-banned for ignoring rate limits. Back off.",
                451: "unavailable from this region. Configure another provider.",
            },
        ).get

    # ------------------------------------------------------------ interface
    def supports(self, dataset: Dataset, market: Market, interval: Interval | None = None) -> bool:
        return market in self._MARKETS.get(dataset, set())

    @staticmethod
    def to_exchange_symbol(symbol: str) -> str:
        return symbol.replace("-", "")

    def fetch_ohlcv(self, symbol, market, interval, start, end) -> FetchResult:
        url = f"{self._spot}/api/v3/klines" if market is Market.SPOT else f"{self._futures}/fapi/v1/klines"
        pages, rows = [], []
        cursor, end_ms = _to_ms(start), _to_ms(end)
        while cursor <= end_ms:
            page = self._get(url, {
                "symbol": self.to_exchange_symbol(symbol), "interval": interval.value,
                "startTime": cursor, "endTime": end_ms, "limit": self.KLINE_LIMIT,
            })
            pages.append(page)
            data = page.payload
            if not data:
                break
            rows.extend(data)
            next_cursor = int(data[-1][0]) + interval.ms
            if len(data) < self.KLINE_LIMIT or next_cursor <= cursor:
                break
            cursor = next_cursor
        return FetchResult(Dataset.OHLCV, self._klines_to_frame(rows, symbol, market, interval), pages)

    def fetch_funding_rates(self, symbol, start, end) -> FetchResult:
        url = f"{self._futures}/fapi/v1/fundingRate"
        pages, rows = [], []
        cursor, end_ms = _to_ms(start), _to_ms(end)
        while cursor <= end_ms:
            page = self._get(url, {
                "symbol": self.to_exchange_symbol(symbol),
                "startTime": cursor, "endTime": end_ms, "limit": self.FUNDING_LIMIT,
            })
            pages.append(page)
            data = page.payload
            if not data:
                break
            rows.extend(data)
            next_cursor = int(data[-1]["fundingTime"]) + 1
            if len(data) < self.FUNDING_LIMIT or next_cursor <= cursor:
                break
            cursor = next_cursor
        return FetchResult(Dataset.FUNDING, self._funding_to_frame(rows, symbol), pages)

    def fetch_open_interest(self, symbol, interval, start, end) -> FetchResult:
        url = f"{self._futures}/futures/data/openInterestHist"
        earliest = self._clock() - self.OI_MAX_LOOKBACK
        if start < earliest:
            logger.info("Binance OI history is limited to ~30 days; clamping start %s -> %s", start, earliest)
            start = earliest
        # This endpoint returns the *latest* `limit` rows inside [startTime, endTime],
        # so a forward cursor would skip data. Walk fixed windows that each hold
        # at most `limit` periods instead.
        pages, rows = [], []
        cursor, end_ms = _to_ms(start), _to_ms(end)
        window_ms = (self.OI_LIMIT - 1) * interval.ms
        while cursor <= end_ms:
            window_end = min(cursor + window_ms, end_ms)
            page = self._get(url, {
                "symbol": self.to_exchange_symbol(symbol), "period": interval.value,
                "startTime": cursor, "endTime": window_end, "limit": self.OI_LIMIT,
            })
            pages.append(page)
            rows.extend(page.payload or [])
            cursor = window_end + 1
        return FetchResult(Dataset.OPEN_INTEREST, self._oi_to_frame(rows, symbol, interval), pages)

    # -------------------------------------------------------------- mapping
    # Numeric fields use errors="coerce": malformed values become NaN and are
    # then quarantined by validation instead of crashing ingestion.
    def _klines_to_frame(self, rows, symbol, market, interval) -> pd.DataFrame:
        if not rows:
            return empty_frame(Dataset.OHLCV)
        raw = pd.DataFrame([r[: len(_KLINE_FIELDS)] for r in rows], columns=_KLINE_FIELDS)
        num = lambda col: pd.to_numeric(raw[col], errors="coerce")  # noqa: E731
        frame = pd.DataFrame({
            "ts": pd.to_datetime(num("open_time"), unit="ms", utc=True),
            "provider": self.name,
            "market": market.value,
            "symbol": symbol,
            "interval": interval.value,
            "open": num("open"),
            "high": num("high"),
            "low": num("low"),
            "close": num("close"),
            "volume": num("volume"),
            "quote_volume": num("quote_volume"),
            "trade_count": num("trade_count").round().astype("Int64"),
            "taker_buy_volume": num("taker_buy_volume"),
            "taker_buy_quote_volume": num("taker_buy_quote_volume"),
            "ingested_at": pd.Timestamp(self._clock()),
        })
        return coerce(frame, Dataset.OHLCV)

    def _funding_to_frame(self, rows, symbol) -> pd.DataFrame:
        if not rows:
            return empty_frame(Dataset.FUNDING)
        raw = pd.DataFrame(rows)
        frame = pd.DataFrame({
            "ts": pd.to_datetime(pd.to_numeric(raw["fundingTime"], errors="coerce"), unit="ms", utc=True),
            "provider": self.name,
            "market": Market.PERP.value,
            "symbol": symbol,
            "funding_rate": pd.to_numeric(raw["fundingRate"], errors="coerce"),
            "mark_price": pd.to_numeric(raw.get("markPrice"), errors="coerce"),
            "ingested_at": pd.Timestamp(self._clock()),
        })
        return coerce(frame, Dataset.FUNDING)

    def _oi_to_frame(self, rows, symbol, interval) -> pd.DataFrame:
        if not rows:
            return empty_frame(Dataset.OPEN_INTEREST)
        raw = pd.DataFrame(rows)
        frame = pd.DataFrame({
            "ts": pd.to_datetime(pd.to_numeric(raw["timestamp"], errors="coerce"), unit="ms", utc=True),
            "provider": self.name,
            "market": Market.PERP.value,
            "symbol": symbol,
            "interval": interval.value,
            "open_interest": pd.to_numeric(raw["sumOpenInterest"], errors="coerce"),
            "open_interest_value": pd.to_numeric(raw["sumOpenInterestValue"], errors="coerce"),
            "ingested_at": pd.Timestamp(self._clock()),
        })
        return coerce(frame, Dataset.OPEN_INTEREST)
