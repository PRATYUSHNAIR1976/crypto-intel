"""Market-data provider interface.

Any free data source (Binance, Bybit, OKX, Coinbase, Kraken, CSV files...) is
added by implementing ``MarketDataProvider``. The rest of the platform depends
only on this interface and the canonical schemas in ``domain.schemas``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from cryptointel.domain.models import Dataset, Interval, Market


class ProviderError(RuntimeError):
    """Non-retryable provider failure (bad symbol, geo-block, ban, bad request)."""


class RetryableProviderError(RuntimeError):
    """Transient provider failure (timeout, 5xx, rate limit)."""


@dataclass
class RawPage:
    """One HTTP response exactly as received — kept for lineage and replay."""

    url: str
    params: dict[str, Any]
    status_code: int
    fetched_at: datetime
    payload: Any


@dataclass
class FetchResult:
    dataset: Dataset
    frame: pd.DataFrame  # canonical columns, NOT yet validated
    pages: list[RawPage] = field(default_factory=list)


class MarketDataProvider(ABC):
    name: str

    @abstractmethod
    def supports(self, dataset: Dataset, market: Market) -> bool: ...

    @abstractmethod
    def fetch_ohlcv(
        self, symbol: str, market: Market, interval: Interval, start: datetime, end: datetime
    ) -> FetchResult: ...

    @abstractmethod
    def fetch_funding_rates(self, symbol: str, start: datetime, end: datetime) -> FetchResult: ...

    @abstractmethod
    def fetch_open_interest(
        self, symbol: str, interval: Interval, start: datetime, end: datetime
    ) -> FetchResult: ...
