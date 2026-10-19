"""Market-data provider interfaces.

A provider is an identity (``MarketDataProvider``) plus the capabilities it
really has. Each capability is a separate interface, so a provider implements
only what its source offers:

    MarketDataProvider
      ├── OHLCVSource          fetch_ohlcv
      ├── FundingSource        fetch_funding_rates
      └── OpenInterestSource   fetch_open_interest

    BinanceProvider   = OHLCV + Funding + OpenInterest
    CoinGeckoProvider = OHLCV

Providers map their payloads into the canonical schemas in ``domain.schemas``.
Nothing downstream of ingestion imports this package.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from cryptointel.domain.models import Dataset, Interval, Market


class ProviderError(RuntimeError):
    """Non-retryable provider failure (bad symbol, geo-block, ban, bad request, unsupported request)."""


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
    def supports(self, dataset: Dataset, market: Market, interval: Interval | None = None) -> bool:
        """Whether this provider can serve the series. False for any dataset it has no capability for."""

    def capabilities(self) -> set[Dataset]:
        return {dataset for dataset, cap in CAPABILITIES.items() if isinstance(self, cap)}


class OHLCVSource(ABC):
    @abstractmethod
    def fetch_ohlcv(
        self, symbol: str, market: Market, interval: Interval, start: datetime, end: datetime
    ) -> FetchResult: ...


class FundingSource(ABC):
    @abstractmethod
    def fetch_funding_rates(self, symbol: str, start: datetime, end: datetime) -> FetchResult: ...


class OpenInterestSource(ABC):
    @abstractmethod
    def fetch_open_interest(
        self, symbol: str, interval: Interval, start: datetime, end: datetime
    ) -> FetchResult: ...


CAPABILITIES: dict[Dataset, type] = {
    Dataset.OHLCV: OHLCVSource,
    Dataset.FUNDING: FundingSource,
    Dataset.OPEN_INTEREST: OpenInterestSource,
}
