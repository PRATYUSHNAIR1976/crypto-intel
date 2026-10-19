"""``MarketDataReader`` implementation: resolves ONE source per series, strips provenance."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from cryptointel.domain.models import Dataset, Interval, Market
from cryptointel.domain.ports import MarketDataReader, empty_series, without_provenance
from cryptointel.storage.query import MarketDataRepository


class SourceResolvingReader(MarketDataReader):
    """For each requested series, picks the first provider in ``source_priority`` that has
    data for it and reads only that provider's rows. Rows from different providers are
    never interleaved, and callers never see which provider was used.
    """

    def __init__(self, repo: MarketDataRepository, source_priority: list[str]) -> None:
        self._repo = repo
        self._priority = list(source_priority)

    def resolve_source(self, dataset: Dataset, filters: dict[str, str]) -> str | None:
        available = self._repo.providers_for(dataset, filters)
        return next((name for name in self._priority if name in available), None)

    def candles(self, symbol: str, interval: Interval, market: Market = Market.SPOT,
                start: datetime | None = None, end: datetime | None = None, limit: int = 1000) -> pd.DataFrame:
        filters = {"symbol": symbol, "market": Market(market).value, "interval": Interval(interval).value}
        provider = self.resolve_source(Dataset.OHLCV, filters)
        if provider is None:
            return empty_series(Dataset.OHLCV)
        return without_provenance(self._repo.candles(
            symbol, filters["market"], filters["interval"], start, end, limit, provider=provider))

    def funding(self, symbol: str, start: datetime | None = None, end: datetime | None = None,
                limit: int = 1000) -> pd.DataFrame:
        provider = self.resolve_source(Dataset.FUNDING, {"symbol": symbol})
        if provider is None:
            return empty_series(Dataset.FUNDING)
        return without_provenance(self._repo.funding(symbol, start, end, limit, provider=provider))

    def open_interest(self, symbol: str, interval: Interval, start: datetime | None = None,
                      end: datetime | None = None, limit: int = 1000) -> pd.DataFrame:
        filters = {"symbol": symbol, "interval": Interval(interval).value}
        provider = self.resolve_source(Dataset.OPEN_INTEREST, filters)
        if provider is None:
            return empty_series(Dataset.OPEN_INTEREST)
        return without_provenance(self._repo.open_interest(
            symbol, filters["interval"], start, end, limit, provider=provider))
