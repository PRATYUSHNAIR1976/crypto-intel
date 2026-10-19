"""Read port for everything downstream of storage (features, regime, strategy, scanner, risk).

This is the ONLY market-data interface those layers may depend on. Frames contain
canonical columns without provenance (``provider``, ``ingested_at``), so analytics
cannot know, or branch on, where the data came from. Enforced by tests/test_architecture.py.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

import pandas as pd

from cryptointel.domain.models import Dataset, Interval, Market
from cryptointel.domain.schemas import empty_frame

PROVENANCE_COLUMNS = ("provider", "ingested_at")


def without_provenance(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=list(PROVENANCE_COLUMNS), errors="ignore")


def empty_series(dataset: Dataset) -> pd.DataFrame:
    return without_provenance(empty_frame(dataset))


class MarketDataReader(Protocol):
    def candles(self, symbol: str, interval: Interval, market: Market = Market.SPOT,
                start: datetime | None = None, end: datetime | None = None, limit: int = 1000) -> pd.DataFrame: ...

    def funding(self, symbol: str, start: datetime | None = None, end: datetime | None = None,
                limit: int = 1000) -> pd.DataFrame: ...

    def open_interest(self, symbol: str, interval: Interval, start: datetime | None = None,
                      end: datetime | None = None, limit: int = 1000) -> pd.DataFrame: ...
