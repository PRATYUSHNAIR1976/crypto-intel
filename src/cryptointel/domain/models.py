"""Provider-agnostic domain vocabulary: markets, datasets, intervals, symbols."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import StrEnum


class Market(StrEnum):
    SPOT = "spot"
    PERP = "perp"  # USD-margined perpetual futures


class Dataset(StrEnum):
    OHLCV = "ohlcv"
    FUNDING = "funding"
    OPEN_INTEREST = "open_interest"


_INTERVAL_MS = {
    "15m": 15 * 60_000,
    "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}


class Interval(StrEnum):
    # Only intervals that align to the Unix epoch in UTC (1w does not), so
    # timestamp-alignment checks stay exact.
    M15 = "15m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"

    @property
    def ms(self) -> int:
        return _INTERVAL_MS[self.value]

    @property
    def timedelta(self) -> timedelta:
        return timedelta(milliseconds=self.ms)


def normalize_symbol(symbol: str) -> str:
    """Return the canonical ``BASE-QUOTE`` form, e.g. ``btc/usdt`` -> ``BTC-USDT``."""
    canonical = symbol.strip().upper().replace("/", "-").replace("_", "-")
    parts = canonical.split("-")
    if len(parts) != 2 or not all(parts):
        raise ValueError(f"Symbol must look like BASE-QUOTE (e.g. BTC-USDT), got {symbol!r}")
    return canonical


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
