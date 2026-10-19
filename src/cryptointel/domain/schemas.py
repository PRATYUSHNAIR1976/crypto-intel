"""Canonical (normalized) table schemas shared by every provider.

Providers map their payloads into these columns; validation, storage and the
query layer only ever see these schemas.
"""

from __future__ import annotations

import pandas as pd

from cryptointel.domain.models import Dataset

TS = "datetime64[ns, UTC]"

SCHEMAS: dict[Dataset, dict[str, str]] = {
    Dataset.OHLCV: {
        "ts": TS,  # candle open time
        "provider": "string",
        "market": "string",
        "symbol": "string",
        "interval": "string",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",  # base-asset volume
        "quote_volume": "float64",
        "trade_count": "Int64",
        "taker_buy_volume": "float64",  # base volume from aggressive buyers (input for CVD later)
        "taker_buy_quote_volume": "float64",
        "ingested_at": TS,
    },
    Dataset.FUNDING: {
        "ts": TS,  # funding settlement time
        "provider": "string",
        "market": "string",
        "symbol": "string",
        "funding_rate": "float64",  # per funding period, as a fraction (0.0001 = 0.01%)
        "mark_price": "float64",
        "ingested_at": TS,
    },
    Dataset.OPEN_INTEREST: {
        "ts": TS,
        "provider": "string",
        "market": "string",
        "symbol": "string",
        "interval": "string",
        "open_interest": "float64",  # contracts, in base asset
        "open_interest_value": "float64",  # notional, in quote asset
        "ingested_at": TS,
    },
}

PRIMARY_KEYS: dict[Dataset, list[str]] = {
    Dataset.OHLCV: ["provider", "market", "symbol", "interval", "ts"],
    Dataset.FUNDING: ["provider", "market", "symbol", "ts"],
    Dataset.OPEN_INTEREST: ["provider", "market", "symbol", "interval", "ts"],
}

REQUIRED_NON_NULL: dict[Dataset, list[str]] = {
    Dataset.OHLCV: ["ts", "open", "high", "low", "close", "volume"],
    Dataset.FUNDING: ["ts", "funding_rate"],
    Dataset.OPEN_INTEREST: ["ts", "open_interest"],
}


class SchemaError(ValueError):
    """Raised when a frame cannot be coerced to its canonical schema."""


def empty_frame(dataset: Dataset) -> pd.DataFrame:
    return pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in SCHEMAS[dataset].items()})


def coerce(df: pd.DataFrame, dataset: Dataset) -> pd.DataFrame:
    """Select canonical columns in canonical order with canonical dtypes."""
    schema = SCHEMAS[dataset]
    missing = [col for col in schema if col not in df.columns]
    if missing:
        raise SchemaError(f"{dataset} frame is missing columns: {missing}")
    try:
        return df[list(schema)].astype(schema)
    except (TypeError, ValueError) as exc:
        raise SchemaError(f"{dataset} frame has incompatible dtypes: {exc}") from exc
