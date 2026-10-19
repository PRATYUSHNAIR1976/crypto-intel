"""Normalized, validated market data as Parquet — one file per series.

Layout: normalized/{dataset}/provider=../market=../symbol=../[interval=../]data.parquet

Data volumes are small (≈25k rows per 1h series over 3 years), so each series
is merged in memory and rewritten atomically. Swap for Delta Lake later
without touching callers.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from cryptointel.domain.models import Dataset
from cryptointel.domain.schemas import PRIMARY_KEYS, coerce, empty_frame


class ParquetStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def path_for(self, dataset: Dataset, provider: str, market: str, symbol: str, interval: str | None) -> Path:
        path = self.root / dataset.value / f"provider={provider}" / f"market={market}" / f"symbol={symbol}"
        if interval:
            path = path / f"interval={interval}"
        return path / "data.parquet"

    def read(self, dataset: Dataset, provider: str, market: str, symbol: str, interval: str | None = None) -> pd.DataFrame:
        path = self.path_for(dataset, provider, market, symbol, interval)
        if not path.exists():
            return empty_frame(dataset)
        return coerce(pd.read_parquet(path), dataset)

    def latest_ts(self, dataset: Dataset, provider: str, market: str, symbol: str,
                  interval: str | None = None) -> pd.Timestamp | None:
        df = self.read(dataset, provider, market, symbol, interval)
        return None if df.empty else df["ts"].max()

    def upsert(self, dataset: Dataset, df: pd.DataFrame) -> int:
        """Merge rows into their series files (last write wins per key). Returns rows newly added."""
        if df.empty:
            return 0
        df = coerce(df, dataset)
        keys = PRIMARY_KEYS[dataset]
        series_cols = ["provider", "market", "symbol"] + (["interval"] if "interval" in keys else [])
        added = 0
        for values, part in df.groupby(series_cols, sort=False):
            meta = dict(zip(series_cols, values))
            existing = self.read(dataset, meta["provider"], meta["market"], meta["symbol"], meta.get("interval"))
            merged = (
                pd.concat([existing, part], ignore_index=True)
                .drop_duplicates(subset=keys, keep="last")
                .sort_values(keys)
                .reset_index(drop=True)
            )
            added += len(merged) - len(existing)
            self._atomic_write(merged, self.path_for(dataset, meta["provider"], meta["market"],
                                                     meta["symbol"], meta.get("interval")))
        return added

    def list_series(self, dataset: Dataset) -> list[Path]:
        return sorted((self.root / dataset.value).glob("**/data.parquet"))

    @staticmethod
    def _atomic_write(df: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".parquet.tmp")
        df.to_parquet(tmp, index=False)
        os.replace(tmp, path)


class QuarantineStore:
    """Rows that failed ERROR-level checks, one file per run, with ``dq_reason``."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def write(self, dataset: Dataset, df: pd.DataFrame, run_id: str) -> Path | None:
        if df.empty:
            return None
        path = self.root / dataset.value / f"{run_id}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.assign(run_id=run_id).to_parquet(path, index=False)
        return path
