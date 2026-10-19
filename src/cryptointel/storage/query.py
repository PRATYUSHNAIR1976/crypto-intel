"""Read-side repository: DuckDB SQL over the normalized Parquet lake."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from cryptointel.domain.models import Dataset


class MarketDataRepository:
    def __init__(self, normalized_root: Path) -> None:
        self.root = Path(normalized_root)

    def _source(self, dataset: Dataset) -> str | None:
        if not any((self.root / dataset.value).glob("**/data.parquet")):
            return None
        glob = str(self.root / dataset.value / "**" / "data.parquet").replace("'", "''")
        return f"read_parquet('{glob}', union_by_name=true, hive_partitioning=false)"

    def query(self, sql: str, params: list | None = None) -> pd.DataFrame:
        # A fresh in-memory connection per call keeps this safe under FastAPI's threadpool.
        with duckdb.connect() as conn:
            conn.execute("SET TimeZone = 'UTC'")
            return conn.execute(sql, params or []).df()

    def _series(self, dataset: Dataset, filters: dict[str, str], start: datetime | None,
                end: datetime | None, limit: int) -> pd.DataFrame:
        source = self._source(dataset)
        if source is None:
            return pd.DataFrame()
        clauses = [f"{col} = ?" for col in filters]
        params: list = list(filters.values())
        if start is not None:
            clauses.append("ts >= ?"); params.append(start)
        if end is not None:
            clauses.append("ts < ?"); params.append(end)
        params.append(limit)
        sql = (f"SELECT * FROM (SELECT * FROM {source} WHERE {' AND '.join(clauses)} "
               f"ORDER BY ts DESC LIMIT ?) ORDER BY ts")
        return self.query(sql, params)

    def providers_for(self, dataset: Dataset, filters: dict[str, str]) -> set[str]:
        """Providers that hold data for the series described by ``filters``."""
        source = self._source(dataset)
        if source is None:
            return set()
        where = " AND ".join(f"{col} = ?" for col in filters)
        df = self.query(f"SELECT DISTINCT provider FROM {source} WHERE {where}", list(filters.values()))
        return set(df["provider"])

    @staticmethod
    def _with_provider(filters: dict[str, str], provider: str | None) -> dict[str, str]:
        return {**filters, "provider": provider} if provider else filters

    def candles(self, symbol: str, market: str, interval: str, start: datetime | None = None,
                end: datetime | None = None, limit: int = 1000, provider: str | None = None) -> pd.DataFrame:
        filters = {"symbol": symbol, "market": market, "interval": interval}
        return self._series(Dataset.OHLCV, self._with_provider(filters, provider), start, end, limit)

    def funding(self, symbol: str, start: datetime | None = None, end: datetime | None = None,
                limit: int = 1000, provider: str | None = None) -> pd.DataFrame:
        return self._series(Dataset.FUNDING, self._with_provider({"symbol": symbol}, provider), start, end, limit)

    def open_interest(self, symbol: str, interval: str, start: datetime | None = None,
                      end: datetime | None = None, limit: int = 1000, provider: str | None = None) -> pd.DataFrame:
        filters = {"symbol": symbol, "interval": interval}
        return self._series(Dataset.OPEN_INTEREST, self._with_provider(filters, provider), start, end, limit)

    def summary(self) -> pd.DataFrame:
        parts = []
        for dataset in Dataset:
            source = self._source(dataset)
            if source is None:
                continue
            interval = "interval" if dataset is not Dataset.FUNDING else "NULL::VARCHAR AS interval"
            group = "provider, market, symbol" + (", interval" if dataset is not Dataset.FUNDING else "")
            parts.append(
                f"SELECT '{dataset.value}' AS dataset, provider, market, symbol, {interval}, "
                f"count(*) AS rows, min(ts) AS first_ts, max(ts) AS last_ts FROM {source} GROUP BY {group}"
            )
        if not parts:
            return pd.DataFrame(columns=["dataset", "provider", "market", "symbol", "interval", "rows", "first_ts", "last_ts"])
        return self.query(" UNION ALL ".join(parts) + " ORDER BY dataset, symbol, market, interval")
