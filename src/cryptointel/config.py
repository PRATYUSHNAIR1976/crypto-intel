"""Application settings loaded from environment variables / .env (prefix ``CIP_``)."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def _split_csv(value):
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return value


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="CIP_", extra="ignore")

    data_dir: Path = Path("data")

    symbols: Annotated[list[str], NoDecode] = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
    markets: Annotated[list[str], NoDecode] = ["spot", "perp"]
    intervals: Annotated[list[str], NoDecode] = ["1h", "4h", "1d"]
    history_start: str = "2024-01-01"
    # OHLCV first; derivatives (funding, open_interest) are opt-in via --datasets.
    datasets: Annotated[list[str], NoDecode] = ["ohlcv"]

    # Read side: for each series, the first provider in this list that has data is used.
    # One source per series — rows from different providers are never mixed.
    source_priority: Annotated[list[str], NoDecode] = ["binance", "coinbase", "coingecko"]

    binance_spot_base_url: str = "https://api.binance.com"
    binance_futures_base_url: str = "https://fapi.binance.com"

    coinbase_base_url: str = "https://api.exchange.coinbase.com"

    coingecko_base_url: str = "https://api.coingecko.com/api/v3"
    coingecko_api_key: SecretStr = SecretStr("")  # optional free Demo key; raises rate limits
    coingecko_request_pause_seconds: float = 2.0

    http_timeout_seconds: float = 15.0
    request_pause_seconds: float = 0.15
    retry_max_attempts: int = 4

    log_level: str = "INFO"

    _csv = field_validator("symbols", "markets", "intervals", "datasets", "source_priority", mode="before")(_split_csv)

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def normalized_dir(self) -> Path:
        return self.data_dir / "normalized"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine"

    @property
    def catalog_path(self) -> Path:
        return self.data_dir / "catalog.sqlite"


@lru_cache
def get_settings() -> Settings:
    return Settings()
