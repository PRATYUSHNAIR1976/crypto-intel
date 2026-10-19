"""Application settings loaded from environment variables / .env (prefix ``CIP_``)."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated

from pydantic import field_validator
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

    binance_spot_base_url: str = "https://api.binance.com"
    binance_futures_base_url: str = "https://fapi.binance.com"

    http_timeout_seconds: float = 15.0
    request_pause_seconds: float = 0.15
    retry_max_attempts: int = 4

    log_level: str = "INFO"

    _csv = field_validator("symbols", "markets", "intervals", mode="before")(_split_csv)

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
