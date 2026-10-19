"""Builds configured provider instances by name. The only place that knows every provider."""

from __future__ import annotations

from cryptointel.config import Settings
from cryptointel.providers.base import MarketDataProvider
from cryptointel.providers.binance import BinanceProvider
from cryptointel.providers.coingecko import CoinGeckoProvider

PROVIDER_NAMES = ("binance", "coingecko")


def build_provider(name: str, settings: Settings) -> MarketDataProvider:
    if name == "binance":
        return BinanceProvider(
            spot_base_url=settings.binance_spot_base_url, futures_base_url=settings.binance_futures_base_url,
            timeout=settings.http_timeout_seconds, request_pause_seconds=settings.request_pause_seconds,
            max_attempts=settings.retry_max_attempts,
        )
    if name == "coingecko":
        return CoinGeckoProvider(
            base_url=settings.coingecko_base_url, api_key=settings.coingecko_api_key.get_secret_value(),
            timeout=settings.http_timeout_seconds, request_pause_seconds=settings.coingecko_request_pause_seconds,
            max_attempts=settings.retry_max_attempts,
        )
    raise ValueError(f"Unknown provider {name!r}; available: {', '.join(PROVIDER_NAMES)}")
