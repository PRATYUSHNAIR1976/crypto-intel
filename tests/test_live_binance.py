"""Contract tests against the real Binance public API. Run with: pytest -m live"""

from datetime import timedelta

import pytest

from cryptointel.domain.models import Dataset, Interval, Market, utc_now
from cryptointel.providers.binance import BinanceProvider
from cryptointel.validation.validator import validate

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def provider():
    return BinanceProvider()


@pytest.mark.parametrize("market", [Market.SPOT, Market.PERP])
def test_live_ohlcv_contract(provider, market):
    end = utc_now()
    result = provider.fetch_ohlcv("BTC-USDT", market, Interval.H1, end - timedelta(hours=12), end)
    assert len(result.frame) >= 11
    assert not validate(result.frame, Dataset.OHLCV, Interval.H1).has_errors


def test_live_funding_contract(provider):
    end = utc_now()
    frame = provider.fetch_funding_rates("BTC-USDT", end - timedelta(days=3), end).frame
    assert len(frame) >= 3 and frame["funding_rate"].abs().max() < 0.03


def test_live_coingecko_ohlc_contract():
    from cryptointel.providers.coingecko import CoinGeckoProvider

    end = utc_now()
    frame = CoinGeckoProvider().fetch_ohlcv("BTC-USDT", Market.INDEX, Interval.H4, end - timedelta(days=5), end).frame
    assert len(frame) >= 25
    assert (frame["ts"].astype("int64") // 1_000_000 % Interval.H4.ms == 0).all()
    assert not validate(frame, Dataset.OHLCV, Interval.H4).has_errors


@pytest.mark.parametrize("interval", [Interval.H1, Interval.H4])
def test_live_coinbase_ohlcv_contract(interval):
    from cryptointel.providers.coinbase import CoinbaseProvider

    end = utc_now()
    frame = CoinbaseProvider().fetch_ohlcv("BTC-USDT", Market.SPOT, interval, end - timedelta(days=2), end).frame
    assert len(frame) >= 48 // (interval.ms // Interval.H1.ms) - 2
    assert (frame["volume"] > 0).all()
    assert not validate(frame, Dataset.OHLCV, interval).has_errors


def test_live_open_interest_contract(provider):
    end = utc_now()
    frame = provider.fetch_open_interest("BTC-USDT", Interval.H1, end - timedelta(hours=6), end).frame
    assert len(frame) >= 5 and (frame["open_interest"] > 0).all()
