from datetime import timedelta

import httpx
import pytest

from cryptointel.domain.models import Dataset, Interval, Market
from cryptointel.providers.base import ProviderError
from cryptointel.providers.coingecko import CoinGeckoProvider
from cryptointel.validation.validator import Severity, validate
from tests.conftest import NOW

H4_MS = Interval.H4.ms
NOW_MS = int(NOW.timestamp() * 1000)


def cg_rows(n: int, last_close_ms: int = NOW_MS) -> list[list]:
    """CoinGecko /ohlc wire format: [close_time_ms, open, high, low, close]."""
    return [[last_close_ms - (n - 1 - i) * H4_MS, 100.0 + i, 102.0 + i, 99.0 + i, 101.0 + i] for i in range(n)]


def make_provider(handler, **kwargs) -> CoinGeckoProvider:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return CoinGeckoProvider(client=client, backoff_seconds=0, clock=lambda: NOW, **kwargs)


def test_maps_close_time_to_open_time_and_nulls_volume():
    seen = {}

    def handler(request):
        seen["path"], seen["params"] = request.url.path, dict(request.url.params)
        return httpx.Response(200, json=cg_rows(6))

    result = make_provider(handler).fetch_ohlcv("BTC-USDT", Market.INDEX, Interval.H4, NOW - timedelta(days=5), NOW)

    assert seen["path"] == "/api/v3/coins/bitcoin/ohlc"
    assert seen["params"] == {"vs_currency": "usd", "days": "7"}
    frame = result.frame
    assert frame["ts"].iat[-1] == NOW - timedelta(hours=4)  # last close == NOW -> opened 4h earlier
    assert (frame["open"].iat[0], frame["close"].iat[0]) == (100.0, 101.0)
    assert frame["volume"].isna().all() and frame["trade_count"].isna().all()
    assert set(frame["market"]) == {"index"} and set(frame["provider"]) == {"coingecko"}


@pytest.mark.parametrize("lookback,expected_days", [
    (timedelta(hours=6), "7"), (timedelta(days=7), "7"), (timedelta(days=8), "14"),
    (timedelta(days=20), "30"), (timedelta(days=200), "30"),
])
def test_days_rounded_up_to_an_accepted_value_with_4h_candles(lookback, expected_days):
    seen = {}

    def handler(request):
        # Mirror the real API: any other `days` value is rejected (regression for HTTP 400 seen live).
        if request.url.params["days"] not in {"1", "7", "14", "30", "90", "180", "365", "max"}:
            return httpx.Response(400, json={"error": "Invalid days parameter"})
        seen.update(request.url.params)
        return httpx.Response(200, json=[])

    make_provider(handler).fetch_ohlcv("ETH-USDT", Market.INDEX, Interval.H4, NOW - lookback, NOW)
    assert seen["days"] == expected_days


def test_rows_outside_requested_window_are_dropped():
    provider = make_provider(lambda r: httpx.Response(200, json=cg_rows(18)))  # 3 days of candles
    frame = provider.fetch_ohlcv("SOL-USDT", Market.INDEX, Interval.H4, NOW - timedelta(hours=12), NOW).frame
    assert frame["ts"].min() >= NOW - timedelta(hours=12) and len(frame) == 3


def test_output_validates_with_only_volume_warning():
    frame = make_provider(lambda r: httpx.Response(200, json=cg_rows(6))).fetch_ohlcv(
        "BTC-USDT", Market.INDEX, Interval.H4, NOW - timedelta(days=3), NOW).frame
    result = validate(frame, Dataset.OHLCV, Interval.H4, now=NOW)
    assert not result.has_errors and len(result.clean) == 6
    assert {i.check for i in result.issues if i.severity is not Severity.INFO} == {"missing_volume"}


@pytest.mark.parametrize("market,interval", [(Market.SPOT, Interval.H4), (Market.INDEX, Interval.H1)])
def test_unsupported_requests_fail_fast(market, interval):
    provider = make_provider(lambda r: pytest.fail("must not call the API"))
    with pytest.raises(ProviderError):
        provider.fetch_ohlcv("BTC-USDT", market, interval, NOW - timedelta(days=3), NOW)


def test_unknown_coin_fails_fast():
    provider = make_provider(lambda r: pytest.fail("must not call the API"))
    with pytest.raises(ProviderError, match="no mapping"):
        provider.fetch_ohlcv("DOGE-USDT", Market.INDEX, Interval.H4, NOW - timedelta(days=3), NOW)


@pytest.mark.parametrize("api_key,expected", [("", None), ("demo-123", "demo-123")])
def test_demo_api_key_header_only_when_configured(api_key, expected):
    seen = {}

    def handler(request):
        seen["key"] = request.headers.get("x-cg-demo-api-key")
        return httpx.Response(200, json=[])

    make_provider(handler, api_key=api_key).fetch_ohlcv("BTC-USDT", Market.INDEX, Interval.H4, NOW - timedelta(days=3), NOW)
    assert seen["key"] == expected


def test_capabilities_are_ohlcv_only():
    p = make_provider(lambda r: httpx.Response(200, json=[]))
    assert p.capabilities() == {Dataset.OHLCV}
    assert not hasattr(p, "fetch_funding_rates") and not hasattr(p, "fetch_open_interest")
    assert not p.supports(Dataset.FUNDING, Market.PERP)
