from datetime import timedelta

import httpx
import pytest

from cryptointel.domain.models import Interval, Market
from cryptointel.providers.base import ProviderError, RetryableProviderError
from cryptointel.providers.binance import BinanceProvider
from tests.conftest import NOW, T0, T0_MS, binance_klines


def make_provider(handler, **kwargs) -> BinanceProvider:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return BinanceProvider(client=client, backoff_seconds=0, clock=lambda: NOW, **kwargs)


def test_ohlcv_paginates_and_maps_to_canonical_schema():
    calls = []
    all_rows = binance_klines(T0_MS, 1500, Interval.H1.ms)

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(dict(request.url.params))
        assert request.url.path == "/api/v3/klines"
        start = int(request.url.params["startTime"])
        page = [r for r in all_rows if r[0] >= start][:1000]
        return httpx.Response(200, json=page)

    result = make_provider(handler).fetch_ohlcv("BTC-USDT", Market.SPOT, Interval.H1, T0, T0 + timedelta(days=90))

    assert len(calls) == 2
    assert calls[0]["symbol"] == "BTCUSDT"
    assert int(calls[1]["startTime"]) == T0_MS + 1000 * Interval.H1.ms
    assert len(result.frame) == 1500 and len(result.pages) == 2
    first = result.frame.iloc[0]
    assert first["ts"].isoformat() == "2025-01-01T00:00:00+00:00"
    assert (first["open"], first["high"], first["low"], first["close"]) == (100.0, 102.0, 99.0, 101.0)
    assert first["taker_buy_volume"] == 6.0 and first["trade_count"] == 42
    assert set(result.frame["market"]) == {"spot"} and set(result.frame["symbol"]) == {"BTC-USDT"}


def test_perp_ohlcv_uses_futures_endpoint():
    def handler(request):
        assert request.url.host == "fapi.binance.com" and request.url.path == "/fapi/v1/klines"
        return httpx.Response(200, json=binance_klines(T0_MS, 3, Interval.H1.ms))

    result = make_provider(handler).fetch_ohlcv("ETH-USDT", Market.PERP, Interval.H1, T0, T0 + timedelta(hours=3))
    assert len(result.frame) == 3 and set(result.frame["market"]) == {"perp"}


def test_malformed_numbers_become_nan_not_crash():
    rows = binance_klines(T0_MS, 2, Interval.H1.ms)
    rows[1][4] = "not-a-number"
    provider = make_provider(lambda r: httpx.Response(200, json=rows))
    frame = provider.fetch_ohlcv("BTC-USDT", Market.SPOT, Interval.H1, T0, T0 + timedelta(hours=2)).frame
    assert frame["close"].isna().sum() == 1


def test_funding_mapping_handles_blank_mark_price():
    payload = [
        {"symbol": "BTCUSDT", "fundingTime": T0_MS, "fundingRate": "0.00010000", "markPrice": "95000.1"},
        {"symbol": "BTCUSDT", "fundingTime": T0_MS + 8 * 3_600_000, "fundingRate": "-0.00005", "markPrice": ""},
    ]
    provider = make_provider(lambda r: httpx.Response(200, json=payload))
    frame = provider.fetch_funding_rates("BTC-USDT", T0, T0 + timedelta(days=1)).frame
    assert list(frame["funding_rate"]) == [0.0001, -0.00005]
    assert frame["mark_price"].isna().tolist() == [False, True]
    assert set(frame["market"]) == {"perp"}


def test_open_interest_start_is_clamped_to_30_day_window():
    seen = {}

    def handler(request):
        seen.update(request.url.params)
        assert request.url.path == "/futures/data/openInterestHist"
        ts = int(seen["startTime"])
        return httpx.Response(200, json=[{"symbol": "BTCUSDT", "sumOpenInterest": "100.5",
                                          "sumOpenInterestValue": "9500000", "timestamp": ts}])

    result = make_provider(handler).fetch_open_interest("BTC-USDT", Interval.H1, T0, NOW)
    assert int(seen["startTime"]) >= int((NOW - timedelta(days=30)).timestamp() * 1000)
    assert seen["period"] == "1h"
    assert result.frame["open_interest"].iat[0] == 100.5


def test_open_interest_walks_windows_when_api_returns_latest_rows():
    """Regression: Binance returns the LATEST `limit` rows in [start, end]; we must not lose older rows."""
    start = NOW - timedelta(days=29)
    start_ms = int(start.timestamp() * 1000) // Interval.H1.ms * Interval.H1.ms + Interval.H1.ms
    server_rows = [{"symbol": "BTCUSDT", "sumOpenInterest": "1", "sumOpenInterestValue": "1", "timestamp": t}
                   for t in range(start_ms, int(NOW.timestamp() * 1000), Interval.H1.ms)]

    def handler(request):
        lo, hi, limit = (int(request.url.params[k]) for k in ("startTime", "endTime", "limit"))
        in_range = [r for r in server_rows if lo <= r["timestamp"] <= hi]
        return httpx.Response(200, json=in_range[-limit:])  # latest rows, like the real API

    frame = make_provider(handler).fetch_open_interest("BTC-USDT", Interval.H1, start, NOW).frame
    assert len(frame) == len(server_rows) > BinanceProvider.OI_LIMIT
    assert frame["ts"].is_unique


def test_rate_limit_is_retried_then_succeeds():
    attempts = {"n": 0}

    def handler(request):
        attempts["n"] += 1
        if attempts["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json=[])

    make_provider(handler).fetch_ohlcv("BTC-USDT", Market.SPOT, Interval.H1, T0, T0 + timedelta(hours=1))
    assert attempts["n"] == 2


def test_server_errors_exhaust_retries():
    provider = make_provider(lambda r: httpx.Response(503), max_attempts=2)
    with pytest.raises(RetryableProviderError):
        provider.fetch_ohlcv("BTC-USDT", Market.SPOT, Interval.H1, T0, T0 + timedelta(hours=1))


@pytest.mark.parametrize("status", [400, 418, 451])
def test_client_errors_fail_fast(status):
    attempts = {"n": 0}

    def handler(request):
        attempts["n"] += 1
        return httpx.Response(status, json={"code": -1121, "msg": "Invalid symbol."})

    with pytest.raises(ProviderError):
        make_provider(handler).fetch_ohlcv("XXX-USDT", Market.SPOT, Interval.H1, T0, T0 + timedelta(hours=1))
    assert attempts["n"] == 1


def test_supports_matrix():
    from cryptointel.domain.models import Dataset

    p = make_provider(lambda r: httpx.Response(200, json=[]))
    assert p.capabilities() == {Dataset.OHLCV, Dataset.FUNDING, Dataset.OPEN_INTEREST}
    assert p.supports(Dataset.OHLCV, Market.SPOT)
    assert p.supports(Dataset.FUNDING, Market.PERP)
    assert not p.supports(Dataset.FUNDING, Market.SPOT)
    assert not p.supports(Dataset.OHLCV, Market.INDEX)
