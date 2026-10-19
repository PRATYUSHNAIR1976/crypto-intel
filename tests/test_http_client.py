"""Rate-limit awareness and error handling of the shared HTTP client."""

from datetime import datetime, timezone

import httpx
import pytest

from cryptointel.providers.base import ProviderError, RetryableProviderError
from cryptointel.providers.binance import BinanceProvider
from cryptointel.providers.http import JsonHttpClient

AT_20S = datetime(2025, 1, 1, 0, 0, 20, tzinfo=timezone.utc)


def make_client(handler, sleeps, **kwargs) -> JsonHttpClient:
    return JsonHttpClient(
        "test", client=httpx.Client(transport=httpx.MockTransport(handler)), backoff_seconds=0,
        clock=lambda: AT_20S, sleep=sleeps.append, **kwargs,
    )


def weight_handler(used: int):
    return lambda r: httpx.Response(200, json=[], headers={"x-mbx-used-weight-1m": str(used)})


def test_pauses_until_next_minute_when_weight_near_limit():
    sleeps = []
    client = make_client(weight_handler(2000), sleeps, weight_header="x-mbx-used-weight-1m", weight_limit=2400)
    client.get("https://x/api", {})
    assert sleeps == [pytest.approx(40.5)]  # 20s into the minute -> wait for reset


def test_no_pause_below_threshold_or_without_header():
    sleeps = []
    make_client(weight_handler(100), sleeps, weight_header="x-mbx-used-weight-1m", weight_limit=2400).get("https://x", {})
    make_client(lambda r: httpx.Response(200, json=[]), sleeps, weight_header="x-mbx-used-weight-1m",
                weight_limit=2400).get("https://x", {})
    assert sleeps == []


def test_polite_pause_between_requests():
    sleeps = []
    make_client(lambda r: httpx.Response(200, json=[]), sleeps, request_pause_seconds=0.15).get("https://x", {})
    assert sleeps == [0.15]


def test_429_honours_retry_after_then_succeeds():
    sleeps, calls = [], {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(429, headers={"Retry-After": "7"}) if calls["n"] == 1 else httpx.Response(200, json=[1])

    assert make_client(handler, sleeps).get("https://x", {}).payload == [1]
    assert sleeps[0] == 7.0


def test_transport_errors_are_retried_then_raised():
    def handler(request):
        raise httpx.ConnectTimeout("timed out")

    with pytest.raises(RetryableProviderError):
        make_client(handler, [], max_attempts=3).get("https://x", {})


def test_fail_fast_message_used():
    client = make_client(lambda r: httpx.Response(418), [], fail_fast_messages={418: "banned"})
    with pytest.raises(ProviderError, match="banned"):
        client.get("https://x", {})


def test_binance_is_configured_for_weight_header():
    sleeps = []
    provider = BinanceProvider(client=httpx.Client(transport=httpx.MockTransport(weight_handler(2300))),
                               backoff_seconds=0, clock=lambda: AT_20S)
    provider._http._sleep = sleeps.append  # observe throttling without sleeping
    provider._get("https://api.binance.com/api/v3/klines", {})
    assert len(sleeps) == 1
