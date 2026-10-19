from datetime import datetime, timedelta

import httpx
import pytest

from cryptointel.domain.models import Dataset, Interval, Market
from cryptointel.providers.base import ProviderError
from cryptointel.providers.coinbase import CoinbaseProvider
from cryptointel.validation.validator import Severity, validate
from tests.conftest import NOW, T0


def fake_exchange(n_hours: int, start: datetime = T0, calls: list | None = None):
    """Mimics Coinbase /candles: inclusive start/end, <=300 rows, NEWEST FIRST, [time, low, high, open, close, volume]."""
    t0 = int(start.timestamp())
    book = {t0 + i * 3600: [t0 + i * 3600, 99.0 + i, 102.0 + i, 100.0 + i, 101.0 + i, 1.0 + i] for i in range(n_hours)}

    def handler(request: httpx.Request) -> httpx.Response:
        p = request.url.params
        assert request.url.path.endswith("/candles") and p["granularity"] == "3600"
        lo = int(datetime.fromisoformat(p["start"].replace("Z", "+00:00")).timestamp())
        hi = int(datetime.fromisoformat(p["end"].replace("Z", "+00:00")).timestamp())
        if (hi - lo) // 3600 + 1 > 300:
            return httpx.Response(400, json={"message": "Count of aggregations requested exceeds 300"})
        if calls is not None:
            calls.append(request.url.path)
        rows = [row for t, row in sorted(book.items(), reverse=True) if lo <= t <= hi]
        return httpx.Response(200, json=rows)

    return handler


def make_provider(handler, **kwargs) -> CoinbaseProvider:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return CoinbaseProvider(client=client, backoff_seconds=0, clock=lambda: NOW, **kwargs)


def test_paginates_in_300_candle_windows_and_maps_column_order():
    calls = []
    provider = make_provider(fake_exchange(700, calls=calls))
    frame = provider.fetch_ohlcv("BTC-USDT", Market.SPOT, Interval.H1, T0, T0 + timedelta(hours=699)).frame

    assert len(calls) == 3 and calls[0] == "/products/BTC-USD/candles"  # USDT maps to the USD book
    assert len(frame) == 700 and frame["ts"].is_monotonic_increasing and frame["ts"].is_unique
    first = frame.iloc[0]
    assert first["ts"] == T0
    assert (first["open"], first["high"], first["low"], first["close"], first["volume"]) == (100.0, 102.0, 99.0, 101.0, 1.0)
    assert set(frame["market"]) == {"spot"} and set(frame["provider"]) == {"coinbase"}
    assert frame["quote_volume"].isna().all() and frame["taker_buy_volume"].isna().all()


def test_4h_is_aggregated_from_1h_with_only_complete_buckets():
    provider = make_provider(fake_exchange(12))
    start = T0 + timedelta(hours=1)  # mid-bucket start must still produce whole buckets
    end = T0 + timedelta(hours=10)  # bucket 08:00-12:00 is not complete by 10:00
    frame = provider.fetch_ohlcv("ETH-USDT", Market.SPOT, Interval.H4, start, end).frame

    assert list(frame["ts"]) == [T0 + timedelta(hours=4)]  # 00:00 bucket starts before `start`
    row = frame.iloc[0]  # hours 4..7
    assert (row["open"], row["high"], row["low"], row["close"]) == (104.0, 109.0, 103.0, 108.0)
    assert row["volume"] == 5.0 + 6.0 + 7.0 + 8.0
    assert row["interval"] == "4h"


def test_4h_bucket_counts_from_aligned_start():
    frame = make_provider(fake_exchange(12)).fetch_ohlcv(
        "SOL-USDT", Market.SPOT, Interval.H4, T0, T0 + timedelta(hours=12)).frame
    assert len(frame) == 3 and frame["ts"].iat[0] == T0 and frame["open"].iat[0] == 100.0


def test_output_validates_cleanly():
    frame = make_provider(fake_exchange(48)).fetch_ohlcv(
        "BTC-USDT", Market.SPOT, Interval.H1, T0, T0 + timedelta(hours=47)).frame
    result = validate(frame, Dataset.OHLCV, Interval.H1, now=NOW)
    assert not result.has_errors and len(result.clean) == 48
    assert [i.check for i in result.issues if i.severity is not Severity.INFO] == []


@pytest.mark.parametrize("market,interval", [(Market.PERP, Interval.H1), (Market.INDEX, Interval.H1)])
def test_unsupported_requests_fail_fast(market, interval):
    provider = make_provider(lambda r: pytest.fail("must not call the API"))
    with pytest.raises(ProviderError):
        provider.fetch_ohlcv("BTC-USDT", market, interval, T0, T0 + timedelta(hours=2))


def test_unsupported_quote_fails_fast():
    with pytest.raises(ProviderError, match="quote"):
        make_provider(lambda r: pytest.fail("no call")).fetch_ohlcv(
            "BTC-EUR", Market.SPOT, Interval.H1, T0, T0 + timedelta(hours=2))


def test_capabilities():
    p = make_provider(lambda r: httpx.Response(200, json=[]))
    assert p.capabilities() == {Dataset.OHLCV}
    assert all(p.supports(Dataset.OHLCV, Market.SPOT, i) for i in Interval)
    assert not p.supports(Dataset.FUNDING, Market.PERP)
