"""The read port must hide data provenance and never mix providers within a series."""

import pandas as pd

from cryptointel.domain.models import Dataset, Interval, Market
from cryptointel.domain.ports import PROVENANCE_COLUMNS
from cryptointel.storage.parquet_store import ParquetStore
from cryptointel.storage.query import MarketDataRepository
from cryptointel.storage.reader import SourceResolvingReader
from tests.conftest import ohlcv_frame


def store_two_sources(tmp_path) -> MarketDataRepository:
    """The same series held by two providers, with different prices."""
    store = ParquetStore(tmp_path)
    store.upsert(Dataset.OHLCV, ohlcv_frame(24, base=100.0))
    store.upsert(Dataset.OHLCV, ohlcv_frame(24, base=200.0).assign(provider="coingecko"))
    return MarketDataRepository(tmp_path)


def test_priority_picks_one_source_and_never_mixes(tmp_path):
    repo = store_two_sources(tmp_path)
    assert len(repo.candles("BTC-USDT", "spot", "1h")) == 48  # raw repo sees both sources

    from_binance = SourceResolvingReader(repo, ["binance", "coingecko"]).candles("BTC-USDT", Interval.H1)
    from_gecko = SourceResolvingReader(repo, ["coingecko", "binance"]).candles("BTC-USDT", Interval.H1)

    assert len(from_binance) == len(from_gecko) == 24
    assert from_binance["close"].iat[0] == 100.0 and from_gecko["close"].iat[0] == 200.0


def test_output_hides_provenance_and_is_schema_identical_across_sources(tmp_path):
    repo = store_two_sources(tmp_path)
    a = SourceResolvingReader(repo, ["binance"]).candles("BTC-USDT", Interval.H1)
    b = SourceResolvingReader(repo, ["coingecko"]).candles("BTC-USDT", Interval.H1)
    for frame in (a, b):
        assert not set(PROVENANCE_COLUMNS) & set(frame.columns)
    assert list(a.columns) == list(b.columns)
    assert a.dtypes.equals(b.dtypes)


def test_falls_back_to_next_source_with_data(tmp_path):
    store = ParquetStore(tmp_path)
    store.upsert(Dataset.OHLCV, ohlcv_frame(6, market="index").assign(provider="coingecko"))
    reader = SourceResolvingReader(MarketDataRepository(tmp_path), ["binance", "coingecko"])
    assert len(reader.candles("BTC-USDT", Interval.H1, Market.INDEX)) == 6
    assert reader.resolve_source(Dataset.OHLCV, {"symbol": "BTC-USDT", "market": "index", "interval": "1h"}) == "coingecko"


def test_unlisted_or_missing_source_returns_empty_canonical_frame(tmp_path):
    repo = store_two_sources(tmp_path)
    empty = SourceResolvingReader(repo, ["kraken"]).candles("BTC-USDT", Interval.H1)
    full = SourceResolvingReader(repo, ["binance"]).candles("BTC-USDT", Interval.H1)
    assert empty.empty and list(empty.columns) == list(full.columns)
    assert SourceResolvingReader(MarketDataRepository(tmp_path / "nothing"), ["binance"]).funding("BTC-USDT").empty


def test_strategy_style_consumer_is_indifferent_to_source(tmp_path):
    """A consumer written against the port gets the same shape of answer whichever source is configured."""
    repo = store_two_sources(tmp_path)

    def sma_last(reader, window=5) -> pd.Series:
        df = reader.candles("BTC-USDT", Interval.H1)
        return df.set_index("ts")["close"].rolling(window).mean().dropna()

    a = sma_last(SourceResolvingReader(repo, ["binance"]))
    b = sma_last(SourceResolvingReader(repo, ["coingecko"]))
    assert a.index.equals(b.index) and (b - a).round(9).eq(100.0).all()
