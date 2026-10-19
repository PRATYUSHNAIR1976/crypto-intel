import pandas as pd
import pytest

from cryptointel.config import Settings
from cryptointel.domain.models import Dataset, Interval, normalize_symbol
from cryptointel.domain.schemas import SchemaError, coerce, empty_frame


@pytest.mark.parametrize("raw,expected", [("btc-usdt", "BTC-USDT"), ("ETH/USDT", "ETH-USDT"), (" sol_usdt ", "SOL-USDT")])
def test_normalize_symbol(raw, expected):
    assert normalize_symbol(raw) == expected


@pytest.mark.parametrize("bad", ["BTCUSDT", "BTC-", "A-B-C", ""])
def test_normalize_symbol_rejects_ambiguous(bad):
    with pytest.raises(ValueError):
        normalize_symbol(bad)


def test_interval_ms():
    assert Interval.H1.ms == 3_600_000
    assert Interval.D1.timedelta == pd.Timedelta(days=1)


def test_coerce_missing_column_raises():
    with pytest.raises(SchemaError, match="missing"):
        coerce(pd.DataFrame({"ts": []}), Dataset.FUNDING)


def test_empty_frame_has_canonical_dtypes():
    df = empty_frame(Dataset.OHLCV)
    assert str(df["ts"].dtype) == "datetime64[ns, UTC]"
    assert str(df["trade_count"].dtype) == "Int64"


def test_settings_parse_csv_env(monkeypatch):
    monkeypatch.setenv("CIP_SYMBOLS", "BTC-USDT, ETH-USDT")
    monkeypatch.setenv("CIP_INTERVALS", "1h")
    s = Settings(_env_file=None)
    assert s.symbols == ["BTC-USDT", "ETH-USDT"]
    assert s.intervals == ["1h"]
