from datetime import timedelta

import numpy as np
import pandas as pd

from cryptointel.domain.models import Dataset, Interval
from cryptointel.domain.schemas import coerce
from cryptointel.validation.validator import Severity, validate
from tests.conftest import NOW, T0, ohlcv_frame


def checks(result):
    return {i.check: i for i in result.issues}


def test_clean_data_passes():
    result = validate(ohlcv_frame(), Dataset.OHLCV, Interval.H1, now=NOW)
    assert result.quality_score == 1.0
    assert not result.has_errors and result.issues == []
    assert len(result.clean) == 48


def test_bad_rows_are_quarantined_with_reasons():
    df = ohlcv_frame()
    df.loc[1, "close"] = np.nan
    df.loc[2, "low"] = -5.0
    df.loc[3, "high"] = df.loc[3, "low"] - 1  # high below low
    df.loc[4, "volume"] = -1.0
    df.loc[5, "taker_buy_volume"] = 999.0

    result = validate(df, Dataset.OHLCV, Interval.H1, now=NOW)
    found = checks(result)

    for name in ["null_close", "non_positive_price", "ohlc_inconsistent", "negative_volume", "taker_volume_exceeds_volume"]:
        assert found[name].severity is Severity.ERROR, name
    assert len(result.quarantined) == 5 and len(result.clean) == 43
    assert "non_positive_price" in result.quarantined.set_index(result.quarantined.index).loc[2, "dq_reason"]
    assert result.quality_score == round(43 / 48, 6)


def test_duplicates_keep_last_and_are_reported():
    df = ohlcv_frame(5)
    dup = df.iloc[[2]].copy()
    dup["close"] = 555.0
    dup["high"] = 556.0
    result = validate(pd.concat([df, dup]), Dataset.OHLCV, Interval.H1, now=NOW)
    assert checks(result)["duplicate_rows"].affected_rows == 1
    assert len(result.clean) == 5
    assert result.clean.loc[result.clean["ts"] == df["ts"].iat[2], "close"].iat[0] == 555.0


def test_incomplete_candle_dropped_not_quarantined():
    df = ohlcv_frame(3, start=NOW - timedelta(hours=2))  # last candle opens exactly at NOW, still open
    result = validate(df, Dataset.OHLCV, Interval.H1, now=NOW)
    assert checks(result)["incomplete_candle"].severity is Severity.INFO
    assert len(result.clean) == 2 and result.quarantined.empty and result.dropped_rows == 1


def test_gaps_are_reported_but_not_filled():
    df = ohlcv_frame(10).drop(index=[3, 4]).reset_index(drop=True)
    result = validate(df, Dataset.OHLCV, Interval.H1, now=NOW)
    gap = checks(result)["missing_intervals"]
    assert gap.severity is Severity.WARNING and gap.affected_rows == 2
    assert gap.sample[0] == (T0 + timedelta(hours=3)).isoformat()
    assert len(result.clean) == 8


def test_misaligned_and_future_timestamps():
    df = ohlcv_frame(3)
    df.loc[1, "ts"] = df.loc[1, "ts"] + pd.Timedelta(minutes=7)
    df.loc[2, "ts"] = pd.Timestamp(NOW + timedelta(days=2))
    result = validate(df, Dataset.OHLCV, Interval.H1, now=NOW, drop_incomplete=False)
    found = checks(result)
    assert "misaligned_timestamp" in found and "future_timestamp" in found
    assert len(result.clean) == 1


def test_price_outlier_flagged_and_kept():
    df = ohlcv_frame(60)
    df.loc[40, ["open", "high", "low", "close"]] = [100.0, 1000.0, 99.0, 900.0]
    result = validate(df, Dataset.OHLCV, Interval.H1, now=NOW)
    assert checks(result)["price_outlier"].severity is Severity.WARNING
    assert len(result.clean) == 60


def test_funding_extreme_rate_warning():
    df = coerce(pd.DataFrame({
        "ts": pd.date_range(T0, periods=3, freq="8h", tz="UTC"), "provider": "binance", "market": "perp",
        "symbol": "BTC-USDT", "funding_rate": [0.0001, 0.05, -0.0002], "mark_price": 1.0,
        "ingested_at": pd.Timestamp(NOW),
    }), Dataset.FUNDING)
    result = validate(df, Dataset.FUNDING, now=NOW)
    assert checks(result)["extreme_funding_rate"].affected_rows == 1
    assert len(result.clean) == 3


def test_open_interest_negative_is_error():
    df = coerce(pd.DataFrame({
        "ts": pd.date_range(T0, periods=3, freq="1h", tz="UTC"), "provider": "binance", "market": "perp",
        "symbol": "BTC-USDT", "interval": "1h", "open_interest": [1.0, -2.0, 3.0], "open_interest_value": 1.0,
        "ingested_at": pd.Timestamp(NOW),
    }), Dataset.OPEN_INTEREST)
    result = validate(df, Dataset.OPEN_INTEREST, Interval.H1, now=NOW)
    assert checks(result)["negative_open_interest"].severity is Severity.ERROR
    assert len(result.clean) == 2


def test_empty_frame_is_valid():
    result = validate(ohlcv_frame(0), Dataset.OHLCV, Interval.H1, now=NOW)
    assert result.quality_score == 1.0 and result.issues == []
