"""Every data-quality rule, its configuration, and the never-repair guarantees."""

from datetime import timedelta

import numpy as np
import pandas as pd
import pytest

from cryptointel.domain.models import Dataset, Interval
from cryptointel.domain.schemas import SCHEMAS, coerce
from cryptointel.validation.report import REPORT_COLUMNS, report_frame, report_rows
from cryptointel.validation.rules import load_dq_config
from cryptointel.validation.validator import CHECKS, Status, validate
from tests.conftest import NOW, T0, ohlcv_frame


def run(df, dataset=Dataset.OHLCV, interval=Interval.H1, **kw):
    return validate(df, dataset, interval, now=NOW, **kw)


def config(tmp_path, toml: str):
    path = tmp_path / "rules.toml"
    path.write_text(toml)
    return load_dq_config(path)


def candle(df, i, o, h, lo, c):
    df.loc[i, ["open", "high", "low", "close"]] = [o, h, lo, c]
    return df


# --------------------------------------------------------------------------- baseline
def test_clean_data_passes_every_applicable_check():
    result = run(ohlcv_frame())
    expected = {name for name, datasets in CHECKS.items() if Dataset.OHLCV in datasets}
    assert {c.check_name for c in result.checks} == expected
    assert all(c.status is Status.PASS for c in result.checks)
    assert len(result.validated) == 48 and result.rejected.empty and result.quality_score == 1.0


def test_validation_is_deterministic():
    df = candle(ohlcv_frame(), 3, 100, 99, 101, 100)
    a, b = run(df.copy()), run(df.copy())
    assert [c.to_dict() for c in a.checks] == [c.to_dict() for c in b.checks]
    pd.testing.assert_frame_equal(a.validated, b.validated)
    pd.testing.assert_frame_equal(a.rejected, b.rejected)


def test_nothing_is_repaired_or_lost():
    df = ohlcv_frame(24)
    df.loc[2, "close"] = np.nan
    df.loc[5, "volume"] = -3.0
    df = pd.concat([df, df.iloc[[7]]], ignore_index=True)  # duplicate
    result = run(df)
    assert len(result.validated) + len(result.rejected) == len(df)
    # rejected rows carry their original values untouched
    assert result.rejected["volume"].min() == -3.0 and result.rejected["close"].isna().sum() == 1
    # validated rows are byte-identical to their input rows
    merged = result.validated.merge(df, on=list(df.columns), how="left", indicator=True)
    assert (merged["_merge"] == "both").all()


# --------------------------------------------------------------------------- 8. null values
def test_null_values_rejected():
    df = ohlcv_frame(10)
    df.loc[3, "close"] = np.nan
    df.loc[4, "open"] = np.nan
    result = run(df)
    check = result.check("null_values")
    assert (check.status, check.failed_rows, check.total_rows) == (Status.FAIL, 2, 10)
    assert "close" in check.message and "open" in check.message
    assert set(result.rejected["dq_reason"]) == {"null_values"}


def test_null_timestamp_rejected():
    df = ohlcv_frame(5)
    df["ts"] = df["ts"].astype("object")
    df.loc[1, "ts"] = pd.NaT
    result = run(coerce(df, Dataset.OHLCV))
    assert result.check("null_values").failed_rows == 1 and len(result.validated) == 4


# --------------------------------------------------------------------------- 2. duplicate timestamps
def test_duplicate_timestamps_earlier_copy_rejected_last_kept():
    df = ohlcv_frame(5)
    newer = df.iloc[[2]].copy()
    newer[["close", "high"]] = [555.0, 556.0]
    result = run(pd.concat([df, newer], ignore_index=True))
    check = result.check("duplicate_timestamps")
    assert (check.status, check.failed_rows) == (Status.WARN, 1)
    assert len(result.validated) == 5 and 555.0 in result.validated["close"].values
    assert list(result.rejected["dq_reason"]) == ["duplicate_timestamps"]
    assert result.rejected["close"].iat[0] != 555.0  # the superseded copy is kept as a rejected record


# --------------------------------------------------------------------------- 3. OHLC relationships
@pytest.mark.parametrize("name,ohlc", [
    ("high_below_open", (100.4, 100.2, 99.0, 100.1)),
    ("high_below_close", (100.0, 100.2, 99.0, 100.4)),
    ("low_above_open", (99.5, 101.0, 99.8, 100.0)),
    ("low_above_close", (100.0, 101.0, 99.8, 99.5)),
])
def test_each_ohlc_rule_fires_alone(name, ohlc):
    result = run(candle(ohlcv_frame(10), 4, *ohlc))
    failing = {c.check_name for c in result.checks if c.status is Status.FAIL}
    assert failing == {name}
    assert list(result.rejected["dq_reason"]) == [name] and len(result.validated) == 9


def test_high_below_low():
    result = run(candle(ohlcv_frame(10), 4, 100.0, 99.0, 99.5, 99.2))
    assert result.check("high_below_low").status is Status.FAIL
    assert "high_below_low" in result.rejected["dq_reason"].iat[0]


# --------------------------------------------------------------------------- 4. negative / zero prices
def test_negative_price():
    result = run(candle(ohlcv_frame(10), 2, -1.0, 101.0, -2.0, 100.0))
    assert result.check("negative_price").status is Status.FAIL
    assert "negative_price" in result.rejected["dq_reason"].iat[0]


def test_zero_price():
    result = run(candle(ohlcv_frame(10), 2, 0.0, 101.0, 0.0, 100.0))
    assert result.check("zero_price").failed_rows == 1 and len(result.validated) == 9


# --------------------------------------------------------------------------- 5. negative volume
@pytest.mark.parametrize("col", ["volume", "quote_volume", "taker_buy_volume", "taker_buy_quote_volume"])
def test_negative_volume(col):
    df = ohlcv_frame(10)
    df.loc[6, col] = -0.5
    result = run(df)
    assert result.check("negative_volume").failed_rows == 1
    # a negative total volume is also exceeded by taker volume; both reasons are recorded
    assert "negative_volume" in result.rejected["dq_reason"].iat[0] and len(result.validated) == 9


def test_taker_volume_exceeds_volume():
    df = ohlcv_frame(10)
    df.loc[1, "taker_buy_volume"] = 999.0
    assert run(df).check("taker_volume_exceeds_volume").status is Status.FAIL


# --------------------------------------------------------------------------- 1. missing timestamps
def test_missing_timestamps_reported_not_filled():
    df = ohlcv_frame(10).drop(index=[3, 4]).reset_index(drop=True)
    result = run(df)
    check = result.check("missing_timestamps")
    assert (check.status, check.failed_rows, check.total_rows) == (Status.WARN, 2, 10)
    assert check.sample[0] == (T0 + timedelta(hours=3)).isoformat()
    assert len(result.validated) == 8 and result.rejected.empty


# --------------------------------------------------------------------------- 6. unexpected gaps
def test_single_missing_candle_is_not_an_unexpected_gap_by_default():
    result = run(ohlcv_frame(10).drop(index=[5]).reset_index(drop=True))
    assert result.check("missing_timestamps").status is Status.WARN
    assert result.check("timestamp_gaps").status is Status.PASS


def test_long_gap_is_unexpected():
    result = run(ohlcv_frame(20).drop(index=[2, 10, 11, 12]).reset_index(drop=True))
    gap = result.check("timestamp_gaps")
    assert (gap.status, gap.failed_rows) == (Status.WARN, 3)
    assert gap.sample == [(T0 + timedelta(hours=10)).isoformat()] and "largest 3" in gap.message


def test_gap_tolerance_is_configurable(tmp_path):
    cfg = config(tmp_path, "[params]\nmax_gap_intervals = 5\n")
    result = run(ohlcv_frame(20).drop(index=[10, 11, 12]).reset_index(drop=True), config=cfg)
    assert result.check("timestamp_gaps").status is Status.PASS


def test_misaligned_and_future_timestamps():
    df = ohlcv_frame(3)
    df.loc[1, "ts"] = df.loc[1, "ts"] + pd.Timedelta(minutes=7)
    df.loc[2, "ts"] = pd.Timestamp(NOW + timedelta(days=2))
    result = run(df, check_incomplete=False)
    assert result.check("misaligned_timestamp").failed_rows == 1
    assert result.check("future_timestamp").failed_rows == 1
    assert len(result.validated) == 1


def test_incomplete_candle_is_excluded_as_info_not_failure():
    df = ohlcv_frame(3, start=NOW - timedelta(hours=2))  # last candle opens at NOW
    result = run(df)
    check = result.check("incomplete_candle")
    assert (check.status, check.failed_rows) == (Status.INFO, 1)
    assert list(result.rejected["dq_reason"]) == ["incomplete_candle"]
    assert not result.has_errors and result.quality_score == 1.0 and result.excluded_rows == 1


# --------------------------------------------------------------------------- 7. extreme price jumps
def test_extreme_price_jump_flagged_and_kept():
    df = ohlcv_frame(30)
    df.loc[20:, ["open", "high", "low", "close"]] *= 1.2  # +20% gap up at candle 20 (1h limit 15%)
    result = run(df)
    check = result.check("extreme_price_jump")
    assert (check.status, check.failed_rows) == (Status.WARN, 1)
    assert check.sample == [(T0 + timedelta(hours=20)).isoformat()]
    assert len(result.validated) == 30


def test_moderate_move_is_not_a_jump():
    df = ohlcv_frame(30)
    df.loc[20:, ["open", "high", "low", "close"]] *= 1.10
    assert run(df).check("extreme_price_jump").status is Status.PASS


def test_jump_threshold_is_per_timeframe():
    df = ohlcv_frame(30, interval=Interval.D1)
    df.loc[20:, ["open", "high", "low", "close"]] *= 1.2  # +20% is normal-ish for a daily candle
    assert run(df, interval=Interval.D1).check("extreme_price_jump").status is Status.PASS


def test_jump_severity_error_rejects(tmp_path):
    cfg = config(tmp_path, '[checks.extreme_price_jump]\nseverity = "error"\n')
    df = ohlcv_frame(30)
    df.loc[20:, ["open", "high", "low", "close"]] *= 1.2
    result = run(df, config=cfg)
    assert result.check("extreme_price_jump").status is Status.FAIL
    assert list(result.rejected["dq_reason"]) == ["extreme_price_jump"]


def test_jump_ignores_rows_already_rejected():
    df = ohlcv_frame(30)
    df.loc[10, "close"] = np.nan  # rejected for nulls; must not create a fake jump next to it
    assert run(df).check("extreme_price_jump").status is Status.PASS


# --------------------------------------------------------------------------- volume presence
def test_zero_and_missing_volume_are_warnings():
    df = ohlcv_frame(10)
    df.loc[1, ["volume", "taker_buy_volume"]] = 0.0
    df.loc[2, "volume"] = np.nan
    result = run(df)
    assert result.check("zero_volume").failed_rows == 1 and result.check("missing_volume").failed_rows == 1
    assert len(result.validated) == 10


# --------------------------------------------------------------------------- other datasets
def funding_frame(rates, mark=1.0):
    return coerce(pd.DataFrame({
        "ts": pd.date_range(T0, periods=len(rates), freq="8h", tz="UTC"), "provider": "binance", "market": "perp",
        "symbol": "BTC-USDT", "funding_rate": rates, "mark_price": mark, "ingested_at": pd.Timestamp(NOW),
    }), Dataset.FUNDING)


def test_funding_checks():
    result = run(funding_frame([0.0001, 0.05, None, -0.0002], mark=[1.0, 1.0, 1.0, -5.0]),
                 dataset=Dataset.FUNDING, interval=None)
    assert result.check("extreme_funding_rate").status is Status.WARN
    assert result.check("null_values").failed_rows == 1
    assert result.check("negative_price").failed_rows == 1
    assert {c.check_name for c in result.checks}.isdisjoint({"high_below_low", "missing_timestamps"})
    assert len(result.validated) == 2


def test_open_interest_checks():
    df = coerce(pd.DataFrame({
        "ts": pd.date_range(T0, periods=5, freq="1h", tz="UTC"), "provider": "binance", "market": "perp",
        "symbol": "BTC-USDT", "interval": "1h", "open_interest": [1.0, -2.0, 3.0, 4.0, 5.0],
        "open_interest_value": 1.0, "ingested_at": pd.Timestamp(NOW),
    }), Dataset.OPEN_INTEREST).drop(index=[3])
    result = run(df, dataset=Dataset.OPEN_INTEREST)
    assert result.check("negative_volume").status is Status.FAIL
    assert result.check("missing_timestamps").failed_rows == 2  # row 3 dropped, row 1 rejected


# --------------------------------------------------------------------------- configurability
def test_disabled_check_is_not_run_and_rejects_nothing(tmp_path):
    cfg = config(tmp_path, "[checks.negative_volume]\nenabled = false\n")
    df = ohlcv_frame(10)
    df.loc[1, "quote_volume"] = -1.0
    result = run(df, config=cfg)
    assert "negative_volume" not in {c.check_name for c in result.checks}
    assert len(result.validated) == 10


def test_downgraded_severity_keeps_rows(tmp_path):
    cfg = config(tmp_path, '[checks.null_values]\nseverity = "warning"\n')
    df = ohlcv_frame(10)
    df.loc[1, "close"] = np.nan
    result = run(df, config=cfg)
    assert result.check("null_values").status is Status.WARN and len(result.validated) == 10


# --------------------------------------------------------------------------- DQ report
def test_report_has_required_columns_and_one_row_per_check():
    df = ohlcv_frame(10)
    df.loc[3, "close"] = np.nan
    result = run(df)
    rows = report_rows(result, run_timestamp=NOW, scope="ingest", run_id="r1", dataset="ohlcv",
                       provider="binance", market="spot", symbol="BTC-USDT", timeframe="1h")
    frame = report_frame(rows)
    required = ["dataset", "symbol", "timeframe", "check_name", "status", "failed_rows", "total_rows", "run_timestamp"]
    assert set(required) <= set(frame.columns) and list(frame.columns) == REPORT_COLUMNS
    assert len(frame) == len(result.checks)
    null_row = frame.set_index("check_name").loc["null_values"]
    assert (null_row["status"], null_row["failed_rows"], null_row["total_rows"]) == ("fail", 1, 10)
    assert set(frame["status"]) <= {"pass", "warn", "fail", "info"}
    assert frame["run_timestamp"].nunique() == 1


def test_rejected_frame_keeps_canonical_columns_plus_reason():
    df = ohlcv_frame(5)
    df.loc[0, "close"] = np.nan
    rejected = run(df).rejected
    assert list(rejected.columns) == [*SCHEMAS[Dataset.OHLCV], "dq_reason"]


def test_empty_frame_is_valid():
    result = run(ohlcv_frame(0))
    assert result.quality_score == 1.0 and not result.issues
