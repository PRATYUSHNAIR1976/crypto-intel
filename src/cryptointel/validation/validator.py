"""Deterministic, configurable market-data quality checks.

A batch passes through three stages:
  1. Row checks on the input (nulls, duplicates, timestamps, OHLC relationships, signs).
     Rows failing an ``error`` check are REJECTED. Earlier copies of a duplicated key are
     always rejected (the last copy is kept). Still-open candles are excluded.
  2. Value checks on the surviving rows (price jumps, zero/missing volume, extreme funding).
  3. Series checks on the validated rows (missing timestamps, unexpected gaps); report only.

Every enabled, applicable check produces a ``CheckResult``, including passes, so the DQ
report is complete. Nothing is repaired: rejected rows keep their reasons, gaps are not filled.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum

import numpy as np
import pandas as pd

from cryptointel.domain.models import Dataset, Interval, utc_now
from cryptointel.domain.schemas import PRIMARY_KEYS, REQUIRED_NON_NULL, coerce
from cryptointel.validation.rules import DQConfig, Severity, default_dq_config

__all__ = ["CHECKS", "CheckResult", "Severity", "Status", "ValidationResult", "validate"]

SAMPLE_SIZE = 5
_ALL = frozenset(Dataset)
_OHLCV = frozenset({Dataset.OHLCV})
_GRID = frozenset({Dataset.OHLCV, Dataset.OPEN_INTEREST})  # datasets on a fixed interval grid

# check name -> datasets it applies to
CHECKS: dict[str, frozenset[Dataset]] = {
    "null_values": _ALL,
    "duplicate_timestamps": _ALL,
    "future_timestamp": _ALL,
    "misaligned_timestamp": _GRID,
    "incomplete_candle": _OHLCV,
    "high_below_low": _OHLCV,
    "high_below_open": _OHLCV,
    "high_below_close": _OHLCV,
    "low_above_open": _OHLCV,
    "low_above_close": _OHLCV,
    "negative_price": frozenset({Dataset.OHLCV, Dataset.FUNDING}),
    "zero_price": _OHLCV,
    "negative_volume": _GRID,
    "taker_volume_exceeds_volume": _OHLCV,
    "extreme_price_jump": _OHLCV,
    "zero_volume": _OHLCV,
    "missing_volume": _OHLCV,
    "extreme_funding_rate": frozenset({Dataset.FUNDING}),
    "missing_timestamps": _GRID,
    "timestamp_gaps": _GRID,
}
_NEEDS_INTERVAL = {"misaligned_timestamp", "incomplete_candle", "extreme_price_jump", "missing_timestamps", "timestamp_gaps"}

_PRICE_COLS = {Dataset.OHLCV: ["open", "high", "low", "close"], Dataset.FUNDING: ["mark_price"]}
_VOLUME_COLS = {
    Dataset.OHLCV: ["volume", "quote_volume", "taker_buy_volume", "taker_buy_quote_volume"],
    Dataset.OPEN_INTEREST: ["open_interest", "open_interest_value"],
}


class Status(StrEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    INFO = "info"


_STATUS_FOR = {Severity.ERROR: Status.FAIL, Severity.WARNING: Status.WARN, Severity.INFO: Status.INFO}


@dataclass
class CheckResult:
    check_name: str
    severity: Severity
    status: Status
    failed_rows: int
    total_rows: int
    message: str = ""
    sample: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {**asdict(self), "severity": self.severity.value, "status": self.status.value}


@dataclass
class ValidationResult:
    validated: pd.DataFrame
    rejected: pd.DataFrame  # canonical columns + dq_reason (";"-separated check names)
    checks: list[CheckResult]
    input_rows: int
    excluded_rows: int  # rows held back for non-quality reasons (still-open candles)

    @property
    def issues(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status is not Status.PASS]

    @property
    def has_errors(self) -> bool:
        return any(c.status is Status.FAIL for c in self.checks)

    @property
    def quality_score(self) -> float:
        """Share of assessable input rows that were validated (1.0 = perfect)."""
        assessable = self.input_rows - self.excluded_rows
        return 1.0 if assessable <= 0 else round(len(self.validated) / assessable, 6)

    def check(self, name: str) -> CheckResult:
        return next(c for c in self.checks if c.check_name == name)


def _sample(ts: pd.Series | pd.Index) -> list[str]:
    return [t.isoformat() for t in list(ts)[:SAMPLE_SIZE]]


class _Run:
    """Accumulates check results and per-row rejection reasons for one batch."""

    def __init__(self, df: pd.DataFrame, dataset: Dataset, interval: Interval | None, config: DQConfig) -> None:
        self.df, self.dataset, self.interval, self.config = df, dataset, interval, config
        self.reasons = pd.Series("", index=df.index, dtype="object")
        self.checks: list[CheckResult] = []

    def active(self, name: str) -> bool:
        if not self.config.enabled(name) or self.dataset not in CHECKS[name]:
            return False
        return self.interval is not None or name not in _NEEDS_INTERVAL

    def record(self, name: str, failed: int, total: int, message: str, sample: list[str]) -> Severity:
        severity = self.config.rule(name).severity
        status = _STATUS_FOR[severity] if failed else Status.PASS
        self.checks.append(CheckResult(name, severity, status, failed, total, message if failed else "", sample))
        return severity

    def row_check(self, name: str, mask: pd.Series, message: str, scope: pd.Series, reject: bool | None = None) -> None:
        """Evaluate ``mask`` over rows in ``scope``; reject failures if severity is error (or ``reject``)."""
        if not self.active(name):
            return
        mask = mask.reindex(self.df.index, fill_value=False).fillna(False).astype(bool) & scope
        failed = int(mask.sum())
        severity = self.record(name, failed, int(scope.sum()), message, _sample(self.df.loc[mask, "ts"]))
        if failed and (reject if reject is not None else severity is Severity.ERROR):
            self.reasons.loc[mask] = self.reasons.loc[mask] + name + ";"

    def surviving(self) -> pd.Series:
        return self.reasons == ""


def validate(
    df: pd.DataFrame,
    dataset: Dataset,
    interval: Interval | None = None,
    now: datetime | None = None,
    config: DQConfig | None = None,
    check_incomplete: bool = True,
) -> ValidationResult:
    config = config or default_dq_config()
    now = pd.Timestamp(now or utc_now())
    df = coerce(df, dataset).reset_index(drop=True)
    run = _Run(df, dataset, interval, config)
    everyone = pd.Series(True, index=df.index)
    keys = PRIMARY_KEYS[dataset]

    # ---- stage 1: row checks ------------------------------------------------------
    duplicate = df.duplicated(subset=keys, keep="last")
    run.row_check("duplicate_timestamps", duplicate, "Duplicate primary key; earlier copies rejected, last kept.",
                  everyone, reject=True)
    unique = ~duplicate

    required = REQUIRED_NON_NULL[dataset]
    nulls = df[required].isna()
    null_cols = {c: int(n) for c, n in nulls[unique].sum().items() if n}
    run.row_check("null_values", nulls.any(axis=1), f"Null or unparseable required fields: {null_cols}.", unique)

    run.row_check("future_timestamp", df["ts"] > now + config.future_tolerance, "Timestamp is in the future.", unique)
    if interval is not None:
        epoch_ms = df["ts"].astype("int64") // 1_000_000
        run.row_check("misaligned_timestamp", epoch_ms % interval.ms != 0,
                      f"Timestamp not aligned to the {interval} grid.", unique)
        if check_incomplete:
            run.row_check("incomplete_candle", df["ts"] + interval.timedelta > now,
                          "Candle has not closed yet; excluded until it closes.", unique, reject=True)

    if dataset is Dataset.OHLCV:
        o, h, lo, c = df["open"], df["high"], df["low"], df["close"]
        run.row_check("high_below_low", h < lo, "high < low.", unique)
        run.row_check("high_below_open", h < o, "high < open.", unique)
        run.row_check("high_below_close", h < c, "high < close.", unique)
        run.row_check("low_above_open", lo > o, "low > open.", unique)
        run.row_check("low_above_close", lo > c, "low > close.", unique)
        run.row_check("zero_price", (df[_PRICE_COLS[dataset]] == 0).any(axis=1), "A price is exactly zero.", unique)
        run.row_check("taker_volume_exceeds_volume", df["taker_buy_volume"] > df["volume"] * (1 + 1e-9),
                      "Taker-buy volume exceeds total volume.", unique)
    if dataset in _PRICE_COLS:
        run.row_check("negative_price", (df[_PRICE_COLS[dataset]] < 0).any(axis=1), "A price is negative.", unique)
    if dataset in _VOLUME_COLS:
        run.row_check("negative_volume", (df[_VOLUME_COLS[dataset]] < 0).any(axis=1), "A volume/size is negative.",
                      unique)

    # ---- stage 2: value checks on survivors -------------------------------------
    survivors = run.surviving()
    if dataset is Dataset.OHLCV:
        if interval is not None and run.active("extreme_price_jump"):
            threshold = config.max_abs_return.get(interval.value)
            if threshold is not None:
                ret = _returns(df.loc[survivors], dataset)
                run.row_check("extreme_price_jump", ret.abs() > threshold,
                              f"|close-to-close return| > {threshold:.0%} for {interval}.", survivors)
        run.row_check("zero_volume", df["volume"] == 0, "Volume is zero (halt or maintenance?).", survivors)
        run.row_check("missing_volume", df["volume"].isna(),
                      "Source provides no volume (volume-based features unavailable).", survivors)
    if dataset is Dataset.FUNDING:
        limit = config.extreme_funding_rate
        run.row_check("extreme_funding_rate", df["funding_rate"].abs() > limit,
                      f"|funding rate| > {limit:.2%} per period.", survivors)

    # ---- split ------------------------------------------------------------------
    survivors = run.surviving()
    validated = df.loc[survivors].sort_values(keys).reset_index(drop=True)
    rejected = df.loc[~survivors].assign(dq_reason=run.reasons[~survivors].str.rstrip(";"))
    excluded = int(rejected["dq_reason"].eq("incomplete_candle").sum()) if len(rejected) else 0

    # ---- stage 3: series checks on validated rows -------------------------------
    if interval is not None and dataset in _GRID:
        _series_checks(run, validated, interval, config)

    return ValidationResult(validated, rejected, run.checks, len(df), excluded)


def _returns(frame: pd.DataFrame, dataset: Dataset) -> pd.Series:
    """Close-to-close return within each series (first row of a series has none)."""
    series_keys = [k for k in PRIMARY_KEYS[dataset] if k != "ts"]
    ordered = frame.sort_values(series_keys + ["ts"])
    return ordered.groupby(series_keys, sort=False)["close"].pct_change()


def _series_checks(run: _Run, validated: pd.DataFrame, interval: Interval, config: DQConfig) -> None:
    missing_all: list[pd.DatetimeIndex] = []
    expected_total = 0
    series_keys = [k for k in PRIMARY_KEYS[run.dataset] if k != "ts"]
    for _, series in (validated.groupby(series_keys, sort=False) if len(validated) else []):
        expected = pd.date_range(series["ts"].min(), series["ts"].max(), freq=interval.timedelta)
        expected_total += len(expected)
        missing_all.append(expected.difference(pd.DatetimeIndex(series["ts"])))
    missing = missing_all[0].append(missing_all[1:]) if missing_all else pd.DatetimeIndex([], tz="UTC")

    if run.active("missing_timestamps"):
        run.record("missing_timestamps", len(missing), expected_total,
                   f"{len(missing)} expected {interval} timestamps are missing (not filled).", _sample(missing))

    if run.active("timestamp_gaps"):
        runs = _gap_runs(missing, interval)
        long_runs = [(start, n) for start, n in runs if n > config.max_gap_intervals]
        failed = sum(n for _, n in long_runs)
        largest = max(long_runs, key=lambda r: r[1]) if long_runs else None
        message = (f"{len(long_runs)} gap(s) longer than {config.max_gap_intervals} x {interval}; "
                   f"largest {largest[1]} x {interval} from {largest[0].isoformat()}.") if largest else ""
        run.record("timestamp_gaps", failed, expected_total, message, [s.isoformat() for s, _ in long_runs[:SAMPLE_SIZE]])


def _gap_runs(missing: pd.DatetimeIndex, interval: Interval) -> list[tuple[pd.Timestamp, int]]:
    """Group missing timestamps into runs of consecutive intervals: [(run_start, length)]."""
    if missing.empty:
        return []
    ordered = missing.sort_values()
    breaks = np.flatnonzero(np.diff(ordered.asi8) != interval.ms * 1_000_000) + 1
    starts, ends = np.r_[0, breaks], np.r_[breaks, len(ordered)]
    return [(ordered[s], int(e - s)) for s, e in zip(starts, ends)]
