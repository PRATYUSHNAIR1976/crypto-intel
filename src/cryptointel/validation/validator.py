"""Provider-agnostic data-quality validation and cleaning.

Severity policy:
  ERROR   -> row is quarantined (kept on disk with a reason, never used downstream)
  WARNING -> row is kept; issue is reported (gaps, outliers, duplicates resolved)
  INFO    -> expected housekeeping (e.g. still-open candle dropped)

Validation never invents data: gaps are reported, not filled.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum

import numpy as np
import pandas as pd

from cryptointel.domain.models import Dataset, Interval, utc_now
from cryptointel.domain.schemas import PRIMARY_KEYS, REQUIRED_NON_NULL, coerce

SAMPLE_SIZE = 5
OUTLIER_ROBUST_Z = 12.0  # deliberately loose: crypto has fat tails; flag only egregious moves
OUTLIER_MIN_ROWS = 30
EXTREME_FUNDING_RATE = 0.03  # 3% per funding period
FUTURE_TOLERANCE = timedelta(minutes=1)


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class QualityIssue:
    check: str
    severity: Severity
    affected_rows: int
    message: str
    sample: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {**asdict(self), "severity": self.severity.value}


@dataclass
class ValidationResult:
    clean: pd.DataFrame
    quarantined: pd.DataFrame  # canonical columns + dq_reason
    issues: list[QualityIssue]
    input_rows: int
    dropped_rows: int  # duplicates + incomplete candles (not quarantined, not errors)

    @property
    def has_errors(self) -> bool:
        return any(i.severity is Severity.ERROR for i in self.issues)

    @property
    def quality_score(self) -> float:
        """Share of usable input rows that passed all ERROR checks (1.0 = perfect)."""
        usable = self.input_rows - self.dropped_rows
        return 1.0 if usable <= 0 else round(len(self.clean) / usable, 6)


def _sample(ts: pd.Series) -> list[str]:
    return [t.isoformat() for t in ts.head(SAMPLE_SIZE)]


def _series_keys(dataset: Dataset) -> list[str]:
    return [k for k in PRIMARY_KEYS[dataset] if k != "ts"]


def validate(
    df: pd.DataFrame,
    dataset: Dataset,
    interval: Interval | None = None,
    now: datetime | None = None,
    drop_incomplete: bool = True,
) -> ValidationResult:
    now = now or utc_now()
    df = coerce(df, dataset).reset_index(drop=True)
    input_rows = len(df)
    issues: list[QualityIssue] = []
    dropped = 0

    # --- duplicates: keep the most recently fetched version of each key
    keys = PRIMARY_KEYS[dataset]
    dup_mask = df.duplicated(subset=keys, keep="last")
    if dup_mask.any():
        issues.append(QualityIssue("duplicate_rows", Severity.WARNING, int(dup_mask.sum()),
                                   "Duplicate primary keys; kept the last occurrence.", _sample(df.loc[dup_mask, "ts"])))
        dropped += int(dup_mask.sum())
        df = df.loc[~dup_mask].reset_index(drop=True)

    # --- still-open candle: its OHLCV will change, so never store it
    if dataset is Dataset.OHLCV and drop_incomplete and interval is not None and len(df):
        open_mask = (df["ts"] + interval.timedelta) > pd.Timestamp(now)
        if open_mask.any():
            issues.append(QualityIssue("incomplete_candle", Severity.INFO, int(open_mask.sum()),
                                       "Candle has not closed yet; dropped.", _sample(df.loc[open_mask, "ts"])))
            dropped += int(open_mask.sum())
            df = df.loc[~open_mask].reset_index(drop=True)

    reasons = pd.Series([""] * len(df), index=df.index, dtype="object")

    def flag(mask: pd.Series, check: str, message: str, severity: Severity = Severity.ERROR) -> None:
        mask = mask.fillna(False).astype(bool)
        n = int(mask.sum())
        if not n:
            return
        issues.append(QualityIssue(check, severity, n, message, _sample(df.loc[mask, "ts"])))
        if severity is Severity.ERROR:
            reasons.loc[mask] = reasons.loc[mask] + check + ";"

    # --- row-level ERROR checks
    for col in REQUIRED_NON_NULL[dataset]:
        flag(df[col].isna(), f"null_{col}", f"Required field '{col}' is null or unparseable.")

    flag(df["ts"] > pd.Timestamp(now + FUTURE_TOLERANCE), "future_timestamp", "Timestamp is in the future.")

    if interval is not None and dataset in (Dataset.OHLCV, Dataset.OPEN_INTEREST):
        epoch_ms = df["ts"].astype("int64") // 1_000_000
        flag(epoch_ms % interval.ms != 0, "misaligned_timestamp",
             f"Timestamp is not aligned to the {interval} grid.")

    if dataset is Dataset.OHLCV:
        o, h, l, c = df["open"], df["high"], df["low"], df["close"]
        flag((o <= 0) | (h <= 0) | (l <= 0) | (c <= 0), "non_positive_price", "OHLC contains a price <= 0.")
        flag((df["volume"] < 0) | (df["quote_volume"] < 0), "negative_volume", "Volume is negative.")
        flag((h < np.maximum.reduce([o, c, l])) | (l > np.minimum.reduce([o, c, h])), "ohlc_inconsistent",
             "High/low do not bound open/close.")
        flag(df["taker_buy_volume"] > df["volume"] * (1 + 1e-9), "taker_volume_exceeds_volume",
             "Taker-buy volume exceeds total volume.")

    if dataset is Dataset.OPEN_INTEREST:
        flag(df["open_interest"] < 0, "negative_open_interest", "Open interest is negative.")

    quarantined = df.loc[reasons != ""].assign(dq_reason=reasons[reasons != ""].str.rstrip(";"))
    clean = df.loc[reasons == ""].sort_values(keys).reset_index(drop=True)

    # --- WARNING checks on clean data (reported, rows kept)
    if dataset is Dataset.OHLCV:
        no_volume = clean["volume"].isna()
        if no_volume.any():
            issues.append(QualityIssue("missing_volume", Severity.WARNING, int(no_volume.sum()),
                                       "Source provides no volume for these candles (volume-based features unavailable).",
                                       _sample(clean.loc[no_volume, "ts"])))
        zero = clean["volume"] == 0
        if zero.any():
            issues.append(QualityIssue("zero_volume", Severity.WARNING, int(zero.sum()),
                                       "Candle has zero volume (possible halt/maintenance).", _sample(clean.loc[zero, "ts"])))

    if dataset is Dataset.FUNDING:
        extreme = clean["funding_rate"].abs() > EXTREME_FUNDING_RATE
        if extreme.any():
            issues.append(QualityIssue("extreme_funding_rate", Severity.WARNING, int(extreme.sum()),
                                       f"|funding rate| > {EXTREME_FUNDING_RATE:.0%} per period.", _sample(clean.loc[extreme, "ts"])))

    if interval is not None and dataset in (Dataset.OHLCV, Dataset.OPEN_INTEREST) and len(clean):
        for _, series in clean.groupby(_series_keys(dataset), sort=False):
            issues.extend(_gap_issues(series, interval))
            if dataset is Dataset.OHLCV:
                issues.extend(_outlier_issues(series))

    return ValidationResult(clean, quarantined, issues, input_rows, dropped)


def _gap_issues(series: pd.DataFrame, interval: Interval) -> list[QualityIssue]:
    ts = series["ts"]
    expected = pd.date_range(ts.min(), ts.max(), freq=interval.timedelta)
    missing = expected.difference(pd.DatetimeIndex(ts))
    if missing.empty:
        return []
    label = f"{series['symbol'].iat[0]} {series['market'].iat[0]} {interval}"
    return [QualityIssue("missing_intervals", Severity.WARNING, len(missing),
                         f"{label}: {len(missing)} expected timestamps missing between {ts.min()} and {ts.max()} (not filled).",
                         [t.isoformat() for t in missing[:SAMPLE_SIZE]])]


def _outlier_issues(series: pd.DataFrame) -> list[QualityIssue]:
    if len(series) < OUTLIER_MIN_ROWS:
        return []
    log_ret = np.log(series["close"]).diff()
    median = log_ret.median()
    mad = (log_ret - median).abs().median()
    if not mad or np.isnan(mad):
        return []
    robust_z = 0.6745 * (log_ret - median) / mad
    mask = robust_z.abs() > OUTLIER_ROBUST_Z
    if not mask.any():
        return []
    return [QualityIssue("price_outlier", Severity.WARNING, int(mask.sum()),
                         f"Close-to-close move with robust z-score > {OUTLIER_ROBUST_Z:g} (kept; verify against another source).",
                         _sample(series.loc[mask, "ts"]))]
