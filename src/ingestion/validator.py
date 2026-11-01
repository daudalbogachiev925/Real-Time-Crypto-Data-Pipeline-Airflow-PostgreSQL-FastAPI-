"""Data quality validators: freshness, gaps, price sanity, OHLC invariants."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import polars as pl

from src.config import ValidationConfig

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    passed: bool
    errors: list[str]
    warnings: list[str]


def validate_ohlc_invariants(df: pl.DataFrame) -> ValidationResult:
    """Check OHLC arithmetic: low <= open,close <= high; volume >= 0."""
    errors: list[str] = []
    warnings: list[str] = []
    if df.height == 0:
        return ValidationResult(passed=True, errors=[], warnings=["Empty dataframe"])

    bad_high = df.filter(
        (pl.col("high") < pl.col("open")) | (pl.col("high") < pl.col("close"))
    )
    if bad_high.height > 0:
        errors.append(f"{bad_high.height} rows with high < open/close")

    bad_low = df.filter(
        (pl.col("low") > pl.col("open")) | (pl.col("low") > pl.col("close"))
    )
    if bad_low.height > 0:
        errors.append(f"{bad_low.height} rows with low > open/close")

    bad_volume = df.filter(pl.col("volume") < 0)
    if bad_volume.height > 0:
        errors.append(f"{bad_volume.height} rows with negative volume")

    return ValidationResult(passed=len(errors) == 0, errors=errors, warnings=warnings)


def validate_price_change(
    df: pl.DataFrame, threshold_pct: float = 25.0
) -> ValidationResult:
    """Flag suspicious single-candle price spikes."""
    if df.height < 2:
        return ValidationResult(passed=True, errors=[], warnings=[])
    diffs = df.select(
        (pl.col("close") / pl.col("close").shift(1) - 1.0).abs().alias("pct_change")
    ).drop_nulls()
    spikes = diffs.filter(pl.col("pct_change") * 100.0 > threshold_pct)
    warnings: list[str] = []
    if spikes.height > 0:
        warnings.append(
            f"{spikes.height} candles with |price change| > {threshold_pct}%"
        )
    return ValidationResult(passed=True, errors=[], warnings=warnings)


def validate_freshness(
    latest_open_time: datetime | None, cfg: ValidationConfig
) -> ValidationResult:
    """Check that latest candle is not too old."""
    errors: list[str] = []
    if latest_open_time is None:
        return ValidationResult(
            passed=False, errors=["No candles found"], warnings=[]
        )
    now = datetime.now(tz=timezone.utc)
    if latest_open_time.tzinfo is None:
        latest_open_time = latest_open_time.replace(tzinfo=timezone.utc)
    age_min = (now - latest_open_time).total_seconds() / 60.0
    if age_min > cfg.max_freshness_minutes:
        errors.append(
            f"Data is stale: last candle is {age_min:.1f} min old "
            f"(threshold {cfg.max_freshness_minutes} min)"
        )
    return ValidationResult(passed=len(errors) == 0, errors=errors, warnings=[])


def validate_gaps(
    existing_times: list[datetime], interval_seconds: int, window_hours: int = 24
) -> ValidationResult:
    """Detect missing candles in the recent window."""
    if not existing_times:
        return ValidationResult(
            passed=False, errors=["No candles in window"], warnings=[]
        )
    existing = sorted(existing_times)
    gaps: list[datetime] = []
    window_start = datetime.now(tz=timezone.utc) - timedelta(hours=window_hours)
    existing_set = {t for t in existing if t >= window_start}
    if not existing_set:
        return ValidationResult(
            passed=False, errors=["No recent candles"], warnings=[]
        )
    cursor = min(existing_set)
    last = max(existing_set)
    delta = timedelta(seconds=interval_seconds)
    while cursor <= last:
        if cursor not in existing_set:
            gaps.append(cursor)
        cursor += delta
    if gaps:
        return ValidationResult(
            passed=False,
            errors=[f"{len(gaps)} missing candles in last {window_hours}h"],
            warnings=[f"first gap at {gaps[0].isoformat()}"],
        )
    return ValidationResult(passed=True, errors=[], warnings=[])
