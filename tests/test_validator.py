"""Validator tests: OHLC invariants, freshness, gaps, price spikes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from src.config import ValidationConfig
from src.ingestion.validator import (
    validate_freshness,
    validate_gaps,
    validate_ohlc_invariants,
    validate_price_change,
)


def _candle_frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "open_time": pl.Datetime("us", "UTC"),
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
        },
    )


def test_ohlc_valid() -> None:
    df = _candle_frame(
        [
            {"open_time": datetime.now(tz=timezone.utc), "open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 1000.0},
        ]
    )
    r = validate_ohlc_invariants(df)
    assert r.passed


def test_ohlc_high_below_close() -> None:
    df = _candle_frame(
        [
            {"open_time": datetime.now(tz=timezone.utc), "open": 100.0, "high": 100.5, "low": 95.0, "close": 102.0, "volume": 1000.0},
        ]
    )
    r = validate_ohlc_invariants(df)
    assert not r.passed
    assert any("high" in e for e in r.errors)


def test_ohlc_negative_volume() -> None:
    df = _candle_frame(
        [
            {"open_time": datetime.now(tz=timezone.utc), "open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": -1.0},
        ]
    )
    r = validate_ohlc_invariants(df)
    assert not r.passed


def test_freshness_ok() -> None:
    cfg = ValidationConfig(max_freshness_minutes=10)
    latest = datetime.now(tz=timezone.utc) - timedelta(minutes=2)
    r = validate_freshness(latest, cfg)
    assert r.passed


def test_freshness_stale() -> None:
    cfg = ValidationConfig(max_freshness_minutes=10)
    latest = datetime.now(tz=timezone.utc) - timedelta(minutes=30)
    r = validate_freshness(latest, cfg)
    assert not r.passed


def test_freshness_missing() -> None:
    cfg = ValidationConfig()
    r = validate_freshness(None, cfg)
    assert not r.passed


def test_gaps_detected() -> None:
    now = datetime.now(tz=timezone.utc)
    # 5-minute interval, expect 12 candles in last hour but provide 10
    times = [now - timedelta(minutes=5 * i) for i in range(12)]
    # remove 2 candles to create gaps
    times = [t for i, t in enumerate(times) if i not in (2, 5)]
    r = validate_gaps(times, interval_seconds=300, window_hours=1)
    assert not r.passed


def test_gaps_none() -> None:
    now = datetime.now(tz=timezone.utc)
    times = [now - timedelta(minutes=5 * i) for i in range(12)]
    r = validate_gaps(times, interval_seconds=300, window_hours=1)
    assert r.passed


def test_price_spike_warning() -> None:
    now = datetime.now(tz=timezone.utc)
    df = _candle_frame(
        [
            {"open_time": now, "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "volume": 1.0},
            {"open_time": now + timedelta(minutes=5), "open": 100.0, "high": 150.0, "low": 100.0, "close": 150.0, "volume": 1.0},
        ]
    )
    r = validate_price_change(df, threshold_pct=25.0)
    assert r.passed  # warnings only, not errors
    assert len(r.warnings) > 0
