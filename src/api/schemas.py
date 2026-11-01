"""Pydantic v2 schemas for the API."""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    version: str


class CandlePoint(BaseModel):
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trades: int


class CandlesResponse(BaseModel):
    symbol: str
    interval: str
    count: int
    candles: list[CandlePoint]


class LatestCandleResponse(BaseModel):
    symbol: str
    interval: str
    candle: CandlePoint | None
    age_minutes: float | None


class DataQualityResponse(BaseModel):
    symbol: str
    interval: str
    has_gaps: bool
    gap_count: int
    is_fresh: bool
    latest_age_minutes: float | None
    checked_at: datetime


class SymbolInfo(BaseModel):
    symbol: str
    intervals: list[str]
    candle_count: int
