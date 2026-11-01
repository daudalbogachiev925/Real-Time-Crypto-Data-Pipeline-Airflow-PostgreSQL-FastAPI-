"""FastAPI app: candles, latest, quality, symbols."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import db_session
from src.api.schemas import (
    CandlePoint,
    CandlesResponse,
    DataQualityResponse,
    HealthResponse,
    LatestCandleResponse,
    SymbolInfo,
)
from src.config import get_app_config, get_settings
from src.db.models import OhlcvCandle
from src.db.repository import (
    fetch_candles_async,
    find_gaps_async,
    latest_candle_time,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=get_settings().log_level)

INTERVAL_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info("Crypto API starting")
    yield
    logger.info("Crypto API shutting down")


app = FastAPI(title="Crypto Data Pipeline API", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok", version=app.version)


@app.get("/symbols", response_model=list[SymbolInfo])
async def symbols(session: AsyncSession = Depends(db_session)) -> list[SymbolInfo]:
    stmt = select(
        OhlcvCandle.symbol,
        OhlcvCandle.interval,
        func.count(OhlcvCandle.id).alias("cnt"),
    ).group_by(OhlcvCandle.symbol, OhlcvCandle.interval)
    rows = (await session.execute(stmt)).all()
    grouped: dict[str, dict] = {}
    for symbol, interval, cnt in rows:
        grouped.setdefault(symbol, {"intervals": [], "count": 0})
        grouped[symbol]["intervals"].append(interval)
        grouped[symbol]["count"] += int(cnt)
    return [
        SymbolInfo(
            symbol=sym,
            intervals=sorted(data["intervals"]),
            candle_count=data["count"],
        )
        for sym, data in sorted(grouped.items())
    ]


@app.get("/candles", response_model=CandlesResponse)
async def candles(
    symbol: str = Query(..., description="e.g. BTCUSDT"),
    interval: str = Query("5m", description="1m|5m|15m|30m|1h|4h|1d"),
    start: datetime | None = Query(None),
    end: datetime | None = Query(None),
    limit: int = Query(1000, ge=1, le=10000),
    session: AsyncSession = Depends(db_session),
) -> CandlesResponse:
    if interval not in INTERVAL_SECONDS:
        raise HTTPException(status_code=400, detail=f"Invalid interval: {interval}")
    rows = await fetch_candles_async(
        session, symbol=symbol, interval=interval, start=start, end=end, limit=limit
    )
    points = [
        CandlePoint(
            open_time=c.open_time,
            close_time=c.close_time,
            open=c.open,
            high=c.high,
            low=c.low,
            close=c.close,
            volume=c.volume,
            quote_volume=c.quote_volume,
            trades=c.trades,
        )
        for c in rows
    ]
    return CandlesResponse(
        symbol=symbol, interval=interval, count=len(points), candles=points
    )


@app.get("/latest", response_model=LatestCandleResponse)
async def latest(
    symbol: str = Query(...),
    interval: str = Query("5m"),
    session: AsyncSession = Depends(db_session),
) -> LatestCandleResponse:
    if interval not in INTERVAL_SECONDS:
        raise HTTPException(status_code=400, detail=f"Invalid interval: {interval}")
    rows = await fetch_candles_async(
        session, symbol=symbol, interval=interval, limit=1
    )
    # we want the most recent — reverse sort
    from sqlalchemy import desc

    stmt = (
        select(OhlcvCandle)
        .where(OhlcvCandle.symbol == symbol, OhlcvCandle.interval == interval)
        .order_by(desc(OhlcvCandle.open_time))
        .limit(1)
    )
    row = (await session.execute(stmt)).scalar_one_or_none()
    if row is None:
        return LatestCandleResponse(
            symbol=symbol, interval=interval, candle=None, age_minutes=None
        )
    now = datetime.now(tz=timezone.utc)
    open_time = row.open_time
    if open_time.tzinfo is None:
        open_time = open_time.replace(tzinfo=timezone.utc)
    age_min = (now - open_time).total_seconds() / 60.0
    return LatestCandleResponse(
        symbol=symbol,
        interval=interval,
        candle=CandlePoint(
            open_time=row.open_time,
            close_time=row.close_time,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            volume=row.volume,
            quote_volume=row.quote_volume,
            trades=row.trades,
        ),
        age_minutes=round(age_min, 2),
    )


@app.get("/quality", response_model=DataQualityResponse)
async def quality(
    symbol: str = Query(...),
    interval: str = Query("5m"),
    session: AsyncSession = Depends(db_session),
) -> DataQualityResponse:
    if interval not in INTERVAL_SECONDS:
        raise HTTPException(status_code=400, detail=f"Invalid interval: {interval}")
    cfg = get_app_config()
    latest_t = await latest_candle_time(session, symbol, interval)
    gaps = await find_gaps_async(
        session,
        symbol=symbol,
        interval=interval,
        window_hours=cfg.validation.gap_check_window_hours,
        interval_seconds=INTERVAL_SECONDS[interval],
    )
    now = datetime.now(tz=timezone.utc)
    age_min: float | None = None
    is_fresh = False
    if latest_t is not None:
        lt = latest_t if latest_t.tzinfo is not None else latest_t.replace(tzinfo=timezone.utc)
        age_min = (now - lt).total_seconds() / 60.0
        is_fresh = age_min <= cfg.validation.max_freshness_minutes
    return DataQualityResponse(
        symbol=symbol,
        interval=interval,
        has_gaps=len(gaps) > 0,
        gap_count=len(gaps),
        is_fresh=is_fresh,
        latest_age_minutes=round(age_min, 2) if age_min is not None else None,
        checked_at=now,
    )
