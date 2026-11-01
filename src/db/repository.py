"""Repository: idempotent upsert + query helpers."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import polars as pl
from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.db.models import IngestionRun, OhlcvCandle

logger = logging.getLogger(__name__)


def candle_to_dict(
    symbol: str,
    interval: str,
    open_time: datetime,
    close_time: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    quote_volume: float,
    trades: int,
    source: str,
) -> dict:
    return {
        "symbol": symbol,
        "interval": interval,
        "open_time": open_time,
        "close_time": close_time,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "quote_volume": quote_volume,
        "trades": trades,
        "source": source,
        "ingested_at": datetime.utcnow(),
    }


async def upsert_candles_async(session: AsyncSession, rows: list[dict]) -> int:
    """Insert candles with ON CONFLICT DO NOTHING. Returns inserted count."""
    if not rows:
        return 0
    stmt = pg_insert(OhlcvCandle).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        constraint="uq_candle"
    )
    result = await session.execute(stmt)
    await session.flush()
    return result.rowcount or 0


def upsert_candles_sync(session: Session, rows: list[dict]) -> int:
    """Sync version for Airflow tasks."""
    if not rows:
        return 0
    stmt = pg_insert(OhlcvCandle).values(rows)
    stmt = stmt.on_conflict_do_nothing(constraint="uq_candle")
    result = session.execute(stmt)
    session.flush()
    return result.rowcount or 0


async def fetch_candles_async(
    session: AsyncSession,
    symbol: str,
    interval: str,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 1000,
) -> list[OhlcvCandle]:
    stmt = select(OhlcvCandle).where(
        OhlcvCandle.symbol == symbol,
        OhlcvCandle.interval == interval,
    )
    if start is not None:
        stmt = stmt.where(OhlcvCandle.open_time >= start)
    if end is not None:
        stmt = stmt.where(OhlcvCandle.open_time <= end)
    stmt = stmt.order_by(OhlcvCandle.open_time.asc()).limit(limit)
    result = await session.execute(stmt)
    return list(result.scalars().all())


def fetch_candles_sync(
    session: Session,
    symbol: str,
    interval: str,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 1000,
) -> list[OhlcvCandle]:
    stmt = select(OhlcvCandle).where(
        OhlcvCandle.symbol == symbol,
        OhlcvCandle.interval == interval,
    )
    if start is not None:
        stmt = stmt.where(OhlcvCandle.open_time >= start)
    if end is not None:
        stmt = stmt.where(OhlcvCandle.open_time <= end)
    stmt = stmt.order_by(OhlcvCandle.open_time.asc()).limit(limit)
    result = session.execute(stmt)
    return list(result.scalars().all())


def candles_to_polars(candles: list[OhlcvCandle]) -> pl.DataFrame:
    if not candles:
        return pl.DataFrame(
            schema={
                "open_time": pl.Datetime("us", "UTC"),
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "quote_volume": pl.Float64,
                "trades": pl.Int64,
            }
        )
    return pl.DataFrame(
        {
            "open_time": [c.open_time for c in candles],
            "open": [c.open for c in candles],
            "high": [c.high for c in candles],
            "low": [c.low for c in candles],
            "close": [c.close for c in candles],
            "volume": [c.volume for c in candles],
            "quote_volume": [c.quote_volume for c in candles],
            "trades": [c.trades for c in candles],
        }
    ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))


async def latest_candle_time(
    session: AsyncSession, symbol: str, interval: str
) -> datetime | None:
    stmt = select(func.max(OhlcvCandle.open_time)).where(
        OhlcvCandle.symbol == symbol,
        OhlcvCandle.interval == interval,
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def find_gaps_async(
    session: AsyncSession,
    symbol: str,
    interval: str,
    window_hours: int = 24,
    interval_seconds: int = 300,
) -> list[datetime]:
    """Return open_times where expected candles are missing in the last N hours."""
    now = datetime.utcnow()
    start = now - timedelta(hours=window_hours)
    stmt = select(OhlcvCandle.open_time).where(
        OhlcvCandle.symbol == symbol,
        OhlcvCandle.interval == interval,
        OhlcvCandle.open_time >= start,
    ).order_by(OhlcvCandle.open_time.asc())
    result = await session.execute(stmt)
    existing = [row[0] for row in result.all()]
    if not existing:
        return []

    expected_delta = timedelta(seconds=interval_seconds)
    expected: list[datetime] = []
    cursor = existing[0]
    last = existing[-1]
    existing_set = set(existing)
    while cursor <= last:
        if cursor not in existing_set:
            expected.append(cursor)
        cursor += expected_delta
    return expected


async def record_ingestion_run(
    session: AsyncSession,
    dag_id: str,
    task_id: str,
    symbol: str,
    interval: str,
    started_at: datetime,
    finished_at: datetime,
    candles_fetched: int,
    candles_inserted: int,
    status: str,
    error: str | None = None,
) -> None:
    run = IngestionRun(
        dag_id=dag_id,
        task_id=task_id,
        symbol=symbol,
        interval=interval,
        started_at=started_at,
        finished_at=finished_at,
        candles_fetched=candles_fetched,
        candles_inserted=candles_inserted,
        status=status,
        error=error,
    )
    session.add(run)
    await session.flush()
