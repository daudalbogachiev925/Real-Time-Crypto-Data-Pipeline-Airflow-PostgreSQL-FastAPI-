"""SQLAlchemy ORM models."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class OhlcvCandle(Base):
    """Open-High-Low-Close-Volume candle.

    Unique on (symbol, interval, open_time) — guarantees idempotency.
    """
    __tablename__ = "ohlcv_candles"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    interval: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    close_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    quote_volume: Mapped[float] = mapped_column(Float, nullable=False)
    trades: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="binance")
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("symbol", "interval", "open_time", name="uq_candle"),
        Index("ix_candle_symbol_interval_time", "symbol", "interval", "open_time"),
    )

    def __repr__(self) -> str:
        return (
            f"OhlcvCandle({self.symbol}, {self.interval}, "
            f"{self.open_time.isoformat()}, close={self.close})"
        )


class IngestionRun(Base):
    """Audit log: one row per ingestion job execution."""
    __tablename__ = "ingestion_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    dag_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    task_id: Mapped[str] = mapped_column(String(200), nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    interval: Mapped[str] = mapped_column(String(10), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    candles_fetched: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    candles_inserted: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    error: Mapped[str | None] = mapped_column(String(2000), nullable=True)
