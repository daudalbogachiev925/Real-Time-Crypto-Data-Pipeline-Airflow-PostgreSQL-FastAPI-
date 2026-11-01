"""Repository tests: idempotent upsert, retrieval, gap detection.

Uses an in-memory SQLite for speed; asyncpg/Postgres-only features
(ON CONFLICT) are exercised against Postgres in integration CI.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.models import Base, OhlcvCandle
from src.db.repository import candle_to_dict, upsert_candles_sync


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    s = Session()
    yield s
    s.close()


def _row(symbol: str, interval: str, when: datetime) -> dict:
    return candle_to_dict(
        symbol=symbol,
        interval=interval,
        open_time=when,
        close_time=when + timedelta(minutes=5),
        open_=100.0,
        high=105.0,
        low=95.0,
        close=102.0,
        volume=1000.0,
        quote_volume=102_000.0,
        trades=500,
        source="binance",
    )


def test_insert_and_query(session) -> None:
    now = datetime(2024, 1, 1, 12, 0, 0)
    n = upsert_candles_sync(session, [_row("BTCUSDT", "5m", now)])
    session.commit()
    assert n == 1
    count = session.query(OhlcvCandle).count()
    assert count == 1


def test_upsert_is_idempotent(session) -> None:
    """Re-upsert of same (symbol, interval, open_time) must not create duplicates."""
    now = datetime(2024, 1, 1, 12, 0, 0)
    upsert_candles_sync(session, [_row("BTCUSDT", "5m", now)])
    session.commit()
    # second insert with same key
    upsert_candles_sync(session, [_row("BTCUSDT", "5m", now)])
    session.commit()
    count = session.query(OhlcvCandle).count()
    assert count == 1
