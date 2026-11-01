"""API tests with mocked DB session."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.models import Base, OhlcvCandle
from src.api import main as api_main


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path}/test.db")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)

    # seed test data
    now = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    with Session() as s:
        for i in range(10):
            s.add(
                OhlcvCandle(
                    symbol="BTCUSDT",
                    interval="5m",
                    open_time=now - timedelta(minutes=5 * i),
                    close_time=now - timedelta(minutes=5 * i) + timedelta(minutes=5),
                    open=100.0 + i,
                    high=105.0 + i,
                    low=95.0 + i,
                    close=102.0 + i,
                    volume=1000.0,
                    quote_volume=102_000.0,
                    trades=500,
                    source="binance",
                    ingested_at=now,
                )
            )
        s.commit()

    def override_db():
        with Session() as s:
            yield s

    api_main.app.dependency_overrides[api_main.db_session] = override_db
    with TestClient(api_main.app) as c:
        yield c
    api_main.app.dependency_overrides.clear()


def test_health(client: TestClient) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_symbols(client: TestClient) -> None:
    r = client.get("/symbols")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["symbol"] == "BTCUSDT"
    assert "5m" in data[0]["intervals"]
    assert data[0]["candle_count"] == 10


def test_candles(client: TestClient) -> None:
    r = client.get("/candles", params={"symbol": "BTCUSDT", "interval": "5m", "limit": 5})
    assert r.status_code == 200
    data = r.json()
    assert data["count"] == 5
    assert len(data["candles"]) == 5
    for c in data["candles"]:
        assert c["low"] <= c["open"] <= c["high"]
        assert c["low"] <= c["close"] <= c["high"]


def test_candles_invalid_interval(client: TestClient) -> None:
    r = client.get("/candles", params={"symbol": "BTCUSDT", "interval": "99m"})
    assert r.status_code == 400


def test_latest(client: TestClient) -> None:
    r = client.get("/latest", params={"symbol": "BTCUSDT", "interval": "5m"})
    assert r.status_code == 200
    data = r.json()
    assert data["candle"] is not None
    assert data["age_minutes"] is not None


def test_latest_unknown_symbol(client: TestClient) -> None:
    r = client.get("/latest", params={"symbol": "DOGEUSDT", "interval": "5m"})
    assert r.status_code == 200
    data = r.json()
    assert data["candle"] is None


def test_quality(client: TestClient) -> None:
    r = client.get("/quality", params={"symbol": "BTCUSDT", "interval": "5m"})
    assert r.status_code == 200
    data = r.json()
    assert data["symbol"] == "BTCUSDT"
    assert "gap_count" in data
    assert "is_fresh" in data
