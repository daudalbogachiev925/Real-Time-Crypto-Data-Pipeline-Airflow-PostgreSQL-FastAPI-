"""Ingestion client tests using httpx mock transport."""
from __future__ import annotations

import httpx
import pytest

from src.config import BinanceConfig, CoinGeckoConfig
from src.ingestion.binance import BinanceClient
from src.ingestion.coingecko import CoinGeckoClient

BINANCE_PAYLOAD = [
    [
        1700000000000, "100.0", "105.0", "95.0", "102.0", "1000.0",
        1700000299999, "102000.0", 500, "500.0", "51000.0", "0",
    ],
    [
        1700000300000, "102.0", "108.0", "101.0", "107.0", "900.0",
        1700000599999, "96300.0", 400, "400.0", "42800.0", "0",
    ],
]


@pytest.mark.asyncio
async def test_binance_fetch_klines() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v3/klines"
        assert request.url.params.get("symbol") == "BTCUSDT"
        return httpx.Response(200, json=BINANCE_PAYLOAD)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(
        base_url="https://api.binance.com", transport=transport
    ) as http_client:
        client = BinanceClient(BinanceConfig(), client=http_client)
        candles = await client.fetch_klines("BTCUSDT", "5m", limit=2)
    assert len(candles) == 2
    assert candles[0].open == 100.0
    assert candles[0].close == 102.0
    assert candles[0].trades == 500


@pytest.mark.asyncio
async def test_binance_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(dict(request.url.params))
        start = int(request.url.params.get("startTime", "0"))
        # emit 1 candle per page starting at `start`
        payload = [
            [
                start, "100.0", "101.0", "99.0", "100.5", "100.0",
                start + 299_999, "10050.0", 10, "0.0", "0.0", "0",
            ]
        ]
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(
        base_url="https://api.binance.com", transport=transport
    ) as http_client:
        client = BinanceClient(BinanceConfig(), client=http_client)
        candles = await client.fetch_klines_range(
            "BTCUSDT", "5m", start_ms=0, end_ms=1_000_000
        )
    assert len(candles) >= 1
    assert len(calls) >= 1


@pytest.mark.asyncio
async def test_coingecko_market_chart() -> None:
    payload = {
        "prices": [
            [1700000000000, 50000.0],
            [1700086400000, 51000.0],
            [1700172800000, 52000.0],
        ],
        "total_volumes": [
            [1700000000000, 1_000_000.0],
            [1700086400000, 1_200_000.0],
            [1700172800000, 1_500_000.0],
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert "/coins/bitcoin/market_chart" in request.url.path
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(
        base_url="https://api.coingecko.com/api/v3", transport=transport
    ) as http_client:
        cfg = CoinGeckoConfig(coin_ids={"BTCUSDT": "bitcoin"}, rate_limit_pause_seconds=0.0)
        client = CoinGeckoClient(cfg, client=http_client)
        rows = await client.fetch_daily_prices("BTCUSDT", days=3)
    assert len(rows) == 3
    for row in rows:
        assert row["low"] <= row["open"] <= row["high"]
        assert row["low"] <= row["close"] <= row["high"]
        assert row["source"] == "coingecko"
