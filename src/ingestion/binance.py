"""Binance OHLCV client (public, no auth)."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import BinanceConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BinanceCandle:
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trades: int


class BinanceClient:
    """Async client for Binance /api/v3/klines endpoint."""

    def __init__(self, config: BinanceConfig, client: httpx.AsyncClient | None = None):
        self.config = config
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "BinanceClient":
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.config.base_url,
                timeout=self.config.timeout_seconds,
            )
        return self

    async def __aexit__(self, *args) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 100,
        end_time: int | None = None,
    ) -> list[BinanceCandle]:
        """Fetch up to `limit` most recent candles (or ending at end_time, in ms)."""
        assert self._client is not None
        limit = min(limit, self.config.max_candles_per_request)
        params: dict = {"symbol": symbol, "interval": interval, "limit": limit}
        if end_time is not None:
            params["endTime"] = end_time
        resp = await self._client.get("/api/v3/klines", params=params)
        resp.raise_for_status()
        raw = resp.json()
        candles: list[BinanceCandle] = []
        for row in raw:
            try:
                candles.append(
                    BinanceCandle(
                        open_time=datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc),
                        close_time=datetime.fromtimestamp(row[6] / 1000, tz=timezone.utc),
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        volume=float(row[5]),
                        quote_volume=float(row[7]),
                        trades=int(row[8]),
                    )
                )
            except (ValueError, IndexError) as exc:
                logger.warning("Skipping malformed candle: %s (%s)", row, exc)
        logger.info(
            "Binance: fetched %s candles for %s %s", len(candles), symbol, interval
        )
        return candles

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def fetch_klines_range(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
    ) -> list[BinanceCandle]:
        """Fetch candles in [start_ms, end_ms] with pagination (max 1000/request)."""
        assert self._client is not None
        all_candles: list[BinanceCandle] = []
        cursor = start_ms
        page = 0
        while cursor < end_ms:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": self.config.max_candles_per_request,
            }
            resp = await self._client.get("/api/v3/klines", params=params)
            resp.raise_for_status()
            raw = resp.json()
            if not raw:
                break
            for row in raw:
                try:
                    all_candles.append(
                        BinanceCandle(
                            open_time=datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc),
                            close_time=datetime.fromtimestamp(row[6] / 1000, tz=timezone.utc),
                            open=float(row[1]),
                            high=float(row[2]),
                            low=float(row[3]),
                            close=float(row[4]),
                            volume=float(row[5]),
                            quote_volume=float(row[7]),
                            trades=int(row[8]),
                        )
                    )
                except (ValueError, IndexError) as exc:
                    logger.warning("Skipping malformed candle: %s (%s)", row, exc)
            # advance cursor past the last candle's close_time + 1ms
            last_close_ms = int(raw[-1][6])
            if last_close_ms + 1 >= end_ms:
                break
            cursor = last_close_ms + 1
            page += 1
            logger.info(
                "Binance: page %s fetched (%s candles, cursor=%s)",
                page,
                len(raw),
                cursor,
            )
        logger.info(
            "Binance range: %s candles for %s %s", len(all_candles), symbol, interval
        )
        return all_candles
