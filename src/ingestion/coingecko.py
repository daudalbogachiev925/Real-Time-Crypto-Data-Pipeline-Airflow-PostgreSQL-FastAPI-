"""CoinGecko historical daily data client (free tier, rate-limited)."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import CoinGeckoConfig

logger = logging.getLogger(__name__)


class CoinGeckoClient:
    """Fetch daily historical prices. Free tier: 10-30 req/min."""

    def __init__(self, config: CoinGeckoConfig, client: httpx.AsyncClient | None = None):
        self.config = config
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> "CoinGeckoClient":
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
        wait=wait_exponential(multiplier=2, min=3, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def _fetch_market_chart(
        self, coin_id: str, days: int, vs_currency: str = "usd"
    ) -> dict:
        assert self._client is not None
        resp = await self._client.get(
            f"/coins/{coin_id}/market_chart",
            params={"vs_currency": vs_currency, "days": days, "interval": "daily"},
        )
        if resp.status_code == 429:
            logger.warning("CoinGecko rate-limited; retrying with backoff")
            raise httpx.HTTPError("Rate limited")
        resp.raise_for_status()
        return resp.json()

    async def fetch_daily_prices(
        self, symbol: str, days: int = 90
    ) -> list[dict]:
        """Return list of {open_time, open, high, low, close, volume, source}.

        CoinGecko only provides price + volume (no OHLC granularity in free tier).
        We synthesize OHLC from consecutive price points: open = close = price,
        high = low = price, except we set high/low from adjacent range.
        """
        coin_id = self.config.coin_ids.get(symbol)
        if not coin_id:
            logger.warning("No CoinGecko id configured for %s", symbol)
            return []
        await asyncio.sleep(self.config.rate_limit_pause_seconds)
        data = await self._fetch_market_chart(coin_id, days=days)
        prices = data.get("prices", [])
        volumes = data.get("total_volumes", [])
        volume_map = {
            int(ts // (24 * 3600 * 1000)): float(vol) for ts, vol in volumes
        }

        rows: list[dict] = []
        for i, (ts_ms, price) in enumerate(prices):
            # daily bucket
            day_ms = int(ts_ms // (24 * 3600 * 1000)) * (24 * 3600 * 1000)
            open_time = datetime.fromtimestamp(day_ms / 1000, tz=timezone.utc)
            close_time = datetime.fromtimestamp(
                (day_ms + 24 * 3600 * 1000 - 1) / 1000, tz=timezone.utc
            )
            prev_price = float(prices[i - 1][1]) if i > 0 else float(price)
            next_price = float(prices[i + 1][1]) if i < len(prices) - 1 else float(price)
            o = prev_price
            c = float(price)
            h = max(o, c, next_price)
            l = min(o, c, next_price)
            vol = volume_map.get(day_ms // (24 * 3600 * 1000), 0.0)
            rows.append(
                {
                    "open_time": open_time,
                    "close_time": close_time,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": vol,
                    "quote_volume": vol * c,
                    "trades": 0,
                    "source": "coingecko",
                }
            )
        logger.info("CoinGecko: %s daily rows for %s", len(rows), symbol)
        return rows
