"""CLI backfill: `python scripts/backfill.py --symbol BTCUSDT --interval 1h --days 30`."""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl

from src.config import get_app_config, get_settings
from src.db.repository import candle_to_dict, upsert_candles_async
from src.db.session import get_async_session
from src.ingestion.binance import BinanceClient
from src.storage.parquet_writer import write_candles_partitioned
from src.utils.logging import configure_logging

configure_logging(get_settings().log_level)
logger = logging.getLogger("backfill")


async def run(symbol: str, interval: str, days: int) -> int:
    cfg = get_app_config()
    end_ms = int(datetime.utcnow().timestamp() * 1000)
    start_ms = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)

    async with BinanceClient(cfg.binance) as client:
        candles = await client.fetch_klines_range(
            symbol=symbol, interval=interval, start_ms=start_ms, end_ms=end_ms
        )
    if not candles:
        logger.warning("No candles for %s %s", symbol, interval)
        return 0

    rows = [
        candle_to_dict(
            symbol=symbol,
            interval=interval,
            open_time=c.open_time,
            close_time=c.close_time,
            open_=c.open,
            high=c.high,
            low=c.low,
            close=c.close,
            volume=c.volume,
            quote_volume=c.quote_volume,
            trades=c.trades,
            source="binance",
        )
        for c in candles
    ]
    inserted_total = 0
    async with get_async_session() as session:
        for i in range(0, len(rows), 1000):
            inserted_total += await upsert_candles_async(session, rows[i : i + 1000])

    df = pl.DataFrame(
        [
            {
                "open_time": c.open_time,
                "close_time": c.close_time,
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
                "quote_volume": c.quote_volume,
                "trades": c.trades,
            }
            for c in candles
        ]
    )
    write_candles_partitioned(df, cfg.storage.parquet_dir, symbol)

    logger.info(
        "Backfill %s %s: fetched=%s inserted=%s", symbol, interval,
        len(candles), inserted_total,
    )
    return inserted_total


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--interval", default="1h")
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    asyncio.run(run(args.symbol, args.interval, args.days))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
