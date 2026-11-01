"""Historical backfill DAG: daily run that fills any gaps from Binance."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import polars as pl
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.config import get_app_config
from src.db.repository import (
    candle_to_dict,
    record_ingestion_run,
    upsert_candles_sync,
)
from src.db.session import get_sync_session
from src.ingestion.binance import BinanceClient
from src.storage.parquet_writer import write_candles_partitioned

logger = logging.getLogger(__name__)


def backfill_symbol_interval(symbol: str, interval: str, days: int, **context) -> dict:
    """Fetch the last `days` of candles for one (symbol, interval)."""
    import asyncio

    cfg = get_app_config()
    started = datetime.utcnow()
    fetched = 0
    inserted = 0
    status = "success"
    error: str | None = None

    try:
        end_ms = int(datetime.utcnow().timestamp() * 1000)
        start_ms = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)

        async def _fetch():
            async with BinanceClient(cfg.binance) as client:
                return await client.fetch_klines_range(
                    symbol=symbol,
                    interval=interval,
                    start_ms=start_ms,
                    end_ms=end_ms,
                )

        candles = asyncio.run(_fetch())
        fetched = len(candles)
        if not candles:
            logger.warning("No candles fetched for %s %s (last %s days)", symbol, interval, days)
            return {"symbol": symbol, "interval": interval, "fetched": 0, "inserted": 0}

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

        # batch upsert to avoid giant single statement
        batch_size = 1000
        with get_sync_session() as session:
            for i in range(0, len(rows), batch_size):
                inserted += upsert_candles_sync(session, rows[i : i + batch_size])

        write_candles_partitioned(df, cfg.storage.parquet_dir, symbol)
        logger.info(
            "Backfill %s %s: fetched=%s inserted=%s (window %s days)",
            symbol, interval, fetched, inserted, days,
        )
    except Exception as exc:
        status = "failed"
        error = str(exc)[:2000]
        logger.exception("Backfill failed for %s %s: %s", symbol, interval, exc)
        raise
    finally:
        async def _record():
            from src.db.session import get_async_session

            async with get_async_session() as session:
                await record_ingestion_run(
                    session=session,
                    dag_id="crypto_historical",
                    task_id=f"backfill_{symbol}_{interval}",
                    symbol=symbol,
                    interval=interval,
                    started_at=started,
                    finished_at=datetime.utcnow(),
                    candles_fetched=fetched,
                    candles_inserted=inserted,
                    status=status,
                    error=error,
                )

        try:
            asyncio.run(_record())
        except Exception as rec_exc:  # pragma: no cover
            logger.warning("Failed to record ingestion run: %s", rec_exc)

    return {"symbol": symbol, "interval": interval, "fetched": fetched, "inserted": inserted}


def backfill_all_historical(**context) -> dict:
    """Backfill daily candles for all configured symbols."""
    cfg = get_app_config()
    total = {"symbols": 0, "fetched": 0, "inserted": 0}
    for symbol in cfg.pipeline.symbols:
        result = backfill_symbol_interval(
            symbol=symbol,
            interval="1d",
            days=cfg.pipeline.historical_days,
            **context,
        )
        total["symbols"] += 1
        total["fetched"] += int(result.get("fetched", 0))
        total["inserted"] += int(result.get("inserted", 0))
    return total


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="crypto_historical",
    description="Daily backfill of historical candles from Binance",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",  # every day at 02:00
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "backfill"],
) as dag:
    backfill = PythonOperator(
        task_id="backfill_daily_candles",
        python_callable=backfill_all_historical,
    )
