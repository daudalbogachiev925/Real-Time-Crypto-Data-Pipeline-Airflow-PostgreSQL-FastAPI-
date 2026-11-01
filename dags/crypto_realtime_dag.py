"""Real-time crypto ingestion DAG: every 5 minutes, fetch latest OHLCV.

Steps:
1. Fetch latest N candles from Binance for each (symbol, interval) in scope
2. Validate OHLC invariants
3. Upsert to Postgres (idempotent on unique constraint)
4. Append to Parquet archive
5. Record ingestion run
"""
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
from src.ingestion.validator import validate_ohlc_invariants
from src.storage.parquet_writer import write_candles_partitioned

logger = logging.getLogger(__name__)


def _symbols_for_realtime() -> list[str]:
    return get_app_config().pipeline.symbols


def ingest_symbol_interval(symbol: str, interval: str, **context) -> dict:
    """Fetch, validate, upsert, archive one (symbol, interval)."""
    import asyncio

    cfg = get_app_config()
    dag_run = context.get("dag_run")
    dag_id = "crypto_realtime"
    task_id = f"ingest_{symbol}_{interval}"
    started = datetime.utcnow()
    fetched = 0
    inserted = 0
    status = "success"
    error: str | None = None

    try:
        async def _fetch():
            async with BinanceClient(cfg.binance) as client:
                return await client.fetch_klines(
                    symbol=symbol,
                    interval=interval,
                    limit=cfg.pipeline.realtime_lookback_candles,
                )

        candles = asyncio.run(_fetch())
        fetched = len(candles)
        if not candles:
            raise RuntimeError("No candles fetched")

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
        val = validate_ohlc_invariants(df)
        if not val.passed:
            raise RuntimeError(f"Validation failed: {val.errors}")

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
        with get_sync_session() as session:
            inserted = upsert_candles_sync(session, rows)

        write_candles_partitioned(df, cfg.storage.parquet_dir, symbol)
        logger.info(
            "Ingested %s %s: fetched=%s inserted=%s",
            symbol, interval, fetched, inserted,
        )
    except Exception as exc:
        status = "failed"
        error = str(exc)[:2000]
        logger.exception("Ingestion failed: %s", exc)
        raise
    finally:
        import asyncio

        async def _record():
            from src.db.session import get_async_session

            async with get_async_session() as session:
                await record_ingestion_run(
                    session=session,
                    dag_id=dag_id,
                    task_id=task_id,
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


def check_freshness(**context) -> None:
    """Fail DAG if any tracked symbol has stale data."""
    import asyncio

    cfg = get_app_config()
    interval = cfg.pipeline.realtime_interval

    async def _check():
        from src.db.repository import latest_candle_time
        from src.db.session import get_async_session
        from src.ingestion.validator import validate_freshness

        async with get_async_session() as session:
            for symbol in cfg.pipeline.symbols:
                lt = await latest_candle_time(session, symbol, interval)
                result = validate_freshness(lt, cfg.validation)
                if not result.passed:
                    raise RuntimeError(f"{symbol}: {result.errors}")

    asyncio.run(_check())
    logger.info("Freshness check passed for all symbols")


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="crypto_realtime",
    description="Ingest realtime OHLCV from Binance every 5 min",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "realtime"],
) as dag:
    cfg = get_app_config()
    ingest_tasks = []
    for symbol in cfg.pipeline.symbols:
        for interval in [cfg.pipeline.realtime_interval]:
            t = PythonOperator(
                task_id=f"ingest_{symbol}_{interval}",
                python_callable=ingest_symbol_interval,
                op_kwargs={"symbol": symbol, "interval": interval},
            )
            ingest_tasks.append(t)

    freshness_check = PythonOperator(
        task_id="check_freshness",
        python_callable=check_freshness,
    )

    ingest_tasks >> freshness_check
