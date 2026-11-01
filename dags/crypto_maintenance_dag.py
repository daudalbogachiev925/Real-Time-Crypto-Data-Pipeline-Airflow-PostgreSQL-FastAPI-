"""Maintenance DAG: daily health checks + old data pruning + report.

Tasks:
1. detect_gaps — verify no missing candles in last 24h for each symbol
2. prune_old_data — delete Postgres candles older than retention period
3. produce_report — log summary metrics
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import delete

from src.config import get_app_config
from src.db.models import OhlcvCandle
from src.db.session import get_sync_session

logger = logging.getLogger(__name__)

INTERVAL_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


def detect_gaps_task(**context) -> dict:
    """Report gaps per (symbol, interval) in last N hours."""
    from src.db.repository import find_gaps_async
    from src.db.session import get_async_session

    cfg = get_app_config()
    report: dict = {"symbols": {}}

    async def _run():
        async with get_async_session() as session:
            for symbol in cfg.pipeline.symbols:
                for interval in [cfg.pipeline.realtime_interval, "1h", "1d"]:
                    if interval not in INTERVAL_SECONDS:
                        continue
                    gaps = await find_gaps_async(
                        session,
                        symbol=symbol,
                        interval=interval,
                        window_hours=cfg.validation.gap_check_window_hours,
                        interval_seconds=INTERVAL_SECONDS[interval],
                    )
                    report["symbols"].setdefault(symbol, {})[interval] = len(gaps)

    asyncio.run(_run())
    total_gaps = sum(v for sym in report["symbols"].values() for v in sym.values())
    logger.info("Gap detection: %s total gaps across all symbols. Details: %s",
                total_gaps, report["symbols"])
    if total_gaps > 100:
        raise RuntimeError(f"Too many gaps detected: {total_gaps}")
    return report


def prune_old_data(**context) -> dict:
    """Delete Postgres candles older than retention_days (Parquet archive remains)."""
    cfg = get_app_config()
    cutoff = datetime.utcnow() - timedelta(days=cfg.storage.postgres_retention_days)
    deleted = 0
    with get_sync_session() as session:
        result = session.execute(
            delete(OhlcvCandle).where(OhlcvCandle.open_time < cutoff)
        )
        deleted = result.rowcount or 0
    logger.info("Pruned %s rows older than %s", deleted, cutoff.isoformat())
    return {"deleted": deleted, "cutoff": cutoff.isoformat()}


def produce_report(**context) -> dict:
    """Log counts per symbol/interval for audit."""
    from sqlalchemy import func, select

    with get_sync_session() as session:
        stmt = select(
            OhlcvCandle.symbol,
            OhlcvCandle.interval,
            func.count(OhlcvCandle.id).label("cnt"),
            func.max(OhlcvCandle.open_time).label("latest"),
        ).group_by(OhlcvCandle.symbol, OhlcvCandle.interval)
        rows = session.execute(stmt).all()
    report = [
        {
            "symbol": r[0],
            "interval": r[1],
            "count": int(r[2]),
            "latest": r[3].isoformat() if r[3] else None,
        }
        for r in rows
    ]
    logger.info("Coverage report: %s rows across %s (symbol, interval) pairs",
                sum(r["count"] for r in report), len(report))
    return {"report": report}


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_maintenance",
    description="Daily data quality + pruning",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="30 3 * * *",  # 03:30 daily
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "maintenance"],
) as dag:
    gaps = PythonOperator(task_id="detect_gaps", python_callable=detect_gaps_task)
    prune = PythonOperator(task_id="prune_old_data", python_callable=prune_old_data)
    report = PythonOperator(task_id="produce_report", python_callable=produce_report)

    gaps >> prune >> report
