"""Partitioned Parquet writer: cold archive of all ingested candles.

Layout: {base_dir}/symbol={SYMBOL}/year=YYYY/month=MM/data.parquet
Existing files are read + deduped + rewritten (small scale); in production
this would use Delta Lake / Iceberg.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def _partition_dir(base: Path, symbol: str, dt: datetime) -> Path:
    return base / f"symbol={symbol}" / f"year={dt.year:04d}" / f"month={dt.month:02d}"


def write_candles_partitioned(
    df: pl.DataFrame,
    base_dir: str | Path,
    symbol: str,
) -> list[Path]:
    """Append candles to monthly partitions, dedupe on (open_time)."""
    if df.height == 0:
        return []
    base = Path(base_dir)
    base.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    df = df.with_columns(
        pl.col("open_time").dt.year().alias("_year"),
        pl.col("open_time").dt.month().alias("_month"),
    )

    for (year, month), group in df.group_by(["_year", "_month"]):
        year_int, month_int = int(year), int(month)
        partition = _partition_dir(base, symbol, datetime(year_int, month_int, 1))
        partition.mkdir(parents=True, exist_ok=True)
        file_path = partition / "data.parquet"
        new_data = group.drop(["_year", "_month"])

        if file_path.exists():
            existing = pl.read_parquet(file_path)
            combined = pl.concat([existing, new_data], how="vertical").unique(
                subset=["open_time"], keep="last"
            ).sort("open_time")
        else:
            combined = new_data.sort("open_time")

        combined.write_parquet(file_path)
        written.append(file_path)
        logger.info(
            "Parquet partition written: %s (%s rows)", file_path, combined.height
        )
    return written


def read_partitioned(
    base_dir: str | Path,
    symbol: str,
    start: datetime | None = None,
    end: datetime | None = None,
) -> pl.DataFrame:
    base = Path(base_dir) / f"symbol={symbol}"
    if not base.exists():
        return pl.DataFrame()
    files = sorted(base.rglob("*.parquet"))
    if not files:
        return pl.DataFrame()
    frames: list[pl.DataFrame] = []
    for f in files:
        try:
            frames.append(pl.read_parquet(f))
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to read %s: %s", f, exc)
    if not frames:
        return pl.DataFrame()
    out = pl.concat(frames, how="vertical")
    if start is not None:
        out = out.filter(pl.col("open_time") >= start)
    if end is not None:
        out = out.filter(pl.col("open_time") <= end)
    return out.sort("open_time")
