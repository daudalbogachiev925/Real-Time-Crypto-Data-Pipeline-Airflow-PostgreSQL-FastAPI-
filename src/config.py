"""Configuration loader."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineConfig(BaseModel):
    symbols: list[str] = Field(default_factory=lambda: ["BTCUSDT", "ETHUSDT"])
    intervals: list[str] = Field(default_factory=lambda: ["5m", "1h", "1d"])
    realtime_interval: str = "5m"
    realtime_lookback_candles: int = 12
    historical_days: int = 90


class BinanceConfig(BaseModel):
    base_url: str = "https://api.binance.com"
    timeout_seconds: int = 30
    max_candles_per_request: int = 1000


class CoinGeckoConfig(BaseModel):
    base_url: str = "https://api.coingecko.com/api/v3"
    timeout_seconds: int = 30
    rate_limit_pause_seconds: float = 3.0
    coin_ids: dict[str, str] = Field(default_factory=dict)


class StorageConfig(BaseModel):
    parquet_dir: str = "/data/parquet"
    postgres_retention_days: int = 90


class ValidationConfig(BaseModel):
    max_freshness_minutes: int = 10
    gap_check_window_hours: int = 24
    price_change_alert_pct: float = 25.0


class AppConfig(BaseModel):
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    binance: BinanceConfig = Field(default_factory=BinanceConfig)
    coingecko: CoinGeckoConfig = Field(default_factory=CoinGeckoConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://crypto:crypto_secret@localhost:5432/crypto_db"
    database_url_sync: str = "postgresql+psycopg2://crypto:crypto_secret@localhost:5432/crypto_db"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"
    parquet_dir: str = "/data/parquet"
    config_path: str = "./configs/config.yaml"

    def load_app_config(self) -> AppConfig:
        path = Path(self.config_path)
        if not path.exists():
            return AppConfig()
        with path.open("r", encoding="utf-8") as f:
            raw: dict[str, Any] = yaml.safe_load(f) or {}
        return AppConfig(**raw)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


@lru_cache(maxsize=1)
def get_app_config() -> AppConfig:
    return get_settings().load_app_config()
