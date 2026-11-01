# Real-Time Crypto Data Pipeline

Production-ready data pipeline для сбора, валидации и раздачи данных криптовалют:
- **Real-time ingestion** каждые 5 минут (OHLCV от Binance)
- **Historical backfill** по расписанию (дневные свечи от CoinGecko)
- **Оркестрация** через Apache Airflow 2.8
- **Хранение** в PostgreSQL (горячие данные) + Parquet (холодный архив)
- **REST API** на FastAPI для бэктестинга и аналитики
- **Мониторинг** данных: проверки свежести, разрывов, аномалий

## Бизнес-задача
Quant-команде нужен надёжный источник крипто-данных для бэктестинга
торговых стратегий. Требования:
- Данные обновляются ≤ 5 минут задержки
- Гарантия отсутствия дублей (idempotent ingestion)
- Автоматическое обнаружение пропусков в свечах
- REST API с фильтрацией по символу, интервалу, датам
- Сохранение полной истории независимо от TTL Postgres

## Архитектура
┌──────────────┐
│ Binance API │ (real-time OHLCV)
└──────┬───────┘
│
┌──────▼───────┐
│ Airflow DAG │ ← каждые 5 мин
│ ingestion │
└──────┬───────┘
│
┌────────────┼────────────┐
│ │
┌──────▼──────┐ ┌──────▼──────┐
│ PostgreSQL │ │ Parquet │
│ (hot, 30d) │ │ (cold, all) │
└──────┬──────┘ └──────┬──────┘
│ │
┌──────▼─────────────────────────▼──────┐
│ FastAPI + Repository │
└─────────────────┬─────────────────────┘
│
┌────▼────┐
│ Client │ (quant / backtest)
└─────────┘


## Стек
- Python 3.11
- Apache Airflow 2.8 (LocalExecutor + Postgres metadata)
- PostgreSQL 15
- Polars + PyArrow (Parquet)
- SQLAlchemy 2.0 (async)
- FastAPI + Pydantic v2
- Docker Compose (весь стек одной командой)

## Данные
- **Binance** (`https://api.binance.com/api/v3/klines`) — OHLCV свечи, free, no auth
- **CoinGecko** (`https://api.coingecko.com/api/v3/coins/{id}/market_chart`) — исторические дневные данные, rate-limited (10-30 req/min free)
- Символы по умолчанию: BTC/USDT, ETH/USDT, SOL/USDT, BNB/USDT, XRP/USDT

## Инженерные гарантии
- **Idempotency**: уникальный constraint `(symbol, interval, open_time)` в Postgres; повторная загрузка не создаёт дубли
- **Freshness monitoring**: DAG проверяет, что последняя свеча моложе 10 минут
- **Gap detection**: DAG проверяет отсутствующие свечи за последние 24 часа
- **Retry logic**: Airflow `retries=3` с exponential backoff
- **Cold archive**: каждая свеча дублируется в партиционированный Parquet (`year=YYYY/month=MM/`)
- **Rate limiting**: CoinGecko client вставляет 3-секундные паузы между запросами

## Быстрый старт
```bash
make env      # копирует .env.example -> .env
make up       # docker compose up -d (airflow + postgres + api)
make init     # инициализация схемы БД
make api      # http://localhost:8000/docs
make airflow  # http://localhost:8080 (admin/admin)

---

## `pyproject.toml`

```toml
[project]
name = "crypto-data-pipeline"
version = "0.1.0"
description = "Real-time crypto data pipeline with Airflow + PostgreSQL + FastAPI"
requires-python = ">=3.11"
dependencies = [
    "apache-airflow>=2.8,<2.9",
    "apache-airflow-providers-postgres>=5.10",
    "sqlalchemy>=2.0",
    "psycopg2-binary>=2.9",
    "asyncpg>=0.29",
    "polars>=0.20.0",
    "pyarrow>=15.0",
    "httpx>=0.27",
    "fastapi>=0.110",
    "uvicorn[standard]>=0.29",
    "pydantic>=2.6",
    "pydantic-settings>=2.2",
    "pyyaml>=6.0",
    "python-dotenv>=1.0",
    "tenacity>=8.2",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-cov>=5.0",
    "pytest-asyncio>=0.23",
    "ruff>=0.3",
    "mypy>=1.9",
    "httpx>=0.27",
]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
include = ["src*"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-q --asyncio-mode=auto"

[tool.ruff]
line-length = 100
target-version = "py311"
