.PHONY: env up down init api airflow logs test lint clean

env:
	cp .env.example .env

up:
	docker compose up -d --build

down:
	docker compose down

init:
	docker compose exec api python scripts/init_db.py

api:
	@echo "API:     http://localhost:8000/docs"
	@echo "Airflow: http://localhost:8080 (admin/admin)"

airflow:
	@echo "Airflow: http://localhost:8080 (admin/admin)"

logs:
	docker compose logs -f api

test:
	.venv/bin/pytest --cov=src

lint:
	.venv/bin/ruff check src tests scripts dags

clean:
	rm -rf airflow_home postgres_data parquet_data
