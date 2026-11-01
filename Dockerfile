FROM apache/airflow:2.8.4-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY pyproject.toml README.md /opt/airflow/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e ".[dev]"

COPY src /opt/airflow/src
COPY dags /opt/airflow/dags
COPY configs /opt/airflow/configs
COPY scripts /opt/airflow/scripts

WORKDIR /opt/airflow
