"""Optional Airflow DAG for the nightly analytical job.

The batch job is deliberately a plain `spark-submit` with a `--days` argument,
so orchestrating it is a scheduling concern and nothing more. This DAG drives
the same container `make batch` runs.

To use it, point an existing Airflow at this folder and give the worker access
to the Docker socket (`/var/run/docker.sock`). Without Airflow, either:

    make batch                                   # on demand
    0 2 * * *  cd /path/to/repo && make batch    # a two-line crontab
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

PROJECT_DIR = os.getenv("GDEP_PROJECT_DIR", "/opt/ghana-dep")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
}

with DAG(
    dag_id="ghana_dep_nightly_batch",
    description="Daily OHLC, cedi trend, cocoa seasonality and weather correlations",
    default_args=default_args,
    # 02:00 UTC - after the US close, before the Ghanaian working day
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ghana", "spark", "batch"],
) as dag:

    # The job is idempotent per partition, so reprocessing a 30-day window
    # every night is a feature: late-arriving ticks get folded in for free.
    daily_batch = BashOperator(
        task_id="spark_daily_batch",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "docker compose run --rm spark-batch "
            "spark-submit --driver-memory 1g /opt/app/batch/daily_batch.py --days 30"
        ),
    )

    verify = BashOperator(
        task_id="verify_rows_written",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "docker compose exec -T postgres psql -U ghana -d ghana_dep -tAc "
            "\"SELECT status FROM market.pipeline_runs ORDER BY started_at DESC LIMIT 1\" "
            "| grep -q succeeded"
        ),
    )

    daily_batch >> verify
