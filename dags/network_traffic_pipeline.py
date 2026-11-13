import subprocess

from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import dag, task


ROOT = Path(__file__).resolve().parent.parent
PYTHON = ROOT / ".venv" / "bin" / "python"


def run_module(module):
    subprocess.run(
        [str(PYTHON), "-m", module],
        cwd=ROOT,
        check=True,
    )

@dag(
    dag_id="network_traffic_pipeline",
    description="Network traffic data quality ETL pipeline",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["etl", "postgresql", "data-quality"],
)

def network_traffic_pipeline():

    @task
    def load_raw():
        run_module("pipeline.load")

    @task
    def transform():
        run_module("pipeline.transform")

    @task
    def quality():
        run_module("pipeline.quality")

    @task
    def build_mart():
        run_module("pipeline.mart")

    raw = load_raw()
    clean = transform()

    dq = quality()
    mart = build_mart()

    raw >> clean
    clean >> [dq, mart]

network_traffic_pipeline()
