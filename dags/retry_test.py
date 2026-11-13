from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import dag, task, get_current_context


ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = ROOT / ".airflow" / "retry_tests"


@dag(
    dag_id="retry_test",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["testing", "reliability"],
)
def retry_test():

    @task
    def simulate_temporary_failure():
        context = get_current_context()

        run_id = context["run_id"]

        STATE_DIR.mkdir(parents=True, exist_ok=True)

        # Use the DAG run ID to distinguish separate executions.
        import hashlib

        run_hash = hashlib.sha256(
            run_id.encode()
        ).hexdigest()

        marker = STATE_DIR / run_hash

        if not marker.exists():
            marker.touch()
            print("Simulating a temporary failure.")
            raise RuntimeError("Temporary failure")

        print("Retry successful!")

    simulate_temporary_failure()

retry_test()
