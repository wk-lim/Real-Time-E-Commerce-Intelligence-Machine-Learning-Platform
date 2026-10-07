from datetime import datetime, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="phase5_airflow_smoke",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["phase5"],
)
def phase5_airflow_smoke():
    @task
    def check_runtime():
        print("Phase 5 Airflow smoke test passed")

    check_runtime()

phase5_airflow_smoke()