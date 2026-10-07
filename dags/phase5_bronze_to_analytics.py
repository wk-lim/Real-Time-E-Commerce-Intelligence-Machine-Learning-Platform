from datetime import datetime, timezone
import subprocess
import sys

from airflow.sdk import dag, task


PROJECT_DIR = "/opt/project"


@dag(
    dag_id="phase5_bronze_to_analytics",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["phase5"],
)
def phase5_bronze_to_analytics():
    @task
    def load_products():
        subprocess.run(
            [
                sys.executable,
                "-m",
                "ecommerce_intelligence.storage.incremental_loader",
                "--source", "products",
                "--max-new-files", "1",
                "--max-file-rows", "10000",
                "--apply",
            ],
            cwd=PROJECT_DIR,
            check=True,
        )

    @task
    def load_events():
        subprocess.run(
            [
                sys.executable,
                "-m",
                "ecommerce_intelligence.storage.incremental_loader",
                "--source", "events",
                "--max-new-files", "1",
                "--max-file-rows", "10000",
                "--apply",
            ],
            cwd=PROJECT_DIR,
            check=True,
        )

    @task
    def build_analytics():
        subprocess.run(
            [
                "dbt", "build",
                "--project-dir", PROJECT_DIR,
                "--profiles-dir", PROJECT_DIR,
            ],
            cwd=PROJECT_DIR,
            check=True,
        )

    load_products() >> load_events() >> build_analytics()

phase5_bronze_to_analytics()