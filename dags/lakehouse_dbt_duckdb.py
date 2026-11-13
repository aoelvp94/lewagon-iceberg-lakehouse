"""dbt medallion on DuckDB, reading Iceberg directly (dbt-duckdb iceberg plugin).

Runs automatically after ``lakehouse_ingest`` (asset schedule). One Airflow task
per dbt model + its tests, rendered by Cosmos.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag
from cosmos import DbtTaskGroup

from include.assets import ICEBERG_RAW
from include.cosmos_config import execution_config, profile, project_config, render_config


@dag(
    schedule=[ICEBERG_RAW],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # DuckDB = one writer per file
    tags=["lakehouse", "dbt", "duckdb", "cosmos"],
    doc_md=__doc__,
)
def lakehouse_dbt_duckdb():
    DbtTaskGroup(
        group_id="dbt_duckdb",
        project_config=project_config,
        profile_config=profile("duckdb"),
        execution_config=execution_config,
        render_config=render_config,
        operator_args={"install_deps": False},
    )


lakehouse_dbt_duckdb()
