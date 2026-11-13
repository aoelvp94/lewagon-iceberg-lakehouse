"""Iceberg -> Postgres copy, then the same dbt medallion on Postgres.

Postgres cannot read Iceberg, so a second transfer operator mirrors each raw
table into the ``raw`` schema before Cosmos runs the dbt models.
"""

from __future__ import annotations

import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag
from cosmos import DbtTaskGroup

from include.assets import ICEBERG_RAW
from include.cosmos_config import execution_config, profile, project_config, render_config
from include.lakehouse.entities import ENTITIES
from include.operators.transfers import IcebergToPostgresOperator


@dag(
    schedule=[ICEBERG_RAW],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "dbt", "postgres", "cosmos"],
    doc_md=__doc__,
)
def lakehouse_dbt_postgres():
    # Parallel dbt tasks would race on `create schema if not exists`: create them once here.
    schemas = SQLExecuteQueryOperator(
        task_id="create_medallion_schemas",
        conn_id="warehouse_postgres",
        sql=[f"create schema if not exists {s}" for s in ("bronze", "silver", "gold")],
    )
    copies = [
        IcebergToPostgresOperator(
            task_id=f"iceberg_to_postgres__{e.name}",
            table_identifier=e.table_identifier,
            postgres_conn_id="warehouse_postgres",
            target_schema="raw",
        )
        for e in ENTITIES.values()
    ]
    dbt = DbtTaskGroup(
        group_id="dbt_postgres",
        project_config=project_config,
        profile_config=profile("postgres"),
        execution_config=execution_config,
        render_config=render_config,
        operator_args={"install_deps": False},
    )
    copies >> schemas >> dbt


lakehouse_dbt_postgres()
