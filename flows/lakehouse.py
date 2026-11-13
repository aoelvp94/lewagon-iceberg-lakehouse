"""Prefect 3 version of the pipeline: same include/lakehouse code, same dbt project.

    ingest (one task per entity, run concurrently)
      └─> dbt build --target duckdb            (DuckDB reads Iceberg directly)
      └─> Iceberg -> Postgres copy -> dbt build --target postgres

`python flows/lakehouse.py` serves the deployment `lakehouse-pipeline/lakehouse`
(the `runner` container does this on start-up).
"""

from __future__ import annotations

import os
from pathlib import Path

import fsspec
import psycopg2
from prefect import flow, get_run_logger, task, unmapped
from prefect.artifacts import create_table_artifact
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from include.lakehouse.catalog import get_catalog
from include.lakehouse.entities import ENTITIES
from include.lakehouse.ingest import land_to_iceberg
from include.lakehouse.serve import iceberg_to_postgres

REPO = Path(__file__).resolve().parents[1]
DBT_DIR = REPO / "include" / "dbt" / "shop"


def landing_fs():
    fs = fsspec.filesystem(
        "s3",
        key=os.environ["MINIO_ROOT_USER"],
        secret=os.environ["MINIO_ROOT_PASSWORD"],
        client_kwargs={"endpoint_url": os.environ.get("MINIO_ENDPOINT", "http://minio:9000")},
    )
    return fs, os.environ.get("LANDING_URI", "s3://landing/shop").removeprefix("s3://")


# ---------------------------------------------------------------- transfer tasks
@task(retries=2, retry_delay_seconds=5, task_run_name="landing-to-iceberg-{entity}")
def landing_to_iceberg(entity: str, mode: str = "append") -> dict:
    fs, root = landing_fs()
    result = land_to_iceberg(fs=fs, landing_root=root, entity=ENTITIES[entity],
                             catalog=get_catalog(), mode=mode)
    if result.schema_changes:
        get_run_logger().warning("Schema evolved on %s: %s", entity, result.schema_changes)
    return result.as_dict()


@task(task_run_name="iceberg-to-postgres-{table_identifier}")
def iceberg_to_pg(table_identifier: str) -> int:
    conn = psycopg2.connect(
        host=os.environ["WAREHOUSE_PG_HOST"], port=os.environ.get("WAREHOUSE_PG_PORT", "5432"),
        user=os.environ["WAREHOUSE_PG_USER"], password=os.environ["WAREHOUSE_PG_PASSWORD"],
        dbname=os.environ["WAREHOUSE_PG_DB"],
    )
    try:
        return iceberg_to_postgres(catalog=get_catalog(), table_identifier=table_identifier, pg_conn=conn)
    finally:
        conn.close()


# ---------------------------------------------------------------- dbt
def dbt_build(target: str) -> None:
    # PrefectDbtRunner turns every dbt node into a Prefect task run (like Cosmos does in Airflow)
    runner = PrefectDbtRunner(settings=PrefectDbtSettings(project_dir=DBT_DIR, profiles_dir=DBT_DIR))
    runner.invoke(["build", "--target", target])


@flow(name="dbt-duckdb")
def dbt_duckdb_flow():
    dbt_build("duckdb")


@flow(name="dbt-postgres")
def dbt_postgres_flow():
    copies = iceberg_to_pg.map([e.table_identifier for e in ENTITIES.values()])
    copies.wait()
    dbt_build("postgres")


# ---------------------------------------------------------------- main flow
@flow(name="lakehouse-pipeline", log_prints=True)
def lakehouse_pipeline(mode: str = "append", run_duckdb: bool = True, run_postgres: bool = True):
    results = landing_to_iceberg.map(list(ENTITIES), mode=unmapped(mode)).result()
    create_table_artifact(
        key="ingest-summary",
        table=[{k: str(v) for k, v in r.items()} for r in results],
        description=f"Landing -> Iceberg ({mode})",
    )
    for r in results:
        print(f"{r['table']}: loaded={r['batches_loaded']} skipped={r['batches_skipped']} "
              f"rows={r['rows_written']} schema_changes={r['schema_changes']}")
    # subflows: DuckDB first (single writer), then Postgres
    if run_duckdb:
        dbt_duckdb_flow()
    if run_postgres:
        dbt_postgres_flow()


if __name__ == "__main__":
    lakehouse_pipeline.serve(name="lakehouse", tags=["lewagon", "iceberg"])
