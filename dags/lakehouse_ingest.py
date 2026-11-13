"""Landing zone -> Iceberg raw tables, one transfer task per entity.

Trigger it with ``just ingest`` (or any ``just demo-*``). When it succeeds it
updates the ``iceberg_raw`` asset, which starts both dbt DAGs.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.assets import ICEBERG_RAW
from include.lakehouse.entities import ENTITIES
from include.operators.transfers import LandingToIcebergOperator


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lakehouse", "ingest", "iceberg"],
    params={"mode": "append"},  # try "upsert" from the trigger form
    doc_md=__doc__,
)
def lakehouse_ingest():
    @task
    def prepare_catalog() -> None:
        """Catalog tables + `raw` namespace, created once before the parallel loads."""
        from include.lakehouse.catalog import prepare_catalog

        prepare_catalog()

    # Dynamic task mapping: one mapped task instance per entity, created at run time.
    loads = LandingToIcebergOperator.partial(
        task_id="landing_to_iceberg",
        source_uri="{{ var.value.get('landing_uri', 's3://landing/shop') }}",
        source_conn_id="minio",
        mode="{{ params.mode }}",
        map_index_template="{{ task.entity }}",  # label each instance in the UI
    ).expand(entity=list(ENTITIES))

    @task(outlets=[ICEBERG_RAW])
    def summarize(results: list[dict]) -> list[dict]:
        """Receives the return value of every mapped instance."""
        for r in results:
            print(f"{r['table']}: loaded={r['batches_loaded']} skipped={r['batches_skipped']} "
                  f"rows={r['rows_written']} schema_changes={r['schema_changes']}")
        return list(results)

    prepare_catalog() >> loads
    summarize(loads.output)


lakehouse_ingest()
