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
    loads = [
        LandingToIcebergOperator(
            task_id=f"landing_to_iceberg__{name}",
            entity=name,
            source_uri="{{ var.value.get('landing_uri', 's3://landing/shop') }}",
            source_conn_id="minio",
            mode="{{ params.mode }}",
        )
        for name in ENTITIES
    ]

    @task(outlets=[ICEBERG_RAW])
    def summarize(**context) -> dict:
        ti = context["ti"]
        results = {t.task_id: ti.xcom_pull(task_ids=t.task_id) for t in loads}
        for task_id, r in results.items():
            print(f"{task_id}: loaded={r['batches_loaded']} skipped={r['batches_skipped']} "
                  f"rows={r['rows_written']} schema_changes={r['schema_changes']}")
        return results

    loads >> summarize()


lakehouse_ingest()
