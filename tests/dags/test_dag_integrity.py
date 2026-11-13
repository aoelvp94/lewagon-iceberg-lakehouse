"""Run inside Astro: `just test-dags` (astro dev pytest)."""

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

EXPECTED = {"lakehouse_ingest", "lakehouse_dbt_duckdb", "lakehouse_dbt_postgres"}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags(dagbag):
    assert EXPECTED <= set(dagbag.dag_ids)


def test_dbt_dags_have_medallion_tasks(dagbag):
    for dag_id in ("lakehouse_dbt_duckdb", "lakehouse_dbt_postgres"):
        ids = " ".join(dagbag.get_dag(dag_id).task_ids)
        for layer in ("bronze_", "dim_customers", "gold_daily_revenue"):
            assert layer in ids, f"{layer} missing in {dag_id}"
