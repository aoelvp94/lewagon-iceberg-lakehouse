"""Airflow transfer operators for the lakehouse demo.

Naming follows the Airflow providers convention ``<Source>To<Destination>Operator``.
Both are thin wrappers: all logic lives in ``include.lakehouse`` so Prefect and
Kestra run *exactly* the same code.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.sdk import BaseOperator, ObjectStoragePath

if TYPE_CHECKING:
    from airflow.sdk import Context


class LandingToIcebergOperator(BaseOperator):
    """Load landing-zone batches (JSONL / Parquet on object storage) into an Iceberg table.

    :param entity: one of ``include.lakehouse.entities.ENTITIES`` (customers, orders, ...)
    :param source_uri: landing root, e.g. ``s3://landing/shop``
    :param source_conn_id: Airflow connection used to build the fsspec filesystem
    :param mode: ``append`` (keep CDC history, bronze dedups) or ``upsert`` (merge on PK)
    :param catalog_name: pyiceberg catalog name (configured via PYICEBERG_CATALOG__* env vars)
    """

    template_fields: Sequence[str] = ("source_uri", "mode")
    ui_color = "#9fd3f5"

    def __init__(
        self,
        *,
        entity: str,
        source_uri: str,
        source_conn_id: str | None = "minio",
        mode: str = "append",
        catalog_name: str = "lakehouse",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.entity = entity
        self.source_uri = source_uri
        self.source_conn_id = source_conn_id
        self.mode = mode
        self.catalog_name = catalog_name

    def execute(self, context: Context) -> dict:
        from include.lakehouse.catalog import get_catalog
        from include.lakehouse.entities import ENTITIES
        from include.lakehouse.ingest import land_to_iceberg

        root = ObjectStoragePath(self.source_uri, conn_id=self.source_conn_id)
        result = land_to_iceberg(
            fs=root.fs,
            landing_root=root.path,
            entity=ENTITIES[self.entity],
            catalog=get_catalog(self.catalog_name),
            mode=self.mode,  # type: ignore[arg-type]
        )
        self.log.info("Ingest result: %s", result.as_dict())
        if result.schema_changes:
            self.log.warning("Schema evolved: %s", result.schema_changes)
        return result.as_dict()


class IcebergToPostgresOperator(BaseOperator):
    """Mirror the current snapshot of an Iceberg table into a Postgres schema (full refresh)."""

    template_fields: Sequence[str] = ("table_identifier", "target_schema")
    ui_color = "#c7e9c0"

    def __init__(
        self,
        *,
        table_identifier: str,
        postgres_conn_id: str = "warehouse_postgres",
        target_schema: str = "raw",
        catalog_name: str = "lakehouse",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.table_identifier = table_identifier
        self.postgres_conn_id = postgres_conn_id
        self.target_schema = target_schema
        self.catalog_name = catalog_name

    def execute(self, context: Context) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from include.lakehouse.catalog import get_catalog
        from include.lakehouse.serve import iceberg_to_postgres

        conn = PostgresHook(postgres_conn_id=self.postgres_conn_id).get_conn()
        try:
            return iceberg_to_postgres(
                catalog=get_catalog(self.catalog_name),
                table_identifier=self.table_identifier,
                pg_conn=conn,
                target_schema=self.target_schema,
            )
        finally:
            conn.close()
