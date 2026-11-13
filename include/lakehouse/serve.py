"""Iceberg ``raw`` table -> Postgres ``raw`` schema (full refresh mirror).

Two ways to feed dbt in this repo:

* DuckDB reads Iceberg *directly* (dbt-duckdb iceberg plugin) -> "lakehouse" style.
* Postgres cannot read Iceberg natively, so we *copy* the current snapshot into
  Postgres first -> classic "load the warehouse" style.

The copy streams Arrow record batches as CSV into ``COPY ... FROM STDIN``.
"""

from __future__ import annotations

import io
import logging

import pyarrow as pa
import pyarrow.csv as pa_csv
from pyiceberg.catalog import Catalog

log = logging.getLogger(__name__)


def _pg_type(t: pa.DataType) -> str:
    if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t):
        return "integer"
    if pa.types.is_integer(t):
        return "bigint"
    if pa.types.is_floating(t):
        return "double precision"
    if pa.types.is_decimal(t):
        return f"numeric({t.precision},{t.scale})"
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_timestamp(t):
        return "timestamptz" if t.tz else "timestamp"
    if pa.types.is_date(t):
        return "date"
    return "text"


def iceberg_to_postgres(
    *,
    catalog: Catalog,
    table_identifier: str,
    pg_conn,  # a DB-API connection (psycopg2 from PostgresHook, or psycopg2.connect)
    target_schema: str = "raw",
    target_table: str | None = None,
    snapshot_id: int | None = None,
) -> int:
    """Replace ``target_schema.target_table`` with the Iceberg snapshot. Returns row count."""
    table = catalog.load_table(table_identifier)
    target_table = target_table or table_identifier.split(".")[-1]
    reader = table.scan(snapshot_id=snapshot_id).to_arrow_batch_reader()
    schema = reader.schema
    columns_ddl = ", ".join(f'"{f.name}" {_pg_type(f.type)}' for f in schema)
    fq = f'"{target_schema}"."{target_table}"'
    tmp = f'"{target_schema}"."{target_table}__loading"'

    rows = 0
    with pg_conn.cursor() as cur:
        cur.execute(f'create schema if not exists "{target_schema}"')
        cur.execute(f"drop table if exists {tmp}")
        cur.execute(f"create table {tmp} ({columns_ddl})")
        for batch in reader:
            buf = io.BytesIO()
            pa_csv.write_csv(pa.Table.from_batches([batch]), buf)
            buf.seek(0)
            cur.copy_expert(f"copy {tmp} from stdin with (format csv, header true)", buf)
            rows += batch.num_rows
        # swap atomically so dashboards never see a half-loaded table
        cur.execute(f"drop table if exists {fq} cascade")
        cur.execute(f'alter table {tmp} rename to "{target_table}"')
    pg_conn.commit()
    log.info("Copied %s rows from %s into %s", rows, table_identifier, fq)
    return rows
