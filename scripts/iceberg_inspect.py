# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[sql-postgres,pyarrow,s3fs]>=0.11"]
# ///
"""Look inside the Iceberg tables from your laptop (talks to MinIO + the Postgres catalog).

    uv run scripts/iceberg_inspect.py tables
    uv run scripts/iceberg_inspect.py schema raw.customers
    uv run scripts/iceberg_inspect.py snapshots raw.customers
    uv run scripts/iceberg_inspect.py time-travel raw.customers       # rows per snapshot
    uv run scripts/iceberg_inspect.py schema-history raw.order_items  # schema per snapshot
    uv run scripts/iceberg_inspect.py peek raw.customers --where "customer_id = 7"
"""

from __future__ import annotations

import argparse
import datetime as dt
import os

from pyiceberg.catalog import load_catalog

HOST_DEFAULTS = {
    "type": "sql",
    "uri": "postgresql+psycopg2://lakehouse:lakehouse@localhost:5433/iceberg_catalog",
    "warehouse": "s3://warehouse",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": os.getenv("MINIO_ROOT_USER", "minio"),
    "s3.secret-access-key": os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
    "s3.region": "us-east-1",
}


def catalog():
    # env vars (PYICEBERG_CATALOG__LAKEHOUSE__*) win over the host defaults
    env_set = any(k.startswith("PYICEBERG_CATALOG__LAKEHOUSE__") for k in os.environ)
    return load_catalog("lakehouse", **({} if env_set else HOST_DEFAULTS))


def summary(snapshot) -> dict:
    """Snapshot summary as a plain dict (pyiceberg's ``Summary`` keeps custom keys apart)."""
    sm = snapshot.summary
    return {"operation": str(sm.operation.value), **sm.additional_properties} if sm else {}


def when(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).strftime("%Y-%m-%d %H:%M:%S")


def cmd_tables(cat, _args):
    for ns in cat.list_namespaces():
        for ident in cat.list_tables(ns):
            print(".".join(ident))


def cmd_schema(cat, args):
    print(cat.load_table(args.table).schema())


def cmd_snapshots(cat, args):
    t = cat.load_table(args.table)
    print(f"{'snapshot_id':>20}  {'committed (UTC)':19}  {'op':8} {'batch':6} {'+rows':>7} {'total':>7}  schema")
    for s in t.snapshots():
        sm = summary(s)
        print(f"{s.snapshot_id:>20}  {when(s.timestamp_ms)}  {sm.get('operation', ''):8} "
              f"{sm.get('lakehouse.batch-id', ''):6} {sm.get('added-records', ''):>7} "
              f"{sm.get('total-records', ''):>7}  {s.schema_id}")


def cmd_time_travel(cat, args):
    t = cat.load_table(args.table)
    for s in t.snapshots():
        rows = t.scan(snapshot_id=s.snapshot_id).to_arrow()
        print(f"snapshot {s.snapshot_id} @ {when(s.timestamp_ms)} -> {rows.num_rows} rows, "
              f"columns={rows.column_names}")


def cmd_schema_history(cat, args):
    t = cat.load_table(args.table)
    for schema in t.metadata.schemas:
        print(f"--- schema_id={schema.schema_id}")
        print(schema)


def cmd_peek(cat, args):
    t = cat.load_table(args.table)
    scan = t.scan(row_filter=args.where, limit=args.limit) if args.where else t.scan(limit=args.limit)
    for row in scan.to_arrow().to_pylist():  # one line per row, easier to read than Arrow's columns
        print({k: (v.isoformat(sep=" ") if isinstance(v, dt.datetime) else v) for k, v in row.items()})


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("tables")
    for name in ("schema", "snapshots", "time-travel", "schema-history", "peek"):
        sp = sub.add_parser(name)
        sp.add_argument("table")
        if name == "peek":
            sp.add_argument("--where")
            sp.add_argument("--limit", type=int, default=20)
    args = p.parse_args()
    handlers = {"tables": cmd_tables, "schema": cmd_schema, "snapshots": cmd_snapshots,
                "time-travel": cmd_time_travel, "schema-history": cmd_schema_history, "peek": cmd_peek}
    handlers[args.cmd](catalog(), args)


if __name__ == "__main__":
    main()
