"""Command-line entry points, for orchestrators that run *commands* (Kestra, cron, CI...).

    python -m include.lakehouse.cli ingest --entity customers --mode append
    python -m include.lakehouse.cli to-postgres --table raw.customers

Configuration comes from env vars (see runner/Dockerfile). The last line printed
is JSON so the orchestrator can capture it as an output.
"""

from __future__ import annotations

import argparse
import json
import logging
import os

import fsspec
import psycopg2

from include.lakehouse.catalog import get_catalog
from include.lakehouse.entities import ENTITIES
from include.lakehouse.ingest import land_to_iceberg
from include.lakehouse.serve import iceberg_to_postgres


def _landing():
    fs = fsspec.filesystem(
        "s3",
        key=os.environ["MINIO_ROOT_USER"],
        secret=os.environ["MINIO_ROOT_PASSWORD"],
        client_kwargs={"endpoint_url": os.environ.get("MINIO_ENDPOINT", "http://minio:9000")},
    )
    return fs, os.environ.get("LANDING_URI", "s3://landing/shop").removeprefix("s3://")


def cmd_ingest(args) -> dict:
    fs, root = _landing()
    return land_to_iceberg(fs=fs, landing_root=root, entity=ENTITIES[args.entity],
                           catalog=get_catalog(), mode=args.mode).as_dict()


def cmd_to_postgres(args) -> dict:
    conn = psycopg2.connect(
        host=os.environ["WAREHOUSE_PG_HOST"], port=os.environ.get("WAREHOUSE_PG_PORT", "5432"),
        user=os.environ["WAREHOUSE_PG_USER"], password=os.environ["WAREHOUSE_PG_PASSWORD"],
        dbname=os.environ["WAREHOUSE_PG_DB"],
    )
    try:
        rows = iceberg_to_postgres(catalog=get_catalog(), table_identifier=args.table, pg_conn=conn)
    finally:
        conn.close()
    return {"table": args.table, "rows": rows}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="lakehouse")
    sub = p.add_subparsers(dest="cmd", required=True)
    i = sub.add_parser("ingest")
    i.add_argument("--entity", required=True, choices=list(ENTITIES))
    i.add_argument("--mode", default="append", choices=["append", "upsert"])
    t = sub.add_parser("to-postgres")
    t.add_argument("--table", required=True)
    args = p.parse_args()
    result = {"ingest": cmd_ingest, "to-postgres": cmd_to_postgres}[args.cmd](args)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
