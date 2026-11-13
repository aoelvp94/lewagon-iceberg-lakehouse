#!/bin/bash
# Idempotent Superset bootstrap: admin user + two database connections.
set -e
superset db upgrade
superset fab create-admin --username admin --firstname Le --lastname Wagon \
  --email admin@example.com --password admin 2>/dev/null || true
superset init
# Postgres gold (written by the Postgres dbt run)
superset set-database-uri -d "Warehouse Postgres" \
  -u "postgresql+psycopg2://lakehouse:lakehouse@warehouse-postgres:5432/warehouse" || true
# DuckDB gold (written by the DuckDB dbt run). Read-only: dbt may hold the write lock.
superset set-database-uri -d "Warehouse DuckDB" \
  -u "duckdb:////data/warehouse.duckdb?access_mode=READ_ONLY" || true
exec superset run -h 0.0.0.0 -p 8088 --with-threads
