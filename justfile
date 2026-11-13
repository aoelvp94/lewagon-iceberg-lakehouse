# Le Wagon · Iceberg lakehouse demo · Airflow (Astro CLI) + Cosmos + Superset
# `just` lists every recipe. Host tools needed: astro, docker, uv, just.

set shell := ["bash", "-euo", "pipefail", "-c"]

# Host-side view of the stack (scripts run on your laptop, not in containers)
export MINIO_ENDPOINT := "http://localhost:9000"
export LANDING_URI := "s3://landing/shop"

default:
    @just --list --unsorted

# ─── lifecycle ──────────────────────────────────────────────────────────────

# First time only: create .env from the example
setup:
    @test -f .env || cp .env.example .env
    @mkdir -p include/data
    @echo "✔ .env ready. Next: just up"

# Start Airflow + MinIO + Postgres + Superset
up: setup build-images
    astro dev start --wait 5m
    @just urls

# Build the images used by docker-compose.override.yml (the Astro CLI does not build them)
build-images:
    docker build -q -t lewagon/superset:local infra/superset
    docker build -q -t lewagon/jupyter:local infra/jupyter

# Stop containers (keeps data)
down:
    astro dev stop

# Stop and delete ALL data (Iceberg files, catalog, DuckDB, Superset)
nuke:
    astro dev kill
    rm -f include/data/*.duckdb include/data/*.wal

# Rebuild the image after changing requirements / Dockerfile
restart:
    astro dev restart

logs:
    astro dev logs -f

# Where everything lives
urls:
    @echo "Airflow    http://localhost:8080"
    @echo "MinIO      http://localhost:9001   (minio / minio123)"
    @echo "Superset   http://localhost:8088   (admin / admin)"
    @echo "Postgres   localhost:5433          (lakehouse / lakehouse, db=warehouse)"
    @echo "Jupyter    http://localhost:8888/lab/tree/notebooks/explore_lakehouse.ipynb"

# ─── data generation + ingestion ────────────────────────────────────────────

# Drop a source batch in the landing zone: initial | updates | schema-evolution | late-arriving | all
generate scenario="initial":
    uv run scripts/generate_data.py {{ scenario }}

# Run the transfer operators (landing -> Iceberg). mode: append | upsert
ingest mode="append":
    astro dev run dags unpause lakehouse_ingest >/dev/null
    astro dev run dags unpause lakehouse_dbt_duckdb >/dev/null
    astro dev run dags unpause lakehouse_dbt_postgres >/dev/null
    astro dev run dags trigger lakehouse_ingest --conf '{"mode": "{{ mode }}"}'
    @echo "→ follow it at http://localhost:8080 (dbt DAGs start automatically after it)"

# ─── classroom demos ────────────────────────────────────────────────────────

# 1) first load: 200 customers, 40 products, 1000 orders
demo-initial: (generate "initial") ingest

# 2) updates + deletes + duplicates: raw grows, bronze stays 1 row per key
demo-updates: (generate "updates") ingest

# 3) new column loyalty_tier + quantity int -> long (Parquet batch)
demo-schema-evolution: (generate "schema-evolution") ingest

# 4) old events arriving late: dedup must use updated_at, not arrival order
demo-late-arriving: (generate "late-arriving") ingest

# Re-run a scenario: nothing happens (batch already committed -> idempotent)
demo-idempotency: (generate "initial") ingest

# Merge in place with Iceberg upsert instead of append. Run on a fresh stack: just nuke && just up
demo-upsert: (generate "initial") (generate "updates") (ingest "upsert")

# ─── look inside Iceberg ────────────────────────────────────────────────────

tables:
    uv run scripts/iceberg_inspect.py tables

# Snapshot list: one per batch, with batch id and row counts
snapshots table="customers":
    uv run scripts/iceberg_inspect.py snapshots raw.{{ table }}

# Current schema (field ids survive renames / promotions)
schema table="customers":
    uv run scripts/iceberg_inspect.py schema raw.{{ table }}

# Every schema version the table has had
schema-history table="customers":
    uv run scripts/iceberg_inspect.py schema-history raw.{{ table }}

# Row count and columns at each snapshot
time-travel table="customers":
    uv run scripts/iceberg_inspect.py time-travel raw.{{ table }}

# All raw versions of one key, e.g. just history customers "customer_id = 7"
history table="customers" where="customer_id = 7":
    uv run scripts/iceberg_inspect.py peek raw.{{ table }} --where "{{ where }}"

# ─── query the medallion layers ─────────────────────────────────────────────

# SQL against the Postgres warehouse
psql sql="select * from gold.gold_order_status_funnel":
    docker exec -i $(docker ps -qf "name=warehouse-postgres") psql -U lakehouse -d warehouse -c "{{ sql }}"

# SQL against the DuckDB warehouse file (read-only)
duck sql="select * from gold.gold_order_status_funnel":
    uv run --with duckdb python -c "import duckdb,sys; print(duckdb.connect('include/data/warehouse.duckdb', read_only=True).sql(sys.argv[1]))" "{{ sql }}"

# Compare both engines: same model, same numbers?
compare model="gold.gold_order_status_funnel":
    @echo "── DuckDB"; just duck "select * from {{ model }} order by 1"
    @echo "── Postgres"; just psql "select * from {{ model }} order by 1"

# Show dedup at work: rows in raw vs bronze
dedup-check:
    just psql "select 'raw' as layer, count(*) from raw.customers union all select 'bronze', count(*) from bronze.bronze_customers"

# ─── notebooks ──────────────────────────────────────────────────────────────

# Open the exploration notebook (jupysql on Postgres + DuckDB, files in MinIO)
notebook:
    open "http://localhost:8888/lab/tree/notebooks/explore_lakehouse.ipynb" || echo "http://localhost:8888/lab/tree/notebooks/explore_lakehouse.ipynb"

# Run the notebook headless inside the container (checks every snippet still works)
notebook-check:
    docker exec $(docker ps -qf "name=jupyter") jupyter nbconvert --to notebook --execute \
      --output /tmp/explore_lakehouse.out.ipynb notebooks/explore_lakehouse.ipynb

# ─── dbt from the host (handy to debug models without Airflow) ──────────────

# e.g. just dbt build --target postgres --select silver
dbt *args:
    cd include/dbt/shop && \
    DUCKDB_PATH=../../data/warehouse.duckdb WAREHOUSE_PG_HOST=localhost WAREHOUSE_PG_PORT=5433 \
    PYICEBERG_CATALOG__LAKEHOUSE__TYPE=sql \
    PYICEBERG_CATALOG__LAKEHOUSE__URI=postgresql+psycopg2://lakehouse:lakehouse@localhost:5433/iceberg_catalog \
    PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE=s3://warehouse \
    PYICEBERG_CATALOG__LAKEHOUSE__S3__ENDPOINT=http://localhost:9000 \
    PYICEBERG_CATALOG__LAKEHOUSE__S3__ACCESS_KEY_ID=minio \
    PYICEBERG_CATALOG__LAKEHOUSE__S3__SECRET_ACCESS_KEY=minio123 \
    PYICEBERG_CATALOG__LAKEHOUSE__S3__REGION=us-east-1 \
    uv run --with "dbt-core>=1.11,<2" --with "dbt-duckdb>=1.10" --with "dbt-postgres>=1.10" \
      --with "pyiceberg[sql-postgres,pyarrow,s3fs]>=0.11.1" dbt {{ args }} --profiles-dir .

# ─── tests ──────────────────────────────────────────────────────────────────

# Unit tests for the ingestion logic (no Docker: SQLite catalog + local files)
test:
    uv run --with "pyiceberg[sql-sqlite,pyarrow]>=0.11.1" --with fsspec --with pytest \
      pytest tests/lakehouse -q

# DAG integrity tests inside the Astro image
test-dags:
    astro dev pytest tests/dags

# dbt data tests + unit tests on both engines
test-dbt:
    just dbt build --target duckdb --select "test_type:unit"
    just dbt test --target duckdb
    just dbt test --target postgres

# ─── run the whole story end to end ─────────────────────────────────────────

demo-all:
    just demo-initial
    @read -p "Initial load triggered. Check Airflow, then press Enter for updates… " _
    just demo-updates
    @read -p "Press Enter for schema evolution… " _
    just demo-schema-evolution
    @read -p "Press Enter for late-arriving events… " _
    just demo-late-arriving
    just snapshots customers
