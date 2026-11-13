# Le Wagon · Iceberg lakehouse demo · Prefect 3 + prefect-dbt + Streamlit
# `just` lists every recipe. Host tools needed: docker, uv, just.

set shell := ["bash", "-euo", "pipefail", "-c"]

export MINIO_ENDPOINT := "http://localhost:9000"
export LANDING_URI := "s3://landing/shop"

default:
    @just --list --unsorted

# ─── lifecycle ──────────────────────────────────────────────────────────────

setup:
    @mkdir -p include/data
    @echo "✔ nothing else to configure (credentials live in docker-compose.yml)"

# Start Prefect server + runner + MinIO + Postgres + Streamlit
up: setup
    docker compose up -d --build
    @just urls

down:
    docker compose down

# Stop and delete ALL data
nuke:
    docker compose down -v
    rm -f include/data/*.duckdb include/data/*.wal

restart:
    docker compose up -d --build --force-recreate runner streamlit

logs service="runner":
    docker compose logs -f {{ service }}

urls:
    @echo "Prefect    http://localhost:4200"
    @echo "Streamlit  http://localhost:8501"
    @echo "MinIO      http://localhost:9001   (minio / minio123)"
    @echo "Postgres   localhost:5433          (lakehouse / lakehouse, db=warehouse)"

# ─── data generation + ingestion ────────────────────────────────────────────

# Drop a source batch in the landing zone: initial | updates | schema-evolution | late-arriving | all
generate scenario="initial":
    uv run scripts/generate_data.py {{ scenario }}

# Run the deployment: ingest (transfer tasks) -> dbt DuckDB -> copy + dbt Postgres. mode: append | upsert
ingest mode="append":
    docker compose exec runner prefect deployment run 'lakehouse-pipeline/lakehouse' \
      --param mode={{ mode }} --watch
    @echo "→ Streamlit: http://localhost:8501"

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

# ─── dbt from the host (handy to debug models without the orchestrator) ──────────────

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

# dbt data tests + unit tests on both engines
test-dbt:
    just dbt build --target duckdb --select "test_type:unit"
    just dbt test --target duckdb
    just dbt test --target postgres

# ─── run the whole story end to end ─────────────────────────────────────────

demo-all:
    just demo-initial
    @read -p "Initial load done. Check Prefect + Streamlit, then press Enter for updates… " _
    just demo-updates
    @read -p "Press Enter for schema evolution… " _
    just demo-schema-evolution
    @read -p "Press Enter for late-arriving events… " _
    just demo-late-arriving
    just snapshots customers
