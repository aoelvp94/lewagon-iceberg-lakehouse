# Le Wagon · Iceberg lakehouse demo · Kestra + dbt CLI plugin + Metabase
# `just` lists every recipe. Host tools needed: docker, uv, just, curl.

set shell := ["bash", "-euo", "pipefail", "-c"]

export MINIO_ENDPOINT := "http://localhost:9000"
export LANDING_URI := "s3://landing/shop"
kestra := "http://localhost:8080/api/v1/main"
kestra_auth := "admin@kestra.io:Admin1234"

default:
    @just --list --unsorted

# ─── lifecycle ──────────────────────────────────────────────────────────────

setup:
    @mkdir -p include/data /tmp/kestra-wd
    @echo "✔ nothing else to configure (credentials live in docker-compose.yml)"

# Start Kestra + MinIO + Postgres + Metabase (and build the task image)
up: setup
    docker compose up -d --build
    @just urls
    @echo "Flows in ./flows are synced into Kestra automatically. Then: just metabase-setup"

down:
    docker compose down

# Stop and delete ALL data
nuke:
    docker compose down -v
    rm -f include/data/*.duckdb include/data/*.wal

# Rebuild the task image after changing runner/Dockerfile
rebuild-runner:
    docker compose build lakehouse-runner

logs service="kestra":
    docker compose logs -f {{ service }}

urls:
    @echo "Kestra     http://localhost:8080   (admin@kestra.io / Admin1234)"
    @echo "Metabase   http://localhost:3000   (admin@lewagon.example / LeWagon2026!)"
    @echo "MinIO      http://localhost:9001   (minio / minio123)"
    @echo "Postgres   localhost:5433          (lakehouse / lakehouse, db=warehouse)"

# Push flows through the API (only needed if the file watcher is off)
deploy:
    for f in flows/*.yml; do \
      curl -sfu {{ kestra_auth }} -X POST "{{ kestra }}/flows/import" -F fileUpload=@$f >/dev/null && echo "✔ $f"; \
    done

# Admin user, Postgres connection and a gold dashboard in Metabase
metabase-setup:
    uv run scripts/metabase_setup.py

# Optional: community DuckDB driver so Metabase can read warehouse.duckdb too
metabase-duckdb-driver:
    curl -fL -o infra/metabase/plugins/duckdb.metabase-driver.jar \
      https://github.com/motherduckdb/metabase_duckdb_driver/releases/latest/download/duckdb.metabase-driver.jar
    docker compose restart metabase

# ─── data generation + ingestion ────────────────────────────────────────────

# Drop a source batch in the landing zone: initial | updates | schema-evolution | late-arriving | all
generate scenario="initial":
    uv run scripts/generate_data.py {{ scenario }}

# Execute lakehouse_ingest (waits); its SUCCESS triggers both dbt flows. mode: append | upsert
ingest mode="append":
    curl -sfu {{ kestra_auth }} -X POST "{{ kestra }}/executions/lewagon.lakehouse/lakehouse_ingest?wait=true" \
      -F mode={{ mode }} | uv run python -c "import json,sys; e=json.load(sys.stdin); print('ingest', e['state']['current'], '→ http://localhost:8080/ui/main/executions/lewagon.lakehouse/lakehouse_ingest/' + e['id'])"
    @echo "→ dbt flows start now (Flow triggers). Metabase: http://localhost:3000"

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
    @read -p "Initial load done. Check Kestra + Metabase, then press Enter for updates… " _
    just demo-updates
    @read -p "Press Enter for schema evolution… " _
    just demo-schema-evolution
    @read -p "Press Enter for late-arriving events… " _
    just demo-late-arriving
    just snapshots customers
