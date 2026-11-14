# Le Wagon · Iceberg lakehouse demo · Kestra + dbt CLI plugin + Metabase
# `just` lists every recipe. Host tools needed: docker, uv, just, curl.

set shell := ["bash", "-euo", "pipefail", "-c"]
# .env holds the host ports (written by `just side-by-side`); docker compose reads it too
set dotenv-load

minio_port := env("MINIO_PORT", "9000")
minio_console_port := env("MINIO_CONSOLE_PORT", "9001")
pg_port := env("PG_PORT", "5433")
kestra_port := env("KESTRA_PORT", "8080")
metabase_port := env("METABASE_PORT", "3000")
jupyter_port := env("JUPYTER_PORT", "8888")
kestra_auth := "admin@kestra.io:Admin1234"

# Host-side view of the stack (scripts run on your laptop, not in containers)
export MINIO_ENDPOINT := "http://localhost:" + minio_port
export LANDING_URI := "s3://landing/shop"
export KESTRA_URL := "http://localhost:" + kestra_port
export METABASE_URL := "http://localhost:" + metabase_port
export PYICEBERG_CATALOG__LAKEHOUSE__TYPE := "sql"
export PYICEBERG_CATALOG__LAKEHOUSE__URI := "postgresql+psycopg2://lakehouse:lakehouse@localhost:" + pg_port + "/iceberg_catalog"
export PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE := "s3://warehouse"
export PYICEBERG_CATALOG__LAKEHOUSE__S3__ENDPOINT := MINIO_ENDPOINT
export PYICEBERG_CATALOG__LAKEHOUSE__S3__ACCESS_KEY_ID := "minio"
export PYICEBERG_CATALOG__LAKEHOUSE__S3__SECRET_ACCESS_KEY := "minio123"
export PYICEBERG_CATALOG__LAKEHOUSE__S3__REGION := "us-east-1"

default:
    @just --list --unsorted

# ─── lifecycle ──────────────────────────────────────────────────────────────

setup:
    @mkdir -p include/data /tmp/kestra-wd
    @echo "✔ nothing else to configure (credentials live in docker-compose.yml)"

# Start Kestra + MinIO + Postgres + Metabase + Jupyter (and build the task image)
up: setup
    docker compose up -d --build
    @echo "waiting for Kestra…"; until curl -sfu {{ kestra_auth }} -o /dev/null "$KESTRA_URL/api/v1/main/flows/search"; do sleep 3; done
    @just deploy
    @just urls
    @echo "Then: just demo-initial"

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
    @echo "Kestra     http://localhost:{{ kestra_port }}   (admin@kestra.io / Admin1234)"
    @echo "Metabase   http://localhost:{{ metabase_port }}   (admin@lewagon.example / LeWagon2026!)"
    @echo "MinIO      http://localhost:{{ minio_console_port }}   (minio / minio123)"
    @echo "Postgres   localhost:{{ pg_port }}          (lakehouse / lakehouse, db=warehouse)"
    @echo "Jupyter    http://localhost:{{ jupyter_port }}/lab/tree/notebooks/explore_lakehouse.ipynb"

# Run next to the Airflow (main) and Prefect stacks: move every host port to 2xxxx. Then: just up
side-by-side:
    printf 'MINIO_PORT=29000\nMINIO_CONSOLE_PORT=29001\nPG_PORT=25433\nKESTRA_PORT=28080\nMETABASE_PORT=23000\nJUPYTER_PORT=28888\n' > .env
    @echo "✔ .env written: ports 2xxxx. Run: just down && just up"

# Back to the standard ports (when this is the only stack running). Then: just up
default-ports:
    rm -f .env
    @echo "✔ standard ports. Run: just down && just up"

# Is every system up, and is there data in each layer? One line per check.
check:
    #!/usr/bin/env bash
    ok()  { printf "  \033[32m✔\033[0m %-22s %s\n" "$1" "$2"; }
    ko()  { printf "  \033[31m✘\033[0m %-22s %s\n" "$1" "$2"; }
    http() { [ "$(curl -s -o /dev/null -w '%{http_code}' ${3:-} "$2")" = 200 ] && ok "$1" "$2" || ko "$1" "$2 (not answering: just up)"; }
    echo "Services"
    http "Kestra"    http://localhost:{{ kestra_port }}/api/v1/main/flows/search "-u {{ kestra_auth }}"
    http "MinIO"     http://localhost:{{ minio_port }}/minio/health/live
    http "Metabase"  http://localhost:{{ metabase_port }}/api/health
    http "Jupyter"   http://localhost:{{ jupyter_port }}/api
    docker compose exec -T warehouse-postgres pg_isready -q -U lakehouse -d warehouse 2>/dev/null && ok "Postgres" "localhost:{{ pg_port }}" || ko "Postgres" "localhost:{{ pg_port }}"
    echo "Data"
    n=$(uv run -q scripts/iceberg_inspect.py tables 2>/dev/null | grep -c '^raw\.')
    [ "$n" -ge 4 ] && ok "Iceberg raw tables" "$n tables" || ko "Iceberg raw tables" "$n tables (just demo-initial)"
    n=$(docker compose exec -T warehouse-postgres psql -tAq -U lakehouse -d warehouse -c "select count(*) from information_schema.tables where table_schema = 'gold'" 2>/dev/null)
    [ "${n:-0}" -ge 5 ] && ok "Postgres gold" "$n tables" || ko "Postgres gold" "${n:-0} tables (wait for lakehouse_dbt_postgres)"
    n=$(uv run -q --with duckdb python -c "import duckdb; print(duckdb.connect('include/data/warehouse.duckdb', read_only=True).sql(\"select count(*) from information_schema.tables where table_schema = 'gold'\").fetchall()[0][0])" 2>/dev/null)
    [ "${n:-0}" -ge 5 ] && ok "DuckDB gold" "$n tables" || ko "DuckDB gold" "${n:-0} tables (wait for lakehouse_dbt_duckdb, or file locked by dbt)"

# Push the flows in ./flows to Kestra (create or update). Run it after editing a flow.
deploy:
    @for f in flows/*.yml; do \
      curl -sfu {{ kestra_auth }} -X POST "$KESTRA_URL/api/v1/main/flows/import" -F fileUpload=@$f >/dev/null && echo "✔ $f"; \
    done

# Admin user, Postgres connection, "Shop · Executive" / "Shop · Operations" dashboards (idempotent)
metabase-setup:
    uv run scripts/metabase_setup.py

# Optional: community DuckDB driver so Metabase can read warehouse.duckdb too
metabase-duckdb-driver:
    curl -fL -o infra/metabase/plugins/duckdb.metabase-driver.jar \
      https://github.com/motherduckdb/metabase_duckdb_driver/releases/latest/download/duckdb.metabase-driver.jar
    docker compose restart metabase

# Check out prefect and kestra next to this repo (git worktree), ready to run side by side.
# Then: cd <dir>/lakehouse-prefect && just up   (same in lakehouse-kestra). See docs/orquestadores.md
worktrees dir="..":
    git fetch -q origin prefect kestra
    test -d {{ dir }}/lakehouse-prefect || git worktree add {{ dir }}/lakehouse-prefect prefect
    test -d {{ dir }}/lakehouse-kestra || git worktree add {{ dir }}/lakehouse-kestra kestra
    cd {{ dir }}/lakehouse-prefect && just side-by-side
    cd {{ dir }}/lakehouse-kestra && just side-by-side

# ─── data generation + ingestion ────────────────────────────────────────────

# Drop a source batch in the landing zone: initial | updates | schema-evolution | late-arriving | all
generate scenario="initial":
    uv run scripts/generate_data.py {{ scenario }}

# Execute lakehouse_ingest; its SUCCESS triggers both dbt flows. Waits for all three. mode: append | upsert
ingest mode="append":
    uv run scripts/kestra_run.py --mode {{ mode }}
    @echo "→ Metabase: http://localhost:{{ metabase_port }}"

# ─── classroom demos ────────────────────────────────────────────────────────

# 1) first load: 200 customers, 40 products, 1000 orders (+ Metabase dashboards)
demo-initial: (generate "initial") ingest metabase-setup

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

# SQL against the Postgres warehouse (db=iceberg_catalog to see the Iceberg catalog tables)
psql sql="select * from gold.gold_order_status_funnel" db="warehouse":
    docker compose exec -T warehouse-postgres psql -U lakehouse -d {{ db }} -c "{{ sql }}"

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
    open "http://localhost:{{ jupyter_port }}/lab/tree/notebooks/explore_lakehouse.ipynb" || echo "http://localhost:{{ jupyter_port }}/lab/tree/notebooks/explore_lakehouse.ipynb"

# Run the notebook headless inside the container (checks every snippet still works)
notebook-check:
    docker compose exec -T jupyter jupyter nbconvert --to notebook --execute \
      --output /tmp/explore_lakehouse.out.ipynb notebooks/explore_lakehouse.ipynb

# ─── dbt from the host (handy to debug models without the orchestrator) ──────────────

# e.g. just dbt build --target postgres --select silver
dbt *args:
    cd include/dbt/shop && \
    DUCKDB_PATH=../../data/warehouse.duckdb WAREHOUSE_PG_HOST=localhost WAREHOUSE_PG_PORT={{ pg_port }} \
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
