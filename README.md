# Le Wagon · Lakehouse con Iceberg, dbt y tres orquestadores · rama `kestra`

Pipeline de punta a punta para clase:

**landing (JSONL/Parquet en MinIO) → _transfer operator_ → Apache Iceberg → dbt (bronze · silver · gold) en DuckDB y en Postgres → dashboards**

La lógica de negocio es la misma en todas las ramas; lo único que cambia es el orquestador y la herramienta de BI:

| Rama | Orquestador | dbt | BI |
|---|---|---|---|
| `main` | **Airflow 3.3** (Astro CLI) | Cosmos (`DbtTaskGroup`) | **Superset** |
| `prefect` | **Prefect 3** | `prefect-dbt` (`PrefectDbtRunner`) | **Streamlit** |
| `kestra` ← **estás acá** | **Kestra** | `io.kestra.plugin.dbt.cli.DbtCLI` | **Metabase** |

Todo se maneja con `just` (`just` a secas lista las recetas).

---

## Arquitectura

```mermaid
flowchart LR
    gen["scripts/generate_data.py<br/>(sistema fuente falso)"] -->|batch=000N| landing[(MinIO<br/>s3://landing/shop)]
    landing -->|flow lakehouse_ingest| raw[(Iceberg raw.*<br/>s3://warehouse + catálogo SQL en Postgres)]
    raw -->|plugin iceberg de dbt-duckdb| duck[(DuckDB<br/>bronze · silver · gold)]
    raw -->|cli to-postgres| pgraw[(Postgres raw.*)]
    pgraw --> pg[(Postgres<br/>bronze · silver · gold)]
    duck -.->|driver opcional| bi[Metabase]
    pg --> bi
```

* **Transfer**: en `main` es el `LandingToIcebergOperator` de Airflow; acá es un comando, `python -m include.lakehouse.cli ingest --entity …`, que llama a la misma función (`include/lakehouse/ingest.py`, pyarrow + fsspec + pyiceberg, sin pandas).
* **Flows Kestra** (`flows/*.yml`, namespace `lewagon.lakehouse`), sincronizados solos desde la carpeta:
  * `lakehouse_ingest`: `ForEach` sobre las 4 entidades, en paralelo.
  * `lakehouse_dbt_duckdb` y `lakehouse_dbt_postgres`: arrancan con un **Flow trigger** cuando la ingesta termina en `SUCCESS` (equivalente a los Assets de Airflow).
* Cada task corre en un contenedor `lakehouse-runner:local` (Docker task runner) con el repo montado en `/app`: Kestra orquesta y el código sigue siendo el mismo.
* **Iceberg**: catálogo SQL de pyiceberg guardado en Postgres; datos y metadata en MinIO. Cada batch = un snapshot (con la propiedad `lakehouse.batch-id`), lo que da idempotencia y _time travel_ por batch.
* **Dos motores, un proyecto dbt** (`include/dbt/shop`, targets `duckdb` y `postgres`):
  * DuckDB lee Iceberg **directo** → estilo _lakehouse_.
  * Postgres no lee Iceberg → se **copia** el snapshot actual al esquema `raw` → estilo _warehouse_ clásico.

### Medallion

| Capa | Qué hace | Modelos |
|---|---|---|
| **bronze** | Dedup: última versión de cada clave de negocio (por `updated_at`, desempate por `_ingested_at`). Los deletes quedan marcados con `is_deleted`. | `bronze_customers`, `bronze_products`, `bronze_orders`, `bronze_order_items` |
| **silver** | Entidades del ERD con FKs testeadas. PII enmascarada para clientes borrados. | `dim_customers`, `dim_products`, `fct_orders`, `fct_order_items` |
| **gold** | Tablas agregadas para los dashboards. | `gold_daily_revenue`, `gold_customer_lifetime_value`, `gold_product_performance`, `gold_revenue_by_country_tier`, `gold_order_status_funnel` |

```mermaid
erDiagram
    dim_customers ||--o{ fct_orders : places
    fct_orders ||--|{ fct_order_items : contains
    dim_products ||--o{ fct_order_items : "sold as"
```

---

## Requisitos

* Docker, [uv](https://docs.astral.sh/uv/), [just](https://just.systems), curl
* ~6 GB de RAM libres para Docker

## Arranque rápido

```bash
just up             # Kestra + MinIO + Postgres + Metabase (+ imagen de tareas)
just demo-initial   # genera el batch inicial y ejecuta lakehouse_ingest
just metabase-setup # admin + conexión Postgres + dashboard "Shop · Lakehouse"
```

| Servicio | URL | Usuario |
|---|---|---|
| Kestra | http://localhost:8080 | admin@kestra.io / Admin1234 |
| Metabase | http://localhost:3000 | admin@lewagon.example / LeWagon2026! |
| MinIO | http://localhost:9001 | minio / minio123 |
| Postgres (warehouse) | localhost:5433 | lakehouse / lakehouse |

---

## Guion de clase

Cada demo genera un batch con id fijo y dispara la ingesta. Después de cada uno, mirá Kestra (Executions → Gantt / Logs), Iceberg y Metabase.

| # | Comando | Qué pasa | Cómo verlo |
|---|---|---|---|
| 1 | `just demo-initial` | Primer batch: 200 clientes, 40 productos, 1000 pedidos. Se crean las tablas Iceberg. | `just tables` · `just snapshots customers` |
| 2 | `just demo-updates` | Updates (emails, países, estados de pedido, precios), 3 deletes (GDPR) y 5 filas duplicadas exactas. `raw` crece; `bronze` sigue con 1 fila por clave. | `just dedup-check` · `just history customers "customer_id = 7"` · `just time-travel customers` |
| 3 | `just demo-schema-evolution` | La fuente pasa a exportar Parquet: aparece `loyalty_tier` en customers y `quantity` pasa de `int` a `long`. Iceberg evoluciona el esquema sin reescribir datos. | `just schema-history customers` · `just schema order_items` · gold `gold_revenue_by_country_tier` deja de decir `unknown` |
| 4 | `just demo-late-arriving` | Llegan eventos **viejos** después de los nuevos. Bronze debe ignorarlos porque ordena por `updated_at`. | Test singular `assert_bronze_dedup_keeps_latest` en verde · `gold_order_status_funnel` no cambia |
| 5 | `just demo-idempotency` | Re-ejecutar un batch ya cargado no escribe nada (snapshot property). | Log de `landing_to_iceberg`: `batches_skipped=['0001']` · `just snapshots` sin snapshot nuevo |
| 6 | `just nuke && just up && just demo-upsert` | Mismos batches 1 y 2 pero con `table.upsert()` (merge por PK) en vez de append, en un stack limpio. Discutir: ¿historia en raw o estado actual? | `just snapshots orders` (operación `overwrite`/`delete`) |

`just demo-all` corre 1→4 con pausas.

### Puntos para discutir

* **Append + dedup en bronze vs. upsert en raw.** Append conserva la historia (auditoría, _time travel_, reprocesar); upsert simplifica bronze pero pierde eventos intermedios.
* **Contratos de esquema.** El operador acepta columnas nuevas y promociones seguras (`int→long`, `float→double`). Un cambio incompatible (p. ej. `customer_id` pasa a string) **falla a propósito** (ver `test_incompatible_type_change_fails_loudly`).
* **Schema evolution aguas abajo.** `bronze` toma las columnas en tiempo de ejecución (`dedup_latest`); `silver` usa `column_or_default` para que `loyalty_tier` funcione antes y después del cambio.
* **DuckDB vs Postgres.** Mismos modelos, mismos números (`just compare`), dos formas de leer Iceberg.

---

## Tests

| Comando | Qué prueba |
|---|---|
| `just test` | Lógica de ingesta sin Docker (catálogo SQLite + archivos locales): carga inicial, idempotencia, historia + time travel, columna nueva, `int→long`, upsert, cambio incompatible. |
| `just test-dbt` | Unit test de dbt (`gold_daily_revenue`) + data tests (unique, not_null, relationships, accepted_values, dedup) en ambos motores. |

## Metabase

`just metabase-setup` crea el admin, la conexión **Warehouse Postgres** y el dashboard **Shop · Lakehouse** (revenue diario, funnel de estados, revenue por tier, top productos) con SQL nativo sobre `gold`.

Para comparar con DuckDB: `just metabase-duckdb-driver` descarga el driver comunitario de MotherDuck y `just metabase-setup` agrega **Warehouse DuckDB** (solo lectura). Es opcional: Metabase no trae DuckDB de fábrica.

## Airflow vs Kestra (para la discusión)

| | Airflow (`main`) | Kestra (esta rama) |
|---|---|---|
| Definición | Python (DAGs) | YAML declarativo |
| Código de negocio | operador custom en el proceso de Airflow | comandos en contenedores (Docker task runner) |
| Fan-out por entidad | lista de operadores / dynamic mapping | `ForEach` + `concurrencyLimit` |
| Encadenar dbt | Assets | Flow trigger con `preconditions` |
| dbt | Cosmos: 1 task por modelo | `DbtCLI`: un `dbt build` (logs por modelo en la UI) |
| Concurrencia DuckDB | `max_active_tasks=1` | `concurrency.limit: 1` |

## Estructura

```
├── flows/                     # Kestra: ingest, dbt DuckDB, dbt Postgres (YAML)
├── runner/                    # imagen donde corre cada task de Kestra
├── include/
│   ├── lakehouse/             # lógica compartida (+ cli.py para orquestadores "de comandos")
│   ├── dbt/shop/              # proyecto dbt (bronze/silver/gold, 2 targets)
│   └── data/                  # warehouse.duckdb (ignorado por git)
├── scripts/                   # generador, inspector de Iceberg, setup de Metabase (uv)
├── infra/                     # init de Postgres/MinIO, plugins de Metabase
├── tests/                     # pytest (ingesta)
├── docker-compose.yml
└── justfile
```

## Versiones

Kestra (latest) · plugin dbt `DbtCLI` · Metabase (latest) · pyiceberg ≥ 0.11.1 · dbt-core 1.x + dbt-duckdb + dbt-postgres · DuckDB ≥ 1.5.4 · Postgres 18.

> dbt Fusion (v2) todavía no se usa acá: el plugin de Iceberg de dbt-duckdb corre sobre dbt Core.
