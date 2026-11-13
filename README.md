# Le Wagon · Lakehouse con Iceberg, dbt y tres orquestadores · rama `prefect`

Pipeline de punta a punta para clase:

**landing (JSONL/Parquet en MinIO) → _transfer operator_ → Apache Iceberg → dbt (bronze · silver · gold) en DuckDB y en Postgres → dashboards**

La lógica de negocio es la misma en todas las ramas; lo único que cambia es el orquestador y la herramienta de BI:

| Rama | Orquestador | dbt | BI |
|---|---|---|---|
| `main` | **Airflow 3.3** (Astro CLI) | Cosmos (`DbtTaskGroup`) | **Superset** |
| `prefect` ← **estás acá** | **Prefect 3** | `prefect-dbt` (`PrefectDbtRunner`) | **Streamlit** |
| `kestra` | **Kestra** | `io.kestra.plugin.dbt.cli.DbtCLI` | **Metabase** |

Todo se maneja con `just` (`just` a secas lista las recetas).

---

## Arquitectura

```mermaid
flowchart LR
    gen["scripts/generate_data.py<br/>(sistema fuente falso)"] -->|batch=000N| landing[(MinIO<br/>s3://landing/shop)]
    landing -->|task landing_to_iceberg| raw[(Iceberg raw.*<br/>s3://warehouse + catálogo SQL en Postgres)]
    raw -->|plugin iceberg de dbt-duckdb| duck[(DuckDB<br/>bronze · silver · gold)]
    raw -->|task iceberg_to_pg| pgraw[(Postgres raw.*)]
    pgraw --> pg[(Postgres<br/>bronze · silver · gold)]
    duck --> bi[Streamlit]
    pg --> bi
```

* **Transfer**: en `main` es el `LandingToIcebergOperator` de Airflow; acá es la task `landing_to_iceberg` (`flows/lakehouse.py`). Las dos llaman a la misma función, `include/lakehouse/ingest.py` (pyarrow + fsspec + pyiceberg, sin pandas).
* **Flujo Prefect**: `lakehouse-pipeline` → tasks de ingesta mapeadas por entidad (en paralelo) → subflow `dbt-duckdb` → subflow `dbt-postgres` (copias Iceberg→Postgres + dbt). `PrefectDbtRunner` convierte cada nodo de dbt en una task, como hace Cosmos en Airflow.
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

* Docker, [uv](https://docs.astral.sh/uv/), [just](https://just.systems)
* ~6 GB de RAM libres para Docker

## Arranque rápido

```bash
just up           # Prefect server + runner + MinIO + Postgres + Streamlit
just demo-initial # genera el batch inicial y corre el deployment (espera a que termine)
```

| Servicio | URL | Usuario |
|---|---|---|
| Prefect | http://localhost:4200 | — |
| Streamlit | http://localhost:8501 | — |
| MinIO | http://localhost:9001 | minio / minio123 |
| Postgres (warehouse) | localhost:5433 | lakehouse / lakehouse |

---

## Guion de clase

Cada demo genera un batch con id fijo y dispara la ingesta. Después de cada uno, mirá Prefect (flow runs + artifact `ingest-summary`), Iceberg y Streamlit.

| # | Comando | Qué pasa | Cómo verlo |
|---|---|---|---|
| 1 | `just demo-initial` | Primer batch: 200 clientes, 40 productos, 1000 pedidos. Se crean las tablas Iceberg. | `just tables` · `just snapshots customers` |
| 2 | `just demo-updates` | Updates (emails, países, estados de pedido, precios), 3 deletes (GDPR) y 5 filas duplicadas exactas. `raw` crece; `bronze` sigue con 1 fila por clave. | `just dedup-check` · `just history customers "customer_id = 7"` · `just time-travel customers` |
| 3 | `just demo-schema-evolution` | La fuente pasa a exportar Parquet: aparece `loyalty_tier` en customers y `quantity` pasa de `int` a `long`. Iceberg evoluciona el esquema sin reescribir datos. | `just schema-history customers` · `just schema order_items` · gold `gold_revenue_by_country_tier` deja de decir `unknown` |
| 4 | `just demo-late-arriving` | Llegan eventos **viejos** después de los nuevos. Bronze debe ignorarlos porque ordena por `updated_at`. | Test singular `assert_bronze_dedup_keeps_latest` en verde · `gold_order_status_funnel` no cambia |
| 5 | `just demo-idempotency` | Re-ejecutar un batch ya cargado no escribe nada (snapshot property). | Artifact `ingest-summary`: `batches_skipped=['0001']` · `just snapshots` sin snapshot nuevo |
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

## Streamlit

`dashboard/app.py`: KPIs, revenue diario, funnel de estados, revenue por tier y top productos. El selector de la barra lateral cambia entre **DuckDB** y **Postgres**: mismos modelos gold, dos motores. Las consultas devuelven tablas Arrow (sin pandas en nuestro código).

> DuckDB admite un solo escritor: si Streamlit muestra un error de _lock_ mientras corre dbt, esperá a que termine el flow y apretá `R`.

## Airflow vs Prefect (para la discusión)

| | Airflow (`main`) | Prefect (esta rama) |
|---|---|---|
| Unidad | DAG declarado, parseado por el scheduler | Funciones Python decoradas (`@flow`, `@task`) |
| Fan-out por entidad | lista de operadores (o dynamic task mapping) | `task.map(...)` |
| Encadenar dbt tras la ingesta | Assets (data-aware scheduling) entre DAGs | subflows dentro del mismo flow |
| dbt | Cosmos: 1 task por modelo + tests | `PrefectDbtRunner`: 1 task por nodo |
| Reutilización | operador custom (`LandingToIcebergOperator`) | task que envuelve la misma función |

## Estructura

```
├── flows/                     # Prefect: flow + Dockerfile del runner
├── dashboard/                 # Streamlit
├── include/
│   ├── lakehouse/             # lógica compartida (sin orquestador)
│   ├── dbt/shop/              # proyecto dbt (bronze/silver/gold, 2 targets)
│   └── data/                  # warehouse.duckdb (ignorado por git)
├── scripts/                   # generador de datos + inspector de Iceberg (uv)
├── infra/                     # init de Postgres/MinIO
├── tests/                     # pytest (ingesta)
├── docker-compose.yml
└── justfile
```

## Versiones

Prefect 3 · prefect-dbt ≥ 0.7 · Streamlit · pyiceberg ≥ 0.11.1 · dbt-core 1.x + dbt-duckdb + dbt-postgres · DuckDB ≥ 1.5.4 · Postgres 18.

> dbt Fusion (v2) todavía no se usa acá: `prefect-dbt` y el plugin de Iceberg de dbt-duckdb corren sobre dbt Core.
