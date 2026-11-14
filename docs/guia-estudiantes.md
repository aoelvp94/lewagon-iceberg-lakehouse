# Guía práctica: explorá el lakehouse sistema por sistema

Esta guía es para hacerla con las manos en el teclado. Primero recorrés cada sistema, después
corrés los 4 escenarios de datos mirando qué cambia en cada uno, y al final hay desafíos.

> Todos los comandos se corren desde la raíz del repo. `just` sin argumentos lista todas las recetas.

---

## 0. Antes de empezar

Necesitás Docker, [uv](https://docs.astral.sh/uv/) y [just](https://just.systems). Esta es la rama **`prefect`**: el orquestador es Prefect 3 y el BI es Streamlit.

```bash
just setup          # crea .env (solo la primera vez)
just up             # levanta todo (la primera vez tarda: construye imágenes)
just check          # ✔ / ✘ por cada sistema y por cada capa de datos
```

Si es la primera vez, todavía no hay datos: `just check` va a marcar ✘ en *Data*. Está bien.

### Mapa de sistemas

| Sistema | Para qué está | URL | Usuario |
|---|---|---|---|
| **MinIO** | Object storage (S3): archivos fuente y archivos de Iceberg | http://localhost:9001 | minio / minio123 |
| **Prefect** | Orquesta: ingesta → Iceberg → dbt | http://localhost:4200 | — |
| **Postgres** | Catálogo de Iceberg + warehouse (raw, bronze, silver, gold) | localhost:5433 | lakehouse / lakehouse |
| **DuckDB** | El mismo medallion, construido leyendo Iceberg directo | `include/data/warehouse.duckdb` | — |
| **Streamlit** | BI: dashboard en Python sobre `gold.*` | http://localhost:8501 | — |
| **Jupyter** | Notebooks para consultar todo con SQL y Python | http://localhost:8888 | sin token |

`just urls` imprime esta lista.

### El recorrido de un dato

```
generate_data.py ──► MinIO landing/ ──► task landing_to_iceberg ──► Iceberg raw.* (MinIO warehouse/ + catálogo en Postgres)
                                                                          │
                                ┌─────────────────────────────────────────┴──────────────────────┐
                                ▼                                                                ▼
                 dbt + DuckDB (lee Iceberg directo)                       task iceberg_to_pg → Postgres raw.* → dbt + Postgres
                                │                                                                │
                      bronze → silver → gold                                           bronze → silver → gold
                                │                                                                │
                     Streamlit, Jupyter                                            Streamlit, Jupyter
```

---

## 1. Primera carga

```bash
just demo-initial
```

Esto (1) escribe el batch `0001` en MinIO y (2) corre el deployment `lakehouse-pipeline/lakehouse` en
Prefect, esperando a que termine (`--watch`). Mientras tanto, abrí Prefect y mirá cómo corre.

Cuando termine: `just check` debería estar todo en ✔.

---

## 2. Recorré cada sistema

### 2.1 MinIO: los archivos

Entrá a http://localhost:9001 (minio / minio123).

1. Bucket **`landing`** → `shop/customers/batch=0001/part-000.jsonl`. Es lo que "manda" el sistema
   fuente. Descargalo y abrilo: es un JSON por línea, con la columna `_op` (I/U/D, estilo CDC).
2. Bucket **`warehouse`** → `raw/customers/`. Esto es una **tabla Iceberg**:
   * `data/` → archivos Parquet con los datos.
   * `metadata/` → `*.metadata.json` (esquemas, snapshots) + manifests Avro.

**Preguntas**
* ¿Cuántos archivos `*.metadata.json` hay? ¿Por qué hay más de uno si cargaste un solo batch?
  (pista: crear la tabla también es un commit).
* ¿Dónde está guardado *cuál* es el metadata actual? → en el **catálogo** (tabla `iceberg_tables`
  en la base `iceberg_catalog` de Postgres):

```bash
docker exec -i $(docker ps -qf "name=warehouse-postgres") \
  psql -U lakehouse -d iceberg_catalog -c "select table_name, metadata_location from iceberg_tables"
```

### 2.2 Prefect: la orquestación

Entrá a http://localhost:4200 y abrí el flow run más reciente de **lakehouse-pipeline**
(el código está en `flows/lakehouse.py`: son funciones Python con `@flow` y `@task`).

1. `prepare_iceberg_catalog` crea el namespace `raw` una sola vez (si las 4 cargas lo hicieran en paralelo, chocarían).
2. `landing-to-iceberg-<entidad>`: **una task por entidad**, creadas con `landing_to_iceberg.map(...)` y
   corriendo en paralelo. Las 4 llaman a la misma función que usa Airflow en `main` (`include/lakehouse/ingest.py`).
3. **Artifacts** → `ingest-summary`: tabla con lo que cargó cada entidad (batches cargados, salteados, filas, cambios de esquema).
4. Subflow **dbt-duckdb** y subflow **dbt-postgres**: `PrefectDbtRunner` convierte cada modelo y test de dbt
   en una task (como Cosmos en Airflow). `dbt-postgres` primero copia Iceberg → Postgres con `iceberg_to_pg.map(...)`.
5. **Deployments** → `lakehouse-pipeline/lakehouse`: lo sirve el contenedor `runner`. Desde **Run → Custom run**
   podés cambiar el parámetro `mode` (`append` / `upsert`) o apagar uno de los motores.

**Desde la terminal**

```bash
just ingest                        # vuelve a correr el pipeline (no hay batches nuevos → no carga nada)
just logs                          # logs del runner
```

**Preguntas**
* Después de `just ingest` sin datos nuevos, ¿qué dice el artifact `ingest-summary`? ¿Por qué no se duplicaron filas?
  (pista: `lakehouse.batch-id` en los snapshots, sección 2.3).
* En Airflow los DAGs de dbt arrancan por un *asset*; acá son subflows. ¿Qué ventajas tiene cada forma?

### 2.3 Iceberg: tablas con historia

```bash
just tables                 # las 4 tablas raw.*
just snapshots customers    # un snapshot por batch, con el batch id y las filas agregadas
just schema customers       # esquema actual (fíjate en los field ids)
just schema-history order_items
just time-travel orders     # filas y columnas en cada snapshot
just history customers "customer_id = 7"   # todas las versiones de un cliente en raw
```

**Preguntas**
* `raw` es *append-only*: ¿qué pasa con un cliente que se actualiza? ¿Y con uno que se borra?
* ¿Por qué el `snapshot_id` sirve para "volver en el tiempo"?

### 2.4 dbt: el medallion

El proyecto está en `include/dbt/shop/`. Todos los modelos son SQL plano: abrilos.

| Capa | Qué hace | Ejemplo |
|---|---|---|
| bronze | 1 fila por clave: la versión más nueva (`row_number()` por `updated_at`) | `models/bronze/bronze_customers.sql` |
| silver | Entidades del negocio (dimensiones y hechos), con tests de relaciones | `models/silver/fct_orders.sql` |
| gold | Agregados listos para BI | `models/gold/gold_daily_revenue.sql` |

```bash
just dbt show --target postgres --select gold_order_status_funnel    # ver el resultado de un modelo
just dbt show --target duckdb   --select dim_customers --limit 5
just dbt compile --select bronze_customers                           # SQL final en target/compiled/...
just dbt build --target duckdb --select gold                         # reconstruir solo gold
just test-dbt                                                        # unit test + data tests en los 2 motores
```

> DuckDB admite **un solo escritor**: si el subflow de dbt está corriendo, esperá a que termine antes de
> correr dbt desde tu terminal contra DuckDB.

**Preguntas**
* En `bronze_customers.sql`, ¿por qué se ordena por `updated_at desc, _ingested_at desc` y no solo por `_ingested_at`?
* ¿Qué hace el `{% if %}` de `loyalty_tier`? ¿Qué pasaría sin él antes del escenario de schema evolution?

### 2.5 Postgres: el warehouse "clásico"

```bash
just psql "select table_schema, count(*) from information_schema.tables where table_schema in ('raw','bronze','silver','gold') group by 1"
just psql "select * from gold.gold_customer_lifetime_value order by lifetime_value desc limit 5"
just dedup-check            # filas en raw vs bronze
```

Con un cliente SQL (DBeaver, DataGrip, `psql`): `postgresql://lakehouse:lakehouse@localhost:5433/warehouse`.

### 2.6 DuckDB: el mismo modelo, otro motor

```bash
just duck "select * from gold.gold_daily_revenue order by order_date desc limit 5"
just compare                                  # misma tabla en DuckDB y en Postgres
just compare gold.gold_revenue_by_country_tier
```

**Pregunta:** DuckDB lee Iceberg directo (plugin `iceberg` de dbt-duckdb) y Postgres necesita una copia.
¿Qué ventajas y desventajas tiene cada camino?

### 2.7 Streamlit: el dashboard

http://localhost:8501 (`just dashboard`).

1. En la barra lateral cambiá **Engine** entre DuckDB y Postgres: los números tienen que ser iguales.
2. Abrí `dashboard/app.py`, cambiá un título y guardá: la página se recarga sola.

### 2.8 Jupyter: todo junto

`just notebook` → `notebooks/explore_lakehouse.ipynb`. Corré las celdas en orden:

1. `jupysql` contra Postgres y DuckDB (`%sql`, `%%sql`, cambiar de conexión por alias).
2. Archivos de MinIO con `s3fs` + `pyarrow` (JSONL y Parquet).
3. DuckDB leyendo MinIO directo con `httpfs` (`read_json_auto('s3://landing/...')`).
4. Tablas Iceberg con `pyiceberg`: snapshots y time travel.

> Al terminar con DuckDB, corré `%sql --close duck` para liberar el archivo (dbt necesita escribirlo).

---

## 3. Laboratorio: los 4 escenarios

En cada escenario: corré el comando (espera solo a que el flow termine), mirá el flow run en Prefect y
completá la tabla mirando los sistemas.

### 3.1 Updates, deletes y duplicados

```bash
just demo-updates
```

El batch `0002` trae: clientes actualizados y borrados, 5 filas **duplicadas exactas**, y 300 órdenes que avanzan de estado.

| Dónde mirar | Qué esperar |
|---|---|
| `just snapshots customers` | un segundo snapshot, batch `0002` |
| `just dedup-check` | `raw` = 238 filas, `bronze` = 200 (una por cliente) |
| `just history customers "customer_id = 7"` | si el cliente cambió, ves las dos versiones |
| Streamlit → *Order status funnel* | aparecen `paid`, `shipped`, `delivered` |

### 3.2 Schema evolution

```bash
just demo-schema-evolution
```

El batch `0003` llega en **Parquet** y trae una columna nueva (`customers.loyalty_tier`) y un tipo más
ancho (`order_items.quantity` int → long, por 50 pedidos B2B gigantes).

| Dónde mirar | Qué esperar |
|---|---|
| Logs de `landing-to-iceberg-customers` / `-order_items` en Prefect | `Schema evolved: ['add column loyalty_tier ...']`, `promote column quantity int32 -> int64` |
| `just schema-history customers` | `schema_id=0` sin `loyalty_tier`, `schema_id=1` con ella |
| `just history customers "customer_id = 7"` | la versión vieja tiene `loyalty_tier: None` |
| Streamlit → *Revenue by loyalty tier* | ahora hay bronze / silver / gold / platinum |
| Streamlit → *Daily revenue* | un pico enorme en abril: son los 50 pedidos B2B |


### 3.3 Eventos que llegan tarde

```bash
just demo-late-arriving
```

El batch `0004` trae 20 órdenes con estado `placed` pero con un `updated_at` **más viejo** que el que ya
tenemos (eventos atrasados).

| Dónde mirar | Qué esperar |
|---|---|
| `just time-travel orders` | `raw.orders` crece 20 filas |
| Streamlit → *Order status funnel* | **no cambia**: bronze se queda con la versión de `updated_at` más nuevo |
| `just test-dbt` | pasa `assert_bronze_dedup_keeps_latest` |

**Pregunta:** si bronze ordenara por `_ingested_at` (orden de llegada), ¿qué cambiaría en el funnel?

### 3.4 Idempotencia

```bash
just demo-idempotency
```

Vuelve a escribir el batch `0001` y corre el pipeline: **no carga nada**. Mirá el artifact `ingest-summary`
(`batches_skipped` con `0001`) y `just snapshots customers` (no hay snapshot nuevo).

### 3.5 (Opcional) Upsert en vez de append

Borra todo y arranca de cero:

```bash
just nuke && just up
just demo-upsert
just dedup-check     # ahora raw ya tiene una fila por clave
```

Con `upsert`, Iceberg hace el *merge* en la tabla `raw` y se pierde la historia. Compará con `append`.

---

## 4. Desafíos

1. **Nuevo modelo gold.** Creá `models/gold/gold_revenue_by_category.sql` (revenue y unidades por
   categoría a partir de `fct_order_items` y `dim_products`). Corré `just dbt build --target postgres --select gold_revenue_by_category`
   y agregá un gráfico en `dashboard/app.py`.
2. **Test propio.** Agregá en `models/gold/_gold.yml` un test `not_null` y uno `accepted_values` para tu modelo.
3. **Romper el esquema a propósito.** Subí a MinIO (consola → `landing/shop/customers/batch=9999/`) un
   Parquet donde `customer_id` sea texto. Corré `just ingest`. ¿Qué task falla en Prefect y con qué mensaje? ¿Por qué
   Iceberg no deja hacer ese cambio? (Después borrá la carpeta `batch=9999`).
4. **SQL directo sobre archivos.** En Jupyter, contá cuántos eventos `D` (deletes) llegaron en todos los
   batches de `customers` leyendo los JSONL con DuckDB `httpfs`, sin pasar por Iceberg.
5. **Time travel en SQL.** En Jupyter, cargá `raw.orders` en el primer y el último snapshot con
   `pyiceberg` y calculá con `%sql` cuántas órdenes cambiaron de estado.
6. **Streamlit.** Agregá a `dashboard/app.py` un gráfico de revenue por categoría (`gold.gold_product_performance`).
7. **Mismo resultado, dos motores.** Elegí un modelo silver y comprobá con `just compare silver.<modelo>` que
   DuckDB y Postgres dan exactamente lo mismo. Si no, ¿por qué?

---

## 5. Problemas comunes

| Síntoma | Qué hacer |
|---|---|
| `just check` marca ✘ en un servicio | `just up` (si sigue, `just logs <servicio>`) |
| `just ingest` dice que no encuentra el deployment | el `runner` todavía está arrancando: `just logs` y esperá a ver `serving` |
| El subflow dbt-duckdb falla con *lock* | Cerrá conexiones a DuckDB (Jupyter `%sql --close duck`, `just duck` abierto) y volvé a correr `just ingest` |
| Streamlit muestra un error | esperá a que termine el flow y apretá `R` |
| Quiero empezar de cero | `just nuke && just up && just demo-initial` |
