# Guía práctica: explorá el lakehouse sistema por sistema

Esta guía es para hacerla con las manos en el teclado. Primero recorrés cada sistema, después
corrés los 4 escenarios de datos mirando qué cambia en cada uno, y al final hay desafíos.

> Todos los comandos se corren desde la raíz del repo. `just` sin argumentos lista todas las recetas.

---

## 0. Antes de empezar

Necesitás Docker, [uv](https://docs.astral.sh/uv/) y [just](https://just.systems). Esta es la rama **`kestra`**: el orquestador es Kestra y el BI es Metabase.

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
| **Kestra** | Orquesta: ingesta → Iceberg → dbt | http://localhost:8080 | admin@kestra.io / Admin1234 |
| **Postgres** | Catálogo de Iceberg + warehouse (raw, bronze, silver, gold) | localhost:5433 | lakehouse / lakehouse |
| **DuckDB** | El mismo medallion, construido leyendo Iceberg directo | `include/data/warehouse.duckdb` | — |
| **Metabase** | BI: dashboards sobre `gold.*` | http://localhost:3000 | admin@lewagon.example / LeWagon2026! |
| **Jupyter** | Notebooks para consultar todo con SQL y Python | http://localhost:8888 | sin token |

`just urls` imprime esta lista.

### El recorrido de un dato

```
generate_data.py ──► MinIO landing/ ──► flow lakehouse_ingest ───► Iceberg raw.* (MinIO warehouse/ + catálogo en Postgres)
                                                                          │
                                ┌─────────────────────────────────────────┴──────────────────────┐
                                ▼                                                                ▼
                 dbt + DuckDB (lee Iceberg directo)                       cli to-postgres → Postgres raw.* → dbt + Postgres
                                │                                                                │
                      bronze → silver → gold                                           bronze → silver → gold
                                │                                                                │
                          Jupyter                                              Metabase, Jupyter
```

---

## 1. Primera carga

```bash
just demo-initial
```

Esto (1) escribe el batch `0001` en MinIO, (2) ejecuta el flow `lakehouse_ingest` en Kestra y espera a
que terminen los dos flows de dbt que dispara, y (3) crea los dashboards de Metabase. Mientras tanto,
abrí Kestra → **Executions** y mirá cómo corre.

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
just psql "select table_name, metadata_location from iceberg_tables" iceberg_catalog
```

### 2.2 Kestra: la orquestación

Entrá a http://localhost:8080 (admin@kestra.io / Admin1234) → **Flows** → namespace `lewagon.lakehouse`.
Los flows son YAML (`flows/*.yml`) y se sincronizan solos desde la carpeta.

1. `lakehouse_ingest` → pestaña **Topology**: `prepare_catalog` crea el catálogo una sola vez y después un
   `ForEach` corre `landing_to_iceberg` para las 4 entidades en paralelo (`concurrencyLimit: 4`).
2. Cada task corre en un **contenedor** (`lakehouse-runner:local`, Docker task runner) con el repo montado
   en `/app`: ejecuta `python -m include.lakehouse.cli ingest --entity ...`, la misma función que usa Airflow en `main`.
3. `lakehouse_dbt_duckdb` y `lakehouse_dbt_postgres` tienen un **Flow trigger**: arrancan solos cuando
   `lakehouse_ingest` termina en `SUCCESS`. Abrí sus ejecuciones: la pestaña **Gantt** muestra cada task y
   **Logs** muestra la salida de `dbt build` modelo por modelo.
4. **Executions** de `lakehouse_ingest` → **Outputs**: el JSON que imprimió cada carga (batches cargados, salteados, filas).

**Desde la terminal**

```bash
just ingest        # ejecuta lakehouse_ingest y espera a los 2 flows de dbt (no hay batches nuevos → no carga nada)
just logs          # logs de Kestra
```

**Preguntas**
* Después de `just ingest` sin datos nuevos, ¿qué dicen los outputs de `landing_to_iceberg`? ¿Por qué no se
  duplicaron filas? (pista: `lakehouse.batch-id` en los snapshots, sección 2.3).
* En Airflow los DAGs de dbt arrancan por un *asset*; acá por un Flow trigger. ¿Qué se parecen y en qué difieren?

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

> DuckDB admite **un solo escritor**: si el flow de dbt está corriendo, esperá a que termine antes de
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

### 2.7 Metabase: BI

Entrá a http://localhost:3000 (admin@lewagon.example / LeWagon2026!) → **Dashboards**:

* **Shop · Executive**: revenue, órdenes, ticket promedio, clientes; revenue diario; país × loyalty tier; top clientes.
* **Shop · Operations**: órdenes por estado, revenue por categoría, unidades por día, performance por producto.

Son los mismos dashboards que Superset en `main`. Después:
1. **+ New → SQL query** sobre **Warehouse Postgres** y consultá cualquier tabla de `gold`.
2. Guardá la pregunta y agregala a un dashboard.

`just metabase-setup` vuelve a crear/actualizar las preguntas y los dashboards (no duplica nada).

### 2.8 Jupyter: todo junto

`just notebook` → `notebooks/explore_lakehouse.ipynb`. Corré las celdas en orden:

1. `jupysql` contra Postgres y DuckDB (`%sql`, `%%sql`, cambiar de conexión por alias).
2. Archivos de MinIO con `s3fs` + `pyarrow` (JSONL y Parquet).
3. DuckDB leyendo MinIO directo con `httpfs` (`read_json_auto('s3://landing/...')`).
4. Tablas Iceberg con `pyiceberg`: snapshots y time travel.

> Al terminar con DuckDB, corré `%sql --close duck` para liberar el archivo (dbt necesita escribirlo).

---

## 3. Laboratorio: los 4 escenarios

En cada escenario: corré el comando (espera solo a que terminen los 3 flows), mirá las ejecuciones en
Kestra y completá la tabla mirando los sistemas.

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
| Metabase → *Orders by status* | aparecen `paid`, `shipped`, `delivered` |

### 3.2 Schema evolution

```bash
just demo-schema-evolution
```

El batch `0003` llega en **Parquet** y trae una columna nueva (`customers.loyalty_tier`) y un tipo más
ancho (`order_items.quantity` int → long, por 50 pedidos B2B gigantes).

| Dónde mirar | Qué esperar |
|---|---|
| Logs de `landing_to_iceberg` (customers / order_items) en Kestra | `Schema evolved: ['add column loyalty_tier ...']`, `promote column quantity int32 -> int64` |
| `just schema-history customers` | `schema_id=0` sin `loyalty_tier`, `schema_id=1` con ella |
| `just history customers "customer_id = 7"` | la versión vieja tiene `loyalty_tier: None` |
| Metabase → *Revenue by country and loyalty tier* | ahora hay bronze / silver / gold / platinum |
| Metabase → *Daily revenue* | un pico enorme en abril: son los 50 pedidos B2B |


### 3.3 Eventos que llegan tarde

```bash
just demo-late-arriving
```

El batch `0004` trae 20 órdenes con estado `placed` pero con un `updated_at` **más viejo** que el que ya
tenemos (eventos atrasados).

| Dónde mirar | Qué esperar |
|---|---|
| `just time-travel orders` | `raw.orders` crece 20 filas |
| Metabase → *Orders by status* | **no cambia**: bronze se queda con la versión de `updated_at` más nuevo |
| `just test-dbt` | pasa `assert_bronze_dedup_keeps_latest` |

**Pregunta:** si bronze ordenara por `_ingested_at` (orden de llegada), ¿qué cambiaría en el funnel?

### 3.4 Idempotencia

```bash
just demo-idempotency
```

Vuelve a escribir el batch `0001` y ejecuta la ingesta: **no carga nada**. Mirá los outputs de
`landing_to_iceberg` (`batches_skipped` con `0001`) y `just snapshots customers` (no hay snapshot nuevo).

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
   y graficalo en Metabase con una SQL query.
2. **Test propio.** Agregá en `models/gold/_gold.yml` un test `not_null` y uno `accepted_values` para tu modelo.
3. **Romper el esquema a propósito.** Subí a MinIO (consola → `landing/shop/customers/batch=9999/`) un
   Parquet donde `customer_id` sea texto. Corré `just ingest`. ¿Qué task falla en Kestra y con qué mensaje? ¿Por qué
   Iceberg no deja hacer ese cambio? (Después borrá la carpeta `batch=9999`).
4. **SQL directo sobre archivos.** En Jupyter, contá cuántos eventos `D` (deletes) llegaron en todos los
   batches de `customers` leyendo los JSONL con DuckDB `httpfs`, sin pasar por Iceberg.
5. **Time travel en SQL.** En Jupyter, cargá `raw.orders` en el primer y el último snapshot con
   `pyiceberg` y calculá con `%sql` cuántas órdenes cambiaron de estado.
6. **Metabase.** Agregá a *Shop · Executive* una pregunta con revenue por categoría (`gold.gold_product_performance`).
7. **Mismo resultado, dos motores.** Elegí un modelo silver y comprobá con `just compare silver.<modelo>` que
   DuckDB y Postgres dan exactamente lo mismo. Si no, ¿por qué?

---

## 5. Problemas comunes

| Síntoma | Qué hacer |
|---|---|
| `just check` marca ✘ en un servicio | `just up` (si sigue, `just logs <servicio>`) |
| `just ingest` falla con *connection refused* | Kestra todavía está arrancando: esperá a que `just check` marque ✔ |
| El flow de dbt DuckDB falla con *lock* | Cerrá conexiones a DuckDB (Jupyter `%sql --close duck`, `just duck` abierto) y volvé a correr `just ingest` |
| Metabase sin dashboards | `just metabase-setup` |
| Quiero empezar de cero | `just nuke && just up && just demo-initial` |
