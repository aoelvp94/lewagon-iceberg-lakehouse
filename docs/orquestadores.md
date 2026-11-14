# Tres orquestadores, una misma arquitectura

El lakehouse es siempre el mismo. Lo único que cambia entre ramas es **quién orquesta** (y con qué
BI se mira el resultado):

| Rama | Orquestador | Cómo corre dbt | Encadenar dbt después de la ingesta | BI |
|---|---|---|---|---|
| `main` | **Airflow 3.3** (Astro CLI) | **Cosmos**: una task por modelo + sus tests | **Assets**: `lakehouse_ingest` actualiza `iceberg_raw` y eso dispara los 2 DAGs de dbt | **Superset** (+ Streamlit como plugin de Airflow) |
| `prefect` | **Prefect 3** | **prefect-dbt** (`PrefectDbtRunner`): una task por nodo de dbt | **Subflows** dentro del mismo flow | **Streamlit** |
| `kestra` | **Kestra** | **DbtCLI**: una task que corre `dbt build` en un contenedor | **Flow triggers**: los flows de dbt escuchan el `SUCCESS` de la ingesta | **Metabase** |

Lo que **no** cambia (y por eso los números tienen que dar idénticos en los tres):

* `include/lakehouse/`: ingesta landing → Iceberg (pyarrow + fsspec + pyiceberg) y copia Iceberg → Postgres.
  Los tres orquestadores llaman a **las mismas funciones**.
* `include/dbt/shop/`: el mismo proyecto dbt (bronze → silver → gold) con dos targets, DuckDB y Postgres.
* `scripts/generate_data.py`: los mismos 4 escenarios (`initial`, `updates`, `schema-evolution`, `late-arriving`).
* Los mismos dashboards: **Shop · Executive** y **Shop · Operations** (Superset en `main`, Metabase en `kestra`)
  y la misma app de Streamlit (`main` y `prefect`).

```mermaid
flowchart LR
    gen[generate_data.py] --> landing[(MinIO landing)]
    landing -->|ingesta en paralelo, 1 por entidad| raw[(Iceberg raw.*)]
    raw -->|dbt-duckdb lee Iceberg| duck[(DuckDB bronze/silver/gold)]
    raw -->|copia| pgraw[(Postgres raw.*)] --> pg[(Postgres bronze/silver/gold)]
    duck --> bi[Dashboards]
    pg --> bi
    subgraph orq [Orquestador: Airflow · Prefect · Kestra]
      direction LR
      i[ingesta] --> d1[dbt DuckDB]
      i --> d2[copia + dbt Postgres]
    end
```

---

## Opción A: correr uno solo

Lo más simple. Cada rama tiene su propio `docker compose`, sus volúmenes y sus datos, así que podés ir
y venir sin perder nada.

```bash
git switch main      # o: prefect | kestra
just up
just check           # todo en ✔
just demo-initial
```

Para cambiar de orquestador:

```bash
just down
git switch prefect   # o kestra / main
just up
```

| | `main` (Airflow) | `prefect` | `kestra` |
|---|---|---|---|
| Orquestador | http://localhost:8080 | http://localhost:4200 | http://localhost:8080 (admin@kestra.io / Admin1234) |
| BI | Superset http://localhost:8088 (admin / admin) | Streamlit http://localhost:8501 | Metabase http://localhost:3000 (admin@lewagon.example / LeWagon2026!) |
| MinIO | http://localhost:9001 | http://localhost:9001 | http://localhost:9001 |
| Postgres | localhost:5433 | localhost:5433 | localhost:5433 |
| Jupyter | http://localhost:8888 | http://localhost:8888 | http://localhost:8888 |

---

## Opción B: los tres a la vez (lado a lado)

Ideal para comparar en vivo en clase. Cada rama va en su propia carpeta con
[`git worktree`](https://git-scm.com/docs/git-worktree) y `prefect` / `kestra` mueven sus puertos con
`just side-by-side` para no chocar con `main`.

```bash
# en el repo (rama main): Airflow con los puertos estándar
just up

# carpetas hermanas para las otras dos ramas (git worktree) con los puertos ya movidos
just worktrees

cd ../lakehouse-prefect && just up     # puertos 1xxxx
cd ../lakehouse-kestra  && just up     # puertos 2xxxx
```

`just worktrees` equivale a `git worktree add ../lakehouse-prefect prefect` (y lo mismo para `kestra`) más
`just side-by-side` en cada carpeta. Después, en cada carpeta: `just demo-initial` (y el resto de los demos). `just check` y `just urls`
muestran los puertos de cada stack.

| | `main` (Airflow) | `prefect` (1xxxx) | `kestra` (2xxxx) |
|---|---|---|---|
| Orquestador | http://localhost:8080 | http://localhost:14200 | http://localhost:28080 |
| BI | http://localhost:8088 (Superset) · http://localhost:8501 (Streamlit) | http://localhost:18501 (Streamlit) | http://localhost:23000 (Metabase) |
| MinIO consola | http://localhost:9001 | http://localhost:19001 | http://localhost:29001 |
| Postgres | localhost:5433 | localhost:15433 | localhost:25433 |
| Jupyter | http://localhost:8888 | http://localhost:18888 | http://localhost:28888 |

> **Memoria.** Con 8 GB asignados a Docker entran **dos** stacks a la vez (probado: Airflow + Prefect y
> Airflow + Kestra). Para los tres, subí la memoria de Docker Desktop a 12 GB o hacé `just down` en el que
> no estés mirando. Kestra y Metabase corren con `JAVA_OPTS=-Xmx1g` para no llevarse toda la memoria.
>
> **Descargas.** La primera vez, `just up` baja imágenes grandes: Kestra ~11 GB (trae todos sus plugins),
> Metabase ~2 GB, Jupyter ~3 GB. Hacelo antes de la clase y con buena conexión.
>
> `just default-ports` vuelve `prefect` / `kestra` a los puertos estándar.

---

## Airflow (`main`)

**Ingesta.** `LandingToIcebergOperator` con *dynamic task mapping* (`.partial().expand(entity=...)`): una
instancia por entidad, creadas en tiempo de ejecución. `prepare_catalog` va antes para que las cargas en
paralelo no choquen creando el catálogo.

![DAGs en Airflow](img/airflow-dags.jpg)

![lakehouse_ingest: prepare_catalog → landing_to_iceberg (mapeada) → summarize](img/airflow-ingest-graph.jpg)

**dbt.** Cosmos lee el proyecto dbt y arma un *task group* por modelo, con su `run` y su `test`.
Los tests que tocan dos modelos (relationships) quedan en tareas aparte, después de los dos padres.

![lakehouse_dbt_postgres con los task groups de Cosmos desplegados](img/airflow-dbt-postgres-graph.jpg)

![lakehouse_dbt_duckdb: el mismo proyecto, otro target](img/airflow-dbt-duckdb-graph.jpg)

**BI.** Superset con los dashboards pre-cargados (`just superset-setup`) y la app de Streamlit embebida
en Airflow con un plugin (`plugins/lakehouse_dashboard.py`).

![Superset · Shop · Executive](img/superset-executive.jpg)

![Superset · Shop · Operations](img/superset-operations.jpg)

![Streamlit dentro de Airflow (plugin)](img/airflow-plugin-streamlit.jpg)

---

## Prefect (`prefect`)

**Ingesta.** Un `@flow` de Python: `prepare_iceberg_catalog` y después `landing_to_iceberg.map(...)` (una
task por entidad, en paralelo). El resultado queda en el artifact `ingest-summary`.

**dbt.** Dos subflows, `dbt-duckdb` y `dbt-postgres` (este primero copia Iceberg → Postgres con
`iceberg_to_pg.map(...)`). `PrefectDbtRunner` convierte cada modelo y test en una task.

![Flow runs en Prefect](img/prefect-runs.jpg)

![lakehouse-pipeline: tasks de ingesta y los dos subflows de dbt](img/prefect-pipeline-run-overview.jpg)

![Subflow dbt-postgres desplegado: copias, modelos y tests](img/prefect-pipeline-run.jpg)

![Artifact ingest-summary: qué cargó cada entidad](img/prefect-ingest-summary.jpg)

**BI.** La misma app de Streamlit (`dashboard/app.py`), con selector DuckDB / Postgres.

![Streamlit en la rama prefect](img/prefect-streamlit.jpg)

---

## Kestra (`kestra`)

**Ingesta.** Flows en YAML (`flows/*.yml`, namespace `lewagon.lakehouse`). `lakehouse_ingest` corre
`prepare_catalog` y después un `Loop` sobre las 4 entidades en paralelo. Cada task corre en un contenedor
(`lakehouse-runner:local`, Docker task runner) con el repo montado: ejecuta `python -m include.lakehouse.cli ...`,
las mismas funciones que usan Airflow y Prefect.

![Flows en Kestra](img/kestra-flows.jpg)

![lakehouse_ingest: YAML y topología (prepare_catalog → Loop por entidad)](img/kestra-ingest-edit.jpg)

![Ejecución de lakehouse_ingest: el Loop con sus 4 iteraciones](img/kestra-ingest-gantt.jpg)

**dbt.** `lakehouse_dbt_duckdb` y `lakehouse_dbt_postgres` tienen un **Flow trigger** (`dependsOn` el
`SUCCESS` de `lakehouse_ingest`): Kestra muestra esa relación en la pestaña *Dependencies*. dbt corre con
`DbtCLI` (un `dbt build` por flow); el detalle por modelo está en los logs.

![Dependencies: la ingesta dispara los dos flows de dbt](img/kestra-ingest-dependencies.jpg)

![lakehouse_dbt_postgres: copia Iceberg → Postgres (Loop) y dbt build](img/kestra-dbt-postgres-gantt.jpg)

![Logs de dbt dentro de Kestra](img/kestra-dbt-postgres-logs.jpg)

**BI.** Metabase con los mismos dos dashboards que Superset (`just metabase-setup`, lo corre `just demo-initial`).

![Metabase · Shop · Executive](img/metabase-executive.jpg)

![Metabase · Shop · Operations](img/metabase-operations.jpg)

> **Kestra 2.0.** Los flows usan `Loop` (antes `ForEach`) y el Flow trigger con `dependsOn` (antes
> `preconditions`). `just up` publica los flows por la API; después de editar uno, `just deploy`.

---

## ¿Dan lo mismo? (checklist)

Después de correr los 4 demos (`initial`, `updates`, `schema-evolution`, `late-arriving`) en cualquier
orquestador, tenés que ver exactamente esto:

| Dónde | Valor esperado |
|---|---|
| `just compare` (funnel, DuckDB y Postgres) | placed 700 · paid 150 · shipped 101 · delivered 99 |
| KPIs del dashboard | Revenue ≈ 2.85M · Orders 1,050 · AOV 2,712.48 · Active customers 200 |
| `just dedup-check` | raw 438 · bronze 200 |
| `just schema-history order_items` | `quantity` int → long |
| `just test-dbt` | todo en verde en los dos motores |

Si un orquestador da distinto, el problema **no** está en el SQL ni en la ingesta (son los mismos
archivos): mirá qué corrió y en qué orden.

## Para discutir en clase

* **Granularidad de dbt.** Cosmos y prefect-dbt muestran cada modelo como una task (reintentos y logs por
  modelo). DbtCLI corre `dbt build` entero en una task: más simple, pero si falla un modelo se reintenta todo.
* **Cómo se encadena.** Assets (Airflow) y Flow triggers (Kestra) desacoplan productores y consumidores;
  los subflows de Prefect los dejan en el mismo código. ¿Qué pasa si mañana otro equipo quiere reaccionar
  a "Iceberg actualizado"?
* **Código vs. configuración.** DAGs y flows en Python (Airflow, Prefect) vs. YAML declarativo (Kestra).
* **Dónde corre el código.** En el worker de Airflow, en el runner de Prefect, o en un contenedor por task
  (Kestra con el Docker task runner).
