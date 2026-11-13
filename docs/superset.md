# Dashboard en Superset (5 minutos)

1. Entrá a http://localhost:8088 (admin / admin).
2. **Datasets → + Dataset**: base `Warehouse Postgres`, esquema `gold`, y agregá:
   `gold_daily_revenue`, `gold_order_status_funnel`, `gold_revenue_by_country_tier`, `gold_product_performance`.
   Repetí con la base `Warehouse DuckDB` si querés comparar motores.
3. Charts sugeridos:

| Chart | Dataset | Config |
|---|---|---|
| Line chart "Revenue diario" | `gold_daily_revenue` | X: `order_date` · Métrica: `SUM(revenue)` |
| Big number "Ticket promedio" | `gold_daily_revenue` | `AVG(avg_order_value)` |
| Bar chart "Funnel de estados" | `gold_order_status_funnel` | X: `status` · Métrica: `SUM(orders)` |
| Pivot "País × tier" | `gold_revenue_by_country_tier` | Filas: `country` · Columnas: `loyalty_tier` · `SUM(revenue)` |
| Table "Top productos" | `gold_product_performance` | ordenar por `revenue` desc, límite 10 |

4. Juntalos en un dashboard **Shop · Lakehouse**.

## Qué mirar después de cada demo

* `demo-updates`: el funnel se mueve de `placed` a `paid/shipped/delivered`.
* `demo-schema-evolution`: el pivot pasa de una sola columna `unknown` a `bronze/silver/gold/platinum`.
* `demo-late-arriving`: **no** cambia nada. Ese es el punto.

## Conexiones (creadas por `infra/superset/bootstrap.sh`)

* Postgres: `postgresql+psycopg2://lakehouse:lakehouse@warehouse-postgres:5432/warehouse`
* DuckDB: `duckdb:////data/warehouse.duckdb?access_mode=READ_ONLY`

## Dashboards pre-cargados (`just superset-setup`)

`scripts/superset_setup.py` usa solo la API REST de Superset (sin dependencias) y es idempotente:
crea un dataset por cada tabla `gold.*` de **Warehouse Postgres**, los gráficos y dos dashboards.
`just demo-initial` lo ejecuta solo, esperando a que dbt termine.

* **Shop · Executive**: http://localhost:8088/superset/dashboard/shop-executive/
  revenue, órdenes, ticket promedio y clientes activos; revenue diario; revenue por país y
  `loyalty_tier`; top 10 clientes por lifetime value.
* **Shop · Operations**: http://localhost:8088/superset/dashboard/shop-operations/
  órdenes por estado; revenue por categoría; unidades por día; performance por producto.

Después de `just demo-schema-evolution` volvé a correr `just superset-setup`: refresca los datasets
para que aparezca `loyalty_tier` con valores reales.
