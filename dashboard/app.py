"""Streamlit dashboard over the gold layer — pick the engine in the sidebar.

Runs as the `streamlit` service (http://localhost:8501).

Same gold tables, two engines: DuckDB (file, read-only) and Postgres.
No pandas in our code: queries return Arrow tables that Streamlit renders directly.
"""

from __future__ import annotations

import os

import duckdb
import psycopg2
import pyarrow as pa
import streamlit as st

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/warehouse.duckdb")

st.set_page_config(page_title="Shop · Lakehouse", layout="wide")
st.title("Shop · Lakehouse (gold)")
engine = st.sidebar.radio("Engine", ["DuckDB", "Postgres"])
st.sidebar.caption("Run `just demo-*`, wait for the flow, then press R to refresh.")


def decimals_to_float(table: pa.Table) -> pa.Table:
    """Charts treat Arrow decimals as text categories: plot them as floats."""
    for i, field in enumerate(table.schema):
        if pa.types.is_decimal(field.type):
            table = table.set_column(i, field.name, table[field.name].cast(pa.float64()))
    return table


@st.cache_data(ttl=10)
def query(sql: str, engine: str) -> pa.Table:
    if engine == "DuckDB":
        # read_only: dbt may be writing; DuckDB allows a single writer
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            return decimals_to_float(con.sql(sql).to_arrow_table())
    with psycopg2.connect(
        host=os.getenv("WAREHOUSE_PG_HOST", "warehouse-postgres"), port=os.getenv("WAREHOUSE_PG_PORT", "5432"),
        user=os.getenv("WAREHOUSE_PG_USER", "lakehouse"), password=os.getenv("WAREHOUSE_PG_PASSWORD", "lakehouse"),
        dbname=os.getenv("WAREHOUSE_PG_DB", "warehouse"),
    ) as con, con.cursor() as cur:
        cur.execute(sql)
        names = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cols = list(zip(*rows)) if rows else [[] for _ in names]
        return pa.table({n: [float(v) if hasattr(v, "as_tuple") else v for v in c] for n, c in zip(names, cols)})


try:
    daily = query("select * from gold.gold_daily_revenue order by order_date", engine)
    funnel = query("select * from gold.gold_order_status_funnel order by orders desc", engine)
    tiers = query("select loyalty_tier, sum(revenue) as revenue from gold.gold_revenue_by_country_tier "
                  "group by loyalty_tier order by revenue desc", engine)
    products = query("select product_name, category, units_sold, revenue from gold.gold_product_performance "
                     "order by revenue desc limit 10", engine)
    countries = query("select country, sum(revenue) as revenue from gold.gold_revenue_by_country_tier "
                      "group by country order by revenue desc", engine)
    customers = query("select full_name, country, loyalty_tier, orders, lifetime_value "
                      "from gold.gold_customer_lifetime_value where not is_deleted "
                      "order by lifetime_value desc limit 10", engine)
except Exception as exc:  # noqa: BLE001 - friendly message for class
    st.warning(f"Gold tables not ready on {engine} yet ({exc}). Run `just demo-initial`.")
    st.stop()

revenue = sum(v or 0 for v in daily["revenue"].to_pylist())
orders = sum(daily["orders"].to_pylist())
customers_count = query("select count(*) as n from gold.gold_customer_lifetime_value where not is_deleted",
                        engine)["n"][0].as_py()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Revenue", f"{float(revenue):,.0f}")
c2.metric("Orders", f"{orders:,}")
c3.metric("Avg order value", f"{float(revenue) / max(orders, 1):,.2f}")
c4.metric("Active customers", f"{customers_count:,}")

st.subheader("Daily revenue")
st.line_chart(daily, x="order_date", y="revenue")

left, right = st.columns(2)
with left:
    st.subheader("Order status funnel")
    st.caption("Moves after `demo-updates`; must NOT move after `demo-late-arriving`.")
    st.bar_chart(funnel, x="status", y="orders")
with right:
    st.subheader("Revenue by loyalty tier")
    st.caption("Only 'unknown' until `demo-schema-evolution` adds the column.")
    st.bar_chart(tiers, x="loyalty_tier", y="revenue")

left, right = st.columns(2)
with left:
    st.subheader("Revenue by country")
    st.bar_chart(countries, x="country", y="revenue")
with right:
    st.subheader("Top 10 customers by lifetime value")
    st.dataframe(customers, width="stretch", hide_index=True)

st.subheader("Top 10 products")
st.dataframe(products, width="stretch", hide_index=True)
