"""Create the Superset datasets, charts and dashboards on the gold tables (idempotent).

    uv run scripts/superset_setup.py            # or: just superset-setup
    uv run scripts/superset_setup.py --wait 600 # wait up to 10 min for dbt to create gold.*

Standard library only: everything goes through Superset's REST API.

1. POST /api/v1/security/login            -> access token
2. one dataset per gold.* table on the "Warehouse Postgres" database
3. charts (created or updated by name)
4. dashboards (created or updated by slug) with a real grid layout (position_json)

Re-running it updates everything in place, so it is safe to call after every demo.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = os.getenv("SUPERSET_BASE_URL", "http://localhost:8088")
USERNAME = os.getenv("SUPERSET_USERNAME", "admin")
PASSWORD = os.getenv("SUPERSET_PASSWORD", "admin")
DATABASE = "Warehouse Postgres"
SCHEMA = "gold"
GOLD_TABLES = [
    "gold_daily_revenue",
    "gold_customer_lifetime_value",
    "gold_order_status_funnel",
    "gold_product_performance",
    "gold_revenue_by_country_tier",
]


# --------------------------------------------------------------------------- HTTP
class Superset:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.token: str | None = None

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        req = urllib.request.Request(
            self.base_url + path,
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as err:
            raise SystemExit(f"{method} {path} -> {err.code}: {err.read().decode()[:500]}") from err

    def login(self, username: str, password: str) -> None:
        body = {"username": username, "password": password, "provider": "db", "refresh": True}
        self.token = self.call("POST", "/api/v1/security/login", body)["access_token"]

    def list_all(self, resource: str) -> list[dict]:
        q = urllib.parse.quote("(page_size:100)")
        return self.call("GET", f"/api/v1/{resource}/?q={q}")["result"]


# --------------------------------------------------------------------------- datasets
def database_id(api: Superset) -> int:
    for db in api.list_all("database"):
        if db["database_name"] == DATABASE:
            return db["id"]
    raise SystemExit(f'Database "{DATABASE}" not found: is the Superset bootstrap done?')


def wait_for_gold_tables(api: Superset, db_id: int, timeout_s: int) -> None:
    """dbt runs asynchronously in Airflow: wait until every gold table exists."""
    q = urllib.parse.quote(f"(schema_name:{SCHEMA},force:!t)")
    deadline = time.time() + timeout_s
    while True:
        try:
            found = {t["value"] for t in api.call("GET", f"/api/v1/database/{db_id}/tables/?q={q}")["result"]}
        except SystemExit:  # schema does not exist yet
            found = set()
        missing = [t for t in GOLD_TABLES if t not in found]
        if not missing:
            return
        if time.time() > deadline:
            raise SystemExit(f"Gold tables still missing after {timeout_s}s: {missing}")
        print(f"  waiting for dbt: missing {missing}")
        time.sleep(15)


def ensure_datasets(api: Superset, db_id: int) -> dict[str, int]:
    existing = {
        d["table_name"]: d["id"]
        for d in api.list_all("dataset")
        if d.get("schema") == SCHEMA and d["database"]["id"] == db_id
    }
    ids = {}
    for table in GOLD_TABLES:
        if table in existing:
            ids[table] = existing[table]
            # pick up new columns (e.g. loyalty_tier after the schema-evolution demo)
            api.call("PUT", f"/api/v1/dataset/{ids[table]}/refresh")
        else:
            body = {"database": db_id, "schema": SCHEMA, "table_name": table}
            ids[table] = api.call("POST", "/api/v1/dataset/", body)["id"]
        print(f"  dataset {SCHEMA}.{table} -> {ids[table]}")
    return ids


# --------------------------------------------------------------------------- charts
def metric(sql: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


REVENUE = metric("SUM(revenue)", "Revenue")
ORDERS = metric("SUM(orders)", "Orders")

# name -> (dataset, viz_type, params). Widths are on a 12-column grid.
CHARTS: dict[str, tuple[str, str, dict]] = {
    "Revenue": ("gold_daily_revenue", "big_number_total",
                {"metric": REVENUE, "subheader": "all time", "y_axis_format": "SMART_NUMBER"}),
    "Orders": ("gold_daily_revenue", "big_number_total",
               {"metric": ORDERS, "subheader": "all time", "y_axis_format": "SMART_NUMBER"}),
    "Average order value": ("gold_daily_revenue", "big_number_total",
                            {"metric": metric("SUM(revenue) / NULLIF(SUM(orders), 0)", "AOV"),
                             "subheader": "revenue / orders", "y_axis_format": ",.2f"}),
    "Active customers": ("gold_customer_lifetime_value", "big_number_total",
                         {"metric": metric("COUNT(*) FILTER (WHERE NOT is_deleted)", "Customers"),
                          "subheader": "not deleted", "y_axis_format": "SMART_NUMBER"}),
    "Daily revenue": ("gold_daily_revenue", "echarts_timeseries_line",
                      {"x_axis": "order_date", "time_grain_sqla": "P1D", "metrics": [REVENUE],
                       "groupby": [], "row_limit": 10000, "show_legend": False,
                       "x_axis_title": "", "y_axis_format": "SMART_NUMBER"}),
    "Revenue by country and loyalty tier": (
        "gold_revenue_by_country_tier", "echarts_timeseries_bar",
        {"x_axis": "country", "xAxisForceCategorical": True, "metrics": [REVENUE],
         "groupby": ["loyalty_tier"], "stack": "Stack", "row_limit": 1000,
         "show_legend": True, "y_axis_format": "SMART_NUMBER"}),
    "Top customers by lifetime value": (
        "gold_customer_lifetime_value", "table",
        {"query_mode": "raw",
         "all_columns": ["full_name", "country", "loyalty_tier", "orders", "lifetime_value"],
         "order_by_cols": [json.dumps(["lifetime_value", False])], "row_limit": 10,
         "server_pagination": False}),
    "Orders by status": ("gold_order_status_funnel", "echarts_timeseries_bar",
                         {"x_axis": "status", "xAxisForceCategorical": True, "metrics": [ORDERS],
                          "groupby": [], "row_limit": 100, "show_legend": False,
                          "x_axis_sort": "Orders", "x_axis_sort_asc": False}),
    "Revenue by category": ("gold_product_performance", "pie",
                            {"groupby": ["category"], "metric": REVENUE, "row_limit": 100,
                             "show_labels": True, "label_type": "key_percent", "donut": True}),
    "Product performance": ("gold_product_performance", "table",
                            {"query_mode": "raw",
                             "all_columns": ["product_name", "category", "current_unit_price",
                                             "orders", "units_sold", "revenue"],
                             "order_by_cols": [json.dumps(["revenue", False])], "row_limit": 15,
                             "server_pagination": False}),
    "Units sold per day": ("gold_daily_revenue", "echarts_timeseries_bar",
                           {"x_axis": "order_date", "time_grain_sqla": "P1D",
                            "metrics": [metric("SUM(units)", "Units")], "groupby": [],
                            "row_limit": 10000, "show_legend": False}),
}

# slug -> (title, rows of (chart name, width))
DASHBOARDS: dict[str, tuple[str, list[list[tuple[str, int]]]]] = {
    "shop-executive": ("Shop · Executive", [
        [("Revenue", 3), ("Orders", 3), ("Average order value", 3), ("Active customers", 3)],
        [("Daily revenue", 12)],
        [("Revenue by country and loyalty tier", 7), ("Top customers by lifetime value", 5)],
    ]),
    "shop-operations": ("Shop · Operations", [
        [("Orders by status", 6), ("Revenue by category", 6)],
        [("Units sold per day", 12)],
        [("Product performance", 12)],
    ]),
}


def ensure_charts(api: Superset, datasets: dict[str, int]) -> dict[str, int]:
    existing = {c["slice_name"]: c["id"] for c in api.list_all("chart")}
    ids = {}
    for name, (table, viz_type, params) in CHARTS.items():
        ds_id = datasets[table]
        form_data = {"datasource": f"{ds_id}__table", "viz_type": viz_type, **params}
        body = {"slice_name": name, "viz_type": viz_type, "datasource_id": ds_id,
                "datasource_type": "table", "params": json.dumps(form_data)}
        if name in existing:
            ids[name] = existing[name]
            api.call("PUT", f"/api/v1/chart/{ids[name]}", body)
        else:
            ids[name] = api.call("POST", "/api/v1/chart/", body)["id"]
        print(f"  chart {name!r} -> {ids[name]}")
    return ids


# --------------------------------------------------------------------------- dashboards
def position_json(title: str, rows: list[list[tuple[str, int]]], charts: dict[str, int]) -> dict:
    """Superset's grid layout: ROOT > GRID > ROW > CHART (12 columns wide)."""
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
    }
    for r, row in enumerate(rows):
        row_id = f"ROW-{r}"
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {"type": "ROW", "id": row_id, "children": [],
                          "parents": ["ROOT_ID", "GRID_ID"],
                          "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for name, width in row:
            chart_key = f"CHART-{charts[name]}"
            layout[row_id]["children"].append(chart_key)
            height = 30 if CHARTS[name][1] == "big_number_total" else 60
            layout[chart_key] = {"type": "CHART", "id": chart_key, "children": [],
                                 "parents": ["ROOT_ID", "GRID_ID", row_id],
                                 "meta": {"chartId": charts[name], "width": width,
                                          "height": height, "sliceName": name}}
    return layout


def ensure_dashboards(api: Superset, charts: dict[str, int]) -> None:
    existing = {d["slug"]: d["id"] for d in api.list_all("dashboard") if d.get("slug")}
    for slug, (title, rows) in DASHBOARDS.items():
        body = {"dashboard_title": title, "slug": slug, "published": True,
                "position_json": json.dumps(position_json(title, rows, charts)),
                "json_metadata": json.dumps({"refresh_frequency": 0, "color_scheme": "supersetColors"})}
        if slug in existing:
            dash_id = existing[slug]
            api.call("PUT", f"/api/v1/dashboard/{dash_id}", body)
        else:
            dash_id = api.call("POST", "/api/v1/dashboard/", body)["id"]
        # Charts belong to dashboards through the chart side of the relation
        for name in {n for row in rows for n, _ in row}:
            chart = api.call("GET", f"/api/v1/chart/{charts[name]}")["result"]
            dashboards = sorted({d["id"] for d in chart.get("dashboards", [])} | {dash_id})
            api.call("PUT", f"/api/v1/chart/{charts[name]}", {"dashboards": dashboards})
        print(f"  dashboard {title!r} -> {BASE_URL}/superset/dashboard/{slug}/")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--wait", type=int, default=0, help="seconds to wait for dbt's gold tables")
    args = parser.parse_args()

    api = Superset(BASE_URL)
    api.login(USERNAME, PASSWORD)
    db_id = database_id(api)
    if args.wait:
        wait_for_gold_tables(api, db_id, args.wait)
    print("Datasets")
    datasets = ensure_datasets(api, db_id)
    print("Charts")
    charts = ensure_charts(api, datasets)
    print("Dashboards")
    ensure_dashboards(api, charts)


if __name__ == "__main__":
    main()
