# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""One-shot Metabase bootstrap (stdlib only): admin user, Postgres connection,
optional DuckDB connection, and a "Shop · Lakehouse" dashboard on the gold layer.

    uv run scripts/metabase_setup.py            # after `just up`
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

MB = os.getenv("METABASE_URL", "http://localhost:3000")
ADMIN = {"email": "admin@lewagon.example", "password": "LeWagon2026!", "first_name": "Le", "last_name": "Wagon"}
PG = {"host": "warehouse-postgres", "port": 5432, "dbname": "warehouse", "user": "lakehouse", "password": "lakehouse"}

CARDS = [
    ("Daily revenue", "line", "select order_date, revenue from gold.gold_daily_revenue order by 1",
     {"graph.dimensions": ["order_date"], "graph.metrics": ["revenue"]}),
    ("Order status funnel", "bar", "select status, orders from gold.gold_order_status_funnel order by orders desc",
     {"graph.dimensions": ["status"], "graph.metrics": ["orders"]}),
    ("Revenue by loyalty tier", "bar",
     "select loyalty_tier, sum(revenue) as revenue from gold.gold_revenue_by_country_tier group by 1 order by 2 desc",
     {"graph.dimensions": ["loyalty_tier"], "graph.metrics": ["revenue"]}),
    ("Top 10 products", "table",
     "select product_name, category, units_sold, revenue from gold.gold_product_performance order by revenue desc limit 10",
     {}),
]


def call(method: str, path: str, body: dict | None = None, session: str | None = None):
    req = urllib.request.Request(f"{MB}{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    if session:
        req.add_header("X-Metabase-Session", session)
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else None


def wait_ready():
    for _ in range(120):
        try:
            if call("GET", "/api/health")["status"] == "ok":
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError, KeyError):
            pass
        time.sleep(2)
    raise SystemExit("Metabase did not become healthy")


def main():
    wait_ready()
    token = call("GET", "/api/session/properties").get("setup-token")
    if token:
        call("POST", "/api/setup", {
            "token": token,
            "user": {**ADMIN, "site_name": "Le Wagon Lakehouse"},
            "prefs": {"site_name": "Le Wagon Lakehouse", "allow_tracking": False},
            "database": {"engine": "postgres", "name": "Warehouse Postgres", "details": PG},
        })
        print("✔ admin user + Postgres connection created")
    session = call("POST", "/api/session", {"username": ADMIN["email"], "password": ADMIN["password"]})["id"]

    dbs = {d["name"]: d["id"] for d in call("GET", "/api/database", session=session)["data"]}
    if "Warehouse DuckDB" not in dbs:
        try:  # needs the community driver: just metabase-duckdb-driver
            d = call("POST", "/api/database", {"engine": "duckdb", "name": "Warehouse DuckDB",
                                               "details": {"database_file": "/data/warehouse.duckdb",
                                                           "read_only": True}}, session=session)
            dbs["Warehouse DuckDB"] = d["id"]
            print("✔ DuckDB connection created")
        except urllib.error.HTTPError as exc:
            print(f"· DuckDB connection skipped ({exc.code}); install the driver to compare engines")

    existing = {c["name"] for c in call("GET", "/api/dashboard", session=session)}
    if "Shop · Lakehouse" in existing:
        print("✔ dashboard already exists")
        return
    db_id = dbs["Warehouse Postgres"]
    dashboard = call("POST", "/api/dashboard", {"name": "Shop · Lakehouse"}, session=session)
    dashcards = []
    for i, (name, display, sql, viz) in enumerate(CARDS):
        card = call("POST", "/api/card", {
            "name": name, "display": display, "visualization_settings": viz,
            "dataset_query": {"type": "native", "native": {"query": sql}, "database": db_id},
        }, session=session)
        dashcards.append({"id": -(i + 1), "card_id": card["id"], "row": (i // 2) * 6,
                          "col": (i % 2) * 12, "size_x": 12, "size_y": 6})
    call("PUT", f"/api/dashboard/{dashboard['id']}", {"dashcards": dashcards}, session=session)
    print(f"✔ dashboard: {MB}/dashboard/{dashboard['id']}")


if __name__ == "__main__":
    main()
