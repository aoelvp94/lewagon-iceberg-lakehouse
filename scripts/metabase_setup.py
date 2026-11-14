# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Metabase bootstrap (stdlib only, idempotent): admin user, Postgres connection and the
same two dashboards as Superset on `main`, built on the gold tables.

    uv run scripts/metabase_setup.py              # or: just metabase-setup
    uv run scripts/metabase_setup.py --wait 600   # wait up to 10 min for dbt's gold tables

1. first run only: /api/setup creates the admin user
2. "Warehouse Postgres" database (and "Warehouse DuckDB" if the community driver is installed)
3. a "Le Wagon Lakehouse" collection holding everything below (so nothing else gets touched)
4. one native-SQL question (card) per chart, created or updated by name
5. "Shop · Executive" and "Shop · Operations" dashboards, laid out on Metabase's 24-column grid
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request

MB = os.getenv("METABASE_URL", "http://localhost:3000")
ADMIN = {"email": "admin@lewagon.example", "password": "LeWagon2026!", "first_name": "Le", "last_name": "Wagon"}
PG = {"host": "warehouse-postgres", "port": 5432, "dbname": "warehouse", "user": "lakehouse", "password": "lakehouse"}
DATABASE = "Warehouse Postgres"
COLLECTION = "Le Wagon Lakehouse"
GOLD_TABLES = 5


def bar(dimension: str, metric: str, **extra) -> dict:
    return {"graph.dimensions": [dimension], "graph.metrics": [metric], **extra}


# name -> (display, SQL, visualization settings). Same charts as scripts/superset_setup.py on main.
CARDS: dict[str, tuple[str, str, dict]] = {
    "Revenue": ("scalar", "select sum(revenue) as revenue from gold.gold_daily_revenue", {}),
    "Orders": ("scalar", "select sum(orders) as orders from gold.gold_daily_revenue", {}),
    "Average order value": ("scalar",
                            "select round(sum(revenue) / nullif(sum(orders), 0), 2) as aov from gold.gold_daily_revenue",
                            {}),
    "Active customers": ("scalar",
                         "select count(*) as customers from gold.gold_customer_lifetime_value where not is_deleted",
                         {}),
    "Daily revenue": ("line", "select order_date, revenue from gold.gold_daily_revenue order by order_date",
                      bar("order_date", "revenue")),
    "Revenue by country and loyalty tier": (
        "bar",
        "select country, coalesce(loyalty_tier, 'unknown') as loyalty_tier, sum(revenue) as revenue "
        "from gold.gold_revenue_by_country_tier group by 1, 2 order by 1, 2",
        {"graph.dimensions": ["country", "loyalty_tier"], "graph.metrics": ["revenue"],
         "stackable.stack_type": "stacked"}),
    "Top customers by lifetime value": (
        "table",
        "select full_name, country, loyalty_tier, orders, lifetime_value "
        "from gold.gold_customer_lifetime_value order by lifetime_value desc limit 10", {}),
    "Orders by status": ("bar",
                         "select status, orders from gold.gold_order_status_funnel order by orders desc",
                         bar("status", "orders")),
    "Revenue by category": ("pie",
                            "select category, sum(revenue) as revenue from gold.gold_product_performance "
                            "group by 1 order by 2 desc",
                            {"pie.dimension": "category", "pie.metric": "revenue"}),
    "Units sold per day": ("bar", "select order_date, units from gold.gold_daily_revenue order by order_date",
                           bar("order_date", "units")),
    "Product performance": (
        "table",
        "select product_name, category, current_unit_price, orders, units_sold, revenue "
        "from gold.gold_product_performance order by revenue desc limit 15", {}),
}

# title -> rows of (card name, width, height). Metabase's grid is 24 columns wide.
DASHBOARDS: dict[str, list[list[tuple[str, int, int]]]] = {
    "Shop · Executive": [
        [("Revenue", 6, 3), ("Orders", 6, 3), ("Average order value", 6, 3), ("Active customers", 6, 3)],
        [("Daily revenue", 24, 6)],
        [("Revenue by country and loyalty tier", 14, 8), ("Top customers by lifetime value", 10, 8)],
    ],
    "Shop · Operations": [
        [("Orders by status", 12, 7), ("Revenue by category", 12, 7)],
        [("Units sold per day", 24, 6)],
        [("Product performance", 24, 9)],
    ],
}


# --------------------------------------------------------------------------- HTTP
def call(method: str, path: str, body: dict | None = None, session: str | None = None):
    req = urllib.request.Request(f"{MB}{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    if session:
        req.add_header("X-Metabase-Session", session)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as err:
        raise SystemExit(f"{method} {path} -> {err.code}: {err.read().decode()[:400]}") from err


def wait_ready() -> None:
    for _ in range(150):
        try:
            if call("GET", "/api/health")["status"] == "ok":
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError, KeyError, SystemExit):
            pass
        time.sleep(2)
    raise SystemExit("Metabase did not become healthy")


# --------------------------------------------------------------------------- setup
def login() -> str:
    token = call("GET", "/api/session/properties").get("setup-token")
    if token and not call("GET", "/api/session/properties").get("has-user-setup"):
        call("POST", "/api/setup", {
            "token": token,
            "user": {**ADMIN, "site_name": "Le Wagon Lakehouse"},
            "prefs": {"site_name": "Le Wagon Lakehouse", "site_locale": "en", "allow_tracking": False},
        })
        print("  admin user created")
    return call("POST", "/api/session", {"username": ADMIN["email"], "password": ADMIN["password"]})["id"]


def ensure_database(session: str) -> int:
    dbs = call("GET", "/api/database", session=session)
    dbs = dbs["data"] if isinstance(dbs, dict) else dbs
    by_name = {d["name"]: d["id"] for d in dbs}
    if DATABASE not in by_name:
        by_name[DATABASE] = call("POST", "/api/database", {"engine": "postgres", "name": DATABASE, "details": PG},
                                 session=session)["id"]
        print(f"  database {DATABASE!r} created")
    if "Warehouse DuckDB" not in by_name and os.path.exists("infra/metabase/plugins/duckdb.metabase-driver.jar"):
        call("POST", "/api/database", {"engine": "duckdb", "name": "Warehouse DuckDB",
                                       "details": {"database_file": "/data/warehouse.duckdb", "read_only": True}},
             session=session)
        print("  database 'Warehouse DuckDB' created")
    return by_name[DATABASE]


def wait_for_gold(session: str, db_id: int, timeout_s: int) -> None:
    """dbt runs in Kestra: wait until every gold table exists."""
    sql = "select count(*) from information_schema.tables where table_schema = 'gold'"
    deadline = time.time() + timeout_s
    while True:
        result = call("POST", "/api/dataset", {"type": "native", "native": {"query": sql}, "database": db_id},
                      session=session)
        rows = (result.get("data") or {}).get("rows") or [[0]]
        if rows[0][0] >= GOLD_TABLES:
            return
        if time.time() > deadline:
            raise SystemExit(f"Only {rows[0][0]} gold tables after {timeout_s}s")
        print(f"  waiting for dbt: {rows[0][0]}/{GOLD_TABLES} gold tables")
        time.sleep(15)


# --------------------------------------------------------------------------- cards + dashboards
def ensure_collection(session: str) -> int:
    for c in call("GET", "/api/collection", session=session):
        if c.get("name") == COLLECTION and not c.get("archived"):
            return c["id"]
    return call("POST", "/api/collection", {"name": COLLECTION, "color": "#509EE3"}, session=session)["id"]


def ensure_cards(session: str, db_id: int, collection_id: int) -> dict[str, int]:
    existing = {c["name"]: c["id"] for c in call("GET", "/api/card", session=session)
                if c.get("collection_id") == collection_id and not c.get("archived")}
    ids = {}
    for name, (display, sql, viz) in CARDS.items():
        body = {"name": name, "display": display, "visualization_settings": viz, "collection_id": collection_id,
                "dataset_query": {"type": "native", "native": {"query": sql}, "database": db_id}}
        if name in existing:
            ids[name] = existing[name]
            call("PUT", f"/api/card/{ids[name]}", body, session=session)
        else:
            ids[name] = call("POST", "/api/card", body, session=session)["id"]
        print(f"  card {name!r} -> {ids[name]}")
    return ids


def ensure_dashboards(session: str, cards: dict[str, int], collection_id: int) -> None:
    items = call("GET", f"/api/collection/{collection_id}/items?models=dashboard", session=session)
    items = items["data"] if isinstance(items, dict) else items
    existing = {d["name"]: d["id"] for d in items}
    for title, rows in DASHBOARDS.items():
        dash_id = existing.get(title) or call("POST", "/api/dashboard", {"name": title, "collection_id": collection_id},
                                              session=session)["id"]
        dashcards, y = [], 0
        for row in rows:
            x = 0
            for name, width, height in row:
                dashcards.append({"id": -(len(dashcards) + 1), "card_id": cards[name],
                                  "row": y, "col": x, "size_x": width, "size_y": height})
                x += width
            y += max(h for _, _, h in row)
        # Sending the full list replaces whatever the dashboard had: re-runs stay clean
        call("PUT", f"/api/dashboard/{dash_id}", {"dashcards": dashcards}, session=session)
        print(f"  dashboard {title!r} -> {MB}/dashboard/{dash_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--wait", type=int, default=0, help="seconds to wait for dbt's gold tables")
    args = parser.parse_args()

    wait_ready()
    session = login()
    db_id = ensure_database(session)
    if args.wait:
        wait_for_gold(session, db_id, args.wait)
    collection_id = ensure_collection(session)
    print("Cards")
    cards = ensure_cards(session, db_id, collection_id)
    print("Dashboards")
    ensure_dashboards(session, cards, collection_id)


if __name__ == "__main__":
    main()
