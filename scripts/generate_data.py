# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=21", "fsspec>=2025.1", "s3fs>=2025.1"]
# ///
"""Fake e-commerce source system that drops CDC-like batches into the landing zone.

    uv run scripts/generate_data.py initial
    uv run scripts/generate_data.py updates
    uv run scripts/generate_data.py schema-evolution
    uv run scripts/generate_data.py late-arriving
    uv run scripts/generate_data.py all

Each scenario writes a fixed batch id, so re-running a scenario is harmless:
the ingestion skips batches already committed to Iceberg.

Layout:  <landing>/<entity>/batch=<id>/part-000.(jsonl|parquet)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

SEED = 42
N_CUSTOMERS, N_PRODUCTS, N_ORDERS = 200, 40, 1_000
T0 = dt.datetime(2026, 1, 1, 8, 0, 0)

FIRST = ["Ana", "Bruno", "Camila", "Diego", "Elena", "Facundo", "Gabriela", "Hugo", "Ines", "Joaquin", "Lucia", "Martin"]
LAST = ["Garcia", "Lopez", "Martinez", "Rodriguez", "Perez", "Gomez", "Fernandez", "Diaz", "Alvarez", "Romero"]
COUNTRIES = ["AR", "UY", "CL", "ES", "MX", "FR", "AM", "DE"]
CATEGORIES = ["books", "coffee", "electronics", "garden", "sports", "toys"]
STATUSES = ["placed", "paid", "shipped", "delivered"]

BATCHES = {"initial": "0001", "updates": "0002", "schema-evolution": "0003", "late-arriving": "0004"}


def iso(ts: dt.datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------------------------------------------ deterministic world
def base_world() -> dict[str, list[dict]]:
    rnd = random.Random(SEED)
    customers = []
    for cid in range(1, N_CUSTOMERS + 1):
        first, last = rnd.choice(FIRST), rnd.choice(LAST)
        created = T0 + dt.timedelta(minutes=rnd.randint(0, 60 * 24 * 30))
        customers.append(
            dict(customer_id=cid, first_name=first, last_name=last,
                 email=f"{first}.{last}.{cid}@example.com".lower(), country=rnd.choice(COUNTRIES),
                 created_at=iso(created), updated_at=iso(created), _op="I")
        )
    products = [
        dict(product_id=pid, name=f"{rnd.choice(CATEGORIES)}-item-{pid:03d}", category=rnd.choice(CATEGORIES),
             unit_price=round(rnd.uniform(3, 250), 2), updated_at=iso(T0), _op="I")
        for pid in range(1, N_PRODUCTS + 1)
    ]
    price = {p["product_id"]: p["unit_price"] for p in products}
    orders, items = [], []
    for oid in range(1, N_ORDERS + 1):
        ts = T0 + dt.timedelta(minutes=rnd.randint(60 * 24 * 30, 60 * 24 * 90))
        orders.append(dict(order_id=oid, customer_id=rnd.randint(1, N_CUSTOMERS), order_ts=iso(ts),
                           status="placed", updated_at=iso(ts), _op="I"))
        for line in range(1, rnd.randint(1, 4) + 1):
            pid = rnd.randint(1, N_PRODUCTS)
            items.append(dict(order_id=oid, line_number=line, product_id=pid, quantity=rnd.randint(1, 5),
                              unit_price=price[pid], updated_at=iso(ts), _op="I"))
    return {"customers": customers, "products": products, "orders": orders, "order_items": items}


# ------------------------------------------------------------------ scenarios
def scenario_initial() -> dict[str, list[dict]]:
    return base_world()


def scenario_updates() -> dict[str, list[dict]]:
    """Updates, deletes and exact duplicates -> bronze must dedup to the latest version."""
    rnd = random.Random(SEED + 1)
    w = base_world()
    t = T0 + dt.timedelta(days=100)
    customers = []
    for c in rnd.sample(w["customers"], 30):  # people move / change e-mail
        c = dict(c, email=c["email"].replace("@example.com", "@newmail.com"),
                 country=rnd.choice(COUNTRIES), updated_at=iso(t), _op="U")
        customers.append(c)
    for c in rnd.sample(w["customers"], 3):  # GDPR deletions
        customers.append(dict(c, updated_at=iso(t + dt.timedelta(minutes=5)), _op="D"))
    customers += customers[:5]  # the source re-sends some rows: exact duplicates

    orders = []
    for o in rnd.sample(w["orders"], 300):  # status moves forward
        orders.append(dict(o, status=rnd.choice(STATUSES[1:]), updated_at=iso(t), _op="U"))
    products = [dict(p, unit_price=round(p["unit_price"] * 1.10, 2), updated_at=iso(t), _op="U")
                for p in rnd.sample(w["products"], 5)]
    return {"customers": customers, "orders": orders, "products": products}


def scenario_schema_evolution() -> dict[str, pa.Table]:
    """The source adds a column and widens a type. It also switched to Parquet exports."""
    rnd = random.Random(SEED + 2)
    w = base_world()
    t = T0 + dt.timedelta(days=110)
    tiers = ["bronze", "silver", "gold", "platinum"]
    customers = [dict(c, loyalty_tier=rnd.choice(tiers), updated_at=iso(t), _op="U") for c in w["customers"]]
    new_orders, new_items = [], []
    for i in range(50):  # big B2B orders: quantity needs int64 now
        oid = N_ORDERS + 1 + i
        new_orders.append(dict(order_id=oid, customer_id=rnd.randint(1, N_CUSTOMERS), order_ts=iso(t),
                               status="paid", updated_at=iso(t), _op="I"))
        new_items.append(dict(order_id=oid, line_number=1, product_id=rnd.randint(1, N_PRODUCTS),
                              quantity=rnd.randint(3_000, 5_000),
                              unit_price=9.99, updated_at=iso(t), _op="I"))
    ts = pa.timestamp("us")
    c_schema = pa.schema([("customer_id", pa.int64()), ("first_name", pa.string()), ("last_name", pa.string()),
                          ("email", pa.string()), ("country", pa.string()), ("created_at", ts),
                          ("updated_at", ts), ("_op", pa.string()), ("loyalty_tier", pa.string())])
    o_schema = pa.schema([("order_id", pa.int64()), ("customer_id", pa.int64()), ("order_ts", ts),
                          ("status", pa.string()), ("updated_at", ts), ("_op", pa.string())])
    i_schema = pa.schema([("order_id", pa.int64()), ("line_number", pa.int32()), ("product_id", pa.int64()),
                          ("quantity", pa.int64()),  # <- was int32
                          ("unit_price", pa.float64()), ("updated_at", ts), ("_op", pa.string())])
    return {
        "customers": _to_arrow(customers, c_schema),
        "orders": _to_arrow(new_orders, o_schema),
        "order_items": _to_arrow(new_items, i_schema),
    }


def scenario_late_arriving() -> dict[str, list[dict]]:
    """Events that arrive *after* newer versions: dedup must order by updated_at, not arrival."""
    w = base_world()
    old = T0 + dt.timedelta(days=95)  # older than the 'updates' batch (day 100)
    rnd = random.Random(SEED + 3)
    orders = [dict(o, status="placed", updated_at=iso(old), _op="U") for o in rnd.sample(w["orders"], 20)]
    return {"orders": orders}


def _to_arrow(rows: list[dict], schema: pa.Schema) -> pa.Table:
    cols = {}
    for f in schema:
        values = [r.get(f.name) for r in rows]
        if pa.types.is_timestamp(f.type):
            values = [dt.datetime.fromisoformat(v) if v else None for v in values]
        cols[f.name] = pa.array(values, f.type)
    return pa.table(cols, schema=schema)


SCENARIOS = {
    "initial": scenario_initial,
    "updates": scenario_updates,
    "schema-evolution": scenario_schema_evolution,
    "late-arriving": scenario_late_arriving,
}


# ------------------------------------------------------------------ writing
def filesystem(landing: str):
    if landing.startswith("s3://"):
        return fsspec.filesystem(
            "s3",
            key=os.getenv("MINIO_ROOT_USER", "minio"),
            secret=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
            client_kwargs={"endpoint_url": os.getenv("MINIO_ENDPOINT", "http://localhost:9000")},
        ), landing.removeprefix("s3://")
    path = landing.removeprefix("file://")
    return fsspec.filesystem("file", auto_mkdir=True), path


def write_scenario(name: str, landing: str) -> None:
    fs, root = filesystem(landing)
    batch_id = BATCHES[name]
    for entity, payload in SCENARIOS[name]().items():
        folder = f"{root}/{entity}/batch={batch_id}"
        fs.makedirs(folder, exist_ok=True)
        if isinstance(payload, pa.Table):
            path = f"{folder}/part-000.parquet"
            with fs.open(path, "wb") as fh:
                pq.write_table(payload, fh)
            n = payload.num_rows
        else:
            path = f"{folder}/part-000.jsonl"
            with fs.open(path, "w") as fh:
                fh.write("\n".join(json.dumps(r) for r in payload) + "\n")
            n = len(payload)
        print(f"  {name:17s} {entity:12s} batch={batch_id}  {n:5d} rows -> {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenario", choices=[*SCENARIOS, "all"])
    parser.add_argument("--landing", default=os.getenv("LANDING_URI", "s3://landing/shop"))
    args = parser.parse_args()
    names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    for name in names:
        write_scenario(name, args.landing)


if __name__ == "__main__":
    main()
