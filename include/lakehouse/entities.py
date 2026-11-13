"""Source entities of the fake e-commerce ("shop") system.

The *base* Arrow schemas are what we expect from the source. Extra columns that
show up later are NOT rejected: they are inferred and become a schema evolution
in Iceberg (see ``ingest.read_landing_batch``).
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

TS = pa.timestamp("us")


@dataclass(frozen=True)
class Entity:
    name: str
    primary_key: tuple[str, ...]
    schema: pa.Schema

    @property
    def table_identifier(self) -> str:
        return f"raw.{self.name}"


CUSTOMERS = Entity(
    "customers",
    ("customer_id",),
    pa.schema(
        [
            ("customer_id", pa.int64()),
            ("first_name", pa.string()),
            ("last_name", pa.string()),
            ("email", pa.string()),
            ("country", pa.string()),
            ("created_at", TS),
            ("updated_at", TS),
            ("_op", pa.string()),  # I = insert, U = update, D = delete (CDC style)
        ]
    ),
)

PRODUCTS = Entity(
    "products",
    ("product_id",),
    pa.schema(
        [
            ("product_id", pa.int64()),
            ("name", pa.string()),
            ("category", pa.string()),
            ("unit_price", pa.float64()),
            ("updated_at", TS),
            ("_op", pa.string()),
        ]
    ),
)

ORDERS = Entity(
    "orders",
    ("order_id",),
    pa.schema(
        [
            ("order_id", pa.int64()),
            ("customer_id", pa.int64()),
            ("order_ts", TS),
            ("status", pa.string()),
            ("updated_at", TS),
            ("_op", pa.string()),
        ]
    ),
)

ORDER_ITEMS = Entity(
    "order_items",
    ("order_id", "line_number"),
    pa.schema(
        [
            ("order_id", pa.int64()),
            ("line_number", pa.int32()),
            ("product_id", pa.int64()),
            # int32 on purpose: the schema-evolution scenario widens it to int64
            ("quantity", pa.int32()),
            ("unit_price", pa.float64()),
            ("updated_at", TS),
            ("_op", pa.string()),
        ]
    ),
)

ENTITIES: dict[str, Entity] = {e.name: e for e in (CUSTOMERS, PRODUCTS, ORDERS, ORDER_ITEMS)}
