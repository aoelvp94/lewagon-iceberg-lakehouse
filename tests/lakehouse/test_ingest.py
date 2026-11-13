"""What the transfer operator guarantees, written as tests students can read."""

from __future__ import annotations

import pyarrow.compute as pc
import pytest
from pyiceberg.types import LongType

from include.lakehouse.entities import CUSTOMERS, ENTITIES, ORDER_ITEMS, ORDERS
from include.lakehouse.ingest import BATCH_PROPERTY, committed_batches, land_to_iceberg


def load(entity, catalog, landing, mode="append"):
    fs, root = landing
    return land_to_iceberg(fs=fs, landing_root=root, entity=entity, catalog=catalog, mode=mode)


def test_initial_load_creates_tables_with_metadata_columns(catalog, landing, generator):
    generator.write_scenario("initial", f"file://{landing[1]}")
    for entity in ENTITIES.values():
        result = load(entity, catalog, landing)
        assert result.batches_loaded == ["0001"]

    customers = catalog.load_table("raw.customers").scan().to_arrow()
    assert customers.num_rows == generator.N_CUSTOMERS
    assert {"_batch_id", "_source_file", "_ingested_at"} <= set(customers.column_names)


def test_rerun_is_idempotent(catalog, landing, generator):
    generator.write_scenario("initial", f"file://{landing[1]}")
    load(CUSTOMERS, catalog, landing)
    again = load(CUSTOMERS, catalog, landing)

    assert again.batches_loaded == [] and again.batches_skipped == ["0001"]
    table = catalog.load_table("raw.customers")
    assert len(table.snapshots()) == 1
    assert committed_batches(table) == {"0001"}


def test_updates_are_appended_and_history_is_queryable(catalog, landing, generator):
    generator.write_scenario("initial", f"file://{landing[1]}")
    generator.write_scenario("updates", f"file://{landing[1]}")
    load(CUSTOMERS, catalog, landing)

    table = catalog.load_table("raw.customers")
    first, second = table.snapshots()
    assert dict(second.summary)[BATCH_PROPERTY] == "0002"

    # raw keeps every version -> more rows than customers; bronze will dedup
    now = table.scan().to_arrow()
    assert now.num_rows > generator.N_CUSTOMERS
    # time travel: the first snapshot only has the initial batch
    before = table.scan(snapshot_id=first.snapshot_id).to_arrow()
    assert before.num_rows == generator.N_CUSTOMERS
    assert set(pc.unique(now["_op"]).to_pylist()) == {"I", "U", "D"}


def test_schema_evolution_adds_column_and_old_rows_are_null(catalog, landing, generator):
    for s in ("initial", "schema-evolution"):
        generator.write_scenario(s, f"file://{landing[1]}")
    result = load(CUSTOMERS, catalog, landing)

    assert any("loyalty_tier" in c for c in result.schema_changes)
    table = catalog.load_table("raw.customers")
    rows = table.scan().to_arrow()
    old = rows.filter(pc.equal(rows["_batch_id"], "0001"))
    assert old["loyalty_tier"].null_count == old.num_rows
    assert len(table.metadata.schemas) == 2  # schema history is kept


def test_type_widening_int_to_long(catalog, landing, generator):
    for s in ("initial", "schema-evolution"):
        generator.write_scenario(s, f"file://{landing[1]}")
    load(ORDER_ITEMS, catalog, landing)

    table = catalog.load_table("raw.order_items")
    assert isinstance(table.schema().find_field("quantity").field_type, LongType)


def test_upsert_mode_keeps_one_row_per_key(catalog, landing, generator):
    generator.write_scenario("initial", f"file://{landing[1]}")
    generator.write_scenario("updates", f"file://{landing[1]}")
    load(ORDERS, catalog, landing, mode="upsert")

    rows = catalog.load_table("raw.orders").scan().to_arrow()
    assert rows.num_rows == generator.N_ORDERS
    assert len(pc.unique(rows["order_id"])) == rows.num_rows


def test_incompatible_type_change_fails_loudly(catalog, landing, generator, tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    generator.write_scenario("initial", f"file://{landing[1]}")
    load(CUSTOMERS, catalog, landing)
    bad = pa.table({"customer_id": pa.array(["not-a-number"]), "updated_at": pa.array([None], pa.timestamp("us"))})
    folder = tmp_path / "landing" / "customers" / "batch=9999"
    folder.mkdir(parents=True)
    pq.write_table(bad, folder / "part-000.parquet")

    with pytest.raises(TypeError, match="Incompatible change"):
        load(CUSTOMERS, catalog, landing)
