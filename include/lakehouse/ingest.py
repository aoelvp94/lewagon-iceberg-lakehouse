"""Landing zone (object storage) -> Iceberg ``raw`` tables.

This is the logic behind the transfer operator. It is deliberately small and
readable for class:

1. discover batch folders  ``<prefix>/<entity>/batch=<id>/``  (JSONL or Parquet)
2. skip batches already committed (idempotency via an Iceberg *snapshot property*)
3. read the batch with pyarrow, keeping columns we did not expect
4. evolve the Iceberg schema if needed (new columns, int -> long, float -> double)
5. append (keeps full history -> bronze dedups) or upsert (merge on primary key)

One batch == one Iceberg snapshot, so time travel works "per batch".
"""

from __future__ import annotations

import datetime as dt
import logging
import posixpath
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.json as pa_json
import pyarrow.parquet as pq
from fsspec import AbstractFileSystem
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table
from pyiceberg.types import DoubleType, LongType

from include.lakehouse.catalog import ensure_namespace
from include.lakehouse.entities import Entity

log = logging.getLogger(__name__)

BATCH_PROPERTY = "lakehouse.batch-id"
METADATA_COLUMNS = pa.schema(
    [
        ("_batch_id", pa.string()),
        ("_source_file", pa.string()),
        ("_ingested_at", pa.timestamp("us")),
    ]
)
# Promotions allowed by the Iceberg spec that we handle automatically.
_PROMOTIONS = {
    (pa.int32(), pa.int64()): LongType(),
    (pa.float32(), pa.float64()): DoubleType(),
}

Mode = Literal["append", "upsert"]


@dataclass
class IngestResult:
    table: str
    batches_loaded: list[str] = field(default_factory=list)
    batches_skipped: list[str] = field(default_factory=list)
    rows_written: int = 0
    schema_changes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return self.__dict__.copy()


# --------------------------------------------------------------------------- discovery
def list_batches(fs: AbstractFileSystem, root: str, entity: str) -> list[tuple[str, list[str]]]:
    """Return ``[(batch_id, [file, ...]), ...]`` sorted by batch id."""
    base = posixpath.join(root, entity)
    if not fs.exists(base):
        return []
    batches = []
    for path in sorted(fs.ls(base, detail=False)):
        name = posixpath.basename(path.rstrip("/"))
        if not name.startswith("batch="):
            continue
        files = sorted(
            f for f in fs.ls(path, detail=False) if f.endswith((".jsonl", ".json", ".parquet"))
        )
        if files:
            batches.append((name.removeprefix("batch="), files))
    return batches


def committed_batches(table: Table) -> set[str]:
    """Batch ids already written, read back from snapshot summaries."""
    done = set()
    for snapshot in table.snapshots():
        summary = _summary_dict(snapshot.summary)
        if BATCH_PROPERTY in summary:
            done.add(summary[BATCH_PROPERTY])
    return done


def _summary_dict(summary) -> dict[str, str]:
    """Snapshot summary as a plain dict (pyiceberg's ``Summary`` keeps custom keys apart)."""
    if summary is None:
        return {}
    return {"operation": str(summary.operation.value), **summary.additional_properties}


# --------------------------------------------------------------------------- reading
def read_landing_file(fs: AbstractFileSystem, path: str, entity: Entity) -> pa.Table:
    with fs.open(path, "rb") as fh:
        if path.endswith(".parquet"):
            # Parquet carries its own types: this is how a source can *widen* a type.
            return pq.read_table(fh)
        # JSON: enforce the known contract, but keep unexpected columns (inferred).
        return pa_json.read_json(
            fh,
            parse_options=pa_json.ParseOptions(
                explicit_schema=entity.schema, unexpected_field_behavior="infer"
            ),
        )


def read_landing_batch(
    fs: AbstractFileSystem, files: Iterable[str], entity: Entity, batch_id: str
) -> pa.Table:
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    parts = []
    for path in files:
        tbl = read_landing_file(fs, path, entity)
        n = tbl.num_rows
        tbl = tbl.append_column("_batch_id", pa.array([batch_id] * n, pa.string()))
        tbl = tbl.append_column("_source_file", pa.array([path] * n, pa.string()))
        tbl = tbl.append_column("_ingested_at", pa.array([now] * n, pa.timestamp("us")))
        parts.append(tbl)
    return pa.concat_tables(parts, promote_options="permissive")


# --------------------------------------------------------------------------- schema
def create_or_load(catalog: Catalog, entity: Entity, first_batch: pa.Table) -> tuple[Table, bool]:
    ensure_namespace(catalog, entity.table_identifier.split(".")[0])
    try:
        return catalog.load_table(entity.table_identifier), False
    except NoSuchTableError:
        log.info("Creating Iceberg table %s", entity.table_identifier)
        table = catalog.create_table(
            entity.table_identifier,
            schema=first_batch.schema,
            properties={"lakehouse.primary-key": ",".join(entity.primary_key)},
        )
        return table, True


def evolve_schema(table: Table, batch: pa.Table) -> list[str]:
    """Add new columns and apply safe type promotions. Returns human-readable changes."""
    current = table.schema().as_arrow()
    current_by_name = {f.name: f for f in current}
    additions, promotions, changes = [], [], []

    for f in batch.schema:
        existing = current_by_name.get(f.name)
        if existing is None:
            additions.append(pa.field(f.name, f.type, nullable=True))
            changes.append(f"add column {f.name} {f.type}")
            continue
        if _same_type(existing.type, f.type) or _is_narrower(f.type, existing.type):
            continue
        promoted = _PROMOTIONS.get((_norm(existing.type), _norm(f.type)))
        if promoted is None:
            raise TypeError(
                f"Incompatible change on {table.name()}.{f.name}: {existing.type} -> {f.type}. "
                "Iceberg only allows safe promotions (int->long, float->double, decimal precision)."
            )
        promotions.append((f.name, promoted))
        changes.append(f"promote column {f.name} {existing.type} -> {f.type}")

    if additions or promotions:
        with table.update_schema() as update:
            if additions:
                update.union_by_name(pa.schema(additions))
            for name, new_type in promotions:
                update.update_column(name, field_type=new_type)
        log.info("Schema evolved on %s: %s", table.name(), changes)
    return changes


def conform_to_table(table: Table, batch: pa.Table) -> pa.Table:
    """Cast/reorder the batch to the table schema; missing columns become nulls."""
    target = table.schema().as_arrow()
    columns = []
    for f in target:
        if f.name in batch.column_names:
            columns.append(pc.cast(batch[f.name], f.type))
        else:
            columns.append(pa.nulls(batch.num_rows, f.type))
    return pa.Table.from_arrays(columns, schema=target)


def _norm(t: pa.DataType) -> pa.DataType:
    if pa.types.is_large_string(t):
        return pa.string()
    return t


def _same_type(a: pa.DataType, b: pa.DataType) -> bool:
    return _norm(a) == _norm(b)


def _is_narrower(new: pa.DataType, existing: pa.DataType) -> bool:
    """True when ``new`` can be cast losslessly into ``existing`` (e.g. int32 into int64)."""
    return (_norm(new), _norm(existing)) in _PROMOTIONS


# --------------------------------------------------------------------------- main entry
def land_to_iceberg(
    *,
    fs: AbstractFileSystem,
    landing_root: str,
    entity: Entity,
    catalog: Catalog,
    mode: Mode = "append",
) -> IngestResult:
    """Load every not-yet-committed landing batch of ``entity`` into Iceberg."""
    result = IngestResult(table=entity.table_identifier)
    batches = list_batches(fs, landing_root, entity.name)
    if not batches:
        log.info("No landing batches for %s under %s", entity.name, landing_root)
        return result

    table: Table | None = None
    for batch_id, files in batches:
        batch = read_landing_batch(fs, files, entity, batch_id)
        if table is None:
            table, _ = create_or_load(catalog, entity, batch)
        if batch_id in committed_batches(table):
            result.batches_skipped.append(batch_id)
            continue

        result.schema_changes += evolve_schema(table, batch)
        table = table.refresh()
        data = conform_to_table(table, batch)
        props = {BATCH_PROPERTY: batch_id, "lakehouse.source-files": str(len(files))}

        if mode == "upsert":
            # Merge on the primary key: keep only the latest version of each key in the batch
            data = _latest_per_key(data, entity.primary_key)
            table.upsert(data, join_cols=list(entity.primary_key), snapshot_properties=props)
        else:
            table.append(data, snapshot_properties=props)

        table = table.refresh()
        result.batches_loaded.append(batch_id)
        result.rows_written += data.num_rows
        log.info("%s: batch %s -> %s rows (%s)", entity.table_identifier, batch_id, data.num_rows, mode)
    return result


def _latest_per_key(data: pa.Table, keys: tuple[str, ...]) -> pa.Table:
    """Upsert needs unique keys inside the incoming batch."""
    order = data.sort_by([("updated_at", "descending")])
    seen, keep = set(), []
    key_cols = [order[k].to_pylist() for k in keys]
    for i, key in enumerate(zip(*key_cols, strict=True)):
        if key not in seen:
            seen.add(key)
            keep.append(i)
    return order.take(pa.array(keep, pa.int64()))
