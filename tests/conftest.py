"""Shared fixtures: a throw-away Iceberg catalog (SQLite + local warehouse) and landing zone.

No Docker needed: ``just test`` runs these in seconds on a laptop.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import fsspec
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture
def catalog(tmp_path):
    from pyiceberg.catalog.sql import SqlCatalog

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    return SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{warehouse}",
    )


@pytest.fixture
def landing(tmp_path):
    root = tmp_path / "landing"
    root.mkdir()
    return fsspec.filesystem("file", auto_mkdir=True), str(root)


@pytest.fixture
def generator():
    spec = importlib.util.spec_from_file_location("generate_data", ROOT / "scripts" / "generate_data.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
