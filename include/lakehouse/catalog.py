"""Iceberg catalog access.

The catalog is a pyiceberg *SQL catalog* stored in Postgres, with data and
metadata files in MinIO (S3 API). Configuration comes from environment variables
so the same code works on the host (localhost ports) and inside containers
(service names):

    PYICEBERG_CATALOG__LAKEHOUSE__TYPE=sql
    PYICEBERG_CATALOG__LAKEHOUSE__URI=postgresql+psycopg2://lakehouse:lakehouse@warehouse-postgres:5432/iceberg_catalog
    PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE=s3://warehouse
    PYICEBERG_CATALOG__LAKEHOUSE__S3__ENDPOINT=http://minio:9000
    PYICEBERG_CATALOG__LAKEHOUSE__S3__ACCESS_KEY_ID=minio
    PYICEBERG_CATALOG__LAKEHOUSE__S3__SECRET_ACCESS_KEY=minio123
    PYICEBERG_CATALOG__LAKEHOUSE__S3__REGION=us-east-1

pyiceberg maps ``__`` to ``.`` and ``_`` to ``-`` (``S3__ACCESS_KEY_ID`` -> ``s3.access-key-id``).
"""

from __future__ import annotations

import os

from pyiceberg.catalog import Catalog, load_catalog

DEFAULT_CATALOG = os.getenv("LAKEHOUSE_CATALOG", "lakehouse")


def get_catalog(name: str = DEFAULT_CATALOG, **overrides: str) -> Catalog:
    """Load the catalog from env vars; ``overrides`` win (used by the tests)."""
    return load_catalog(name, **overrides)


def ensure_namespace(catalog: Catalog, namespace: str) -> None:
    if (namespace,) not in catalog.list_namespaces():
        catalog.create_namespace(namespace)
