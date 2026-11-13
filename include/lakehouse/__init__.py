"""Orchestrator-agnostic lakehouse helpers.

Everything in this package is plain Python (pyarrow + fsspec + pyiceberg).
Airflow, Prefect and Kestra only *wrap* these functions, so students can compare
orchestrators without the business logic changing underneath them.
"""

from include.lakehouse.catalog import get_catalog
from include.lakehouse.ingest import IngestResult, land_to_iceberg
from include.lakehouse.serve import iceberg_to_postgres

__all__ = ["IngestResult", "get_catalog", "iceberg_to_postgres", "land_to_iceberg"]
