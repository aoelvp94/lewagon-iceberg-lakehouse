from airflow.sdk import Asset

# Emitted by the ingest DAG, consumed by both dbt DAGs (data-aware scheduling).
ICEBERG_RAW = Asset(name="iceberg_raw", uri="s3://warehouse/raw")
