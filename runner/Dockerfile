# Image used by every Kestra task (scripts + dbt). The repo is mounted at /app.
FROM python:3.13-slim
RUN pip install --no-cache-dir uv && uv pip install --system --no-cache \
      "dbt-core>=1.11,<2" "dbt-duckdb>=1.10" "dbt-postgres>=1.10" "duckdb>=1.5.4" \
      "pyiceberg[sql-postgres,pyarrow,s3fs]>=0.11.1" "pyarrow>=21" fsspec s3fs psycopg2-binary
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app \
    MINIO_ROOT_USER=minio MINIO_ROOT_PASSWORD=minio123 MINIO_ENDPOINT=http://minio:9000 \
    LANDING_URI=s3://landing/shop \
    PYICEBERG_CATALOG__LAKEHOUSE__TYPE=sql \
    PYICEBERG_CATALOG__LAKEHOUSE__URI=postgresql+psycopg2://lakehouse:lakehouse@warehouse-postgres:5432/iceberg_catalog \
    PYICEBERG_CATALOG__LAKEHOUSE__WAREHOUSE=s3://warehouse \
    PYICEBERG_CATALOG__LAKEHOUSE__S3__ENDPOINT=http://minio:9000 \
    PYICEBERG_CATALOG__LAKEHOUSE__S3__ACCESS_KEY_ID=minio \
    PYICEBERG_CATALOG__LAKEHOUSE__S3__SECRET_ACCESS_KEY=minio123 \
    PYICEBERG_CATALOG__LAKEHOUSE__S3__REGION=us-east-1 \
    DUCKDB_PATH=/app/include/data/warehouse.duckdb \
    WAREHOUSE_PG_HOST=warehouse-postgres WAREHOUSE_PG_PORT=5432 \
    WAREHOUSE_PG_USER=lakehouse WAREHOUSE_PG_PASSWORD=lakehouse WAREHOUSE_PG_DB=warehouse
WORKDIR /app
