# JupyterLab for exploring the lakehouse: jupysql (Postgres + DuckDB) and MinIO (s3fs / pyiceberg).
FROM quay.io/jupyter/minimal-notebook:python-3.12
RUN pip install --no-cache-dir \
      "jupysql>=0.11" \
      "duckdb>=1.5" "duckdb-engine>=0.17" \
      "psycopg2-binary>=2.9" \
      "pyiceberg[pyarrow,s3fs,sql-postgres]>=0.11.1" \
      "pyarrow>=21" "s3fs>=2025.1"
