FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
RUN pip install --no-cache-dir uv && uv pip install --system --no-cache \
      "prefect>=3.4" "prefect-dbt>=0.7" \
      "dbt-core>=1.11,<2" "dbt-duckdb>=1.10" "dbt-postgres>=1.10" "duckdb>=1.5.4" \
      "pyiceberg[sql-postgres,pyarrow,s3fs]>=0.11.1" "pyarrow>=21" fsspec s3fs psycopg2-binary
WORKDIR /app
CMD ["python", "flows/lakehouse.py"]
