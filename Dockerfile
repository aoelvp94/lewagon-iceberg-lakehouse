# Astro Runtime 3.3 = Airflow 3.3 (https://www.astronomer.io/docs/runtime/runtime-release-notes)
FROM astrocrpublic.azurecr.io/runtime:3.3-2

# dbt lives in its own virtualenv so its dependencies never fight Airflow's.
# Cosmos calls /usr/local/airflow/dbt_venv/bin/dbt (see include/cosmos_config.py).
RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir \
      "dbt-core>=1.11,<2" \
      "dbt-duckdb>=1.10" \
      "dbt-postgres>=1.10" \
      "duckdb>=1.5.4" \
      "pyiceberg[sql-postgres,pyarrow,s3fs]>=0.11.1"

ENV PYTHONPATH=/usr/local/airflow
