ARG SUPERSET_VERSION=latest
FROM apache/superset:${SUPERSET_VERSION}
USER root
# Drivers for the two gold layers: DuckDB file + Postgres.
# Superset >= 5 runs from /app/.venv (uv); fall back to pip for older images.
RUN (uv pip install --python /app/.venv/bin/python --no-cache duckdb duckdb-engine psycopg2-binary) \
 || pip install --no-cache-dir duckdb duckdb-engine psycopg2-binary
COPY bootstrap.sh /app/bootstrap.sh
RUN chmod +x /app/bootstrap.sh
USER superset
CMD ["/app/bootstrap.sh"]
