# Pinned for reproducible classes; 6.1 also ships the built-in MCP service (superset mcp run).
ARG SUPERSET_VERSION=6.1.0
FROM apache/superset:${SUPERSET_VERSION}
USER root
# Drivers for the two gold layers: DuckDB file + Postgres.
# Superset 6 runs from /app/.venv (managed by uv).
RUN uv pip install --python /app/.venv/bin/python --no-cache duckdb duckdb-engine psycopg2-binary
COPY bootstrap.sh /app/bootstrap.sh
RUN chmod +x /app/bootstrap.sh
USER superset
CMD ["/app/bootstrap.sh"]
