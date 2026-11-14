# Streamlit dashboard over the gold layer (DuckDB file + Postgres). Embedded in Airflow by
# plugins/lakehouse_dashboard.py. Built by `just build-images`.
FROM python:3.13-slim
RUN pip install --no-cache-dir "streamlit>=1.50" "duckdb>=1.5.4" "pyarrow>=21" "psycopg2-binary>=2.9"
WORKDIR /app
COPY app.py .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.runOnSave=true"]
