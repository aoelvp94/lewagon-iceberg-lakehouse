import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me")
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
# DuckDB file is mounted read-only from include/data
PREVENT_UNSAFE_DB_CONNECTIONS = False
FEATURE_FLAGS = {"DASHBOARD_RBAC": False}
WTF_CSRF_ENABLED = False  # local classroom demo only
