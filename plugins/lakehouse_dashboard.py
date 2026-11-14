"""Airflow plugin: the Streamlit gold dashboard inside the Airflow UI.

Airflow 3 plugins can add *external views*: a link in the navigation bar that,
when ``url_route`` is set, opens the page embedded (iframe) inside Airflow at
``/plugin/<url_route>``. The dashboard itself is the ``streamlit`` service
(dashboard/app.py); the plugin only tells Airflow where it lives.

Your browser loads the iframe, so the URL is the one *you* use (localhost),
not the Docker service name.
"""

from __future__ import annotations

import os

from airflow.plugins_manager import AirflowPlugin

DASHBOARD_URL = os.getenv("LAKEHOUSE_DASHBOARD_URL", "http://localhost:8501")


class LakehouseDashboardPlugin(AirflowPlugin):
    name = "lakehouse_dashboard"
    external_views = [
        {
            "name": "Lakehouse dashboard",
            "href": f"{DASHBOARD_URL}/?embed=true",  # Streamlit without its own header
            "url_route": "lakehouse-dashboard",  # embedded at /plugin/lakehouse-dashboard
            "destination": "nav",
            "nav_top_level": True,
        },
    ]
