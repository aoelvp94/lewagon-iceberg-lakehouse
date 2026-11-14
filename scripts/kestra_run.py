# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Run lakehouse_ingest in Kestra and wait for the whole chain (stdlib only).

    uv run scripts/kestra_run.py --mode append

1. POST an execution of lakehouse_ingest (input `mode`) and wait for it.
2. Its SUCCESS fires the Flow triggers of lakehouse_dbt_duckdb and lakehouse_dbt_postgres:
   find the executions whose trigger points at our ingest execution and wait for them.
Exit code 1 if anything does not end in SUCCESS.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import time
import urllib.parse
import urllib.request

KESTRA = os.getenv("KESTRA_URL", "http://localhost:8080")
AUTH = base64.b64encode(os.getenv("KESTRA_AUTH", "admin@kestra.io:Admin1234").encode()).decode()
NAMESPACE = "lewagon.lakehouse"
DBT_FLOWS = ("lakehouse_dbt_duckdb", "lakehouse_dbt_postgres")
DONE = {"SUCCESS", "WARNING", "FAILED", "KILLED", "CANCELLED", "SKIPPED"}


def api(method: str, path: str, body: bytes | None = None, content_type: str | None = None) -> dict:
    req = urllib.request.Request(f"{KESTRA}/api/v1/main{path}", method=method, data=body)
    req.add_header("Authorization", f"Basic {AUTH}")
    if content_type:
        req.add_header("Content-Type", content_type)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read() or b"{}")


def start_ingest(mode: str) -> str:
    # Kestra takes flow inputs as multipart/form-data
    boundary = "lakehouse-boundary"
    body = (f'--{boundary}\r\nContent-Disposition: form-data; name="mode"\r\n\r\n{mode}\r\n'
            f"--{boundary}--\r\n").encode()
    execution = api("POST", f"/executions/{NAMESPACE}/lakehouse_ingest", body,
                    f"multipart/form-data; boundary={boundary}")
    return execution["id"]


def wait_for(execution_id: str) -> str:
    while True:
        state = api("GET", f"/executions/{execution_id}")["state"]["current"]
        if state in DONE:
            return state
        time.sleep(3)


def triggered_by(flow_id: str, ingest_id: str) -> str | None:
    q = urllib.parse.urlencode({"namespace": NAMESPACE, "flowId": flow_id, "size": 20})
    for e in api("GET", f"/executions/search?{q}")["results"]:
        if e.get("flowId") != flow_id:  # filter here too: the search may return other flows
            continue
        if ((e.get("trigger") or {}).get("variables") or {}).get("executionId") == ingest_id:
            return e["id"]
    return None


def url(flow_id: str, execution_id: str) -> str:
    return f"{KESTRA}/ui/main/executions/{NAMESPACE}/{flow_id}/{execution_id}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="append", choices=["append", "upsert"])
    parser.add_argument("--timeout", type=int, default=900)
    args = parser.parse_args()

    ingest_id = start_ingest(args.mode)
    state = wait_for(ingest_id)
    print(f"lakehouse_ingest        {state:8} {url('lakehouse_ingest', ingest_id)}")
    if state != "SUCCESS":
        raise SystemExit(1)

    deadline, failed = time.time() + args.timeout, False
    for flow_id in DBT_FLOWS:
        execution_id = None
        while execution_id is None:  # the Flow trigger needs a moment to create it
            if time.time() > deadline:
                raise SystemExit(f"{flow_id}: no execution triggered by {ingest_id}")
            execution_id = triggered_by(flow_id, ingest_id)
            time.sleep(2)
        state = wait_for(execution_id)
        print(f"{flow_id:23} {state:8} {url(flow_id, execution_id)}")
        failed |= state != "SUCCESS"
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
