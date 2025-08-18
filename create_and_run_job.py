#!/usr/bin/env python3
"""
Minimal Hello World Databricks Job Runner (Python)

Env vars required:
- DATABRICKS_HOST (e.g. https://<workspace>.cloud.databricks.com)
- DATABRICKS_TOKEN (PAT)
- DATABRICKS_CLUSTER_ID (existing cluster) OR DATABRICKS_CLUSTER_NAME (to resolve ID)

Usage:
    export DATABRICKS_HOST="https://<your-workspace>.cloud.databricks.com"
    export DATABRICKS_TOKEN="<your_pat_token>"
    export DATABRICKS_CLUSTER_ID="<your_cluster_id>"  # or set DATABRICKS_CLUSTER_NAME
    python3 create_and_run_job.py
"""

import base64
import os
import sys
import time

import requests


def env(name):
    v = os.getenv(name)
    if not v:
        print(f"Missing env: {name}")
        sys.exit(1)
    return v


def api(method, path, version, *, json=None, params=None):
    host = env("DATABRICKS_HOST")
    token = env("DATABRICKS_TOKEN")
    if not host.startswith("http"):
        host = "https://" + host
    url = f"{host}/api/{version}/{path.lstrip('/')}"
    r = requests.request(
        method, url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=json, params=params, timeout=60
    )
    if not r.ok:
        try:
            print(r.json())
        except Exception:
            print(r.text)
        r.raise_for_status()
    return r.json() if r.text.strip() else {}


essential_job_name = "Hello World (Python)"


def upload_python_to_workspace_files(local_path: str) -> str:
    with open(local_path, "rb") as f:
        contents_b64 = base64.b64encode(f.read()).decode("utf-8")

    workspace_target_path = "/Workspace/Shared/simple_job.py"

    api(
        "POST",
        "workspace-files/import",
        "2.0",
        json={
            "path": workspace_target_path,
            "overwrite": True,
            "content": contents_b64,
        },
    )
    print(f"Uploaded {local_path} -> {workspace_target_path}")

    return "workspace://Shared/simple_job.py"


def resolve_cluster_id() -> str:
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
    cluster_name = os.getenv("DATABRICKS_CLUSTER_NAME")

    # Helper to verify ID exists
    def id_exists(cid: str) -> bool:
        try:
            # clusters/get is 2.0
            api("GET", "clusters/get", "2.0", params={"cluster_id": cid})
            return True
        except Exception:
            return False

    if cluster_id:
        if id_exists(cluster_id):
            print(f"Using cluster id: {cluster_id}")
            return cluster_id
        print(f"Provided DATABRICKS_CLUSTER_ID does not exist: {cluster_id}")

    if cluster_name:
        clusters = api("GET", "clusters/list", "2.0").get("clusters", [])
        for c in clusters:
            if c.get("cluster_name") == cluster_name:
                cid = c.get("cluster_id")
                print(f"Resolved cluster name '{cluster_name}' -> id: {cid}")
                return cid
        print(f"No cluster found with name: {cluster_name}")

    print("Set either DATABRICKS_CLUSTER_ID (preferred) or DATABRICKS_CLUSTER_NAME to a valid cluster.")
    sys.exit(1)


def get_job_id_by_name(name):
    res = api("GET", "jobs/list", "2.1")
    for j in (res.get("jobs") or []):
        if j.get("settings", {}).get("name") == name:
            return j.get("job_id")
    return None


def main():
    resolved_cluster_id = resolve_cluster_id()
    local_py = os.path.join(os.path.dirname(__file__), "simple_job.py")

    python_file_uri = upload_python_to_workspace_files(local_py)

    settings = {
        "name": essential_job_name,
        "tasks": [
            {
                "task_key": "hello_world",
                "existing_cluster_id": resolved_cluster_id,
                "spark_python_task": {"python_file": python_file_uri},
            }
        ],
        "format": "MULTI_TASK",
    }

    job_id = get_job_id_by_name(essential_job_name)
    if job_id:
        api("POST", "jobs/reset", "2.1", json={"job_id": job_id, "new_settings": settings})
        print(f"Updated job {job_id}")
    else:
        created = api("POST", "jobs/create", "2.1", json=settings)
        job_id = created["job_id"]
        print(f"Created job {job_id}")

    run = api("POST", "jobs/run-now", "2.1", json={"job_id": job_id})
    run_id = run["run_id"]
    print(f"Run started: {run_id}")

    while True:
        info = api("GET", "jobs/runs/get", "2.1", params={"run_id": run_id})
        state = info.get("state", {})
        life = state.get("life_cycle_state")
        result = state.get("result_state")
        print(f"Status: {life} / {result}")
        if life == "TERMINATED":
            if result == "SUCCESS":
                print("Done")
                return
            print("Failed")
            sys.exit(1)
        time.sleep(5)


if __name__ == "__main__":
    main()
