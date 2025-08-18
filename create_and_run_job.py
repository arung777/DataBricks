#!/usr/bin/env python3
"""
Minimal Hello World Databricks Job Runner (Python)

Env vars required:
- DATABRICKS_HOST (e.g. https://<workspace>.cloud.databricks.com)
- DATABRICKS_TOKEN (PAT)
- DATABRICKS_CLUSTER_ID (existing cluster)

Usage:
    export DATABRICKS_HOST="https://<your-workspace>.cloud.databricks.com"
    export DATABRICKS_TOKEN="<your_pat_token>"
    export DATABRICKS_CLUSTER_ID="<your_cluster_id>"
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
    host = env("DATABRICK_WORKSPACE_URL")
    token = env("DATABRICKS_ACCESS_TOKEN")
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


def upload_py_to_dbfs(local_path, dbfs_path):
    with open(local_path, "rb") as f:
        contents_b64 = base64.b64encode(f.read()).decode("utf-8")
    api("POST", "dbfs/put", "2.0", json={"path": dbfs_path, "contents": contents_b64, "overwrite": True})
    print(f"Uploaded {local_path} -> {dbfs_path}")


def get_job_id_by_name(name):
    res = api("GET", "jobs/list", "2.1")
    for j in (res.get("jobs") or []):
        if j.get("settings", {}).get("name") == name:
            return j.get("job_id")
    return None


def main():
    cluster_id = env("DATABRICKS_CLUSTER_ID")
    local_py = os.path.join(os.path.dirname(__file__), "simple_job.py")
    dbfs_py = "dbfs:/FileStore/simple_job.py"

    upload_py_to_dbfs(local_py, dbfs_py)

    settings = {
        "name": essential_job_name,
        "tasks": [
            {
                "task_key": "hello_world",
                "existing_cluster_id": cluster_id,
                "spark_python_task": {"python_file": dbfs_py}
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
