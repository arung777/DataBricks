#!/usr/bin/env python3
"""
Minimal Databricks SQL Warehouse Hello World

Required env vars:
- DATABRICKS_HOST (e.g. https://dbc-xxxx.cloud.databricks.com or dbc-xxxx.cloud.databricks.com)
- DATABRICKS_TOKEN (PAT)
- DATABRICKS_WAREHOUSE_HTTP_PATH (e.g. /sql/1.0/warehouses/<warehouse_id>)

Usage:
  export DATABRICKS_HOST="https://dbc-xxxx.cloud.databricks.com"
  export DATABRICKS_TOKEN="<your_pat>"
  export DATABRICKS_WAREHOUSE_HTTP_PATH="/sql/1.0/warehouses/<id>"
  python3 create_and_run_job.py
"""

import os
import sys


def env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(f"Missing env: {name}")
        sys.exit(1)
    return value


def normalize_hostname(host: str) -> str:
    if host.startswith("https://"):
        return host[len("https://"):]
    if host.startswith("http://"):
        return host[len("http://"):]
    return host


def main() -> None:
    try:
        from databricks import sql as dbsql
    except Exception:
        print("databricks-sql-connector not installed. Please: pip install databricks-sql-connector")
        sys.exit(1)

    host = normalize_hostname(env("DATABRICKS_HOST"))
    token = env("DATABRICKS_TOKEN")
    http_path = env("DATABRICKS_WAREHOUSE_HTTP_PATH")

    print("Connecting to SQL Warehouse...")
    with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 'Hello World from SQL Warehouse' AS msg")
            rows = cursor.fetchall()
            print(rows)

    print("Done")


if __name__ == "__main__":
    main()
