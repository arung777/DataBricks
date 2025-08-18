# Minimal Databricks Hello World (Python)

This repo contains a tiny Hello World job and a minimal Python script to create and run it on Databricks.

## Files
- `simple_job.py` — prints Hello World
- `create_and_run_job.py` — uploads the script, creates/updates a job, runs it
- `requirements.txt` — only `requests`

## Run
```bash
pip install -r requirements.txt
export DATABRICKS_HOST="https://<your-workspace>.cloud.databricks.com"
export DATABRICKS_TOKEN="<your_pat_token>"
export DATABRICKS_CLUSTER_ID="<your_cluster_id>"
python3 create_and_run_job.py
```

The script will:
- Upload `simple_job.py` to `dbfs:/FileStore/simple_job.py`
- Create/update the job named "Hello World (Python)"
- Trigger a run and wait for completion
