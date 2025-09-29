#!/usr/bin/env bash
set -euo pipefail

# Usage: source .env && bash scripts/run_jobs.sh

: "${DATABRICKS_HOST:?DATABRICKS_HOST is required (e.g., https://adb-<id>.<region>.azuredatabricks.net)}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN is required (PAT)}"
: "${JOB_ID_BRONZE_TO_SILVER:?JOB_ID_BRONZE_TO_SILVER is required}"
: "${JOB_ID_SILVER_TO_GOLD:?JOB_ID_SILVER_TO_GOLD is required}"

echo "Triggering Job: Bronze -> Silver ($JOB_ID_BRONZE_TO_SILVER)"
RUN1=$(curl -s -X POST "$DATABRICKS_HOST/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"job_id": '"$JOB_ID_BRONZE_TO_SILVER"'}')
echo "$RUN1"

echo "Triggering Job: Silver -> Gold ($JOB_ID_SILVER_TO_GOLD)"
RUN2=$(curl -s -X POST "$DATABRICKS_HOST/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"job_id": '"$JOB_ID_SILVER_TO_GOLD"'}')
echo "$RUN2"


