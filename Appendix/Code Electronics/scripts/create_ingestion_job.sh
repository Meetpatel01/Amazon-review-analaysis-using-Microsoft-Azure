#!/usr/bin/env bash

set -euo pipefail

# Validate required environment variables
: "${DATABRICKS_HOST:?DATABRICKS_HOST is required}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN is required}"
: "${STORAGE_ACCOUNT:?STORAGE_ACCOUNT is required}"
: "${CONTAINER:?CONTAINER is required}"
: "${DATA_SOURCE_REVIEWS_URL:?DATA_SOURCE_REVIEWS_URL is required}"
: "${DATA_SOURCE_METADATA_URL:?DATA_SOURCE_METADATA_URL is required}"
: "${MAX_RECORDS_REVIEWS:?MAX_RECORDS_REVIEWS is required}"
: "${MAX_RECORDS_METADATA:?MAX_RECORDS_METADATA is required}"


# Create Databricks job
JOB_RESPONSE=$(curl -s -X POST "$DATABRICKS_HOST/api/2.1/jobs/create" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "01_Data_Ingestion",
    "job_clusters": [
      {
        "job_cluster_key": "data_ingestion_cluster",
        "new_cluster": {
          "spark_version": "13.3.x-scala2.12",
          "node_type_id": "Standard_DS3_v2",
          "num_workers": 1,
          "spark_conf": {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]"
          },
          "custom_tags": {
            "ResourceClass": "SingleNode",
            "Project": "AmazonReviewAnalytics"
          }
        }
      }
    ],
    "tasks": [
      {
        "task_key": "ingest_data",
        "job_cluster_key": "data_ingestion_cluster",
        "notebook_task": {
          "notebook_path": "/databricks/notebooks/00_data_ingestion",
          "base_parameters": {
            "storage_account": "'"$STORAGE_ACCOUNT"'",
            "container": "'"$CONTAINER"'",
            "reviews_url": "'"$DATA_SOURCE_REVIEWS_URL"'",
            "metadata_url": "'"$DATA_SOURCE_METADATA_URL"'",
            "max_records_reviews": "'"$MAX_RECORDS_REVIEWS"'",
            "max_records_metadata": "'"$MAX_RECORDS_METADATA"'"
          }
        },
        "timeout_seconds": 3600,
        "max_retries": 2,
        "retry_on_timeout": true
      }
    ],
    "schedule": {
      "quartz_cron_expression": "0 0 6 * * ?",
      "timezone_id": "UTC",
      "pause_status": "PAUSED"
    },
    "email_notifications": {
      "on_failure": [],
      "on_success": [],
      "no_alert_for_skipped_runs": true
    },
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "format": "MULTI_TASK"
  }')


# Extract job ID from response
JOB_ID=$(echo "$JOB_RESPONSE" | grep -o '"job_id":[0-9]*' | cut -d':' -f2)

if [ -n "$JOB_ID" ]; then
  echo "Job created. ID: $JOB_ID"
  echo "URL: $DATABRICKS_HOST/#job/$JOB_ID"
  # Update .env with job ID
  if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s/JOB_ID_DATA_INGESTION=<FILL_AFTER_CREATING_JOB>/JOB_ID_DATA_INGESTION=$JOB_ID/" .env
  else
    sed -i "s/JOB_ID_DATA_INGESTION=<FILL_AFTER_CREATING_JOB>/JOB_ID_DATA_INGESTION=$JOB_ID/" .env
  fi
  echo ".env updated with JOB_ID_DATA_INGESTION=$JOB_ID"
  echo "Next steps: Upload notebook, test job, enable schedule, update dependencies."
else
  echo "Job creation failed. Response:"
  echo "$JOB_RESPONSE"
  exit 1
fi
