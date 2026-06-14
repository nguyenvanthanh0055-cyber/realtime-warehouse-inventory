#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
ARTIFACT_BUCKET="${ARTIFACT_BUCKET:?ARTIFACT_BUCKET is required}"

GLUE_SCRIPT_PREFIX="${GLUE_SCRIPT_PREFIX:-artifacts/glue/scripts}"
GLUE_LIB_PREFIX="${GLUE_LIB_PREFIX:-artifacts/glue/libs}"
REDSHIFT_SQL_PREFIX="${REDSHIFT_SQL_PREFIX:-artifacts/redshift/sql}"
MWAA_DAGS_PREFIX="${MWAA_DAGS_PREFIX:-artifacts/dags}"
DEPLOY_DAG="${DEPLOY_DAG:-false}"

STREAMING_LIB_ZIP="${STREAMING_LIB_ZIP:-dist/glue/inventory_streaming_libs.zip}"

command -v aws >/dev/null 2>&1 || {
  echo "aws CLI is required but was not found in PATH"
  exit 1
}

if [[ ! -f "${STREAMING_LIB_ZIP}" ]]; then
  echo "Packaging Glue streaming libraries..."
  bash scripts/package_glue_streaming_job.sh
fi

test -f "${STREAMING_LIB_ZIP}"

echo "Deploying artifacts to bucket: s3://${ARTIFACT_BUCKET}"
echo "Glue scripts prefix: s3://${ARTIFACT_BUCKET}/${GLUE_SCRIPT_PREFIX}/"
echo "Glue libs prefix: s3://${ARTIFACT_BUCKET}/${GLUE_LIB_PREFIX}/"
echo "Redshift SQL prefix: s3://${ARTIFACT_BUCKET}/${REDSHIFT_SQL_PREFIX}/"

if [[ "${DEPLOY_DAG}" == "true" ]]; then
  echo "MWAA DAG prefix: s3://${ARTIFACT_BUCKET}/${MWAA_DAGS_PREFIX}/"
else
  echo "MWAA DAG upload disabled"
fi

aws s3 cp \
  spark/streaming_inventory_job.py \
  "s3://${ARTIFACT_BUCKET}/${GLUE_SCRIPT_PREFIX}/streaming_inventory_job.py" \
  --region "${AWS_REGION}"

aws s3 cp \
  aws/glue/daily_reconciliation_job.py \
  "s3://${ARTIFACT_BUCKET}/${GLUE_SCRIPT_PREFIX}/inventory-gold-batch-job.py" \
  --region "${AWS_REGION}"

aws s3 cp \
  aws/glue/redshift_load_gold.py \
  "s3://${ARTIFACT_BUCKET}/${GLUE_SCRIPT_PREFIX}/redshift_load_gold.py" \
  --region "${AWS_REGION}"

aws s3 cp \
  "${STREAMING_LIB_ZIP}" \
  "s3://${ARTIFACT_BUCKET}/${GLUE_LIB_PREFIX}/inventory_streaming_libs.zip" \
  --region "${AWS_REGION}"

aws s3 sync \
  redshift/ \
  "s3://${ARTIFACT_BUCKET}/${REDSHIFT_SQL_PREFIX}/" \
  --exclude "*" \
  --include "*.sql" \
  --region "${AWS_REGION}"

if [[ "${DEPLOY_DAG}" == "true" ]]; then
  aws s3 cp \
    dags/inventory_gold_to_mart_dag.py \
    "s3://${ARTIFACT_BUCKET}/${MWAA_DAGS_PREFIX}/inventory_gold_to_mart_dag.py" \
    --region "${AWS_REGION}"
fi

echo "Uploaded Glue, Redshift SQL, and optional MWAA DAG artifacts to s3://${ARTIFACT_BUCKET}"
