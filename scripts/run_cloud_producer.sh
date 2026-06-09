#!/usr/bin/env bash
set -euo pipefail

COUNT="${1:-20}"
INTERVAL_SECONDS="${2:-1}"

export AWS_REGION="${AWS_REGION:-us-east-1}"
export MSK_AUTH_MODE="${MSK_AUTH_MODE:-iam}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-inventory-events}"
export CAMPAIGN_ID="${CAMPAIGN_ID:-CAMPAIGN_FLASH_0609}"
export WAREHOUSE_ID="${WAREHOUSE_ID:-WH_HCM_01}"
export FAILED_EVENTS_S3_PATH="${FAILED_EVENTS_S3_PATH:-s3://inventory-lake-fox/bronze/failed_producer_events/}"
export PROMOTION_CONFIG_URI="${PROMOTION_CONFIG_URI:-s3://inventory-lake-fox/config/campaign/promotion_config.csv}"

if [[ -z "${KAFKA_BOOTSTRAP_SERVERS:-}" ]]; then
  cat >&2 <<'EOF'
KAFKA_BOOTSTRAP_SERVERS is required.

Example:
export KAFKA_BOOTSTRAP_SERVERS="b-1.<cluster>.kafka.us-east-1.amazonaws.com:9098,b-2.<cluster>.kafka.us-east-1.amazonaws.com:9098,b-3.<cluster>.kafka.us-east-1.amazonaws.com:9098"
EOF
  exit 1
fi

python3 producer/inventory_event_producer.py \
  --count "${COUNT}" \
  --interval-seconds "${INTERVAL_SECONDS}"
