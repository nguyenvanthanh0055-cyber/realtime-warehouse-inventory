#!/usr/bin/env bash
set -euo pipefail

export AWS_REGION="${AWS_REGION:-us-east-1}"
export CAMPAIGN_ID="${CAMPAIGN_ID:-CAMPAIGN_FLASH_0609}"
export SNAPSHOT_DATE="${SNAPSHOT_DATE:-$(date -u +%F)}"

export INITIAL_INVENTORY_URI="${INITIAL_INVENTORY_URI:-s3://inventory-lake-fox/config/campaign/initial_inventory.csv}"
export PROMOTION_CONFIG_URI="${PROMOTION_CONFIG_URI:-s3://inventory-lake-fox/config/campaign/promotion_config.csv}"
export INVENTORY_SNAPSHOT_ROOT="${INVENTORY_SNAPSHOT_ROOT:-s3://inventory-lake-fox/snapshots/inventory_snapshot}"

export DDB_CURRENT_INVENTORY_TABLE="${DDB_CURRENT_INVENTORY_TABLE:-inventory_current_state}"
export DDB_PROMOTION_METRICS_TABLE="${DDB_PROMOTION_METRICS_TABLE:-inventory_promotion_metrics}"

python3 scripts/seed_campaign_dynamodb.py

