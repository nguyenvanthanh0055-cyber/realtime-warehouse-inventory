import argparse
import csv
import io
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable
from urllib.parse import urlparse

import boto3

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CURRENT_INVENTORY_TABLE = os.getenv(
    "DDB_CURRENT_INVENTORY_TABLE",
    "inventory_current_state",
)
PROMOTION_METRICS_TABLE = os.getenv(
    "DDB_PROMOTION_METRICS_TABLE",
    "inventory_promotion_metrics",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed_uri = urlparse(s3_uri)
    if parsed_uri.scheme != "s3" or not parsed_uri.netloc or not parsed_uri.path:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")

    return parsed_uri.netloc, parsed_uri.path.lstrip("/")


def read_csv_rows(s3_uri: str, region: str) -> Iterable[Dict[str, str]]:
    bucket, key = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3", region_name=region)
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    yield from csv.DictReader(io.StringIO(content))


def inventory_key(row: Dict[str, str]) -> str:
    return f"{row['campaign_id']}#{row['sku_id']}#{row['warehouse_id']}"


def promotion_key(row: Dict[str, str]) -> str:
    return (
        f"{row['campaign_id']}#{row['promotion_id']}#"
        f"{row['sku_id']}#{row['warehouse_id']}"
    )


def get_inventory_status(current_sellable_stock: int, low_stock_threshold: int) -> str:
    if current_sellable_stock < 0:
        return "OVERSELL"

    if current_sellable_stock <= low_stock_threshold:
        return "LOW_STOCK"

    return "NORMAL"


def seed_current_inventory(
    table,
    campaign_id: str,
    initial_inventory_uri: str,
    region: str | None = None,
) -> int:
    seeded_count = 0
    now = utc_now_iso()

    for row in read_csv_rows(initial_inventory_uri, region=region):
        if row["campaign_id"] != campaign_id:
            continue

        initial_stock = int(row["initial_sellable_stock"])
        low_stock_threshold = int(row["low_stock_threshold"])
        item = {
            "inventory_key": inventory_key(row),
            "campaign_id": row["campaign_id"],
            "sku_id": row["sku_id"],
            "warehouse_id": row["warehouse_id"],
            "product_name": row["product_name"],
            "initial_sellable_stock": Decimal(initial_stock),
            "current_sellable_stock": Decimal(initial_stock),
            "low_stock_threshold": Decimal(low_stock_threshold),
            "status": get_inventory_status(initial_stock, low_stock_threshold),
            "created_at": now,
            "updated_at": now,
        }

        table.put_item(Item=item)
        seeded_count += 1

    return seeded_count


def seed_promotion_metrics(
    table,
    campaign_id: str,
    promotion_config_uri: str,
    region: str | None = None,
) -> int:
    seeded_count = 0
    now = utc_now_iso()

    for row in read_csv_rows(promotion_config_uri, region=region):
        if row["campaign_id"] != campaign_id:
            continue

        item = {
            "promotion_key": promotion_key(row),
            "campaign_id": row["campaign_id"],
            "promotion_id": row["promotion_id"],
            "sku_id": row["sku_id"],
            "warehouse_id": row["warehouse_id"],
            "promotion_name": row["promotion_name"],
            "promotion_quota": to_decimal(row["promotion_quota"]),
            "promotion_consumed_qty": Decimal(0),
            "promotion_cancelled_qty": Decimal(0),
            "sale_price": to_decimal(row["sale_price"]),
            "normal_price": to_decimal(row["normal_price"]),
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "created_at": now,
            "updated_at": now,
        }

        table.put_item(Item=item)
        seeded_count += 1

    return seeded_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed DynamoDB current inventory and promotion metrics tables."
    )
    parser.add_argument(
        "--campaign-id",
        default=os.getenv("CAMPAIGN_ID"),
        required=os.getenv("CAMPAIGN_ID") is None,
        help="Campaign ID to seed, for example CAMPAIGN_FLASH_0605.",
    )
    parser.add_argument(
        "--initial-inventory-uri",
        default=os.getenv(
            "INITIAL_INVENTORY_URI",
            "s3://inventory-lake-fox/config/campaign/initial_inventory.csv",
        ),
        help="S3 URI for initial_inventory.csv.",
    )
    parser.add_argument(
        "--promotion-config-uri",
        default=os.getenv(
            "PROMOTION_CONFIG_URI",
            "s3://inventory-lake-fox/config/campaign/promotion_config.csv",
        ),
        help="S3 URI for promotion_config.csv.",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"),
        help="AWS region for DynamoDB.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = parse_args()

    dynamodb = boto3.resource("dynamodb", region_name=args.region)
    current_inventory_table = dynamodb.Table(CURRENT_INVENTORY_TABLE)
    promotion_metrics_table = dynamodb.Table(PROMOTION_METRICS_TABLE)

    inventory_count = seed_current_inventory(
        table=current_inventory_table,
        campaign_id=args.campaign_id,
        initial_inventory_uri=args.initial_inventory_uri,
        region=args.region,
    )
    promotion_count = seed_promotion_metrics(
        table=promotion_metrics_table,
        campaign_id=args.campaign_id,
        promotion_config_uri=args.promotion_config_uri,
        region=args.region,
    )

    if inventory_count == 0:
        logger.warning(
            "No current inventory rows were seeded for campaign_id=%s from %s",
            args.campaign_id,
            args.initial_inventory_uri,
        )

    if promotion_count == 0:
        logger.warning(
            "No promotion rows were seeded for campaign_id=%s from %s",
            args.campaign_id,
            args.promotion_config_uri,
        )

    logger.info(
        "Seeded DynamoDB campaign_id=%s current_inventory_items=%s promotion_items=%s",
        args.campaign_id,
        inventory_count,
        promotion_count,
    )


if __name__ == "__main__":
    main()
