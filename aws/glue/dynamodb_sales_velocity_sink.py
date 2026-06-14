import logging
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, Optional

import boto3

logger = logging.getLogger(__name__)

SALES_VELOCITY_5M_TABLE = os.getenv(
    "DDB_SALES_VELOCITY_5M_TABLE",
    "inventory_sales_velocity_5m",
)

VELOCITY_COLUMNS = [
    "campaign_id",
    "sku_id",
    "warehouse_id",
    "promotion_id",
    "window_start",
    "window_end",
    "window_size_minutes",
    "order_count",
    "sold_qty",
    "window_date",
    "window_hour",
    "silver_processed_at",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def as_iso_string(value: Any) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()

    if isinstance(value, date):
        return value.isoformat()

    return str(value)


def to_dynamodb_number(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def compact_item(values: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in values.items() if value is not None}


def build_velocity_scope(row: Dict[str, Any]) -> str:
    promotion_id = row.get("promotion_id") or "NO_PROMOTION"
    return (
        f"{row['campaign_id']}#{row['sku_id']}#{row['warehouse_id']}#"
        f"{promotion_id}"
    )


def normalize_velocity_row(row: Dict[str, Any]) -> Dict[str, Any]:
    window_start = as_iso_string(row.get("window_start"))
    window_end = as_iso_string(row.get("window_end"))
    updated_at = utc_now_iso()

    return compact_item(
        {
            "velocity_scope": build_velocity_scope(row),
            "campaign_id": row.get("campaign_id"),
            "sku_id": row.get("sku_id"),
            "warehouse_id": row.get("warehouse_id"),
            "promotion_id": row.get("promotion_id") or "NO_PROMOTION",
            "window_start": window_start,
            "window_end": window_end,
            "window_size_minutes": to_dynamodb_number(row.get("window_size_minutes")),
            "order_count": to_dynamodb_number(row.get("order_count")),
            "sold_qty": to_dynamodb_number(row.get("sold_qty")),
            "window_date": as_iso_string(row.get("window_date")),
            "window_hour": to_dynamodb_number(row.get("window_hour")),
            "silver_processed_at": as_iso_string(row.get("silver_processed_at")),
            "updated_at": updated_at,
        }
    )


def write_batch_sales_velocity_5m_to_dynamodb(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        logger.info("Sales velocity DynamoDB batch %s empty", batch_id)
        return

    region_name = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    dynamodb = boto3.resource("dynamodb", region_name=region_name)
    table = dynamodb.Table(SALES_VELOCITY_5M_TABLE)
    processed_count = 0

    for row in batch_df.select(*VELOCITY_COLUMNS).toLocalIterator():
        item = normalize_velocity_row(row.asDict(recursive=True))
        table.put_item(Item=item)
        processed_count += 1

    logger.info(
        "Sales velocity DynamoDB batch %s upserted_rows=%s table=%s",
        batch_id,
        processed_count,
        SALES_VELOCITY_5M_TABLE,
    )
