import logging
import os
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger(__name__)


CURRENT_INVENTORY_TABLE = os.getenv(
    "DDB_CURRENT_INVENTORY_TABLE",
    "inventory_current_state",
)
PROCESSED_EVENTS_TABLE = os.getenv(
    "DDB_PROCESSED_EVENTS_TABLE",
    "inventory_processed_events",
)
ALERTS_TABLE = os.getenv("DDB_ALERTS_TABLE", "inventory_alerts")
STATE_HISTORY_TABLE = os.getenv(
    "DDB_STATE_HISTORY_TABLE",
    "inventory_state_history",
)
PROMOTION_METRICS_TABLE = os.getenv(
    "DDB_PROMOTION_METRICS_TABLE",
    "inventory_promotion_metrics",
)
STATE_HISTORY_S3_PATH = os.getenv("STATE_HISTORY_S3_PATH")

EVENT_COLUMNS = [
    "event_id",
    "campaign_id",
    "event_timestamp",
    "business_timestamp",
    "business_date",
    "event_type",
    "order_id",
    "sku_id",
    "warehouse_id",
    "quantity",
    "unit_price",
    "promotion_id",
    "promotion_applied",
    "payment_method",
    "payment_status",
    "reservation_expires_at",
    "source",
    "movement_qty",
    "is_valid_event",
    "invalid_reason",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "json_value",
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


def to_dynamodb_value(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return Decimal(value)

    if isinstance(value, float):
        return Decimal(str(value))

    if isinstance(value, (datetime, date)):
        return as_iso_string(value)

    return value


def compact_item(values: Dict[str, Any]) -> Dict[str, Any]:
    item = {}
    for key, value in values.items():
        dynamodb_value = to_dynamodb_value(value)
        if dynamodb_value is not None:
            item[key] = dynamodb_value
    return item


def inventory_key(event: Dict[str, Any]) -> str:
    return f"{event['campaign_id']}#{event['sku_id']}#{event['warehouse_id']}"


def promotion_key(event: Dict[str, Any]) -> str:
    return (
        f"{event['campaign_id']}#{event['promotion_id']}#"
        f"{event['sku_id']}#{event['warehouse_id']}"
    )


def get_inventory_status(
    current_sellable_stock: Optional[int],
    low_stock_threshold: Optional[int],
) -> str:
    if current_sellable_stock is None:
        return "UNKNOWN"

    if current_sellable_stock < 0:
        return "OVERSELL"

    if low_stock_threshold is not None and current_sellable_stock <= low_stock_threshold:
        return "LOW_STOCK"

    return "NORMAL"


def normalize_event(event: Dict[str, Any]) -> Dict[str, Any]:
    normalized_event = dict(event)
    normalized_event["event_timestamp"] = as_iso_string(
        normalized_event.get("event_timestamp")
    )
    normalized_event["business_timestamp"] = as_iso_string(
        normalized_event.get("business_timestamp")
    )
    normalized_event["reservation_expires_at"] = as_iso_string(
        normalized_event.get("reservation_expires_at")
    )
    normalized_event["business_date"] = as_iso_string(
        normalized_event.get("business_date")
    )
    return normalized_event


def put_alert(
    tables: Dict[str, Any],
    event: Dict[str, Any],
    alert_type: str,
    message: str,
    current_sellable_stock: Optional[int] = None,
) -> None:
    item = compact_item(
        {
            "alert_id": str(uuid.uuid4()),
            "campaign_id": event.get("campaign_id") or "UNKNOWN",
            "alert_type": alert_type,
            "sku_id": event.get("sku_id"),
            "warehouse_id": event.get("warehouse_id"),  
            "current_sellable_stock": current_sellable_stock,
            "event_id": event.get("event_id"),
            "message": message,
            "created_at": utc_now_iso(),
        }
    )
    tables["alerts"].put_item(Item=item)


def insert_processed_event(tables: Dict[str, Any], event: Dict[str, Any]) -> bool:
    item = compact_item(
        {
            "event_id": event["event_id"],
            "campaign_id": event.get("campaign_id"),
            "event_time": event.get("event_timestamp"),
            "event_type": event.get("event_type"),
            "sku_id": event.get("sku_id"),
            "warehouse_id": event.get("warehouse_id"),
            "kafka_topic": event.get("kafka_topic"),
            "kafka_partition": event.get("kafka_partition"),
            "kafka_offset": event.get("kafka_offset"),
            "processed_at": utc_now_iso(),
        }
    )

    try:
        tables["processed_events"].put_item(
            Item=item,
            ConditionExpression=Attr("event_id").not_exists(),
        )
        return True
    except ClientError as error:
        if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def put_state_history(
    tables: Dict[str, Any],
    event: Dict[str, Any],
    previous_sellable_stock: int,
    current_sellable_stock: int,
    status: str,
) -> Dict[str, Any]:
    item = compact_item(
        {
            "history_id": str(uuid.uuid4()),
            "inventory_key": inventory_key(event),
            "event_id": event["event_id"],
            "campaign_id": event["campaign_id"],
            "sku_id": event["sku_id"],
            "warehouse_id": event["warehouse_id"],
            "event_time": event.get("event_timestamp"),
            "business_timestamp": event.get("business_timestamp"),
            "business_date": event.get("business_date"),
            "event_type": event.get("event_type"),
            "quantity": event.get("quantity"),
            "movement_qty": event.get("movement_qty"),
            "previous_sellable_stock": previous_sellable_stock,
            "current_sellable_stock": current_sellable_stock,
            "status": status,
            "processed_at": utc_now_iso(),
        }
    )
    tables["state_history"].put_item(Item=item)
    return item


def update_current_inventory(
    tables: Dict[str, Any],
    event: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    movement_qty = event.get("movement_qty") or 0
    key = inventory_key(event)
    updated_at = utc_now_iso()

    try:
        response = tables["current_inventory"].update_item(
            Key={"inventory_key": key},
            UpdateExpression=(
                "SET last_event_id = :event_id, "
                "last_event_time = :event_time, "
                "updated_at = :updated_at "
                "ADD current_sellable_stock :movement_qty"
            ),
            ConditionExpression=Attr("inventory_key").exists(),
            ExpressionAttributeValues={
                ":movement_qty": Decimal(movement_qty),
                ":event_id": event["event_id"],
                ":event_time": event.get("event_timestamp") or updated_at,
                ":updated_at": updated_at,
            },
            ReturnValues="ALL_OLD",
        )
    except ClientError as error:
        if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
            put_alert(
                tables=tables,
                event=event,
                alert_type="UNKNOWN_SKU",
                message="Inventory item not found in DynamoDB current inventory table",
            )
            return None
        raise

    old_item = response.get("Attributes", {})
    previous_stock = int(old_item.get("current_sellable_stock", 0))
    low_stock_threshold = old_item.get("low_stock_threshold")
    threshold = int(low_stock_threshold) if low_stock_threshold is not None else None
    current_stock = previous_stock + int(movement_qty)
    status = get_inventory_status(current_stock, threshold)

    tables["current_inventory"].update_item(
        Key={"inventory_key": key},
        UpdateExpression="SET #status = :status",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":status": status},
    )

    history_item = put_state_history(
        tables=tables,
        event=event,
        previous_sellable_stock=previous_stock,
        current_sellable_stock=current_stock,
        status=status,
    )

    if status == "OVERSELL":
        put_alert(
            tables=tables,
            event=event,
            alert_type="OVERSELL",
            current_sellable_stock=current_stock,
            message=f"Oversell detected. Current sellable stock={current_stock}",
        )
    elif status == "LOW_STOCK":
        put_alert(
            tables=tables,
            event=event,
            alert_type="LOW_STOCK",
            current_sellable_stock=current_stock,
            message=f"Low stock detected. Current sellable stock={current_stock}",
        )

    return history_item


def update_promotion_metrics(tables: Dict[str, Any], event: Dict[str, Any]) -> None:
    if not event.get("promotion_applied") or not event.get("promotion_id"):
        return

    quantity = event.get("quantity") or 0
    event_type = event.get("event_type")
    update_expression = None

    if event_type in {"STOCK_RESERVED", "COD_CONFIRMED"}:
        update_expression = (
            "SET updated_at = :updated_at ADD promotion_consumed_qty :quantity"
        )
    elif event_type in {"ORDER_CANCELLED", "RESERVATION_EXPIRED"}:
        update_expression = (
            "SET updated_at = :updated_at ADD promotion_cancelled_qty :quantity"
        )

    if update_expression is None:
        return

    try:
        tables["promotion_metrics"].update_item(
            Key={"promotion_key": promotion_key(event)},
            UpdateExpression=update_expression,
            ConditionExpression=Attr("promotion_key").exists(),
            ExpressionAttributeValues={
                ":quantity": Decimal(quantity),
                ":updated_at": utc_now_iso(),
            },
        )
    except ClientError as error:
        if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
            put_alert(
                tables=tables,
                event=event,
                alert_type="UNKNOWN_PROMOTION",
                message="Promotion item not found in DynamoDB promotion metrics table",
            )
            return
        raise


def process_event(
    tables: Dict[str, Any],
    event: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if not event.get("event_id"):
        put_alert(
            tables=tables,
            event=event,
            alert_type="INVALID_EVENT",
            message="Malformed Kafka message or JSON parse failed: missing event_id",
        )
        return None

    if not insert_processed_event(tables=tables, event=event):
        logger.info("Skipping duplicate event_id=%s", event["event_id"])
        return None

    if not event.get("is_valid_event"):
        put_alert(
            tables=tables,
            event=event,
            alert_type="INVALID_EVENT",
            message=event.get("invalid_reason") or "Invalid event",
        )
        logger.info("Handled invalid event_id=%s", event["event_id"])
        return None

    history_item = update_current_inventory(tables=tables, event=event)
    update_promotion_metrics(tables=tables, event=event)

    logger.info(
        "Processed DynamoDB event_id=%s event_type=%s sku_id=%s warehouse_id=%s",
        event["event_id"],
        event.get("event_type"),
        event.get("sku_id"),
        event.get("warehouse_id"),
    )
    return history_item


def build_tables(region_name: Optional[str] = None) -> Dict[str, Any]:
    dynamodb = boto3.resource("dynamodb", region_name=region_name)
    return {
        "current_inventory": dynamodb.Table(CURRENT_INVENTORY_TABLE),
        "processed_events": dynamodb.Table(PROCESSED_EVENTS_TABLE),
        "alerts": dynamodb.Table(ALERTS_TABLE),
        "state_history": dynamodb.Table(STATE_HISTORY_TABLE),
        "promotion_metrics": dynamodb.Table(PROMOTION_METRICS_TABLE),
    }


def get_state_history_s3_path() -> str:
    if STATE_HISTORY_S3_PATH:
        return STATE_HISTORY_S3_PATH.rstrip("/")

    lake_root = os.getenv("LAKE_ROOT")
    if not lake_root:
        raise ValueError(
            "LAKE_ROOT or STATE_HISTORY_S3_PATH is required to write "
            "current inventory state history to S3"
        )

    return f"{lake_root.rstrip('/')}/silver/current_inventory_state_history"


def normalize_history_rows_for_s3(
    history_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows = []
    for item in history_items:
        rows.append(
            {
                "history_id": str(item["history_id"]),
                "inventory_key": item["inventory_key"],
                "event_id": item["event_id"],
                "campaign_id": item["campaign_id"],
                "sku_id": item["sku_id"],
                "warehouse_id": item["warehouse_id"],
                "event_time": item.get("event_time"),
                "business_timestamp": item.get("business_timestamp"),
                "business_date": item.get("business_date"),
                "event_type": item.get("event_type"),
                "quantity": int(item.get("quantity", 0)),
                "movement_qty": int(item.get("movement_qty", 0)),
                "previous_sellable_stock": int(item["previous_sellable_stock"]),
                "current_sellable_stock": int(item["current_sellable_stock"]),
                "status": item["status"],
                "processed_at": item["processed_at"],
            }
        )
    return rows


def write_state_history_to_s3(
    batch_df,
    batch_id: int,
    history_items: List[Dict[str, Any]],
) -> None:
    if not history_items:
        logger.info("DynamoDB batch %s has no state history rows for S3", batch_id)
        return

    spark = batch_df.sparkSession
    output_path = get_state_history_s3_path()
    history_rows = normalize_history_rows_for_s3(history_items)

    (
        spark.createDataFrame(history_rows)
        .write
        .mode("append")
        .format("parquet")
        .partitionBy("business_date")
        .save(output_path)
    )

    logger.info(
        "Wrote current inventory state history to S3 batch_id=%s rows=%s path=%s",
        batch_id,
        len(history_rows),
        output_path,
    )


def write_batch_to_dynamodb(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        logger.info("DynamoDB batch %s empty", batch_id)
        return

    region_name = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    tables = build_tables(region_name=region_name)
    processed_count = 0
    history_items = []

    for row in batch_df.select(*EVENT_COLUMNS).toLocalIterator():
        event = normalize_event(row.asDict(recursive=True))
        history_item = process_event(tables=tables, event=event)
        if history_item:
            history_items.append(history_item)
        processed_count += 1

    write_state_history_to_s3(
        batch_df=batch_df,
        batch_id=batch_id,
        history_items=history_items,
    )

    logger.info(
        "DynamoDB batch %s processed_events=%s state_history_rows=%s",
        batch_id,
        processed_count,
        len(history_items),
    )
