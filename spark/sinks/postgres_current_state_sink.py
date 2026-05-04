from typing import Dict, Any, Iterable
import psycopg2
from psycopg2.extras import RealDictCursor
from spark.common.postgres_config import load_postgres_config

def get_inventory_status(
        current_sellable_stock: int,
        low_stock_threshold: int
        ) -> str:
    
    if current_sellable_stock < 0:
        return "OVERSELL"
    
    if current_sellable_stock <= low_stock_threshold:
        return "LOW_STOCK"
    
    return "NORMAL"


def insert_processed_event(cursor, event: Dict[str, Any]) -> bool:
    sql ="""
        INSERT INTO processed_events(
            event_id,
            campaign_id,
            event_time,
            event_type,
            sku_id,
            warehouse_id
        )
        VALUES(
            %(event_id)s,
            %(campaign_id)s,
            %(event_timestamp)s,
            %(event_type)s,
            %(sku_id)s,
            %(warehouse_id)s
        )
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id;
    """

    cursor.execute(sql,event)
    inserted = cursor.fetchone()
    return inserted is not None


def insert_inventory_alert(
    cursor,
    event: Dict[str, Any],
    alert_type: str,
    current_sellable_stock: int,
    message: str
) -> None:
    sql = """
    INSERT INTO inventory_alerts (
        campaign_id,
        alert_type,
        sku_id,
        warehouse_id,
        current_sellable_stock,
        event_id,
        message,
        created_at
    )
    VALUES(
        %(campaign_id)s,
        %(alert_type)s,
        %(sku_id)s,
        %(warehouse_id)s,
        %(current_sellable_stock)s,
        %(event_id)s,
        %(message)s,
        NOW()
    );
    """
    cursor.execute(
        sql,
        {
        "campaign_id": event["campaign_id"],
        "alert_type": alert_type,
        "sku_id": event["sku_id"],
        "warehouse_id": event["warehouse_id"],
        "current_sellable_stock": current_sellable_stock,
        "event_id": event["event_id"],
        "message": message
        }
    )

def update_current_inventory(cursor, event: Dict[str, Any]) -> None:
    sql ="""
    SELECT
        campaign_id,
        sku_id,
        warehouse_id,
        current_sellable_stock,
        low_stock_threshold
    FROM current_inventory
    WHERE campaign_id = %(campaign_id)s
        AND sku_id = %(sku_id)s
        AND warehouse_id = %(warehouse_id)s
    FOR UPDATE;
    """

    cursor.execute(sql, event)
    inventory_row = cursor.fetchone()
    
    if inventory_row is None:
        insert_inventory_alert(
            cursor=cursor,
            event=event,
            alert_type="UNKNOWN_SKU",
            current_sellable_stock=None,
            message="Inventory item not found in current_inventory"
        )
        return
    
    old_stock = inventory_row["current_sellable_stock"]
    low_stock_threshold = inventory_row["low_stock_threshold"]
    movement_qty = event["movement_qty"] or 0
    new_stock = old_stock + movement_qty

    new_status = get_inventory_status(new_stock, low_stock_threshold)

    update_sql = """
    UPDATE current_inventory
    SET 
        current_sellable_stock = %(new_stock)s,
        status = %(new_status)s,
        last_event_id = %(event_id)s,
        last_event_time = %(event_timestamp)s,
        updated_at = NOW()
    WHERE campaign_id = %(campaign_id)s
    AND sku_id = %(sku_id)s
    AND warehouse_id = %(warehouse_id)s
    """

    cursor.execute(update_sql,
                   {
                       "new_stock": new_stock,
                       "new_status": new_status,
                       "event_id": event["event_id"],
                       "event_timestamp": event["event_timestamp"],
                       "campaign_id": event["campaign_id"],
                       "sku_id": event["sku_id"],
                       "warehouse_id": event["warehouse_id"]
                   })
    
    if new_status == "OVERSELL":
        insert_inventory_alert(
            cursor=cursor,
            event=event,
            alert_type="OVERSELL",
            current_sellable_stock=new_stock,
            message=f"Oversell detected. Current sellable stock= {new_stock}"
        )
    
    if new_status == "LOW_STOCK":
        insert_inventory_alert(
            cursor=cursor,
            event=event,
            alert_type="LOW_STOCK",
            current_sellable_stock=new_stock,
            message=f"Low stock detected. Current sellable stock={new_stock}"
        )

def update_promotion_metrics(cursor, event: dict) -> None:

    if not event.get("promotion_applied"):
        return

    if event.get("promotion_id") is None:
        return

    quantity = event["quantity"] or 0

    if event["event_type"] in ("STOCK_RESERVED", "COD_CONFIRMED"):
        sql = """
            UPDATE promotion_metrics
            SET
                promotion_consumed_qty = promotion_consumed_qty + %(quantity)s,
                deal_sold_out_at =
                    CASE
                        WHEN promotion_consumed_qty + %(quantity)s >= promotion_quota
                             AND deal_sold_out_at IS NULL
                        THEN %(event_timestamp)s
                        ELSE deal_sold_out_at
                    END,
                updated_at = NOW()
            WHERE campaign_id = %(campaign_id)s
              AND promotion_id = %(promotion_id)s
              AND sku_id = %(sku_id)s
              AND warehouse_id = %(warehouse_id)s;
        """

        cursor.execute(
            sql,
            {
                "quantity": quantity,
                "event_timestamp": event["event_timestamp"],
                "campaign_id": event["campaign_id"],
                "promotion_id": event["promotion_id"],
                "sku_id": event["sku_id"],
                "warehouse_id": event["warehouse_id"],
            },
        )

        return

    if event["event_type"] in ("ORDER_CANCELLED", "RESERVATION_EXPIRED"):
        sql = """
            UPDATE promotion_metrics
            SET
                promotion_cancelled_qty = promotion_cancelled_qty + %(quantity)s,
                updated_at = NOW()
            WHERE campaign_id = %(campaign_id)s
              AND promotion_id = %(promotion_id)s
              AND sku_id = %(sku_id)s
              AND warehouse_id = %(warehouse_id)s;
        """

        cursor.execute(
            sql,
            {
                "quantity": quantity,
                "campaign_id": event["campaign_id"],
                "promotion_id": event["promotion_id"],
                "sku_id": event["sku_id"],
                "warehouse_id": event["warehouse_id"],
            },
        )

def handle_invalid_event(cursor, event: Dict[str, Any])-> None:
    insert_inventory_alert(
        cursor=cursor,
        event=event,
        alert_type="INVALID_EVENT",
        current_sellable_stock=None,
        message=event.get("invalid_reason") or "Invalid event"
    )

def process_event(cursor, event: Dict[str, Any]) -> None:
    is_new_event = insert_processed_event(cursor=cursor,event=event)

    if not is_new_event:
        print(f"[SKIP] Duplicate event_id={event['event_id']}")
        return

    if not event.get("is_valid_event"):
        handle_invalid_event(cursor, event)
        print(f"[INVALID] event_id={event['event_id']}")
        return
    

    update_current_inventory(cursor, event)
    update_promotion_metrics(cursor, event)

    print(
        "[PROCESSED]"
        f"event_id={event['event_id']} "
        f"event_type={event['event_type']} "
        f"sku_id={event['sku_id']} "
        f"warehouse_id={event['warehouse_id']}"
        )

def write_batch_to_postgres(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        print(f"[BATCH {batch_id}] Empty")
        return

    config =  load_postgres_config()
    columns = [
        "event_id",
        "campaign_id",
        "event_timestamp",
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
    ]

    events = [row.asDict(recursive=True)
             for row in batch_df.select(*columns).collect()]
    
    print(f"[BATCH {batch_id}] Processing {len(events)} events")

    conn = None

    try:
        conn = psycopg2.connect(config.dsn)
        conn.autocommit = False

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            for event in events:
                process_event(cursor,event)
        
        conn.commit()
        print(f"[BATCH {batch_id}] Commit successful")
    
    except Exception as e:
        if conn is not None:
            conn.rollback()
        print(f"[BATCH {batch_id}] Failed. Rolled back. Error: {e}")
        raise

    finally:
        if conn is not None:
            conn.close()