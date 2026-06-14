import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import(
    col,
    when,
    to_timestamp,
    from_utc_timestamp,
    to_date,
    hour,
    lit
)

VALID_EVENT_TYPES = [
    "STOCK_RESERVED",
    "PAYMENT_CONFIRMED",
    "COD_CONFIRMED",
    "RESERVATION_EXPIRED",
    "ORDER_CANCELLED",
    "RETURN_RECEIVED",
    "STOCK_REPLENISHED",
]

BUSINESS_TIMEZONE = os.getenv(
    "BUSINESS_TIMEZONE",
    os.getenv("TIMEZONE", "Asia/Ho_Chi_Minh")
)

def add_parsed_event_time(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "event_timestamp", 
        to_timestamp(col("event_time"))
        )

def add_utc_partition_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("event_hour", hour(col("event_timestamp")))
    )

def add_business_time_columns(df: DataFrame) -> DataFrame:
    return(
        df
        .withColumn("business_timestamp",
                    from_utc_timestamp(col("event_timestamp"), BUSINESS_TIMEZONE )
                    )
        .withColumn("business_date", to_date(col("business_timestamp")))
        .withColumn("business_hour", hour(col("business_timestamp")))
    )

def add_movement_qty(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "movement_qty",
        when(col("event_type") == "STOCK_RESERVED", -col("quantity"))
        .when(col("event_type") == "PAYMENT_CONFIRMED", lit(0))
        .when(col("event_type") == "COD_CONFIRMED", -col("quantity"))
        .when(col("event_type") == "RESERVATION_EXPIRED", col("quantity"))
        .when(col("event_type") == "ORDER_CANCELLED", col("quantity"))
        .when(col("event_type") == "RETURN_RECEIVED", col("quantity"))
        .when(col("event_type") == "STOCK_REPLENISHED", col("quantity"))
        .otherwise(lit(None))
    )


def add_validation_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "is_valid_event",
            when(col("event_id").isNull(), lit(False))
            .when(col("campaign_id").isNull(), lit(False))
            .when(col("event_timestamp").isNull(), lit(False))
            .when(col("event_type").isNull(), lit(False))
            .when(col("sku_id").isNull(), lit(False))
            .when(col("warehouse_id").isNull(), lit(False))
            .when(col("quantity").isNull(), lit(False))
            .when(col("quantity") <= 0, lit(False))
            .when(~col("event_type").isin(VALID_EVENT_TYPES), lit(False))

            # Payment logic validation
            .when(
                (col("event_type") == "STOCK_RESERVED") &
                ~((col("payment_method") == "E_WALLET") & (col("payment_status") == "PENDING")),
                lit(False)
            )
            .when(
                (col("event_type") == "PAYMENT_CONFIRMED") &
                ~((col("payment_method") == "E_WALLET") & (col("payment_status") == "PAID")),
                lit(False)
            )
            .when(
                (col("event_type") == "RESERVATION_EXPIRED") &
                ~((col("payment_method") == "E_WALLET") & (col("payment_status") == "EXPIRED")),
                lit(False)
            )
            .when(
                (col("event_type") == "COD_CONFIRMED") &
                ~((col("payment_method") == "COD") & (col("payment_status") == "COD_CONFIRMED")),
                lit(False)
            )
            .when(
                (col("event_type") == "ORDER_CANCELLED") &
                ~((col("payment_method").isin(["COD", "E_WALLET"])) & (col("payment_status") == "CANCELLED")),
                lit(False)
            )
            .otherwise(lit(True))
        )
        .withColumn(
            "invalid_reason",
            when(col("event_id").isNull(), lit("missing_event_id"))
            .when(col("campaign_id").isNull(), lit("missing_campaign_id"))
            .when(col("event_timestamp").isNull(), lit("invalid_event_time"))
            .when(col("event_type").isNull(), lit("missing_event_type"))
            .when(col("sku_id").isNull(), lit("missing_sku_id"))
            .when(col("warehouse_id").isNull(), lit("missing_warehouse_id"))
            .when(col("quantity").isNull(), lit("missing_quantity"))
            .when(col("quantity") <= 0, lit("invalid_quantity"))
            .when(~col("event_type").isin(VALID_EVENT_TYPES), lit("invalid_event_type"))

            # Payment logic invalid reasons
            .when(
                (col("event_type") == "STOCK_RESERVED") &
                ~((col("payment_method") == "E_WALLET") & (col("payment_status") == "PENDING")),
                lit("invalid_stock_reserved_payment_logic")
            )
            .when(
                (col("event_type") == "PAYMENT_CONFIRMED") &
                ~((col("payment_method") == "E_WALLET") & (col("payment_status") == "PAID")),
                lit("invalid_payment_confirmed_logic")
            )
            .when(
                (col("event_type") == "RESERVATION_EXPIRED") &
                ~((col("payment_method") == "E_WALLET") & (col("payment_status") == "EXPIRED")),
                lit("invalid_reservation_expired_logic")
            )
            .when(
                (col("event_type") == "COD_CONFIRMED") &
                ~((col("payment_method") == "COD") & (col("payment_status") == "COD_CONFIRMED")),
                lit("invalid_cod_confirmed_logic")
            )
            .when(
                (col("event_type") == "ORDER_CANCELLED") &
                ~((col("payment_method").isin(["COD", "E_WALLET"])) & (col("payment_status") == "CANCELLED")),
                lit("invalid_order_cancelled_logic")
            )
            .otherwise(lit(None))
        )
    )

def transform_inventory_events(df: DataFrame) -> DataFrame:
    return (
        df
        .transform(add_parsed_event_time)
        .transform(add_utc_partition_columns)
        .transform(add_business_time_columns)
        .transform(add_movement_qty)
        .transform(add_validation_columns)
    )
