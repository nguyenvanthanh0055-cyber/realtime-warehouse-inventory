from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    window,
    sum,
    count,
    to_date,
    hour
    )

from spark.common.lake_config import load_lake_config


def build_sales_velocity_5m_df(df: DataFrame) -> DataFrame:
    sales_events_df = (
        df.filter(col("is_valid_event") == lit(True))
        .filter(col("event_type").isin("STOCK_RESERVED", "COD_CONFIRMED"))
        .filter(col("business_timestamp").isNotNull())
        .filter(col("quantity").isNotNull())
        .filter(col("quantity") > lit(0))
    )

    return (
        sales_events_df
        .withWatermark("business_timestamp", "10 minutes")
        .groupBy(
            window(col("business_timestamp"), "5 minutes"),
            col("campaign_id"),
            col("sku_id"),
            col("warehouse_id"),
            col("promotion_id")
        )
        .agg(
            count("order_id").alias("order_count"),
            sum("quantity").alias("sold_qty")
        )
        .select(
            col("campaign_id"),
            col("sku_id"),
            col("warehouse_id"),
            col("promotion_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit(5).alias("window_size_minutes"),
            col("order_count"),
            col("sold_qty")
        )
        .withColumn("window_date", to_date(col("window_start")))
        .withColumn("window_hour", hour(col("window_start")))
        .withColumn("silver_processed_at", current_timestamp())
    )


def write_bronze_raw_inventory_events(df: DataFrame):
    config = load_lake_config()

    bronze_df = (
        df.select(
            col("kafka_key"),
            col("json_value"),
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            col("event_date").alias("event_date_utc"),
            col("event_hour").alias("event_hour_utc"),
        )
        .withColumn("bronze_ingested_at", current_timestamp())
    )

    return (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", config.bronze_raw_inventory_events_path)
        .option("checkpointLocation", config.bronze_checkpoint_path)
        .partitionBy("event_date_utc", "event_hour_utc")
        .start()
    )

def write_silver_inventory_movements(df: DataFrame):
    config = load_lake_config()

    silver_df = (
        df.select(
            col("event_id"),
            col("campaign_id"),
            col("event_timestamp"),
            col("business_timestamp"),
            col("event_type"),
            col("order_id"),
            col("sku_id"),
            col("warehouse_id"),
            col("quantity"),
            col("unit_price"),
            col("promotion_id"),
            col("promotion_applied"),
            col("payment_method"),
            col("payment_status"),
            col("reservation_expires_at"),
            col("source"),
            col("movement_qty"),
            col("is_valid_event"),
            col("invalid_reason"),
            col("kafka_key"),
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            col("event_date"),
            col("business_date"),
            col("event_hour"),
            col("business_hour")
        )
        .withColumn("silver_processed_at", current_timestamp())
    )

    return (
        silver_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", config.silver_inventory_movement_path)
        .option("checkpointLocation", config.silver_movements_checkpoint_path)
        .partitionBy("business_date", "business_hour")
        .start()
    )


def write_silver_invalid_events(df: DataFrame):
    

    config = load_lake_config()

    invalid_df = (
        df.select(
            col("event_id"),
            col("campaign_id"),
            col("event_timestamp"),
            col("business_timestamp"),
            col("event_type"),
            col("sku_id"),
            col("warehouse_id"),
            col("is_valid_event"),
            col("invalid_reason"),
            col("json_value"),
            col("kafka_key"),
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("event_date"),
            col("business_date"),
            col("event_hour"),
            col("business_hour")
        )
        .withColumn("alert_type", lit("INVALID_EVENT"))
        .withColumn("alert_created_at", current_timestamp())

    )

    return (
        invalid_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", config.silver_inventory_alerts_path)
        .option("checkpointLocation", config.silver_alerts_checkpoint_path)
        .partitionBy("business_date", "business_hour")
        .start()
    )

def write_silver_sales_velocity_5m(df: DataFrame):
    
    config = load_lake_config()
    velocity_df = build_sales_velocity_5m_df(df)

    return (
        velocity_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", config.silver_sales_velocity_5m_path)
        .option("checkpointLocation", config.silver_sales_velocity_5m_checkpoint_path)
        .partitionBy("window_date", "window_hour")
        .start()
    )
