from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

from spark.common.lake_config import load_lake_config

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
            col("event_date"),
            col("event_hour"),
        )
        .withColumn("bronze_ingested_at", current_timestamp())
    )

    return (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", config.bronze_raw_inventory_events_path)
        .option("checkpointLocation", config.bronze_checkpoint_path)
        .partitionBy("event_date", "event_hour")
        .start()
    )

def write_silver_inventory_movements(df: DataFrame):
    config = load_lake_config()

    silver_df = (
        df.select(
            col("event_id"),
            col("campaign_id"),
            col("event_timestamp"),
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
            col("event_hour"),
        )
        .withColumn("silver_processed_at", current_timestamp())
    )

    return (
        silver_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", config.silver_inventory_movement_path)
        .option("checkpointLocation", config.silver_movements_checkpoint_path)
        .partitionBy("event_date", "event_hour")
        .start()
    )


def write_silver_invalid_events(df: DataFrame):

    config = load_lake_config()

    invalid_df = (
        df.select(
            col("event_id"),
            col("campaign_id"),
            col("event_timestamp"),
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
            col("event_hour")
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
        .partitionBy("event_date", "event_hour")
        .start()
    )