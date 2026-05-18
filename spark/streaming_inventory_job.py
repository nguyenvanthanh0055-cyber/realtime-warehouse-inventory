import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from common.schemas import inventory_event_schema
from common.transformations import transform_inventory_events
from spark.sinks.postgres_current_state_sink import write_batch_to_postgres
from spark.sinks.lake_sink import (
    write_bronze_raw_inventory_events,
    write_silver_inventory_movements,
    write_silver_invalid_events,
    write_silver_sales_velocity_5m
)

def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )


def parse_kafka_events(kafka_df):
    kafka_values_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("json_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp")
    )
    parsed_df = kafka_values_df.select(
        col("kafka_key"),
        col("json_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        from_json(col("json_value"), inventory_event_schema).alias("event")
    )
    event_df = parsed_df.select(
        "kafka_key",
        "json_value",
        "topic",
        "partition",
        "offset",
        "timestamp",
        col("event.*")
    )
    return event_df



def write_to_postgres(df, checkpoint_location: str):
    return(
        df.writeStream
        .foreachBatch(write_batch_to_postgres)
        .outputMode("update")
        .option("checkPointLocation", checkpoint_location)
        .trigger(processingTime="5 seconds")
        .start()
    )
def main():
    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming job for inventory events"
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
    )

    parser.add_argument(
        "--topic",
        default="inventory-events",
    )

    args = parser.parse_args()

    spark = create_spark_session(
        app_name="inventory-streaming-job"
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka_stream(
        spark=spark,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )

    event_df = parse_kafka_events(kafka_df)

    transformed_df = transform_inventory_events(event_df)

    output_df = transformed_df.select(
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
    col("event_date"),
    col("business_date"),
    col("event_hour"),
    col("business_hour"),
    
    col("kafka_key"),
    col("json_value"),
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
)

    postgres_query = write_to_postgres(
        df=output_df,
        checkpoint_location="data/checkpoints/postgres_current_state")

    bronze_query = write_bronze_raw_inventory_events(output_df)
    silver_movements_query = write_silver_inventory_movements(output_df)
    silver_invalid_query = write_silver_invalid_events(output_df)
    silver_sales_velocity_query = write_silver_sales_velocity_5m(output_df)


    print("Streaming queries started: ")
    print(f"- Postgres current state: {postgres_query.id}")
    print(f"- Bronze raw inventory events: {bronze_query.id}")
    print(f"- Silver inventory movements: {silver_movements_query.id}")
    print(f"- Silver invalid events: {silver_invalid_query.id}")
    print(f"- Silver sales velocity 5m: {silver_sales_velocity_query.id}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
