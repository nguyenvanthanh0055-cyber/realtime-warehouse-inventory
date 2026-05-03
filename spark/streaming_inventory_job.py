import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from common.schemas import inventory_event_schema
from common.transformations import transform_inventory_events


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
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
        col("timestamp").alias("kafka_timestamp")
    )
    parsed_df = kafka_values_df.select(
        col("kafka_key"),
        col("json_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        from_json(col("json_value"), inventory_event_schema).alias("event")
    )
    event_df = parsed_df.select(
        "kafka_key",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        col("event.*")
    )
    return event_df



def write_to_console(df):
    return (
        df.writeStream
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .outputMode("append")
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
        "event_id",
        "campaign_id",
        "event_timestamp",
        "event_type",
        "order_id",
        "sku_id",
        "warehouse_id",
        "quantity",
        "movement_qty",
        "payment_method",
        "payment_status",
        "promotion_id",
        "promotion_applied",
        "is_valid_event",
        "invalid_reason",
        "kafka_key",
        "partition",
        "offset",
    )

    query = write_to_console(output_df)

    query.awaitTermination()


if __name__ == "__main__":
    main()