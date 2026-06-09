import argparse
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from aws.glue.dynamodb_current_state_sink import write_batch_to_dynamodb
from spark.common.schemas import inventory_event_schema
from spark.common.transformations import transform_inventory_events
from aws.glue.dynamodb_sales_velocity_sink import (
    write_batch_sales_velocity_5m_to_dynamodb,
    )
from spark.sinks.lake_sink import (
    build_sales_velocity_5m_df,
    write_bronze_raw_inventory_events,
    write_silver_inventory_movements,
    write_silver_invalid_events,
    write_silver_sales_velocity_5m,
)

logger = logging.getLogger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str,
    auth_mode: str,
):
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )

    if auth_mode == "iam":
        reader = (
            reader
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
            .option(
                "kafka.sasl.jaas.config",
                "software.amazon.msk.auth.iam.IAMLoginModule required;",
            )
            .option(
                "kafka.sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
            )
        )

    return reader.load()


def parse_kafka_events(kafka_df):
    kafka_values_df = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("json_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    )

    parsed_df = kafka_values_df.select(
        col("kafka_key"),
        col("json_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        from_json(col("json_value"), inventory_event_schema).alias("event"),
    )

    return parsed_df.select(
        "kafka_key",
        "json_value",
        "topic",
        "partition",
        "offset",
        "timestamp",
        col("event.*"),
    )


def write_to_dynamodb(df, checkpoint_location: str):
    return (
        df.writeStream
        .foreachBatch(write_batch_to_dynamodb)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="15 seconds")
        .start()
    )


def write_sales_velocity_to_dynamodb(df, checkpoint_location: str):


    velocity_df = build_sales_velocity_5m_df(df)

    return (
        velocity_df.writeStream
        .foreachBatch(write_batch_sales_velocity_5m_to_dynamodb)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime="15 seconds")
        .start()
    )


def parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value

    normalized_value = str(value).strip().lower()
    if normalized_value in {"true", "1", "yes", "y"}:
        return True
    if normalized_value in {"false", "0", "no", "n"}:
        return False

    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.getenv("KAFKA_TOPIC", "inventory-events")
    auth_mode = os.getenv("MSK_AUTH_MODE", "iam")
    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    
    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming job for inventory events"
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=bootstrap_servers,
    )

    parser.add_argument(
        "--topic",
        default=topic,
    )

    parser.add_argument(
        "--auth-mode",
        choices=["iam", "plaintext"],
        default=auth_mode,
    )

    parser.add_argument(
        "--starting-offsets",
        choices=["earliest", "latest"],
        default=starting_offsets,
    )

    parser.add_argument(
        "--lake-root",
        default=os.getenv("LAKE_ROOT"),
    )

    parser.add_argument(
        "--checkpoint-root",
        default=os.getenv("STREAMING_CHECKPOINT_ROOT"),
    )

    parser.add_argument(
        "--kafka-tcp-check-timeout-seconds",
        type=int,
        default=int(os.getenv("KAFKA_TCP_CHECK_TIMEOUT_SECONDS", "10")),
    )


    args, _unknown_args = parser.parse_known_args()

    if not args.bootstrap_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS or --bootstrap-servers is required")

    if args.lake_root:
        os.environ["LAKE_ROOT"] = args.lake_root

    if args.checkpoint_root:
        os.environ["STREAMING_CHECKPOINT_ROOT"] = args.checkpoint_root

    spark = create_spark_session(app_name="inventory-streaming-job")

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka_stream(
        spark=spark,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        starting_offsets=args.starting_offsets,
        auth_mode=args.auth_mode,
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

    checkpoint_root = args.checkpoint_root or os.getenv("STREAMING_CHECKPOINT_ROOT")
    dynamodb_query = write_to_dynamodb(
        df=output_df,
        checkpoint_location=f"{checkpoint_root.rstrip('/')}/dynamodb_current_state"
    )

    bronze_query = write_bronze_raw_inventory_events(output_df)
    silver_movements_query = write_silver_inventory_movements(output_df)
    silver_invalid_query = write_silver_invalid_events(output_df)

    silver_sales_velocity_query = write_silver_sales_velocity_5m(output_df)

    sales_velocity_dynamodb_query = write_sales_velocity_to_dynamodb(
        df=output_df,
        checkpoint_location=(
            f"{checkpoint_root.rstrip('/')}/dynamodb_sales_velocity_5m"
        ),
    )

    logger.info("Streaming queries started")

    if dynamodb_query:
        logger.info("DynamoDB current state query_id=%s", dynamodb_query.id)
    else:
        logger.info("DynamoDB current state disabled")

    logger.info("Bronze raw inventory events query_id=%s", bronze_query.id)
    logger.info("Silver inventory movements query_id=%s", silver_movements_query.id)
    logger.info("Silver invalid events query_id=%s", silver_invalid_query.id)

    if silver_sales_velocity_query:
        logger.info(
            "Silver sales velocity 5m query_id=%s",
            silver_sales_velocity_query.id,
        )
    else:
        logger.info("Silver sales velocity 5m disabled")

    if sales_velocity_dynamodb_query:
        logger.info(
            "DynamoDB sales velocity 5m query_id=%s",
            sales_velocity_dynamodb_query.id,
        )
    else:
        logger.info("DynamoDB sales velocity 5m disabled")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
