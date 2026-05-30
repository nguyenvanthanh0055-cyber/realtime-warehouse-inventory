import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    to_date,
    lit,
    sum,
    count,
    when,
    row_number,
)
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("inventory-gold-batch-job")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )



def read_open_snapshot(
    spark: SparkSession,
    lake_root: str,
    recon_date: str,
    campaign_id: str,
) -> DataFrame:
    snapshot_path = (
        f"{lake_root}/snapshots/inventory_snapshot/"
        f"snapshot_date={recon_date}/campaign_id={campaign_id}"
    )
    logger.info("Reading snapshot from path: %s", snapshot_path)
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(snapshot_path)
    )

    return (
        df.select(
            col("campaign_id"),
            col("sku_id"),
            col("warehouse_id"),
            col("product_name"),
            col("opening_sellable_stock").cast("int"),
            col("low_stock_threshold").cast("int"),
        )
        .filter(col("campaign_id") == campaign_id)
    )



def read_silver_movements(
    spark: SparkSession,
    lake_root: str,
    recon_date: str,
    campaign_id: str,
) -> DataFrame:
    movement_path = f"{lake_root}/silver/inventory_movements/"
    logger.info("Reading silver movements from path: %s", movement_path)
    df = spark.read.parquet(movement_path)

    return (
        df.filter(col("campaign_id") == campaign_id)
        .filter(col("business_date") == recon_date)
        .filter(col("is_valid_event") == lit(True))
        .dropDuplicates(["event_id"])
    )


def compute_net_movement(movement_df: DataFrame) -> DataFrame:
    return (
        movement_df
        .groupBy("campaign_id", "sku_id", "warehouse_id")
        .agg(
            sum(col("movement_qty")).cast("int").alias("net_movement_qty"),
            count("*").cast("int").alias("event_count"),
            sum(when(col("event_type") == "STOCK_RESERVED", col("quantity")).otherwise(lit(0))).cast("int").alias("reserved_qty"),
            sum(when(col("event_type") == "COD_CONFIRMED", col("quantity")).otherwise(lit(0))).cast("int").alias("cod_sold_qty"),
            sum(when(col("event_type") == "ORDER_CANCELLED", col("quantity")).otherwise(lit(0))).cast("int").alias("cancelled_qty"),
            sum(when(col("event_type") == "RESERVATION_EXPIRED", col("quantity")).otherwise(lit(0))).cast("int").alias("expired_qty"),
            sum(when(col("event_type") == "RETURN_RECEIVED", col("quantity")).otherwise(lit(0))).cast("int").alias("returned_qty"),
            sum(when(col("event_type") == "STOCK_REPLENISHED", col("quantity")).otherwise(lit(0))).cast("int").alias("replenished_qty"),
            sum(when(col("event_type") != "STOCK_REPLENISHED", col("movement_qty")).otherwise(lit(0))).cast("int").alias("sales_movement_qty"),
        )
    )


def read_streaming_state_history_from_s3(
    spark: SparkSession,
    lake_root: str,
    campaign_id: str,
    recon_date: str,
) -> DataFrame:
    state_history_path = f"{lake_root}/silver/current_inventory_state_history/"
    logger.info("Reading state history from path: %s", state_history_path)
    state_history_df = (
        spark.read
        .parquet(state_history_path)
        .filter(col("campaign_id") == campaign_id)
        .filter(col("business_date") <= recon_date)
    )

    order_columns = []
    if "history_id" in state_history_df.columns:
        order_columns.append(col("history_id").desc())

    order_columns.extend([
        col("processed_at").desc(),
        col("business_date").desc(),
        col("event_time").desc(),
        col("event_id").desc(),
    ])

    latest_state_window = (
        Window
        .partitionBy("campaign_id", "sku_id", "warehouse_id")
        .orderBy(*order_columns)
    )

    return (
        state_history_df
        .withColumn("row_num", row_number().over(latest_state_window))
        .filter(col("row_num") == 1)
        .select(
            col("campaign_id"),
            col("sku_id"),
            col("warehouse_id"),
            col("current_sellable_stock").cast("int").alias("streaming_sellable_stock"),
            col("status").alias("streaming_status"),
            col("event_id").alias("last_event_id"),
            col("event_time").alias("last_event_time"),
            col("processed_at").alias("updated_at"),
        )
    )


def build_reconciliation_result(
    snapshot_df: DataFrame,
    net_movement_df: DataFrame,
    current_state_df: DataFrame,
    recon_date: str,
) -> DataFrame:
    joined_df = (
        snapshot_df
        .join(net_movement_df, on=["campaign_id", "sku_id", "warehouse_id"], how="left")
        .join(current_state_df, on=["campaign_id", "sku_id", "warehouse_id"], how="left")
    )

    return (
        joined_df
        .withColumn("recon_date", to_date(lit(recon_date)))
        .withColumn("net_movement_qty", coalesce(col("net_movement_qty"), lit(0)).cast("int"))
        .withColumn("event_count", coalesce(col("event_count"), lit(0)).cast("int"))
        .withColumn("has_streaming_state", col("streaming_sellable_stock").isNotNull())
        .withColumn(
            "batch_recomputed_sellable_stock",
            col("opening_sellable_stock") + col("net_movement_qty"),
        )
        .withColumn(
            "streaming_sellable_stock",
            coalesce(col("streaming_sellable_stock").cast("int"), col("opening_sellable_stock")),
        )
        .withColumn(
            "diff_qty",
            col("batch_recomputed_sellable_stock") - col("streaming_sellable_stock"),
        )
        .withColumn(
            "status",
            when(
                ((col("has_streaming_state") == lit(True)) | (col("event_count") == lit(0))) &
                (col("diff_qty") == 0),
                lit("MATCH"),
            ).otherwise(lit("MISMATCH")),
        )
        .withColumn("created_at", current_timestamp())
        .select(
            "recon_date",
            "campaign_id",
            "sku_id",
            "warehouse_id",
            "opening_sellable_stock",
            "net_movement_qty",
            "batch_recomputed_sellable_stock",
            "streaming_sellable_stock",
            "diff_qty",
            "status",
            "created_at",
        )
    )


def build_daily_summary(
    snapshot_df: DataFrame,
    net_movement_df: DataFrame,
    recon_date: str,
) -> DataFrame:
    joined_df = (
        snapshot_df
        .join(net_movement_df, on=["campaign_id", "sku_id", "warehouse_id"], how="left")
    )

    return (
        joined_df
        .withColumn("summary_date", to_date(lit(recon_date)))
        .withColumn("net_movement_qty", coalesce(col("net_movement_qty"), lit(0)).cast("int"))
        .withColumn("event_count", coalesce(col("event_count"), lit(0)).cast("int"))
        .withColumn("total_reserved_qty", coalesce(col("reserved_qty"), lit(0)).cast("int"))
        .withColumn("total_cod_sold_qty", coalesce(col("cod_sold_qty"), lit(0)).cast("int"))
        .withColumn("total_cancelled_qty", coalesce(col("cancelled_qty"), lit(0)).cast("int"))
        .withColumn("total_expired_qty", coalesce(col("expired_qty"), lit(0)).cast("int"))
        .withColumn("total_returned_qty", coalesce(col("returned_qty"), lit(0)).cast("int"))
        .withColumn("total_replenished_qty", coalesce(col("replenished_qty"), lit(0)).cast("int"))
        .withColumn("sales_movement_qty", coalesce(col("sales_movement_qty"), lit(0)).cast("int"))
        .withColumn(
            "closing_sellable_stock",
            col("opening_sellable_stock") + col("total_replenished_qty") + col("sales_movement_qty"),
        )
        .withColumn("created_at", current_timestamp())
        .select(
            "summary_date",
            "campaign_id",
            "sku_id",
            "warehouse_id",
            "product_name",
            "opening_sellable_stock",
            "total_reserved_qty",
            "total_cod_sold_qty",
            "total_cancelled_qty",
            "total_expired_qty",
            "total_returned_qty",
            "total_replenished_qty",
            "sales_movement_qty",
            "net_movement_qty",
            "closing_sellable_stock",
            "event_count",
            "created_at",
        )
    )


def write_gold_outputs(
    reconciliation_df: DataFrame,
    daily_summary_df: DataFrame,
    lake_root: str,
) -> None:
    reconciliation_path = f"{lake_root}/gold/inventory_reconciliation/"
    daily_summary_path = f"{lake_root}/gold/daily_inventory_summary/"

    (
        reconciliation_df
        .write
        .mode("overwrite")
        .partitionBy("recon_date", "campaign_id")
        .parquet(reconciliation_path)
    )

    (
        daily_summary_df
        .write
        .mode("overwrite")
        .partitionBy("summary_date", "campaign_id")
        .parquet(daily_summary_path)
    )

    logger.info(f"[GOLD] Wrote reconciliation to: {reconciliation_path}")
    logger.info(f"[GOLD] Wrote daily summary to: {daily_summary_path}")


def main():
    try:
        args = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "recon_date",
                "campaign_id",
                "lake_root",
            ],
        )

        logger.info(
            "Starting Glue Gold batch job | recon_date=%s | campaign_id=%s | lake_root=%s",
            args["recon_date"],
            args["campaign_id"],
            args["lake_root"],
        )

        spark = build_spark()
        spark.sparkContext.setLogLevel("WARN")

        snapshot_df = read_open_snapshot(
            spark=spark,
            lake_root=args["lake_root"],
            recon_date=args["recon_date"],
            campaign_id=args["campaign_id"],
        )

        movement_df = read_silver_movements(
            spark=spark,
            lake_root=args["lake_root"],
            recon_date=args["recon_date"],
            campaign_id=args["campaign_id"],
        )

        net_movement_df = compute_net_movement(movement_df)

        current_state_df = read_streaming_state_history_from_s3(
            spark=spark,
            lake_root=args["lake_root"],
            campaign_id=args["campaign_id"],
            recon_date=args["recon_date"],
        )

        reconciliation_df = build_reconciliation_result(
            snapshot_df=snapshot_df,
            net_movement_df=net_movement_df,
            current_state_df=current_state_df,
            recon_date=args["recon_date"],
        )

        daily_summary_df = build_daily_summary(
            snapshot_df=snapshot_df,
            net_movement_df=net_movement_df,
            recon_date=args["recon_date"],
        )

        write_gold_outputs(
            reconciliation_df=reconciliation_df,
            daily_summary_df=daily_summary_df,
            lake_root=args["lake_root"],
        )

        logger.info("Completed inventory Gold batch job successfully")

    except Exception:
        logger.exception("Glue Gold batch job failed")
        raise

if __name__ == "__main__":
    main()
