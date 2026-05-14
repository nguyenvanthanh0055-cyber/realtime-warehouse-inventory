import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    lit,
    sum,
    count,
    when
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("daily-inventory-reconciliation")
        .getOrCreate()
    )


def read_open_snapshot(
    spark: SparkSession,
    lake_root: str,
    recon_date: str,
    campaign_id: str
) -> DataFrame:
    snapshot_path = (
        f"{lake_root}/snapshots/inventory_snapshot/"
        f"snapshot_date={recon_date}/campaign_id={campaign_id}"
    )
    
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
            col("low_stock_threshold").cast("int")
        )
        .filter(col("campaign_id") == campaign_id)
    )


def read_silver_movements(
    spark: SparkSession,
    lake_root: str,
    recon_date: str,
    campaign_id: str
) -> DataFrame:
    movement_path = f"{lake_root}/silver/inventory_movements/"
    
    df = spark.read.parquet(movement_path)

    return (
        df.filter(col("campaign_id") == campaign_id)
        .filter(col("business_date") == recon_date)
        .filter(col("is_valid_event") == lit(True))
    )


def compute_net_movement(movement_df: DataFrame) -> DataFrame:
    return (
        movement_df
        .groupBy("campaign_id", "sku_id", "warehouse_id")
        .agg(
            sum(col("movement_qty")).cast("int").alias("net_movement_qty"),
            count("*").cast("int").alias("event_count"),
            sum(
                when(col("event_type") == "STOCK_RESERVED", col("quantity")).otherwise(lit(0))
            ).cast("int").alias("reserved_qty"),
            sum(
                when(col("event_type") == "COD_CONFIRMED", col("quantity")).otherwise(lit(0))
            ).cast("int").alias("cod_sold_qty"),
            sum(
                when(col("event_type") == "ORDER_CANCELLED", col("quantity")).otherwise(lit(0))
            ).cast("int").alias("cancelled_qty"),
            sum(
                when(col("event_type") == "RESERVATION_EXPIRED", col("quantity")).otherwise(lit(0))
            ).cast("int").alias("expired_qty"),
            sum(
                when(col("event_type") == "RETURN_RECEIVED", col("quantity")).otherwise(lit(0))
            ).cast("int").alias("returned_qty"),
            sum(
                when(col("event_type") == "STOCK_REPLENISHED", col("quantity")).otherwise(lit(0))
            ).cast("int").alias("replenished_qty")
        )
    )


def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def read_streaming_current_state(
    spark: SparkSession,
    postgres_url: str,
    postgres_user: str,
    postgres_password: str,
    campaign_id: str
) -> DataFrame:
    escaped_campaign_id = sql_literal(campaign_id)
    query = f"""
    (
        SELECT
            campaign_id,
            sku_id,
            warehouse_id,
            current_sellable_stock as streaming_sellable_stock,
            status as streaming_status,
            last_event_id,
            last_event_time,
            updated_at
        FROM current_inventory
        WHERE campaign_id = '{escaped_campaign_id}'
    ) AS current_inventory_filtered
    """

    return (
        spark.read
        .format("jdbc")
        .option("url", postgres_url)
        .option("dbtable", query)
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def build_reconciliation_result(
    snapshot_df: DataFrame,
    net_movement_df: DataFrame,
    current_state_df: DataFrame,
    recon_date: str
) -> DataFrame:
    
    joined_df = (
        snapshot_df
        .join(
            net_movement_df,
            on=["campaign_id", "sku_id", "warehouse_id"],
            how="left"
        )
        .join(
            current_state_df,
            on=["campaign_id", "sku_id", "warehouse_id"],
            how="left"
        )
    )

    result_df = (
        joined_df
        .withColumn("recon_date", lit(recon_date))
        .withColumn("net_movement_qty", coalesce(col("net_movement_qty"), lit(0)).cast("int"))
        .withColumn("event_count", coalesce(col("event_count"), lit(0)))
        .withColumn(
            "batch_recomputed_sellable_stock",
            col("opening_sellable_stock") + col("net_movement_qty")
        )
        .withColumn(
            "streaming_sellable_stock",
            col("streaming_sellable_stock").cast("int")
        )
        .withColumn(
            "diff_qty",
            col("batch_recomputed_sellable_stock") - col("streaming_sellable_stock")
        )
        .withColumn(
            "status",
            when(
                col("streaming_sellable_stock").isNotNull() & (col("diff_qty") == 0),
                lit("MATCH")
            ).otherwise(lit("MISMATCH"))
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
            "created_at"
        )
    )
    return result_df


def build_daily_summary(
    snapshot_df: DataFrame,
    net_movement_df: DataFrame,
    recon_date: str
) -> DataFrame:
    joined_df = (
        snapshot_df
        .join(
            net_movement_df,
            on=["campaign_id", "sku_id", "warehouse_id"],
            how="left"
        )
    )
    result_df = (
        joined_df
        .withColumn("summary_date", lit(recon_date))
        .withColumn("net_movement_qty", coalesce(col("net_movement_qty"), lit(0)).cast("int"))
        .withColumn("event_count", coalesce(col("event_count"), lit(0)).cast("int"))
        .withColumn("total_reserved_qty", coalesce(col("reserved_qty"), lit(0)).cast("int"))
        .withColumn("total_cod_sold_qty", coalesce(col("cod_sold_qty"), lit(0)).cast("int"))
        .withColumn("total_cancelled_qty", coalesce(col("cancelled_qty"), lit(0)).cast("int"))
        .withColumn("total_expired_qty", coalesce(col("expired_qty"), lit(0)).cast("int"))
        .withColumn("total_returned_qty", coalesce(col("returned_qty"), lit(0)).cast("int"))
        .withColumn("total_replenished_qty", coalesce(col("replenished_qty"), lit(0)).cast("int"))
        .withColumn(
            "closing_sellable_stock",
            col("opening_sellable_stock") + col("net_movement_qty")
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
            "net_movement_qty",
            "closing_sellable_stock",
            "event_count",
            "created_at"
        )
    )

    return result_df


def write_gold_outputs(
    reconciliation_df: DataFrame,
    daily_summary_df: DataFrame,
    lake_root: str,
    recon_date: str,
    campaign_id: str
) -> None:
    reconciliation_path = (
        f"{lake_root}/gold/inventory_reconciliation/"
        f"recon_date={recon_date}/campaign_id={campaign_id}"
    )

    daily_summary_path = (
        f"{lake_root}/gold/daily_inventory_summary/"
        f"recon_date={recon_date}/campaign_id={campaign_id}"
    )

    reconciliation_df \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(reconciliation_path)
    
    daily_summary_df \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(daily_summary_path)

    print(f"[GOLD] Wrote reconciliation to: {reconciliation_path}")
    print(f"[GOLD] Wrote daily summary to: {daily_summary_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Daily batch reconciliation for sellable inventory."
    )

    parser.add_argument(
        "--recon-date",
        required=True,
        help="Reconciliation date in yyyy-mm-dd format.",
    )

    parser.add_argument(
        "--campaign-id",
        required=True,
        help="Campaign ID to reconcile.",
    )

    parser.add_argument(
        "--lake-root",
        default="data/lake",
        help="Local data lake root. Default: data/lake",
    )

    parser.add_argument(
        "--postgres-url",
        default="jdbc:postgresql://localhost:5432/inventory_db",
        help="PostgreSQL JDBC URL.",
    )

    parser.add_argument(
        "--postgres-user",
        default="inventory_user",
        help="PostgreSQL user.",
    )

    parser.add_argument(
        "--postgres-password",
        default="inventory_password",
        help="PostgreSQL password.",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark()

    snapshot_df = read_open_snapshot(
        spark=spark,
        lake_root=args.lake_root,
        recon_date=args.recon_date,
        campaign_id=args.campaign_id
    )

    movement_df = read_silver_movements(
        spark=spark,
        lake_root=args.lake_root,
        recon_date=args.recon_date,
        campaign_id=args.campaign_id
    )

    net_movement_df = compute_net_movement(movement_df)

    current_state_df = read_streaming_current_state(
        spark=spark,
        postgres_url=args.postgres_url,
        postgres_user=args.postgres_user,
        postgres_password=args.postgres_password,
        campaign_id=args.campaign_id
    )

    reconciliation_df = build_reconciliation_result(
        snapshot_df=snapshot_df,
        net_movement_df=net_movement_df,
        current_state_df=current_state_df,
        recon_date=args.recon_date
    )

    daily_summary_df = build_daily_summary(
        snapshot_df=snapshot_df,
        net_movement_df=net_movement_df,
        recon_date=args.recon_date
    )

    print("[Phase 8] Reconciliation preview")
    reconciliation_df.show(truncate=False)

    print("[Phase 8] Daily summary preview")
    daily_summary_df.show(truncate=False)

    write_gold_outputs(
        reconciliation_df=reconciliation_df,
        daily_summary_df=daily_summary_df,
        lake_root=args.lake_root,
        recon_date=args.recon_date,
        campaign_id=args.campaign_id
    )

    print("[Phase 8] Completed daily reconciliation")

if __name__ == "__main__":
    main()
