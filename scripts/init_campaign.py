import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()


PROJECT_ROOT = Path(__file__).resolve().parents[1]

INITIAL_INVENTORY_PATH = PROJECT_ROOT / "data" / "input" / "initial_inventory.csv"
PROMOTION_CONFIG_PATH = PROJECT_ROOT / "data" / "input" / "promotion_config.csv"

LOCAL_LAKE_ROOT = Path(os.getenv("LOCAL_LAKE_ROOT", PROJECT_ROOT / "data" / "lake"))


def get_postgres_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "inventory_db")
    user = os.getenv("POSTGRES_USER", "inventory_user")
    password = os.getenv("POSTGRES_PASSWORD", "inventory_password")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def validate_initial_inventory(df: pd.DataFrame) -> None:
    required_columns = {
        "campaign_id",
        "sku_id",
        "warehouse_id",
        "product_name",
        "initial_sellable_stock",
        "low_stock_threshold",
    }

    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns in initial_inventory.csv: {missing_columns}")

    if df[["campaign_id", "sku_id", "warehouse_id"]].duplicated().any():
        raise ValueError(
            "Duplicate campaign_id + sku_id + warehouse_id found in initial_inventory.csv"
        )

    if (df["initial_sellable_stock"] < 0).any():
        raise ValueError("initial_sellable_stock must be >= 0")

    if (df["low_stock_threshold"] < 0).any():
        raise ValueError("low_stock_threshold must be >= 0")


def validate_promotion_config(
    promotion_df: pd.DataFrame,
    inventory_df: pd.DataFrame,
) -> None:
    required_columns = {
        "campaign_id",
        "promotion_id",
        "sku_id",
        "warehouse_id",
        "promotion_name",
        "promotion_quota",
        "sale_price",
        "normal_price",
        "start_time",
        "end_time",
    }

    missing_columns = required_columns - set(promotion_df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns in promotion_config.csv: {missing_columns}")

    if promotion_df[["campaign_id", "promotion_id", "sku_id", "warehouse_id"]].duplicated().any():
        raise ValueError(
            "Duplicate campaign_id + promotion_id + sku_id + warehouse_id found in promotion_config.csv"
        )

    if (promotion_df["promotion_quota"] < 0).any():
        raise ValueError("promotion_quota must be >= 0")

    if (promotion_df["sale_price"] <= 0).any():
        raise ValueError("sale_price must be > 0")

    if (promotion_df["normal_price"] <= 0).any():
        raise ValueError("normal_price must be > 0")

    inventory_keys = set(
        zip(
            inventory_df["campaign_id"],
            inventory_df["sku_id"],
            inventory_df["warehouse_id"],
        )
    )

    promotion_keys = set(
        zip(
            promotion_df["campaign_id"],
            promotion_df["sku_id"],
            promotion_df["warehouse_id"],
        )
    )

    unknown_keys = promotion_keys - inventory_keys
    if unknown_keys:
        raise ValueError(
            f"Promotion references unknown campaign_id + sku_id + warehouse_id: {unknown_keys}"
        )

    for _, row in promotion_df.iterrows():
        start_time = pd.to_datetime(row["start_time"], utc=True)
        end_time = pd.to_datetime(row["end_time"], utc=True)

        if end_time <= start_time:
            raise ValueError(
                f"Invalid promotion time range for {row['promotion_id']}: end_time <= start_time"
            )


def calculate_status(current_sellable_stock: int, low_stock_threshold: int) -> str:
    if current_sellable_stock < 0:
        return "OVERSELL"

    if current_sellable_stock <= low_stock_threshold:
        return "LOW_STOCK"

    return "NORMAL"


def clear_runtime_tables_for_campaign(engine, campaign_id: str) -> None:
    """
    Local dev/test reset for one campaign.
    In production, this would usually be handled by partitioning by campaign_id/date,
    not by deleting data manually.
    """
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM processed_events WHERE campaign_id = :campaign_id"),
            {"campaign_id": campaign_id},
        )
        conn.execute(
            text("DELETE FROM inventory_alerts WHERE campaign_id = :campaign_id"),
            {"campaign_id": campaign_id},
        )
        conn.execute(
            text("DELETE FROM sales_velocity_window WHERE campaign_id = :campaign_id"),
            {"campaign_id": campaign_id},
        )
        conn.execute(
            text("DELETE FROM reconciliation_result WHERE campaign_id = :campaign_id"),
            {"campaign_id": campaign_id},
        )


def seed_current_inventory(
    engine,
    inventory_df: pd.DataFrame,
) -> None:
    sql = text("""
        INSERT INTO current_inventory (
            campaign_id,
            sku_id,
            warehouse_id,
            product_name,
            current_sellable_stock,
            low_stock_threshold,
            status,
            last_event_id,
            last_event_time,
            updated_at
        )
        VALUES (
            :campaign_id,
            :sku_id,
            :warehouse_id,
            :product_name,
            :current_sellable_stock,
            :low_stock_threshold,
            :status,
            NULL,
            NULL,
            NOW()
        )
        ON CONFLICT (campaign_id, sku_id, warehouse_id)
        DO UPDATE SET
            product_name = EXCLUDED.product_name,
            current_sellable_stock = EXCLUDED.current_sellable_stock,
            low_stock_threshold = EXCLUDED.low_stock_threshold,
            status = EXCLUDED.status,
            last_event_id = NULL,
            last_event_time = NULL,
            updated_at = NOW()
    """)

    rows = []

    for _, row in inventory_df.iterrows():
        current_sellable_stock = int(row["initial_sellable_stock"])
        low_stock_threshold = int(row["low_stock_threshold"])

        rows.append({
            "campaign_id": row["campaign_id"],
            "sku_id": row["sku_id"],
            "warehouse_id": row["warehouse_id"],
            "product_name": row["product_name"],
            "current_sellable_stock": current_sellable_stock,
            "low_stock_threshold": low_stock_threshold,
            "status": calculate_status(current_sellable_stock, low_stock_threshold),
        })

    with engine.begin() as conn:
        conn.execute(sql, rows)


def seed_promotion_metrics(
    engine,
    promotion_df: pd.DataFrame,
) -> None:
    sql = text("""
        INSERT INTO promotion_metrics (
            campaign_id,
            promotion_id,
            sku_id,
            warehouse_id,
            promotion_quota,
            promotion_sold_qty,
            promotion_cancelled_qty,
            deal_sold_out_at,
            updated_at
        )
        VALUES (
            :campaign_id,
            :promotion_id,
            :sku_id,
            :warehouse_id,
            :promotion_quota,
            0,
            0,
            NULL,
            NOW()
        )
        ON CONFLICT (campaign_id, promotion_id, sku_id, warehouse_id)
        DO UPDATE SET
            promotion_quota = EXCLUDED.promotion_quota,
            promotion_sold_qty = 0,
            promotion_cancelled_qty = 0,
            deal_sold_out_at = NULL,
            updated_at = NOW()
    """)

    rows = []

    for _, row in promotion_df.iterrows():
        rows.append({
            "campaign_id": row["campaign_id"],
            "promotion_id": row["promotion_id"],
            "sku_id": row["sku_id"],
            "warehouse_id": row["warehouse_id"],
            "promotion_quota": int(row["promotion_quota"]),
        })

    with engine.begin() as conn:
        conn.execute(sql, rows)


def write_opening_snapshot(
    inventory_df: pd.DataFrame,
    snapshot_date: str,
    campaign_id: str,
) -> Path:
    snapshot_df = inventory_df.copy()

    snapshot_df.insert(0, "snapshot_date", snapshot_date)

    snapshot_df = snapshot_df.rename(
        columns={
            "initial_sellable_stock": "opening_sellable_stock"
        }
    )

    output_dir = (
        LOCAL_LAKE_ROOT
        / "snapshots"
        / "inventory_snapshot"
        / f"snapshot_date={snapshot_date}"
        / f"campaign_id={campaign_id}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "inventory_snapshot.csv"
    snapshot_df.to_csv(output_path, index=False)

    return output_path


def main():
    print("os: ", os.getenv("CAMPAIGN_ID"))
    campaign_id = os.getenv("CAMPAIGN_ID")
    snapshot_date = os.getenv("SNAPSHOT_DATE")

    if not campaign_id:
        raise ValueError("CAMPAIGN_ID is required. Example: CAMPAIGN_ID=CAMPAIGN_FLASH_0427")

    if not snapshot_date:
        raise ValueError("SNAPSHOT_DATE is required. Example: SNAPSHOT_DATE=2026-04-27")

    print("Starting campaign initialization...")
    print(f"Campaign ID: {campaign_id}")
    print(f"Snapshot date: {snapshot_date}")
    print(f"Initial inventory path: {INITIAL_INVENTORY_PATH}")
    print(f"Promotion config path: {PROMOTION_CONFIG_PATH}")

    inventory_df_all = pd.read_csv(INITIAL_INVENTORY_PATH)
    promotion_df_all = pd.read_csv(PROMOTION_CONFIG_PATH)

    validate_initial_inventory(inventory_df_all)
    validate_promotion_config(promotion_df_all, inventory_df_all)

    inventory_df = inventory_df_all[inventory_df_all["campaign_id"] == campaign_id].copy()
    promotion_df = promotion_df_all[promotion_df_all["campaign_id"] == campaign_id].copy()

    if inventory_df.empty:
        raise ValueError(f"No inventory rows found for campaign_id={campaign_id}")

    if promotion_df.empty:
        raise ValueError(f"No promotion rows found for campaign_id={campaign_id}")

    engine = get_postgres_engine()

    clear_runtime_tables_for_campaign(engine, campaign_id)
    seed_current_inventory(engine, inventory_df)
    seed_promotion_metrics(engine, promotion_df)

    snapshot_path = write_opening_snapshot(
        inventory_df=inventory_df,
        snapshot_date=snapshot_date,
        campaign_id=campaign_id,
    )

    print("Campaign initialization completed.")
    print(f"Seeded current_inventory rows: {len(inventory_df)}")
    print(f"Seeded promotion_metrics rows: {len(promotion_df)}")
    print(f"Opening snapshot written to: {snapshot_path}")


if __name__ == "__main__":
    main()