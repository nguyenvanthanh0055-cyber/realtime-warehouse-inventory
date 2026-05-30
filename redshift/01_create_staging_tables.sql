CREATE TABLE IF NOT EXISTS staging.daily_inventory_summary_stg (
    summary_date DATE,
    campaign_id VARCHAR(100),
    sku_id VARCHAR(100),
    warehouse_id VARCHAR(100),
    product_name VARCHAR(255),

    opening_sellable_stock INTEGER,
    total_reserved_qty INTEGER,
    total_cod_sold_qty INTEGER,
    total_cancelled_qty INTEGER,
    total_expired_qty INTEGER,
    total_returned_qty INTEGER,
    total_replenished_qty INTEGER,
    sales_movement_qty INTEGER,
    net_movement_qty INTEGER,
    closing_sellable_stock INTEGER,
    event_count BIGINT,

    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.reconciliation_result_stg (
    recon_date DATE,
    campaign_id VARCHAR(100),
    sku_id VARCHAR(100),
    warehouse_id VARCHAR(100),

    opening_sellable_stock INTEGER,
    net_movement_qty INTEGER,
    batch_recomputed_sellable_stock INTEGER,
    streaming_sellable_stock INTEGER,
    diff_qty INTEGER,
    status VARCHAR(50),

    created_at TIMESTAMP
);
