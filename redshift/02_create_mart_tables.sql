CREATE TABLE IF NOT EXISTS mart.fact_daily_inventory_summary (
    summary_date DATE NOT NULL,
    campaign_id VARCHAR(100) NOT NULL,
    sku_id VARCHAR(100) NOT NULL,
    warehouse_id VARCHAR(100) NOT NULL,
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

    created_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY (summary_date, campaign_id, sku_id);

CREATE TABLE IF NOT EXISTS mart.fact_reconciliation_result (
    recon_date DATE NOT NULL,
    campaign_id VARCHAR(100) NOT NULL,
    sku_id VARCHAR(100) NOT NULL,
    warehouse_id VARCHAR(100) NOT NULL,

    opening_sellable_stock INTEGER,
    net_movement_qty INTEGER,
    batch_recomputed_sellable_stock INTEGER,
    streaming_sellable_stock INTEGER,
    diff_qty INTEGER,
    status VARCHAR(50),

    created_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY (recon_date, campaign_id, status);
