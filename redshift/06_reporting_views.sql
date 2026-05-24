CREATE OR REPLACE VIEW mart.vw_inventory_dashboard AS
SELECT
    summary_date,
    campaign_id,
    sku_id,
    warehouse_id,
    product_name,

    opening_sellable_stock,
    total_reserved_qty,
    total_cod_sold_qty,
    total_cancelled_qty,
    total_expired_qty,
    total_returned_qty,
    total_replenished_qty,
    net_movement_qty,
    closing_sellable_stock,
    event_count,

    created_at,
    loaded_at
FROM mart.fact_daily_inventory_summary;

CREATE OR REPLACE VIEW mart.vw_reconciliation_dashboard AS
SELECT
    recon_date,
    campaign_id,
    sku_id,
    warehouse_id,

    opening_sellable_stock,
    net_movement_qty,
    batch_recomputed_sellable_stock,
    streaming_sellable_stock,
    diff_qty,
    status,

    created_at,
    loaded_at
FROM mart.fact_reconciliation_result;

CREATE OR REPLACE VIEW mart.vw_reconciliation_summary AS
SELECT
    recon_date,
    campaign_id,
    status,
    COUNT(*) AS sku_warehouse_count,
    SUM(ABS(COALESCE(diff_qty, 0))) AS total_abs_diff_qty,
    MAX(loaded_at) AS latest_loaded_at
FROM mart.fact_reconciliation_result
GROUP BY
    recon_date,
    campaign_id,
    status;

CREATE OR REPLACE VIEW mart.vw_monthly_campaign_sku_inventory_summary AS
WITH ranked_inventory AS (
    SELECT
        DATE_TRUNC('month', summary_date)::DATE AS month_date,
        summary_date,
        campaign_id,
        sku_id,
        warehouse_id,
        product_name,
        opening_sellable_stock,
        closing_sellable_stock,
        total_reserved_qty,
        total_cod_sold_qty,
        total_cancelled_qty,
        total_expired_qty,
        total_returned_qty,
        total_replenished_qty,
        net_movement_qty,
        event_count,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('month', summary_date)::DATE, campaign_id, sku_id, warehouse_id
            ORDER BY summary_date ASC, loaded_at ASC
        ) AS first_row_num,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('month', summary_date)::DATE, campaign_id, sku_id, warehouse_id
            ORDER BY summary_date DESC, loaded_at DESC
        ) AS latest_row_num
    FROM mart.fact_daily_inventory_summary
),
monthly_agg AS (
    SELECT
        month_date,
        campaign_id,
        sku_id,
        warehouse_id,
        MAX(CASE WHEN latest_row_num = 1 THEN product_name END) AS product_name,
        SUM(total_reserved_qty) AS total_reserved_qty,
        SUM(total_cod_sold_qty) AS total_cod_sold_qty,
        SUM(total_cancelled_qty) AS total_cancelled_qty,
        SUM(total_expired_qty) AS total_expired_qty,
        SUM(total_returned_qty) AS total_returned_qty,
        SUM(total_replenished_qty) AS total_replenished_qty,
        SUM(net_movement_qty) AS total_net_movement_qty,
        SUM(event_count) AS total_event_count,
        MAX(CASE WHEN first_row_num = 1 THEN opening_sellable_stock END) AS first_opening_sellable_stock,
        MAX(CASE WHEN latest_row_num = 1 THEN closing_sellable_stock END) AS latest_closing_sellable_stock
    FROM ranked_inventory
    GROUP BY
        month_date,
        campaign_id,
        sku_id,
        warehouse_id
)
SELECT
    month_date,
    campaign_id,
    sku_id,
    warehouse_id,
    product_name,
    total_reserved_qty,
    total_cod_sold_qty,
    total_cancelled_qty,
    total_expired_qty,
    total_returned_qty,
    total_replenished_qty,
    total_net_movement_qty,
    total_event_count,
    first_opening_sellable_stock,
    latest_closing_sellable_stock
FROM monthly_agg;
