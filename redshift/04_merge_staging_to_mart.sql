SELECT
    summary_date,
    campaign_id,
    sku_id,
    warehouse_id,
    COUNT(*) AS duplicate_count
FROM staging.daily_inventory_summary_stg
WHERE summary_date = '<recon_date>'::DATE
  AND campaign_id = '<campaign_id>'
GROUP BY
    summary_date,
    campaign_id,
    sku_id,
    warehouse_id
HAVING COUNT(*) > 1;

SELECT
    recon_date,
    campaign_id,
    sku_id,
    warehouse_id,
    COUNT(*) AS duplicate_count
FROM staging.reconciliation_result_stg
WHERE recon_date = '<recon_date>'::DATE
  AND campaign_id = '<campaign_id>'
GROUP BY
    recon_date,
    campaign_id,
    sku_id,
    warehouse_id
HAVING COUNT(*) > 1;

BEGIN;

MERGE INTO mart.fact_daily_inventory_summary AS target
USING (
    SELECT *
    FROM staging.daily_inventory_summary_stg
    WHERE summary_date = '<recon_date>'::DATE
      AND campaign_id = '<campaign_id>'
) AS source
ON target.summary_date = source.summary_date
   AND target.campaign_id = source.campaign_id
   AND target.sku_id = source.sku_id
   AND target.warehouse_id = source.warehouse_id

WHEN MATCHED THEN UPDATE SET
    product_name = source.product_name,
    opening_sellable_stock = source.opening_sellable_stock,
    total_reserved_qty = source.total_reserved_qty,
    total_cod_sold_qty = source.total_cod_sold_qty,
    total_cancelled_qty = source.total_cancelled_qty,
    total_expired_qty = source.total_expired_qty,
    total_returned_qty = source.total_returned_qty,
    total_replenished_qty = source.total_replenished_qty,
    net_movement_qty = source.net_movement_qty,
    closing_sellable_stock = source.closing_sellable_stock,
    event_count = source.event_count,
    created_at = source.created_at,
    loaded_at = GETDATE()

WHEN NOT MATCHED THEN INSERT (
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
)
VALUES (
    source.summary_date,
    source.campaign_id,
    source.sku_id,
    source.warehouse_id,
    source.product_name,
    source.opening_sellable_stock,
    source.total_reserved_qty,
    source.total_cod_sold_qty,
    source.total_cancelled_qty,
    source.total_expired_qty,
    source.total_returned_qty,
    source.total_replenished_qty,
    source.net_movement_qty,
    source.closing_sellable_stock,
    source.event_count,
    source.created_at,
    GETDATE()
);


MERGE INTO mart.fact_reconciliation_result AS target
USING (
    SELECT *
    FROM staging.reconciliation_result_stg
    WHERE recon_date = '<recon_date>'::DATE
      AND campaign_id = '<campaign_id>'
) AS source
ON target.recon_date = source.recon_date
   AND target.campaign_id = source.campaign_id
   AND target.sku_id = source.sku_id
   AND target.warehouse_id = source.warehouse_id

WHEN MATCHED THEN UPDATE SET
    opening_sellable_stock = source.opening_sellable_stock,
    net_movement_qty = source.net_movement_qty,
    batch_recomputed_sellable_stock = source.batch_recomputed_sellable_stock,
    streaming_sellable_stock = source.streaming_sellable_stock,
    diff_qty = source.diff_qty,
    status = source.status,
    created_at = source.created_at,
    loaded_at = GETDATE()

WHEN NOT MATCHED THEN INSERT (
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
)
VALUES (
    source.recon_date,
    source.campaign_id,
    source.sku_id,
    source.warehouse_id,
    source.opening_sellable_stock,
    source.net_movement_qty,
    source.batch_recomputed_sellable_stock,
    source.streaming_sellable_stock,
    source.diff_qty,
    source.status,
    source.created_at,
    GETDATE()
);

COMMIT;
