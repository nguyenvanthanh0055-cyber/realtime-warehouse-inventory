CREATE OR REPLACE PROCEDURE  staging.sp_load_daily_inventory_summary(
    p_summary_date DATE,
    p_campaign_id VARCHAR,
    p_daily_summary_s3_path VARCHAR,
    p_redshift_iam_role VARCHAR,
    p_load_id VARCHAR,
    p_load_started_at TIMESTAMP
)
LANGUAGE plpgsql
AS $$
DECLARE v_daily_duplicate_count INTEGER := 0;
BEGIN

DROP TABLE IF EXISTS daily_inventory_summary_raw_tmp;

CREATE TEMP TABLE daily_inventory_summary_raw_tmp(
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
    net_movement_qty INTEGER,
    closing_sellable_stock INTEGER,
    event_count BIGINT,
    created_at TIMESTAMP
);

EXECUTE
    'COPY daily_inventory_summary_raw_tmp
    FROM ' || quote_literal(p_daily_summary_s3_path) || '
    IAM_ROLE ' || quote_literal(p_redshift_iam_role) || '
    FORMAT AS PARQUET';

DELETE FROM staging.daily_inventory_summary_stg
WHERE summary_date = p_summary_date
AND campaign_id = p_campaign_id;



INSERT INTO staging.daily_inventory_summary_stg (
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
    created_at
)
SELECT
    p_summary_date AS summary_date,
    p_campaign_id AS campaign_id,
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
    created_at
FROM daily_inventory_summary_raw_tmp;

SELECT COUNT(*)
INTO v_daily_duplicate_count
FROM (
    SELECT
        summary_date,
        campaign_id,
        sku_id,
        warehouse_id,
        COUNT(*) AS duplicate_count
    FROM staging.daily_inventory_summary_stg
    WHERE summary_date = p_summary_date
    AND campaign_id = p_campaign_id
    GROUP BY
        summary_date,
        campaign_id,
        sku_id,
        warehouse_id
    HAVING COUNT(*) > 1
) dup_summary;

IF v_daily_duplicate_count > 0 THEN
    RAISE EXCEPTION
        'Duplicate keys in daily summary staging. summary_date=%, campaign_id=%, duplicate_key_count=%',
        p_summary_date,
        p_campaign_id,
        v_daily_duplicate_count;
END IF;


MERGE INTO mart.fact_daily_inventory_summary
USING (
    SELECT *
    FROM staging.daily_inventory_summary_stg
    WHERE summary_date = p_summary_date
    AND campaign_id = p_campaign_id
) AS source
ON mart.fact_daily_inventory_summary.summary_date = source.summary_date
   AND mart.fact_daily_inventory_summary.campaign_id = source.campaign_id
   AND mart.fact_daily_inventory_summary.sku_id = source.sku_id
   AND mart.fact_daily_inventory_summary.warehouse_id = source.warehouse_id

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


INSERT INTO audit.redshift_load_log(
    load_id,
    pipeline_name,
    source_s3_path,
    target_staging_table,
    target_mart_table,
    business_date,
    campaign_id,
    load_started_at,
    load_finished_at,
    rows_loaded,
    status,
    error_message,
    created_at
)
SELECT
    p_load_id || '_daily_inventory_summary' AS load_id,
    'redshift_gold_load' AS pipeline_name,
    p_daily_summary_s3_path AS source_s3_path,
    'staging.daily_inventory_summary_stg' AS target_staging_table,
    'mart.fact_daily_inventory_summary' AS target_mart_table,
    p_summary_date AS business_date,
    p_campaign_id AS campaign_id,
    p_load_started_at  AS load_started_at,
    GETDATE() AS load_finished_at,
    count(*) AS rows_loaded,
    'SUCCESS' AS status,
    NULL AS error_message,
    GETDATE() AS created_at
FROM staging.daily_inventory_summary_stg
WHERE summary_date = p_summary_date
AND campaign_id = p_campaign_id;




END;
$$;