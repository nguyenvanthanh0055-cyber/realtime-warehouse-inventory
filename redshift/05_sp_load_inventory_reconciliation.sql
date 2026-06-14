CREATE OR REPLACE PROCEDURE staging.sp_load_inventory_reconciliation(
    p_recon_date DATE,
    p_campaign_id VARCHAR,
    p_reconciliation_s3_path VARCHAR,
    p_redshift_iam_role VARCHAR,
    p_load_id VARCHAR,
    p_load_started_at TIMESTAMP
)
LANGUAGE plpgsql
AS $$
DECLARE v_reconciliation_duplicate_count INTEGER :=0;
BEGIN


DROP TABLE IF EXISTS reconciliation_result_raw_tmp;

CREATE TEMP TABLE reconciliation_result_raw_tmp(
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


EXECUTE
    'COPY reconciliation_result_raw_tmp
    FROM ' || quote_literal(p_reconciliation_s3_path) || '
    IAM_ROLE ' || quote_literal(p_redshift_iam_role) || '
    FORMAT AS PARQUET';


DELETE FROM staging.reconciliation_result_stg
WHERE recon_date = p_recon_date
AND campaign_id = p_campaign_id;


INSERT INTO staging.reconciliation_result_stg (
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
    created_at
)
SELECT
    p_recon_date AS recon_date,
    p_campaign_id AS campaign_id,
    sku_id,
    warehouse_id,
    opening_sellable_stock,
    net_movement_qty,
    batch_recomputed_sellable_stock,
    streaming_sellable_stock,
    diff_qty,
    status,
    created_at
FROM reconciliation_result_raw_tmp;

SELECT COUNT(*)
INTO v_reconciliation_duplicate_count
FROM (
    SELECT
        recon_date,
        campaign_id,
        sku_id,
        warehouse_id
    FROM staging.reconciliation_result_stg
    WHERE recon_date = p_recon_date
    AND campaign_id = p_campaign_id
    GROUP BY 1, 2, 3, 4
    HAVING COUNT(*) > 1
) dup_reconciliation;

IF v_reconciliation_duplicate_count > 0 THEN
    RAISE EXCEPTION
        'Duplicate keys in reconciliation staging. recon_date=%, campaign_id=%, duplicate_key_count=%',
        p_recon_date,
        p_campaign_id,
        v_reconciliation_duplicate_count;
END IF;

MERGE INTO mart.fact_reconciliation_result
USING (
    SELECT *
    FROM staging.reconciliation_result_stg
    WHERE recon_date = p_recon_date
      AND campaign_id = p_campaign_id
) AS source
ON mart.fact_reconciliation_result.recon_date = source.recon_date
   AND mart.fact_reconciliation_result.campaign_id = source.campaign_id
   AND mart.fact_reconciliation_result.sku_id = source.sku_id
   AND mart.fact_reconciliation_result.warehouse_id = source.warehouse_id

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
    p_load_id || '_inventory_reconciliation' AS load_id,
    'redshift_gold_load' AS pipeline_name,
    p_reconciliation_s3_path AS source_s3_path,
    'staging.reconciliation_result_stg' AS target_staging_table,
    'mart.fact_reconciliation_result' AS target_mart_table,
    p_recon_date AS business_date,
    p_campaign_id AS campaign_id,
    p_load_started_at AS load_started_at,
    GETDATE() AS load_finished_at,
    count(*) AS rows_loaded,
    'SUCCESS' AS status,
    NULL AS error_message,
    GETDATE() AS created_at
FROM staging.reconciliation_result_stg
WHERE recon_date = p_recon_date
AND campaign_id = p_campaign_id;

END;
$$;





