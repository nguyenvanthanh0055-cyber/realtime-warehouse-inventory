BEGIN;

DELETE FROM staging.daily_inventory_summary_stg
WHERE summary_date = '<recon_date>'::DATE
  AND campaign_id = '<campaign_id>';

COPY staging.daily_inventory_summary_stg
FROM 's3://inventory-lake/gold/daily_inventory_summary/recon_date=<recon_date>/campaign_id=<campaign_id>/'
IAM_ROLE 'arn:aws:iam::<account-id>:role/<redshift-copy-role>'
FORMAT AS PARQUET;

DELETE FROM staging.reconciliation_result_stg
WHERE recon_date = '<recon_date>'::DATE
  AND campaign_id = '<campaign_id>';

COPY staging.reconciliation_result_stg
FROM 's3://inventory-lake/gold/inventory_reconciliation/recon_date=<recon_date>/campaign_id=<campaign_id>/'
IAM_ROLE 'arn:aws:iam::<account-id>:role/<redshift-copy-role>'
FORMAT AS PARQUET;

COMMIT;
