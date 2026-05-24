CALL staging.sp_load_inventory_reconciliation(
    DATE '{{ recon_date }}',
    VARCHAR '{{ campaign_id }}',
    VARCHAR '{{ reconciliation_s3_path }}',
    VARCHAR '{{ redshift_iam_role }}',
    VARCHAR '{{ load_id }}',
    TIMESTAMP '{{ load_started_at }}'
);
