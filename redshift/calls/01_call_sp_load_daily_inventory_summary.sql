CALL staging.sp_load_daily_inventory_summary(
    DATE '{{ summary_date }}',
    VARCHAR '{{ campaign_id }}',
    VARCHAR '{{ daily_summary_s3_path }}',
    VARCHAR '{{ redshift_iam_role }}',
    VARCHAR '{{ load_id }}',
    TIMESTAMP '{{ load_started_at }}'
);
