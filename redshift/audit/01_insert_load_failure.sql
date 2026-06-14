INSERT INTO audit.redshift_load_log (
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
VALUES (
    '{{ load_id }}' || '_' || '{{ dataset_name }}',
    'redshift_gold_load',
    '{{ failed_source_s3_path }}',
    '{{ target_staging_table }}',
    '{{ target_mart_table }}',
    DATE '{{ summary_date }}',
    '{{ campaign_id }}',
    TIMESTAMP '{{ load_started_at }}',
    GETDATE(),
    0,
    'FAILED',
    '{{ error_message }}',
    GETDATE()
);