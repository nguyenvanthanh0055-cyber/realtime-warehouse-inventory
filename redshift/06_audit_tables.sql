CREATE TABLE IF NOT EXISTS audit.redshift_load_log (
    load_id VARCHAR(100),
    pipeline_name VARCHAR(200),
    source_s3_path VARCHAR(1000),
    target_staging_table VARCHAR(200),
    target_mart_table VARCHAR(200),

    business_date DATE,
    campaign_id VARCHAR(100),

    load_started_at TIMESTAMP,
    load_finished_at TIMESTAMP,
    rows_loaded BIGINT,
    status VARCHAR(50),
    error_message VARCHAR(1000),

    created_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY (business_date, campaign_id, status);