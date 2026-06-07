# Real-time Warehouse Inventory Monitoring

Cloud-first real-time inventory monitoring project for a flash-sale warehouse
scenario. The main architecture is AWS-based; the local stack is kept as a
dev/test harness so the streaming logic can be validated cheaply.

## Cloud Main Architecture

```text
Producer / Mock Order Service
        |
        v
Amazon MSK Provisioned with IAM auth
        |
        v
AWS Glue Spark Structured Streaming
        |
        |-- Bronze/Silver writes
        |-- Current-state updates
        |-- Alert detection
        |
        +-----------------------> DynamoDB
        |                         current inventory state
        |
        +-----------------------> Amazon S3 Data Lake
                                  Bronze / Silver / Gold
                                      |
                                      v
                              Glue Data Catalog + Athena
                                      |
                                      v
MWAA Daily Batch Orchestration
        |
        v
Glue Batch / Redshift Serverless
        |
        v
BI Dashboard / Reporting Views
```

Cloud details:

- Main streaming job: `spark/streaming_inventory_job.py`
- Cloud current-state sink: `aws/glue/dynamodb_current_state_sink.py`
- Glue runbook: `aws/glue/msk_streaming_glue_runbook.md`
- Current inventory state history path:
  `s3://inventory-lake-fox/silver/current_inventory_state_history/`

## Local Dev/Test Harness

Local is intentionally separate from the cloud story. It is used for fast
development and smoke testing.

```text
Python Producer
        |
        v
Local Kafka / Docker Kafka
        |
        v
Spark Structured Streaming
        |
        +-- PostgreSQL local current-state tables
        +-- data/lake local Bronze/Silver folders
```

Local-only pieces:

- `docker-compose.yml`
- `scripts/init_campaign.py`
- `spark/sinks/postgres_current_state_sink.py`
- `batch/daily_reconciliation_job.py`

See:

- `docs/cloud_main_path.md`
- `docs/local_dev_test_path.md`

