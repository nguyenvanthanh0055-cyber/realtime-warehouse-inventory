## Local Architecture

```text
Mock Order Service / Python Producer
        |
        v
Kafka Docker Topic: inventory-events
        |
        v
Spark Structured Streaming
        |
        |-- Parse JSON events
        |-- Validate business rules
        |-- Derive movement_qty
        |-- Detect invalid events
        |
        v
PostgreSQL Local
        |
        |-- current_inventory
        |-- processed_events
        |-- inventory_alerts
        |-- promotion_metrics
        |-- sales_velocity_window
        |
        v
Streamlit Dashboard / SQL Debug Queries



Và AWS:

```markdown
## Target AWS Architecture

```text
Producer / Mock Order Service
        |
        v
Amazon MSK
        |
        v
AWS Glue Streaming / EMR Serverless Spark
        |
        |-- Bronze/Silver writes
        |-- Current state update
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
Glue Batch / EMR Serverless
        |
        v
Redshift Serverless
        |
        v
BI Dashboard / Reporting Views

## Implementation Phases

### Phase 0 — Project Setup
Initialize project structure, Python environment, Docker Compose, and development conventions.

### Phase 1 — Local Infrastructure
Set up Kafka and PostgreSQL using Docker Compose.

### Phase 2 — Campaign Initialization
Seed campaign-level sellable inventory and promotion quota using input CSV files.

### Phase 3 — Inventory Event Generation
Generate realistic inventory lifecycle events such as stock reservation, payment confirmation, cancellation, expiration, return, and replenishment.

### Phase 4 — Kafka Producer
Publish inventory events to Kafka with campaign/SKU/warehouse-based message keys.

### Phase 5 — Spark Structured Streaming Parser
Consume Kafka events, parse JSON payloads, validate event schema, derive movement quantity, and enrich events with partition columns.

### Phase 6 — Real-time Current State Update
Update PostgreSQL current inventory state using Spark `foreachBatch`, with idempotency handled by `processed_events`.

### Phase 7 — Streaming Data Lake Writes
Write Bronze and Silver streaming outputs to local S3-like data lake folders.

### Phase 8 — Daily Batch Reconciliation
Recompute daily inventory state from raw events and compare it with streaming current state.

### Phase 9 — Analytics Mart and Dashboard
Build reporting tables/views and dashboard for inventory, promotion, alerts, velocity, and reconciliation.

### Phase 10 — AWS Deployment Mapping
Map local services to AWS managed services such as MSK, Glue/EMR Serverless, DynamoDB, S3, Athena, MWAA, Redshift Serverless, and CloudWatch.