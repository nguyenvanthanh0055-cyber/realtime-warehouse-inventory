import argparse
import time
from datetime import datetime, timezone
import logging
from pathlib import Path
from uuid import uuid4
import boto3


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
SQL_DIR = Path(__file__).resolve().parents[1] / "redshift"
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Load S3 Gold partition into Redshift")
    parser.add_argument("--summary-date", required=True)
    parser.add_argument("--recon-date", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument("--redshift-workgroup", required=True)
    parser.add_argument("--redshift-database", required=True)
    parser.add_argument("--redshift-iam-role", required=True)
    parser.add_argument("--aws-region", default="ap-southeast-1")
    return parser.parse_args()

def load_sql(filename: str, params: dict) -> str:
    sql_text = (SQL_DIR / filename).read_text(encoding="utf-8")
    return render_template(sql_text, params)

def render_template(sql_text: str, params: dict) -> str:
    rendered = sql_text

    for key, value in params.items():
        rendered = rendered.replace("{{ " + key + " }}", str(value))

    return rendered

def split_sql_statements(sql_text: str) -> list[str]:
    return [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]

def execute_statement(client, *, workgroup_name: str, database: str, sql: str):
    respone = client.execute_statement(
        WorkgroupName= workgroup_name,
        Database= database,
        Sql= sql
    )
    statement_id = respone["Id"]

    while True:
        desc = client.describe_statement(Id=statement_id)
        status = desc["Status"]

        if status in ["FINISHED", "FAILED", "ABORTED"]:
            break

        time.sleep(2)
    
    if status != "FINISHED":
        error = desc.get("Error", "Unknown Redshift Data API error")
        raise RuntimeError(f"SQL failed: {error}\nSQL:\n{sql}")


def run_sql_file(client, args, filename, params, single_statement=False):
    sql = load_sql(filename, params)

    statements = [sql] if single_statement else split_sql_statements(sql)

    for statement in statements:
        execute_statement(
            client,
            workgroup_name=args.redshift_workgroup,
            database= args.redshift_database,
            sql= statement
        )


def main():
    
    args = parse_args()

    daily_summary_s3_path = (
        f"s3://{args.s3_bucket}/gold/daily_inventory_summary/"
        f"summary_date={args.summary_date}/campaign_id={args.campaign_id}/"
    )

    reconciliation_s3_path = (
        f"s3://{args.s3_bucket}/gold/inventory_reconciliation/"
        f"recon_date={args.recon_date}/campaign_id={args.campaign_id}/"
    )

    load_id = f"redshift_gold_load_{args.summary_date}_{args.campaign_id}_{uuid4().hex[:8]}"
    load_started_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    params = {
        "summary_date": args.summary_date,
        "recon_date": args.recon_date,
        "campaign_id": args.campaign_id,
        "daily_summary_s3_path": daily_summary_s3_path,
        "reconciliation_s3_path": reconciliation_s3_path,
        "redshift_iam_role": args.redshift_iam_role,
        "load_id": load_id,
        "load_started_at": load_started_at
    }

    client = boto3.client("redshift-data", region_name=args.aws_region)
    logger.info("Starting")

    logger.info("Starting load data from gold to redshift")

    logger.info("COPY S3 Gold partitions daily_inventory_summary to Redshift mart")

    try:
        run_sql_file(client, args, "calls/01_call_sp_load_daily_inventory_summary.sql", params, single_statement=True)
    
    except Exception as e:
        failure_params = {
            **params,
            "failed_source_s3_path": daily_summary_s3_path,
            "target_staging_table": "staging.daily_inventory_summary_stg",
            "target_mart_table": "mart.fact_daily_inventory_summary",
            "error_message": str(e).replace("'", "''")[:1000]
        }
        run_sql_file(
            client,
            args,
            "audit/01_insert_load_failure.sql",
            failure_params,
            single_statement=True
        )
        raise
    
    logger.info("COPY S3 Gold partitions inventory_reconciliation to Redshift mart")

    try:
        run_sql_file(client, args, "calls/02_call_sp_load_inventory_reconciliation.sql", params, single_statement=True)

    except Exception as exc:
        failure_params = {
            **params,
            "failed_source_s3_path": reconciliation_s3_path,
            "target_staging_table": "staging.reconciliation_result_stg",
            "target_mart_table": "mart.fact_reconciliation_result",
            "error_message": str(exc).replace("'", "''")[:1000]
        }
        run_sql_file(
            client,
            args,
            "audit/01_insert_load_failure.sql",
            
            failure_params,
            single_statement=True
        )
        raise

if __name__ == "__main__":
    main()