import time
import sys
from datetime import datetime, timezone
import logging
from uuid import uuid4
import boto3
from awsglue.utils import getResolvedOptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args():
    return getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "summary_date",
            "recon_date",
            "campaign_id",
            "lake_root",
            "sql_bucket",
            "sql_prefix",
            "redshift_workgroup",
            "redshift_database",
            "redshift_iam_role",
            "aws_region"
        ]
    )


def load_sql_from_s3(
    s3_client,
    *,
    bucket: str,
    prefix: str,
    filename: str,
    params: dict,
) -> str:
    key = f"{prefix.rstrip('/')}/{filename.lstrip('/')}"

    obj = s3_client.get_object(
        Bucket=bucket,
        Key=key,
    )

    sql_text = obj["Body"].read().decode("utf-8")
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


def run_sql_file(redshift_client ,s3_client, args, file_name, params, single_statement=False):
    sql = load_sql_from_s3(
        s3_client=s3_client,
        bucket=args["sql_bucket"],
        prefix=args["sql_prefix"], 
        filename=file_name,
        params=params
        )

    statements = [sql] if single_statement else split_sql_statements(sql)

    for statement in statements:
        execute_statement(
            redshift_client,
            workgroup_name=args["redshift_workgroup"],
            database=args["redshift_database"],
            sql= statement
        )


def main():
    
    args = parse_args()

    daily_summary_s3_path = (
        f"{args['lake_root']}/gold/daily_inventory_summary/"
        f"summary_date={args['summary_date']}/campaign_id={args['campaign_id']}/"
    )

    reconciliation_s3_path = (
        f"{args['lake_root']}/gold/inventory_reconciliation/"
        f"recon_date={args['recon_date']}/campaign_id={args['campaign_id']}/"
    )
    load_id = f"redshift_gold_load_{args['summary_date']}_{args['campaign_id']}_{uuid4().hex[:8]}"
    load_started_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    params = {
        "summary_date": args["summary_date"],
        "recon_date": args["recon_date"],
        "campaign_id": args["campaign_id"],
        "lake_root": args["lake_root"],
        "daily_summary_s3_path": daily_summary_s3_path,
        "reconciliation_s3_path": reconciliation_s3_path,
        "redshift_iam_role": args["redshift_iam_role"],
        "load_id": load_id,
        "load_started_at": load_started_at,
    }

    redshift_client = boto3.client("redshift-data", region_name=args["aws_region"])
    s3_client = boto3.client("s3", region_name=args["aws_region"])

    logger.info("Starting load data from gold to redshift")

    logger.info("COPY S3 Gold partitions daily_inventory_summary to Redshift mart")

    try:
        run_sql_file(
            redshift_client=redshift_client,
            s3_client=s3_client,
            args=args,
            file_name="calls/01_call_sp_load_daily_inventory_summary.sql",
            params=params,
            single_statement=True)
    
    except Exception as e:
        failure_params = {
            **params,
            "failed_source_s3_path": daily_summary_s3_path,
            "target_staging_table": "staging.daily_inventory_summary_stg",
            "target_mart_table": "mart.fact_daily_inventory_summary",
            "error_message": str(e).replace("'", "''")[:1000]
        }
        run_sql_file(
            redshift_client,
            s3_client,
            args,
            "audit/01_insert_load_failure.sql",
            failure_params,
            single_statement=True
        )
        raise
    
    logger.info("COPY S3 Gold partitions inventory_reconciliation to Redshift mart")

    try:
        run_sql_file(
            redshift_client,
            s3_client,
            args,
            "calls/02_call_sp_load_inventory_reconciliation.sql",
            params,
            single_statement=True)

    except Exception as exc:
        failure_params = {
            **params,
            "failed_source_s3_path": reconciliation_s3_path,
            "target_staging_table": "staging.reconciliation_result_stg",
            "target_mart_table": "mart.fact_reconciliation_result",
            "error_message": str(exc).replace("'", "''")[:1000]
        }
        run_sql_file(
            redshift_client,
            s3_client,
            args,
            "audit/01_insert_load_failure.sql",
            
            failure_params,
            single_statement=True
        )
        raise

if __name__ == "__main__":
    main()
