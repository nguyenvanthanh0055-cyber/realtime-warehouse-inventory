from datetime import datetime, timedelta
import logging
import time

import boto3
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import os

logger = logging.getLogger(__name__)

TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
DAG_ID = "inventory_gold_to_mart"

AWS_CONN_ID = "aws_default"
ACTIVE_GLUE_RUN_STATES = {"STARTING", "RUNNING", "STOPPING", "WAITING"}


def load_runtime_config(name: str, default: str) -> str:
    env_name = name.upper()
    return os.getenv(env_name) or os.getenv(name) or Variable.get(name, default_var=default)


DEFAULT_CAMPAIGN_ID = load_runtime_config(
    "inventory_default_campaign_id",
    "CAMPAIGN_FLASH_0609",
)
LAKE_ROOT = load_runtime_config("inventory_lake_root", "s3://inventory-lake-fox")
sql_bucket = load_runtime_config("inventory_sql_bucket", "inventory-lake-fox")
sql_prefix = load_runtime_config("inventory_sql_prefix", "artifacts/redshift/sql")
redshift_wg = load_runtime_config("inventory_redshift_workgroup", "inventory-dev-wg")
redshift_db = load_runtime_config("inventory_redshift_database", "dev")
redshift_iam_role = load_runtime_config(
    "inventory_redshift_iam_role",
    "arn:aws:iam::946445279560:role/service-role/AmazonRedshift-CommandsAccessRole-20260516T171518",
)
redshift_secret_arn = load_runtime_config(
    "inventory_redshift_secret_arn",
    "arn:aws:secretsmanager:us-east-1:946445279560:secret:redshift_secret_admin-llXzBH",
)
aws_region = load_runtime_config("inventory_aws_region", "us-east-1")


def validate_params(**context) -> None:
    dag_run = context.get("dag_run")
    conf =  dag_run.conf if dag_run and dag_run.conf else {}
    data_interval_start = context["data_interval_start"].in_timezone(TZ)
    default_business_date = data_interval_start.to_date_string()

    recon_date = conf.get("recon_date") or default_business_date
    summary_date = conf.get("summary_date") or default_business_date
    campaign_id = conf.get("campaign_id") or DEFAULT_CAMPAIGN_ID

    if not recon_date:
        raise AirflowException("Missing recon date")

    if not campaign_id:
        raise AirflowException("Missing campaign id")
    
    if not summary_date:
        raise AirflowException("Missing summary date")
    
    try:
        datetime.strptime(recon_date, "%Y-%m-%d")
        datetime.strptime(summary_date, "%Y-%m-%d")
    except ValueError as e:
        raise AirflowException(
            f"Invalid recon_date format. Expected yyyy-mm-dd."
        ) from e

    context["ti"].xcom_push(key="recon_date", value=recon_date)
    context["ti"].xcom_push(key="summary_date", value=summary_date)
    context["ti"].xcom_push(key="campaign_id", value=campaign_id)
    logger.info(
        "Resolved DAG params run_id=%s conf=%s recon_date=%s summary_date=%s "
        "campaign_id=%s default_business_date=%s",
        dag_run.run_id if dag_run else None,
        conf,
        recon_date,
        summary_date,
        campaign_id,
        default_business_date,
    )


def wait_for_glue_job_idle(job_name: str, region_name: str, timeout_minutes: int = 30) -> None:
    glue_client = boto3.client("glue", region_name=region_name)
    deadline = time.monotonic() + timeout_minutes * 60

    while True:
        response = glue_client.get_job_runs(JobName=job_name, MaxResults=10)
        active_runs = [
            run
            for run in response.get("JobRuns", [])
            if run.get("JobRunState") in ACTIVE_GLUE_RUN_STATES
        ]

        if not active_runs:
            return

        run_ids = ", ".join(
            f"{run.get('Id')}:{run.get('JobRunState')}" for run in active_runs
        )

        if time.monotonic() >= deadline:
            raise AirflowException(
                f"Glue job {job_name} is still active after {timeout_minutes} minutes: "
                f"{run_ids}. Stop the old run or wait for it to finish."
            )

        logger.info(
            "Waiting for Glue job %s to become idle. Active runs: %s",
            job_name,
            run_ids,
        )
        time.sleep(60)


default_args = {
    "owner": "thanh",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "email_on_failure": False
}



with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 5, 24, tz=TZ),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    max_active_tasks=1,
):
    validate_runtime_params = PythonOperator(
        task_id = "validate_runtime_params",
        python_callable=validate_params
    )

    wait_for_gold_batch_job_idle = PythonOperator(
        task_id="wait_for_gold_batch_job_idle",
        python_callable=wait_for_glue_job_idle,
        op_kwargs={
            "job_name": "inventory-gold-batch-job",
            "region_name": aws_region,
        },
    )

    gold_to_mart = GlueJobOperator(
        task_id = "inventory-gold-batch-job",
        job_name= "inventory-gold-batch-job",
        script_args={
            "--campaign_id": "{{ti.xcom_pull(task_ids='validate_runtime_params', key='campaign_id')}}",
            "--lake_root": LAKE_ROOT,
            "--recon_date": "{{ti.xcom_pull(task_ids='validate_runtime_params', key='recon_date')}}",
        },
        wait_for_completion= True,
        aws_conn_id=AWS_CONN_ID,
        region_name="us-east-1"
    )

    wait_for_redshift_load_job_idle = PythonOperator(
        task_id="wait_for_redshift_load_job_idle",
        python_callable=wait_for_glue_job_idle,
        op_kwargs={
            "job_name": "redshift_load_gold",
            "region_name": aws_region,
        },
    )

    redshift_load_gold = GlueJobOperator(
        task_id = "redshift_load_gold",
        job_name = "redshift_load_gold",
        script_args = {
            "--summary_date": "{{ti.xcom_pull(task_ids='validate_runtime_params', key='summary_date')}}",
            "--recon_date": "{{ti.xcom_pull(task_ids='validate_runtime_params', key='recon_date')}}",
            "--campaign_id": "{{ti.xcom_pull(task_ids='validate_runtime_params', key='campaign_id')}}",
            "--lake_root": LAKE_ROOT,
            "--sql_bucket": sql_bucket,
            "--sql_prefix": sql_prefix,
            "--redshift_workgroup": redshift_wg,
            "--redshift_database": redshift_db,
            "--redshift_iam_role": redshift_iam_role,
            "--redshift_secret_arn":redshift_secret_arn,
            "--aws_region": aws_region
            
        },
        wait_for_completion= True,
        aws_conn_id=AWS_CONN_ID,
        verbose=True,
        region_name = aws_region
    )


    (
        validate_runtime_params
        >> wait_for_gold_batch_job_idle
        >> gold_to_mart
        >> wait_for_redshift_load_job_idle
        >> redshift_load_gold
    )
