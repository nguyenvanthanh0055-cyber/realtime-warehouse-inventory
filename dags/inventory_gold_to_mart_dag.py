from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import os

TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
DAG_ID = "inventory_gold_to_mart"

AWS_CONN_ID = "aws_default"

GLUE_GOLD_BATCH_JOB_NAME = "inventory-gold-batch-job"

DEFAULT_CAMPAIGN_ID = "CAMPAIGN_FLASH_0527"

LAKE_ROOT = "s3://inventory-lake-fox"

# Nếu Airflow chạy trong Docker, nên mount repo vào path này
PROJECT_ROOT = os.environ.get(
    "PROJECT_ROOT",
    "/opt/airflow/project",
)


def validate_params(**context) -> None:
    dag_run = context.get("dag_run")
    conf =  dag_run.conf if dag_run and dag_run.conf else {}

    recon_date = conf.get("recon_date") or context["ds"]
    campaign_id = conf.get("campaign_id") or DEFAULT_CAMPAIGN_ID

    if not recon_date:
        raise AirflowException("Missing recon date")

    if not campaign_id:
        raise AirflowException("Missing campaign id")
    
    try:
        datetime.strftime(recon_date, "%Y-%m-%d")
    except ValueError as e:
        raise AirflowException(
            f"Invalid recon_date format: {recon_date}. Expected yyyy-mm-dd."
        ) from e

    context["ti"].xcom_push(key="recon_date", value=recon_date)
    context["ti"].xcom_push(key="campaign_id", value=campaign_id)

default_args = {
    "owner": "thanh",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "email_on_failure": False
}



with DAG(
    dag_id="inventory-gold-batch-job",
    start_date=datetime(2026, 5, 24, tzinfo=TZ),
    schedule="@daily",
    catchup=False,
    default_args=default_args
):
    validate_runtime_params = PythonOperator(
        task_id = "validate_runtime_params",
        python_callable=validate_params
    )

    gold_to_mart = GlueJobOperator(
        task_id = "inventory-gold-batch-job",
        job_name= "inventory-gold-batch-job",
        script_args={
            "--campaign_id": "{{ti.xcom_pull(task_ids='validate_runtime_params', key='campaign_id')}}",
            "--lake_root": LAKE_ROOT,
            "--recon_date": "{{ti.xcom_pull(task_ids='validate_runtime_params', key='recon_date')}}"
        },
        wait_for_completion= True,
        aws_conn_id=AWS_CONN_ID,
        region_name="us-east-1"
    )


    validate_params >> gold_to_mart