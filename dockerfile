FROM apache/airflow:2.10.5

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    "apache-airflow-providers-amazon" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"