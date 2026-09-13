"""
Sales ETL DAG.

Runs the extract-clean-transform-load pipeline (etl/etl.py) that populates
the star schema in the sales_dw Postgres warehouse. Scheduled daily; can
also be triggered manually from the Airflow UI/CLI.
"""

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")  # project root is mounted here

default_args = {
    "owner": "data-eng",
    "retries": 1,
}


def run_etl(**context):
    from etl.etl import run
    stats = run()
    context["ti"].xcom_push(key="etl_summary", value=stats)


with DAG(
    dag_id="sales_etl_pipeline",
    description="Extract, clean, transform and load Superstore sales data into a star schema warehouse",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sales", "etl", "warehouse"],
) as dag:

    run_etl_task = PythonOperator(
        task_id="run_sales_etl",
        python_callable=run_etl,
    )
