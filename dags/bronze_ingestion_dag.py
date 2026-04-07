from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import json
import os
import time

sys.path.append('/usr/local/airflow')

from include.transformations.bronze_layer import run_bronze_pipeline

METRICS_PATH = "/usr/local/airflow/metrics/bronze_pipeline"


def run_bronze_with_metrics(**context):

    start_time = time.time()

    run_bronze_pipeline()

    end_time = time.time()

    metrics = {
        "pipeline": "bronze_layer",
        "status": "success",
        "execution_date": str(context["execution_date"]),
        "processing_time_seconds": round(end_time - start_time, 2)
    }

    os.makedirs(METRICS_PATH, exist_ok=True)

    file_name = f"metrics_{context['ds']}.json"

    with open(f"{METRICS_PATH}/{file_name}", "w") as f:
        json.dump(metrics, f, indent=4)


default_args = {
    'owner': 'youssef',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='telecom_bronze_ingestion',
    default_args=default_args,
    description='Ingest raw parquet files from Landing to Bronze with Watermarking',
    schedule_interval='@daily',
    catchup=False,
    tags=['telecom', 'bronze', 'ingestion']
) as dag:

    start_ingestion = EmptyOperator(
        task_id='start_bronze_layer'
    )

    ingest_task = PythonOperator(
        task_id='ingest_landing_to_bronze',
        python_callable=run_bronze_with_metrics,
        execution_timeout=timedelta(hours=1),
        provide_context=True
    )

    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_transformations',
        trigger_dag_id='telecom_silver_transformations',
        wait_for_completion=False,
        reset_dag_run=True
    )

    end_ingestion = EmptyOperator(
        task_id='end_bronze_layer'
    )

    start_ingestion >> ingest_task >> trigger_silver >> end_ingestion