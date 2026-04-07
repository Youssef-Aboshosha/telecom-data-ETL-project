from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yaml
import os
import json
import time

SCHEMA_PATH = "/usr/local/airflow/include/configs/schemas.yaml"
METRICS_PATH = "/usr/local/airflow/metrics/silver_pipeline"


def get_tables():
    """Load tables dynamically from schema config"""
    with open(SCHEMA_PATH, "r") as f:
        config = yaml.safe_load(f)

    return list(config["tables"].keys())


def log_silver_metrics(**context):
    """Save Silver pipeline metrics"""
    start_time = context["ti"].xcom_pull(task_ids="start_silver_layer", key="start_time")
    end_time = time.time()

    metrics = {
        "pipeline": "silver_layer",
        "status": "success",
        "execution_date": str(context["execution_date"]),
        "processing_time_seconds": round(end_time - start_time, 2)
    }

    os.makedirs(METRICS_PATH, exist_ok=True)

    file_name = f"metrics_{context['ds']}.json"

    with open(f"{METRICS_PATH}/{file_name}", "w") as f:
        json.dump(metrics, f, indent=4)


def record_start_time(**context):
    """Record pipeline start time"""
    context["ti"].xcom_push(key="start_time", value=time.time())


default_args = {
    "owner": "youssef",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="telecom_silver_transformations",
    default_args=default_args,
    description="Transform Bronze data into Silver Delta tables",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    tags=["telecom", "silver", "medallion"],
) as dag:

    # start node
    start = PythonOperator(
        task_id="start_silver_layer",
        python_callable=record_start_time,
        provide_context=True
    )

    # end node
    end = EmptyOperator(task_id="end_silver_layer")

    # metrics node
    log_metrics = PythonOperator(
        task_id="log_silver_metrics",
        python_callable=log_silver_metrics,
        provide_context=True
    )

    tables = get_tables()

    table_tasks = []

    for table in tables:

        transform = BashOperator(
            task_id=f"transform_{table}",
            bash_command=f"""
            python3 /usr/local/airflow/include/transformations/silver_layer.py --table {table}
            """,
        )

        start >> transform
        table_tasks.append(transform)

    # wait for all tables then log metrics
    for t in table_tasks:
        t >> log_metrics

    log_metrics >> end