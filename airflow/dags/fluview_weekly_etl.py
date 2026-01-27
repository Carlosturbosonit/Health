from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

RAW_ROOT = "/opt/spark-data/raw/fluview"
CLEAN_ROOT = "/opt/spark-data/clean/fluview"
JAR_PATH = "/jars/fluview-etl.jar"

with DAG(
    dag_id="fluview_weekly_etl",
    start_date=datetime(2025, 1, 1),
    schedule="0 18 * * FRI",
    catchup=False,
    tags=["cdc", "fluview", "spark"],
) as dag:

    wait_raw = FileSensor(
        task_id="wait_for_raw",
        filepath=f"{RAW_ROOT}/ingest_date={{{{ ds }}}}/_SUCCESS",
        poke_interval=30,
        timeout=900,
    )

    clean = SparkSubmitOperator(
        task_id="spark_clean",
        application=JAR_PATH,
        conn_id="spark_standalone",
        application_args=[
            "--mode=clean",
            f"--raw={RAW_ROOT}/ingest_date={{{{ ds }}}}",
            f"--clean={CLEAN_ROOT}/ingest_date={{{{ ds }}}}",
        ],
    )

    wait_raw >> clean

