from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

RAW_ROOT = "/opt/airflow/data/raw/fluview"
CLEAN_ROOT = "/opt/airflow/data/clean/fluview"
JAR_PATH = "/jars/spark-health_2.12-0.1.0.jar"  # ✅ FIXED

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fluview_weekly_etl",
    start_date=datetime(2025, 1, 1),
    schedule="0 18 * * FRI",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cdc", "fluview", "spark"],
) as dag:

    wait_for_raw = FileSensor(
        task_id="wait_for_raw",
        filepath=f"{RAW_ROOT}/ingest_date={{{{ ds }}}}/_SUCCESS",
        poke_interval=30,
        timeout=15 * 60,
        mode="poke",
        fs_conn_id="fs_default",
    )

    spark_clean = SparkSubmitOperator(
        task_id="spark_clean",
        conn_id="spark_standalone",
        application="/jars/spark-health_2.12-0.1.0.jar",
        java_class="com.sparkhealth.SparkHealthETL",  # ✅ CONFIRMADO
        name="fluview-clean",
        application_args=[
            "--mode", "clean",
            "--raw", f"{RAW_ROOT}/ingest_date={{{{ ds }}}}",
            "--clean", f"{CLEAN_ROOT}/ingest_date={{{{ ds }}}}",
    ],
    verbose=True,
)


    wait_for_raw >> spark_clean




