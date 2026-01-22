from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from lib.fluview_ingest import IngestConfig, ingest_fluview

RAW_ROOT = "/data/raw/fluview"
CLEAN_ROOT = "/data/clean/fluview"
JAR_PATH = "/jars/fluview-etl.jar"

REGIONS = "nat," + ",".join([f"hhs{i}" for i in range(1, 11)])
WEEKS_BACK = 52


def extract_to_raw(ds: str, **_):
    cfg = IngestConfig(
        regions=REGIONS,
        weeks_back=WEEKS_BACK,
        ingest_date=ds,
        raw_root=Path(RAW_ROOT),
    )
    return ingest_fluview(cfg)


with DAG(
    dag_id="fluview_weekly_etl",
    start_date=datetime(2025, 1, 1),
    schedule="0 18 * * FRI",   # Friday 18:00 (Madrid) to be safely after CDC publish window
    catchup=False,
    tags=["cdc", "fluview", "weekly"],
    default_args={"retries": 2},
) as dag:

    t_extract = PythonOperator(
        task_id="extract_api_to_raw_json",
        python_callable=extract_to_raw,
    )

    t_wait = FileSensor(
        task_id="wait_for_raw_success",
        filepath=f"{RAW_ROOT}/ingest_date={{{{ ds }}}}/_SUCCESS",
        poke_interval=30,
        timeout=60 * 15,
        mode="poke",
    )

    t_clean = SparkSubmitOperator(
        task_id="spark_clean",
        conn_id="spark_default",
        application=JAR_PATH,
        name="fluview-clean",
        deploy_mode="client",
        application_args=[
            "--mode=clean",
            f"--raw={RAW_ROOT}/ingest_date={{{{ ds }}}}",
            f"--clean={CLEAN_ROOT}/ingest_date={{{{ ds }}}}",
            "--ingest_date={{ ds }}",
        ],
        conf={
            "spark.master": os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
        },
        verbose=True,
    )

    t_join_load = SparkSubmitOperator(
        task_id="spark_join_and_load_postgres",
        conn_id="spark_default",
        application=JAR_PATH,
        name="fluview-join-load",
        deploy_mode="client",
        application_args=[
            "--mode=join_load",
            f"--clean={CLEAN_ROOT}/ingest_date={{{{ ds }}}}",
            "--ingest_date={{ ds }}",
            "--pg_url=jdbc:postgresql://dw-postgres:5432/dw",
            f"--pg_user={os.environ.get('DW_USER', 'dw')}",
            f"--pg_password={os.environ.get('DW_PASSWORD', 'dw')}",
            "--pg_table=health.fluview_weekly",
        ],
        conf={
            "spark.master": os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
        },
        verbose=True,
    )

    t_extract >> t_wait >> t_clean >> t_join_load

