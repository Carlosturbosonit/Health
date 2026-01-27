from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.python import PythonSensor

RAW_DIR = Path("/data/raw")
STAGING_DIR = Path("/data/staging")

FLUVIEW_URL = "https://api.delphi.cmu.edu/epidata/fluview/"
META_URL = "https://api.delphi.cmu.edu/epidata/fluview_meta/"

JAR_PATH = "/opt/airflow/include/jars/fluview-spark-job-all.jar"
SQL_PATH = "/opt/airflow/include/sql"

DEFAULT_REGIONS = "nat,hhs1,hhs2,hhs3,hhs4,hhs5,hhs6,hhs7,hhs8,hhs9,hhs10"
DEFAULT_EPIWEEKS = "202440-202510"


def _get_json(url: str, params: dict | None = None, timeout_s: int = 30) -> dict:
    with requests.Session() as s:
        for attempt in range(1, 6):
            try:
                r = s.get(url, params=params, timeout=timeout_s)
                r.raise_for_status()
                return r.json()
            except Exception:
                if attempt == 5:
                    raise
                time.sleep(2 ** attempt)


def fetch_latest_issue() -> int:
    meta = _get_json(META_URL)
    return int(meta["epidata"][0]["latest_issue"])


def write_json_atomic(payload: dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, out_path)


def extract_fluview(**context) -> str:
    regions = context["params"].get("regions", DEFAULT_REGIONS)
    epiweeks = context["params"].get("epiweeks", DEFAULT_EPIWEEKS)
    latest_issue = fetch_latest_issue()

    payload = _get_json(FLUVIEW_URL, params={"regions": regions, "epiweeks": epiweeks, "issues": str(latest_issue)})
    if payload.get("result") != 1:
        raise RuntimeError(f"API failed: result={payload.get('result')} msg={payload.get('message')}")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = context["run_id"].replace(":", "_")
    out = RAW_DIR / f"fluview_{run_id}_issue={latest_issue}_{ts}.json"
    write_json_atomic(payload, out)
    return str(out)


def file_exists(path: str) -> bool:
    p = Path(path)
    return p.exists() and p.is_file() and p.stat().st_size > 0


def validate_and_clean(raw_path: str, **context) -> str:
    raw_path = Path(raw_path)
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    rows: List[Dict[str, Any]] = payload.get("epidata", [])
    if payload.get("result") != 1 or not isinstance(rows, list) or len(rows) == 0:
        raise ValueError("Invalid payload or empty epidata")

    required = {"region", "epiweek", "issue"}
    cleaned: List[Dict[str, Any]] = []

    for r in rows:
        if not required.issubset(r.keys()):
            continue
        out = dict(r)
        out["epiweek"] = int(out["epiweek"])
        out["issue"] = int(out["issue"])
        for k in ["wili", "ili"]:
            if k in out and out[k] is not None:
                try:
                    out[k] = float(out[k])
                except Exception:
                    out[k] = None
        for k in ["num_ili", "num_patients"]:
            if k in out and out[k] is not None:
                try:
                    out[k] = int(out[k])
                except Exception:
                    out[k] = None
        cleaned.append(out)

    if not cleaned:
        raise ValueError("All rows filtered out")

    run_id = context["run_id"].replace(":", "_")
    out_path = STAGING_DIR / f"fluview_clean_{run_id}.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    tmp = out_path.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for r in cleaned:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, out_path)
    return str(out_path)


with DAG(
    dag_id="fluview_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    template_searchpath=[SQL_PATH],
    tags=["fluview", "etl"],
) as dag:

    t1_extract = PythonOperator(
        task_id="extract",
        python_callable=extract_fluview,
        params={"regions": DEFAULT_REGIONS, "epiweeks": DEFAULT_EPIWEEKS},
    )

    t2_sensor = PythonSensor(
        task_id="arrival_sensor",
        python_callable=file_exists,
        op_args=["{{ ti.xcom_pull(task_ids='extract') }}"],
        poke_interval=15,
        timeout=10 * 60,
        mode="poke",
    )

    t3_clean = PythonOperator(
        task_id="validate_clean",
        python_callable=validate_and_clean,
        op_args=["{{ ti.xcom_pull(task_ids='extract') }}"],
    )

    t4_spark = SparkSubmitOperator(
        task_id="business_logic_spark",
        conn_id="spark_default",
        application=JAR_PATH,
        java_class="com.tuorg.fluview.Main",
        verbose=True,
        application_args=[
            "--input", "{{ ti.xcom_pull(task_ids='validate_clean') }}",
            "--jdbcUrl", "jdbc:postgresql://postgres_dw:5432/dw",
            "--jdbcUser", "dw",
            "--jdbcPassword", "dw",
            "--runId", "{{ run_id }}",
        ],
    )

    t5_load = PostgresOperator(
        task_id="load_to_postgres_gold",
        postgres_conn_id="postgres_dw",
        sql="20_merge_to_gold.sql",
    )

    t1_extract >> t2_sensor >> t3_clean >> t4_spark >> t5_load
