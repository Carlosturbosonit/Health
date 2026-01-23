from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

# =============================================================================
# CONSTANTS
# =============================================================================

DELPHI_BASE = "https://api.delphi.cmu.edu/epidata"
RAW_BASE_PATH = Path("airflow\data\raw\fluview")

# =============================================================================
# DATA MODEL
# =============================================================================

@dataclass(frozen=True)
class IngestConfig:
    regions: str
    weeks_back: int
    ingest_date: str   # YYYY-MM-DD
    raw_root: Path     # /opt/spark-data/raw/fluview


# =============================================================================
# EPIWEEK UTILITIES
# =============================================================================

def _week1_start_sunday(year: int) -> date:
    # Week 1 contains Jan 4; weeks start Sunday.
    jan4 = date(year, 1, 4)
    days_since_sun = (jan4.weekday() + 1) % 7  # Mon=0..Sun=6 -> 0 on Sunday
    return jan4 - timedelta(days=days_since_sun)


def epiweek_to_date(ew: int) -> date:
    year = ew // 100
    week = ew % 100
    return _week1_start_sunday(year) + timedelta(days=7 * (week - 1))


def mmwr_epiweek(d: date) -> int:
    y = d.year
    start = _week1_start_sunday(y)
    next_start = _week1_start_sunday(y + 1)

    if d >= next_start:
        y += 1
        start = next_start
    if d < start:
        y -= 1
        start = _week1_start_sunday(y)

    week = ((d - start).days // 7) + 1
    return y * 100 + week


# =============================================================================
# IO HELPERS
# =============================================================================

def _get_json(endpoint: str, params: dict | None = None) -> dict:
    url = f"{DELPHI_BASE}/{endpoint}/"
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


# =============================================================================
# INGEST LOGIC (PURE PYTHON)
# =============================================================================

def ingest_fluview(cfg: IngestConfig) -> dict:
    out_dir = cfg.raw_root / f"ingest_date={cfg.ingest_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Metadata-driven: latest_issue from fluview_meta
    meta = _get_json("fluview_meta")
    if meta.get("result") != 1 or not meta.get("epidata"):
        raise RuntimeError(f"fluview_meta error: {meta}")

    latest_issue = int(meta["epidata"][0]["latest_issue"])

    # 2) Build epiweek window ending at latest_issue
    end_date = epiweek_to_date(latest_issue)
    start_date = end_date - timedelta(days=7 * cfg.weeks_back)
    start_ew = mmwr_epiweek(start_date)
    end_ew = latest_issue

    params_common = {
        "regions": cfg.regions,
        "epiweeks": f"{start_ew}-{end_ew}",
        "issues": str(latest_issue),
    }

    # 3) Pull datasets
    ili = _get_json("fluview", params_common)
    clinical = _get_json("fluview_clinical", params_common)

    # 4) Write raw
    _write_json(out_dir / "fluview_meta.json", meta)
    _write_json(out_dir / "fluview_ili.json", ili)
    _write_json(out_dir / "fluview_clinical.json", clinical)

    # Marker for downstream sensors
    (out_dir / "_SUCCESS").write_text("", encoding="utf-8")

    return {
        "latest_issue": latest_issue,
        "epiweeks": f"{start_ew}-{end_ew}",
        "rows_ili": len(ili.get("epidata") or []),
        "rows_clinical": len(clinical.get("epidata") or []),
        "out_dir": str(out_dir),
    }


# =============================================================================
# AIRFLOW TASK WRAPPER
# =============================================================================

def run_fluview_ingest(**context):
    cfg = IngestConfig(
        regions="nat",
        weeks_back=12,
        ingest_date=context["ds"],
        raw_root=RAW_BASE_PATH,
    )
    result = ingest_fluview(cfg)
    print(f"Ingest completed: {result}")
    return result


# =============================================================================
# AIRFLOW DAG
# =============================================================================

default_args = {
    "owner": "urbanmove",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fluview_ingest",
    description="Ingest CDC FluView data from Delphi API",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cdc", "flu", "ingest", "api"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_fluview",
        python_callable=run_fluview_ingest,
    )
