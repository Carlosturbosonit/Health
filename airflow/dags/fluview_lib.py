from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.decorators import task

# =============================================================================
# CONSTANTS
# =============================================================================

DELPHI_BASE = "https://api.delphi.cmu.edu/epidata"
RAW_ROOT = Path("/opt/airflow/data/raw/fluview")

REGIONS = "nat"
WEEKS_BACK = 12

# =============================================================================
# DATA MODEL
# =============================================================================

@dataclass(frozen=True)
class IngestContext:
    latest_issue: int
    start_ew: int
    end_ew: int
    out_dir: str

# =============================================================================
# EPIWEEK UTILITIES
# =============================================================================

def _week1_start_sunday(year: int) -> date:
    jan4 = date(year, 1, 4)
    days_since_sun = (jan4.weekday() + 1) % 7
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
# HTTP + IO
# =============================================================================

def get_json(endpoint: str, params: dict | None = None) -> dict:
    r = requests.get(f"{DELPHI_BASE}/{endpoint}/", params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)

# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="fluview_ingest_structured",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "fluview", "ingest", "bronze"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    @task
    def fetch_metadata(ds: str) -> IngestContext:
        meta = get_json("fluview_meta")
        latest_issue = int(meta["epidata"][0]["latest_issue"])

        end_date = epiweek_to_date(latest_issue)
        start_ew = mmwr_epiweek(end_date - timedelta(weeks=WEEKS_BACK))

        out_dir = RAW_ROOT / f"ingest_date={ds}"
        write_json(out_dir / "fluview_meta.json", meta)

        return IngestContext(
            latest_issue=latest_issue,
            start_ew=start_ew,
            end_ew=latest_issue,
            out_dir=str(out_dir),
        )

    @task
    def fetch_ili(ctx: IngestContext):
        params = {
            "regions": REGIONS,
            "epiweeks": f"{ctx.start_ew}-{ctx.end_ew}",
            "issues": str(ctx.end_ew),
        }
        data = get_json("fluview", params)
        write_json(Path(ctx.out_dir) / "fluview_ili.json", data)

    @task
    def fetch_clinical(ctx: IngestContext):
        params = {
            "regions": REGIONS,
            "epiweeks": f"{ctx.start_ew}-{ctx.end_ew}",
            "issues": str(ctx.end_ew),
        }
        data = get_json("fluview_clinical", params)
        write_json(Path(ctx.out_dir) / "fluview_clinical.json", data)

    @task
    def mark_success(ctx: IngestContext):
        Path(ctx.out_dir, "_SUCCESS").write_text("", encoding="utf-8")
        return {"out_dir": ctx.out_dir}

    # ✅ DEPENDENCIES CORRECTAS
    ctx = fetch_metadata()
    ili = fetch_ili(ctx)
    clinical = fetch_clinical(ctx)
    mark_success(ctx) << [ili, clinical]

