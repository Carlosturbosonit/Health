from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import requests

DELPHI_BASE = "https://api.delphi.cmu.edu/epidata"


@dataclass(frozen=True)
class IngestConfig:
    regions: str
    weeks_back: int
    ingest_date: str
    raw_root: Path


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


def _get_json(endpoint: str, params: dict | None = None) -> dict:
    r = requests.get(f"{DELPHI_BASE}/{endpoint}/", params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def ingest_fluview(cfg: IngestConfig) -> dict:
    out_dir = cfg.raw_root / f"ingest_date={cfg.ingest_date}"
    out_dir.mkdir(parents=True, exist_ok=True)

    meta = _get_json("fluview_meta")
    latest_issue = int(meta["epidata"][0]["latest_issue"])

    end_date = epiweek_to_date(latest_issue)
    start_ew = mmwr_epiweek(end_date - timedelta(weeks=cfg.weeks_back))

    params = {
        "regions": cfg.regions,
        "epiweeks": f"{start_ew}-{latest_issue}",
        "issues": str(latest_issue),
    }

    ili = _get_json("fluview", params)
    clinical = _get_json("fluview_clinical", params)

    _write_json(out_dir / "fluview_meta.json", meta)
    _write_json(out_dir / "fluview_ili.json", ili)
    _write_json(out_dir / "fluview_clinical.json", clinical)
    (out_dir / "_SUCCESS").write_text("")

    return {"out_dir": str(out_dir)}
