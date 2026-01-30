CREATE SCHEMA IF NOT EXISTS flu;

-- ========== STAGING ==========
CREATE TABLE IF NOT EXISTS flu.stg_fluview_ili (
  region       text,
  epiweek      integer,
  issue        integer,
  wili         double precision,
  ili          double precision,
  num_ili      bigint,
  num_patients bigint,
  run_id       text
);

CREATE TABLE IF NOT EXISTS flu.stg_fluview_region_week (
  region           text,
  epiweek          integer,
  latest_issue     integer,
  avg_wili         double precision,
  avg_ili          double precision,
  sum_num_ili      bigint,
  sum_num_patients bigint,
  run_id           text
);

-- ========== GOLD ==========
CREATE TABLE IF NOT EXISTS flu.gold_region_week (
  region           text NOT NULL,
  epiweek          integer NOT NULL,
  latest_issue     integer,
  avg_wili         double precision,
  avg_ili          double precision,
  sum_num_ili      bigint,
  sum_num_patients bigint,
  loaded_at        timestamptz NOT NULL DEFAULT now(),
  source_run_id    text,
  PRIMARY KEY (region, epiweek)
);

-- ========== INDEXES (staging) ==========
CREATE INDEX IF NOT EXISTS ix_stg_fluview_ili_run_id
  ON flu.stg_fluview_ili (run_id);

CREATE INDEX IF NOT EXISTS ix_stg_fluview_region_week_run_id
  ON flu.stg_fluview_region_week (run_id);
