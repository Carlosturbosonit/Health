CREATE SCHEMA IF NOT EXISTS flu;

DROP TABLE IF EXISTS flu.stg_fluview_ili;
CREATE TABLE flu.stg_fluview_ili (
  region        TEXT,
  epiweek       INT,
  issue         INT,
  wili          DOUBLE PRECISION,
  ili           DOUBLE PRECISION,
  num_ili       BIGINT,
  num_patients  BIGINT
);

DROP TABLE IF EXISTS flu.stg_fluview_region_week;
CREATE TABLE flu.stg_fluview_region_week (
  region           TEXT,
  epiweek          INT,
  avg_wili         DOUBLE PRECISION,
  sum_num_ili      BIGINT,
  sum_num_patients BIGINT,
  run_id           TEXT
);

DROP TABLE IF EXISTS flu.fact_fluview_region_week;
CREATE TABLE flu.fact_fluview_region_week (
  region           TEXT NOT NULL,
  epiweek          INT  NOT NULL,
  avg_wili         DOUBLE PRECISION,
  sum_num_ili      BIGINT,
  sum_num_patients BIGINT,
  last_run_id      TEXT,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_fact PRIMARY KEY (region, epiweek)
);
