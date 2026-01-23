CREATE SCHEMA IF NOT EXISTS health;

CREATE TABLE IF NOT EXISTS health.fluview_weekly (
  ingest_date           date        NOT NULL,
  region                text        NOT NULL,
  epiweek               integer     NOT NULL,
  issue                 integer     NOT NULL,

  ili                   double precision,
  wili                  double precision,
  num_ili               integer,
  num_patients          integer,
  num_providers         integer,

  percent_positive      double precision,
  total_specimens       integer,
  total_a               integer,
  total_b               integer,

  release_date_ili      date,
  release_date_clinical date,

  ili_per_100k          double precision,
  pos_x_ili             double precision,

  PRIMARY KEY (region, epiweek)
);

CREATE INDEX IF NOT EXISTS idx_fluview_weekly_epiweek
  ON health.fluview_weekly (epiweek);
