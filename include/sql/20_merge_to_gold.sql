INSERT INTO flu.gold_region_week (
  region, epiweek, latest_issue, avg_wili, avg_ili, sum_num_ili, sum_num_patients, loaded_at, source_run_id
)
SELECT
  region,
  epiweek,
  latest_issue,
  avg_wili,
  avg_ili,
  sum_num_ili,
  sum_num_patients,
  now() as loaded_at,
  run_id as source_run_id
FROM flu.stg_fluview_region_week
WHERE run_id = '{{ run_id }}'
ON CONFLICT (region, epiweek)
DO UPDATE SET
  latest_issue     = EXCLUDED.latest_issue,
  avg_wili         = EXCLUDED.avg_wili,
  avg_ili          = EXCLUDED.avg_ili,
  sum_num_ili      = EXCLUDED.sum_num_ili,
  sum_num_patients = EXCLUDED.sum_num_patients,
  loaded_at        = EXCLUDED.loaded_at,
  source_run_id    = EXCLUDED.source_run_id;
