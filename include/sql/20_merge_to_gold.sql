INSERT INTO flu.fact_fluview_region_week (region, epiweek, avg_wili, sum_num_ili, sum_num_patients, last_run_id)
SELECT region, epiweek, avg_wili, sum_num_ili, sum_num_patients, run_id
FROM flu.stg_fluview_region_week
ON CONFLICT (region, epiweek)
DO UPDATE SET
  avg_wili = EXCLUDED.avg_wili,
  sum_num_ili = EXCLUDED.sum_num_ili,
  sum_num_patients = EXCLUDED.sum_num_patients,
  last_run_id = EXCLUDED.last_run_id,
  updated_at = now();
