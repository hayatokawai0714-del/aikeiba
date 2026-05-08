ALTER TABLE race_day_run_log
ADD COLUMN IF NOT EXISTS doctor_overall_status VARCHAR;

CREATE TABLE IF NOT EXISTS doctor_result_log (
  doctor_id VARCHAR PRIMARY KEY,
  run_id VARCHAR NOT NULL,
  check_code VARCHAR NOT NULL,
  check_name VARCHAR NOT NULL,
  severity VARCHAR NOT NULL, -- info/low/medium/high/critical
  status VARCHAR NOT NULL,   -- pass/warn/stop
  message VARCHAR,
  metric_name VARCHAR,
  metric_value DOUBLE,
  threshold VARCHAR,
  race_date DATE NOT NULL,
  snapshot_version VARCHAR NOT NULL,
  feature_snapshot_version VARCHAR NOT NULL,
  model_version VARCHAR NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT now()
);
