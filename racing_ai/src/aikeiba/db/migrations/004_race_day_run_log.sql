CREATE TABLE IF NOT EXISTS race_day_run_log (
  run_id VARCHAR PRIMARY KEY,
  race_date DATE NOT NULL,
  snapshot_version VARCHAR NOT NULL,
  feature_snapshot_version VARCHAR NOT NULL,
  model_version VARCHAR NOT NULL,
  odds_snapshot_version VARCHAR NOT NULL,
  status VARCHAR NOT NULL, -- ok / warn / stop
  stop_reason VARCHAR,
  warning_count INTEGER NOT NULL DEFAULT 0,
  run_summary_path VARCHAR,
  created_at TIMESTAMP NOT NULL DEFAULT now()
);
