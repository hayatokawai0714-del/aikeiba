CREATE TABLE IF NOT EXISTS daily_cycle_run_log (
  run_id VARCHAR PRIMARY KEY,
  race_date DATE NOT NULL,
  cycle_started_at TIMESTAMP,
  cycle_finished_at TIMESTAMP,
  cycle_status VARCHAR NOT NULL,
  race_day_status VARCHAR,
  compare_status VARCHAR,
  stop_reason VARCHAR,
  warning_count INTEGER DEFAULT 0,
  model_version VARCHAR,
  feature_snapshot_version VARCHAR,
  snapshot_version VARCHAR,
  dataset_manifest_path VARCHAR,
  comparison_report_path VARCHAR,
  comparison_view_path VARCHAR,
  selected_experiment_names_json VARCHAR,
  discovered_experiment_names_json VARCHAR,
  created_at TIMESTAMP DEFAULT now()
);

ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS finish_pos_std_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS finish_pos_std_last10 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS margin_std_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS margin_std_last10 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS top3_rate_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS top3_rate_last10 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS board_rate_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS board_rate_last10 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS big_loss_rate_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS big_loss_rate_last10 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS worst_finish_last5 INTEGER;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS worst_finish_last10 INTEGER;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS top3_rate_same_distance_bucket DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS top3_rate_same_course DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS finish_pos_std_same_course DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS margin_std_same_course DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS consecutive_bad_runs INTEGER;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS consecutive_top3_runs INTEGER;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS avg_finish_pos_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS avg_margin_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS min_history_flag BOOLEAN;
