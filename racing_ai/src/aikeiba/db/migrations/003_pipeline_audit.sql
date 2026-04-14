CREATE TABLE IF NOT EXISTS pipeline_audit_log (
  event_id VARCHAR PRIMARY KEY,
  stage VARCHAR NOT NULL, -- raw / normalized / warehouse
  snapshot_version VARCHAR NOT NULL,
  target_race_date DATE NOT NULL,
  event_time TIMESTAMP NOT NULL,
  status VARCHAR NOT NULL, -- ok / warn / stop
  source_file_name VARCHAR,
  source_file_path VARCHAR,
  row_count INTEGER,
  message VARCHAR,
  metrics_json VARCHAR
);
