CREATE TABLE IF NOT EXISTS horse_predictions (
  race_id VARCHAR NOT NULL,
  horse_no INTEGER NOT NULL,
  model_version VARCHAR NOT NULL,
  inference_timestamp TIMESTAMP NOT NULL,

  p_top3 DOUBLE,
  p_win DOUBLE,
  ability DOUBLE,
  stability DOUBLE,

  ai_rank INTEGER,
  role VARCHAR,

  feature_snapshot_version VARCHAR NOT NULL,
  odds_snapshot_version VARCHAR NOT NULL,
  dataset_fingerprint VARCHAR NOT NULL,

  PRIMARY KEY (race_id, horse_no, model_version, inference_timestamp)
);
