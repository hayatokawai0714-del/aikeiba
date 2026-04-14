-- Core normalized tables (JRA only). Keep schema minimal and auditable.

CREATE TABLE IF NOT EXISTS races (
  race_id VARCHAR PRIMARY KEY,
  race_date DATE NOT NULL,
  venue VARCHAR NOT NULL,
  race_no INTEGER NOT NULL,
  post_time VARCHAR,
  surface VARCHAR,
  distance INTEGER,
  track_condition VARCHAR,
  race_class VARCHAR,
  field_size_expected INTEGER,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS entries (
  race_id VARCHAR NOT NULL,
  horse_no INTEGER NOT NULL,
  horse_id VARCHAR,
  horse_name VARCHAR,
  waku INTEGER,
  sex VARCHAR,
  age INTEGER,
  weight_carried DOUBLE,
  jockey_id VARCHAR,
  trainer_id VARCHAR,
  is_scratched BOOLEAN DEFAULT FALSE,
  ingested_at TIMESTAMP DEFAULT now(),
  source_version VARCHAR,
  PRIMARY KEY (race_id, horse_no)
);

CREATE TABLE IF NOT EXISTS results (
  race_id VARCHAR NOT NULL,
  horse_no INTEGER NOT NULL,
  finish_position INTEGER,
  margin DOUBLE,
  last3f_time DOUBLE,
  last3f_rank INTEGER,
  corner_pos_1 INTEGER,
  corner_pos_2 INTEGER,
  corner_pos_3 INTEGER,
  corner_pos_4 INTEGER,
  pop_rank INTEGER,
  odds_win_final DOUBLE,
  ingested_at TIMESTAMP DEFAULT now(),
  source_version VARCHAR,
  PRIMARY KEY (race_id, horse_no)
);

CREATE TABLE IF NOT EXISTS odds (
  race_id VARCHAR NOT NULL,
  odds_snapshot_version VARCHAR NOT NULL,
  captured_at TIMESTAMP,
  odds_type VARCHAR NOT NULL, -- win/place/wide/etc
  horse_no INTEGER NOT NULL DEFAULT -1,   -- for win/place (use -1 when not applicable)
  horse_no_a INTEGER NOT NULL DEFAULT -1, -- for wide (use -1 when not applicable)
  horse_no_b INTEGER NOT NULL DEFAULT -1, -- for wide (use -1 when not applicable)
  odds_value DOUBLE,
  ingested_at TIMESTAMP DEFAULT now(),
  source_version VARCHAR,
  PRIMARY KEY (race_id, odds_snapshot_version, odds_type, horse_no, horse_no_a, horse_no_b)
);

CREATE TABLE IF NOT EXISTS payouts (
  race_id VARCHAR NOT NULL,
  bet_type VARCHAR NOT NULL, -- wide/etc
  bet_key VARCHAR NOT NULL,  -- e.g. "03-12"
  payout DOUBLE,
  popularity INTEGER,
  ingested_at TIMESTAMP DEFAULT now(),
  source_version VARCHAR,
  PRIMARY KEY (race_id, bet_type, bet_key)
);

CREATE TABLE IF NOT EXISTS horse_master (
  horse_id VARCHAR PRIMARY KEY,
  horse_name VARCHAR,
  birth_year INTEGER,
  sex VARCHAR,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS feature_store (
  race_id VARCHAR NOT NULL,
  horse_no INTEGER NOT NULL,
  feature_snapshot_version VARCHAR NOT NULL,
  race_date DATE NOT NULL,
  venue VARCHAR,
  surface VARCHAR,
  distance INTEGER,
  field_size INTEGER,

  prev_last3f_rank INTEGER,
  avg_last3f_rank_3 DOUBLE,
  best_last3f_count INTEGER,
  prev_margin DOUBLE,
  avg_margin_3 DOUBLE,
  margin_std_3 DOUBLE,
  prev_corner4_pos INTEGER,
  avg_corner4_pos_3 DOUBLE,

  dist_change INTEGER,
  course_change BOOLEAN,
  surface_change BOOLEAN,

  finish_pos_std_5 DOUBLE,
  big_loss_count_10 INTEGER,
  itb_rate_10 DOUBLE,

  waku INTEGER,
  horse_no_rel DOUBLE,

  jockey_top3_rate_1y DOUBLE,

  feature_generated_at TIMESTAMP NOT NULL,
  source_race_date_max DATE NOT NULL,
  dataset_fingerprint VARCHAR NOT NULL,
  odds_snapshot_version VARCHAR,

  PRIMARY KEY (race_id, horse_no, feature_snapshot_version)
);

CREATE TABLE IF NOT EXISTS inference_log (
  inference_id VARCHAR PRIMARY KEY,
  race_date DATE NOT NULL,
  inference_timestamp TIMESTAMP NOT NULL,
  feature_snapshot_version VARCHAR NOT NULL,
  model_version VARCHAR NOT NULL,
  odds_snapshot_version VARCHAR NOT NULL,
  dataset_fingerprint VARCHAR NOT NULL,
  stop_reason VARCHAR,
  buy_races INTEGER,
  total_candidates INTEGER,
  warnings_json VARCHAR
);
