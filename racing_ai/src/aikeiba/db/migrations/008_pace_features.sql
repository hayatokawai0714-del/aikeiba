ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS avg_corner4_pos_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS corner4_pos_std_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS front_runner_rate_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS closer_rate_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS avg_last3f_rank_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS pace_finish_delta_last5 DOUBLE;
ALTER TABLE feature_store ADD COLUMN IF NOT EXISTS pace_min_history_flag BOOLEAN;
