ALTER TABLE daily_cycle_run_log ADD COLUMN IF NOT EXISTS empty_files VARCHAR;
ALTER TABLE daily_cycle_run_log ADD COLUMN IF NOT EXISTS row_counts VARCHAR;
