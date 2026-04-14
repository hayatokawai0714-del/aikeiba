ALTER TABLE race_day_run_log
ADD COLUMN IF NOT EXISTS warnings_json VARCHAR;
