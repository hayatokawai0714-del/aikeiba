# daily_cycle_run_log_report

- 実行日時: 2026-04-29T12:38:42
- DB: racing_ai\data\warehouse\aikeiba.duckdb
- limit: 20

## 直近N件一覧
| created_at | command_name | race_date | model_version | status | stop_reason | missing_files | raw_precheck_log_path |
|---|---|---|---|---|---|---|---|
| 2026-04-29 12:35:49.230983 | run-race-day | 2026-04-18 | top3_stability_plus_pace_v3 | stop | missing_required_raw_files | races.csv, entries.csv | data\logs\raw_precheck_2026-04-18_top3_stability_plus_pace_v3.json |
| 2026-04-29 12:35:38.132393 | run-daily-cycle | 2026-04-18 | top3_stability_plus_pace_v3 | stop | missing_required_raw_files | races.csv, entries.csv | data\logs\raw_precheck_2026-04-18_top3_stability_plus_pace_v3.json |
| 2026-04-29 12:35:18.164496 | run-daily-cycle | 2026-04-18 | top3_stability_plus_pace_v3 | stop | missing_required_raw_files | races.csv, entries.csv | data\logs\raw_precheck_2026-04-18_top3_stability_plus_pace_v3.json |

## status別件数
- stop: 3

## stop_reason別件数
- missing_required_raw_files: 3

## command_name別件数
- run-daily-cycle: 2
- run-race-day: 1

## missing_files 頻出
- entries.csv: 3
- races.csv: 3

## raw_precheck_log_path 参照
- data\logs\raw_precheck_2026-04-18_top3_stability_plus_pace_v3.json