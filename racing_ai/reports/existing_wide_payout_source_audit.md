# existing_wide_payout_source_audit

- generated_at: 2026-04-29T20:07:36
- target_period: 2021-01-01 .. 2024-12-31

## 1. ?????????

| scope | payouts.csv????? | ???? | ???? | 2021-2024??wide?? |
|---|---:|---|---|---:|
| raw | 274 | 2025-04-20 00:00:00 | 2026-04-26 00:00:00 | 0 |
| normalized | 212 | 2025-04-20 00:00:00 | 2026-04-26 00:00:00 | 0 |

## 2. DuckDB ????????????

- tables: daily_cycle_run_log, doctor_result_log, entries, feature_store, horse_master, horse_predictions, inference_log, odds, payouts, pipeline_audit_log, race_day_run_log, races, results, schema_migrations

| table | column | type |
|---|---|---|
| payouts | payout | DOUBLE |

### 2021?2024 wide???DB????
- rows: 0
- races: 0
- race_dates: 0
- min_date: NaT
- max_date: NaT

## 3. C:\TXT ??/?????????

- candidate_files_scanned: 57
- keyword_or_2021hit_files: 51

| file | kind | keyword_hit(col/body) | 2021-2024???(??) |
|---|---|---|---:|
| C:\TXT\today_wide_predictions_log.txt | txt | True | 0 |
| C:\TXT\wide_candidates_2026_ability_gap_v1.csv | csv | True | 0 |
| C:\TXT\wide_candidates_2026_history_phase1.csv | csv | True | 0 |
| C:\TXT\wide_candidates_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_candidates_report_2026_history_phase1.txt | txt | True | 0 |
| C:\TXT\wide_candidates_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_diff_filter_optimization_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_diff_filter_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_diff_risk_filter_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_diff_risk_filter_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_diff_segment_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_diff_segment_test_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_horse_selection_with_diff_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_horse_selection_with_diff_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_loss_analysis_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_loss_analysis_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_phase2_current_model_spec.txt | txt | True | 0 |
| C:\TXT\wide_race_selection_2026_history_phase1.csv | csv | True | 0 |
| C:\TXT\wide_race_selection_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_race_selection_fixed_rate_2026_ability_gap_v1.csv | csv | True | 0 |
| C:\TXT\wide_race_selection_fixed_rate_2026_history_phase1.csv | csv | True | 0 |
| C:\TXT\wide_race_selection_fixed_rate_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_race_selection_fixed_rate_report_2026_history_phase1.txt | txt | True | 0 |
| C:\TXT\wide_race_selection_fixed_rate_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_race_selection_report_2026_history_phase1.txt | txt | True | 0 |
| C:\TXT\wide_race_selection_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_roi_monthly_2026_history_phase1.csv | csv | True | 0 |
| C:\TXT\wide_roi_monthly_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_roi_monthly_report_2026_history_phase1.txt | txt | True | 0 |
| C:\TXT\wide_roi_monthly_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_roi_real_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_roi_real_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_roi_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_roi_simulation_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_roi_with_ability_gap_report.txt | txt | True | 0 |
| C:\TXT\wide_roi_with_ability_gap_v1.csv | csv | True | 0 |
| C:\TXT\wide_roi_with_history_phase1_report.txt | txt | True | 0 |
| C:\TXT\wide_roi_with_history_phase2.csv | csv | True | 0 |
| C:\TXT\wide_roi_with_history_phase2_report.txt | txt | True | 0 |
| C:\TXT\wide_selection_final_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_selection_final_with_risk_filter_report_2026.txt | txt | True | 0 |
| C:\TXT\wide_selection_weight_grid_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_selection_weight_grid_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_selection_weight_grid_report_final.txt | txt | True | 0 |
| C:\TXT\wide_selection_with_diff_2026_v1.csv | csv | True | 0 |
| C:\TXT\wide_selection_with_diff_report_2026_v1.txt | txt | True | 0 |
| C:\TXT\wide_v1_strategy_spec_2026.txt | txt | True | 0 |
| C:\TXT\bet_logs\wide_bet_log_20260328A.csv | csv | True | 0 |
| C:\TXT\bet_logs\wide_bet_log_all.csv | csv | True | 0 |
| C:\TXT\bet_logs\wide_bet_log_all_updated.csv | csv | True | 0 |
| C:\TXT\bet_logs\wide_bet_log_all_updated_check.csv | csv | True | 0 |

## 4. TARGET frontier JV / JV-Link ????????

- ????????:
  - C:\ProgramData\JRA-VAN\Data Lab\data
  - C:\ProgramData\JRA-VAN\Data Lab\cache
  - C:\ProgramData\JRA-VAN\Data Lab\event
  - C:\Users\HND2205\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\TARGET frontier JV
- ??????????????????CSV/TXT?????

## 5. ???2021?2024 wide???????

- ??: **??????????**?2021?2024?wide???????CSV/TXT/DB??????????

## 6. ????????????import_wide_payouts_csv.py???

- ???: `race_id, race_date, bet_type, bet_key, payout, source_version`
- `bet_type`=`wide`
- `bet_key`????????? (`03-07`)
- ??????: `python racing_ai\scripts\import_wide_payouts_csv.py --db-path racing_ai\data\warehouse\aikeiba.duckdb --input-csv <csv> --on-conflict skip|replace`