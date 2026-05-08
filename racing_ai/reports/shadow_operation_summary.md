# shadow_operation_summary

- generated_at: 2026-04-29T13:52:26
- model_version: top3_stability_plus_pace_v3
- dates: 2026-04-01, 2026-04-02, 2026-04-03, 2026-04-04, 2026-04-05, 2026-04-06, 2026-04-07, 2026-04-08, 2026-04-09, 2026-04-10, 2026-04-11, 2026-04-12, 2026-04-13, 2026-04-14, 2026-04-15, 2026-04-16, 2026-04-17, 2026-04-18, 2026-04-19
- total_days: 19
- comparable_days: 6
- excluded_days: 13
- model_win_days: 0
- rule_win_days: 0
- tie_days: 6

## Excluded Days
| race_date | status | stop_reason | exclude_reason |
|---|---|---|---|
| 2026-04-01 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |
| 2026-04-02 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |
| 2026-04-06 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |
| 2026-04-07 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |
| 2026-04-08 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |
| 2026-04-09 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |
| 2026-04-13 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |
| 2026-04-14 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |
| 2026-04-15 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |
| 2026-04-16 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |
| 2026-04-17 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |
| 2026-04-18 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |
| 2026-04-19 | stop | zero_rows:races;zero_rows:entries;race_id_missing_rate_entries_high;horse_id_missing_rate_entries_high | zero_rows:races |

## Daily Summary
| race_date | status | candidate_pairs | selected_pairs | model_available | rule_top5_hit | model_top5_hit | model-rule_diff | rank_diff_abs_mean | gate_warn | race_meta_invalid |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-04-03 | warn | 105 | 105 | 105 | 0.21904761904761905 | 0.21904761904761905 | 0.0 | 0.6095238095238096 | 18 | 3 |
| 2026-04-04 | warn | 105 | 105 | 105 | 0.26666666666666666 | 0.26666666666666666 | 0.0 | 0.8 | 21 | 3 |
| 2026-04-05 | warn | 105 | 105 | 105 | 0.26666666666666666 | 0.26666666666666666 | 0.0 | 0.780952380952381 | 21 | 3 |
| 2026-04-10 | warn | 135 | 135 | 135 | 0.2 | 0.2 | 0.0 | 0.5259259259259259 | 26 | 9 |
| 2026-04-11 | warn | 130 | 130 | 130 | 0.2153846153846154 | 0.2153846153846154 | 0.0 | 1.0615384615384615 | 27 | 9 |
| 2026-04-12 | warn | 130 | 130 | 130 | 0.2153846153846154 | 0.2153846153846154 | 0.0 | 1.0153846153846153 | 27 | 9 |

## Race Meta Invalid (Daily)
- 2026-04-03: 3
- 2026-04-04: 3
- 2026-04-05: 3
- 2026-04-10: 9
- 2026-04-11: 9
- 2026-04-12: 9

## Probability Gate Warning (Daily)
- 2026-04-03: 18
- 2026-04-04: 21
- 2026-04-05: 21
- 2026-04-10: 26
- 2026-04-11: 27
- 2026-04-12: 27

## Output Paths
- 2026-04-01: racing_ai\data\race_day\2026-04-01\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-01: racing_ai\data\race_day\2026-04-01\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-01: racing_ai\data\race_day\2026-04-01\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-01: racing_ai\data\race_day\2026-04-01\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-01: racing_ai\data\race_day\2026-04-01\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-02: racing_ai\data\race_day\2026-04-02\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-02: racing_ai\data\race_day\2026-04-02\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-02: racing_ai\data\race_day\2026-04-02\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-02: racing_ai\data\race_day\2026-04-02\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-02: racing_ai\data\race_day\2026-04-02\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-03: racing_ai\data\race_day\2026-04-03\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-03: racing_ai\data\race_day\2026-04-03\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-03: racing_ai\data\race_day\2026-04-03\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-03: racing_ai\data\race_day\2026-04-03\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-03: racing_ai\data\race_day\2026-04-03\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-04: racing_ai\data\race_day\2026-04-04\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-04: racing_ai\data\race_day\2026-04-04\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-04: racing_ai\data\race_day\2026-04-04\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-04: racing_ai\data\race_day\2026-04-04\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-04: racing_ai\data\race_day\2026-04-04\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-05: racing_ai\data\race_day\2026-04-05\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-05: racing_ai\data\race_day\2026-04-05\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-05: racing_ai\data\race_day\2026-04-05\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-05: racing_ai\data\race_day\2026-04-05\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-05: racing_ai\data\race_day\2026-04-05\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-06: racing_ai\data\race_day\2026-04-06\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-06: racing_ai\data\race_day\2026-04-06\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-06: racing_ai\data\race_day\2026-04-06\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-06: racing_ai\data\race_day\2026-04-06\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-06: racing_ai\data\race_day\2026-04-06\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-07: racing_ai\data\race_day\2026-04-07\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-07: racing_ai\data\race_day\2026-04-07\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-07: racing_ai\data\race_day\2026-04-07\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-07: racing_ai\data\race_day\2026-04-07\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-07: racing_ai\data\race_day\2026-04-07\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-08: racing_ai\data\race_day\2026-04-08\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-08: racing_ai\data\race_day\2026-04-08\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-08: racing_ai\data\race_day\2026-04-08\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-08: racing_ai\data\race_day\2026-04-08\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-08: racing_ai\data\race_day\2026-04-08\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-09: racing_ai\data\race_day\2026-04-09\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-09: racing_ai\data\race_day\2026-04-09\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-09: racing_ai\data\race_day\2026-04-09\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-09: racing_ai\data\race_day\2026-04-09\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-09: racing_ai\data\race_day\2026-04-09\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-10: racing_ai\data\race_day\2026-04-10\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-10: racing_ai\data\race_day\2026-04-10\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-10: racing_ai\data\race_day\2026-04-10\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-10: racing_ai\data\race_day\2026-04-10\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-10: racing_ai\data\race_day\2026-04-10\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-11: racing_ai\data\race_day\2026-04-11\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-11: racing_ai\data\race_day\2026-04-11\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-11: racing_ai\data\race_day\2026-04-11\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-11: racing_ai\data\race_day\2026-04-11\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-11: racing_ai\data\race_day\2026-04-11\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-12: racing_ai\data\race_day\2026-04-12\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-12: racing_ai\data\race_day\2026-04-12\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-12: racing_ai\data\race_day\2026-04-12\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-12: racing_ai\data\race_day\2026-04-12\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-12: racing_ai\data\race_day\2026-04-12\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-13: racing_ai\data\race_day\2026-04-13\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-13: racing_ai\data\race_day\2026-04-13\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-13: racing_ai\data\race_day\2026-04-13\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-13: racing_ai\data\race_day\2026-04-13\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-13: racing_ai\data\race_day\2026-04-13\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-14: racing_ai\data\race_day\2026-04-14\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-14: racing_ai\data\race_day\2026-04-14\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-14: racing_ai\data\race_day\2026-04-14\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-14: racing_ai\data\race_day\2026-04-14\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-14: racing_ai\data\race_day\2026-04-14\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-15: racing_ai\data\race_day\2026-04-15\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-15: racing_ai\data\race_day\2026-04-15\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-15: racing_ai\data\race_day\2026-04-15\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-15: racing_ai\data\race_day\2026-04-15\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-15: racing_ai\data\race_day\2026-04-15\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-16: racing_ai\data\race_day\2026-04-16\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-16: racing_ai\data\race_day\2026-04-16\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-16: racing_ai\data\race_day\2026-04-16\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-16: racing_ai\data\race_day\2026-04-16\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-16: racing_ai\data\race_day\2026-04-16\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-17: racing_ai\data\race_day\2026-04-17\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-17: racing_ai\data\race_day\2026-04-17\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-17: racing_ai\data\race_day\2026-04-17\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-17: racing_ai\data\race_day\2026-04-17\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-17: racing_ai\data\race_day\2026-04-17\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-18: racing_ai\data\race_day\2026-04-18\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-18: racing_ai\data\race_day\2026-04-18\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-18: racing_ai\data\race_day\2026-04-18\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-18: racing_ai\data\race_day\2026-04-18\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-18: racing_ai\data\race_day\2026-04-18\top3_stability_plus_pace_v3\pair_shadow_compare_report.md
- 2026-04-19: racing_ai\data\race_day\2026-04-19\top3_stability_plus_pace_v3\run_summary.json
- 2026-04-19: racing_ai\data\race_day\2026-04-19\top3_stability_plus_pace_v3\candidate_pairs.parquet
- 2026-04-19: racing_ai\data\race_day\2026-04-19\top3_stability_plus_pace_v3\race_flags.parquet
- 2026-04-19: racing_ai\data\race_day\2026-04-19\top3_stability_plus_pace_v3\skip_log.parquet
- 2026-04-19: racing_ai\data\race_day\2026-04-19\top3_stability_plus_pace_v3\pair_shadow_compare_report.md