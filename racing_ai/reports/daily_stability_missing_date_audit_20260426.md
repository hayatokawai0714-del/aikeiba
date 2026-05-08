# Daily Stability Missing Date Audit (2026-04-26)

```json
{
  "pairs_csv": "racing_ai\\reports\\pair_shadow_pair_comparison_expanded_20260411_20260426_with_results_external_priority.csv",
  "race_date": "2026-04-26",
  "generated_at": "2026-05-01T20:57:20",
  "total_rows_in_pairs_csv": 7802,
  "total_race_dates_in_pairs_csv": 5,
  "rows_for_race_date": 1564,
  "race_count_for_race_date": 36,
  "columns_present": [
    "actual_wide_hit",
    "bet_key",
    "db_finish_position",
    "db_finish_position_h1",
    "db_finish_position_h2",
    "db_status",
    "db_status_h1",
    "db_status_h2",
    "expected_vs_payout_match",
    "external_finish_position",
    "external_finish_position_h1",
    "external_finish_position_h2",
    "external_status",
    "external_status_h1",
    "external_status_h2",
    "horse1_finish_position",
    "horse1_umaban",
    "horse2_finish_position",
    "horse2_umaban",
    "model_dynamic_final_score",
    "model_dynamic_rank",
    "model_dynamic_selected_flag",
    "model_dynamic_skip_reason",
    "model_top5_flag",
    "pair_edge",
    "pair_edge_log_ratio",
    "pair_edge_pct_gap",
    "pair_edge_rank_gap",
    "pair_edge_ratio",
    "pair_market_implied_prob",
    "pair_model_score",
    "pair_norm",
    "pair_selected_flag",
    "pair_value_score",
    "payout_join_status",
    "payout_wide_count",
    "payout_wide_keys",
    "race_date",
    "race_id",
    "raw_actual_wide_hit",
    "result_expected_wide_keys",
    "result_join_status",
    "result_quality_status",
    "result_source_conflict",
    "result_source_used",
    "result_top3_count",
    "wide_payout"
  ],
  "result_source_used_counts": {
    "external": 1536,
    "db": 16,
    "none": 12
  },
  "result_quality_status_counts": {
    "expected_payout_mismatch": 979,
    "invalid_finish_position": 540,
    "top3_count_not_3": 45
  },
  "actual_wide_hit_non_null": 0,
  "raw_actual_wide_hit_non_null": 1420,
  "wide_payout_non_null": 135,
  "rule_selected_row_count": 180,
  "model_dynamic_non_overlap_row_count": 0,
  "model_dynamic_selected_row_count": 83
}
```

## Notes

- If `rows_for_race_date` is 0, the upstream candidate union CSV likely did not include this date (or `race_date` is missing/incorrect in the input rows).
- If `rows_for_race_date` > 0 but `result_quality_status=ok` race count is 0, daily stability may exclude it when `--quality-ok-only` is used.
