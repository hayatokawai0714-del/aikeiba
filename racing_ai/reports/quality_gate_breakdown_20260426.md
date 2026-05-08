# Quality Gate Breakdown (2026-04-26)

## Summary

```json
{
  "pairs_csv": "racing_ai\\reports\\pair_shadow_pair_comparison_expanded_20260411_20260426_with_results_external_priority.csv",
  "race_date": "2026-04-26",
  "generated_at": "2026-05-01T20:57:20",
  "total_pair_rows": 1564,
  "race_count": 36,
  "quality_ok_race_count": 0,
  "actual_wide_hit_non_null": 0,
  "wide_payout_non_null": 135,
  "rule_selected_count": 180,
  "model_dynamic_non_overlap_count": 0,
  "missing_reason_hint": "If quality_ok_race_count==0 but races_count>0, check results/payout coverage and key mismatches; also confirm race_date column correctness."
}
```

## Breakdown (result_quality_status)

- csv: racing_ai\reports\quality_gate_breakdown_20260426.csv

| result_quality_status | row_count |
|---|---:|
| expected_payout_mismatch | 979 |
| invalid_finish_position | 540 |
| top3_count_not_3 | 45 |
