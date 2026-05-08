# model_dynamic postcompute audit

- generated_at: 2026-05-06T15:33:31
- input: racing_ai\reports\2024_eval_spring_v4\pair_shadow_pair_comparison_expanded_20240106_20240526_with_results_external_priority.csv

## Summary

- total_pair_rows: 64166
- race_count: 1464
- selected_before: 0
- selected_after: 7168
- non_overlap_after: 6739
- selected_race_count: 1454
- zero_selected_race_count: 10

## Thresholds (evaluation-only fallback)

- min_score: 0.04220615474984115
- min_edge: -0.0209924926003747
- min_gap: 0.0
- k: 5

## Skip Reason Counts (race-level)

```json
{
  "DYNAMIC_BUY_OK": 1454,
  "DYNAMIC_SKIP_EDGE_WEAK": 10
}
```
