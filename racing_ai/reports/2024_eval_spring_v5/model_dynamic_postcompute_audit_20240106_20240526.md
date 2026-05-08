# model_dynamic postcompute audit

- generated_at: 2026-05-06T20:46:35
- input: racing_ai\reports\2024_eval_spring_v5\pair_shadow_pair_comparison_expanded_20240106_20240526_with_results_external_priority.csv

## Summary

- total_pair_rows: 64166
- race_count: 1464
- selected_before: 39
- selected_after: 74
- non_overlap_after: 31
- selected_race_count: 36
- zero_selected_race_count: 1428

## Thresholds (evaluation-only fallback)

- min_score: 0.08
- min_edge: 0.0
- min_gap: 0.01
- k: 5

## Skip Reason Counts (race-level)

```json
{
  "DYNAMIC_SKIP_MODEL_SCORE_WEAK": 1428,
  "DYNAMIC_BUY_OK": 36
}
```
