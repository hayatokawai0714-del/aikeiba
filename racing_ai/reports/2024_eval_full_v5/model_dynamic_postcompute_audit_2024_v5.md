# model_dynamic Postcompute Audit (from joined pairs CSV)

- generated_at: 2026-05-06T21:21:58
- input: racing_ai\reports\2024_eval_full_v5\pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv
- total_pair_rows: 149424
- race_count: 3454

## Counts

- model_dynamic_selected_count: 136
- model_dynamic_non_overlap_count: 42
- selected_race_count: 68
- zero_selected_race_count: 3386

## Skip reasons (race-level)

```json
{
  "DYNAMIC_SKIP_MODEL_SCORE_WEAK": 3386,
  "DYNAMIC_BUY_OK": 68
}
```

## Feature coverage

- pair_model_score_non_null_count: 149424
- pair_edge_non_null_count: 149424
- pair_model_score_gap_to_next_non_null_count: 149424

## Notes

- This audit reads existing columns; it does not recompute selection logic.
