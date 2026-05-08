# Evaluation Summary 2025 (Final)

## Coverage

```json
{
  "total_race_count": 2148.0,
  "quality_ok_race_count": 947.0,
  "quality_ng_race_count": 1201.0,
  "raw_actual_wide_hit_coverage": 0.9894165506439262,
  "quality_filtered_actual_wide_hit_coverage": 0.4429059345631744,
  "quality_ok_payout_coverage": 1.0
}
```

## Model Dynamic Selection Diagnostics

```json
{
  "total_pair_rows": 91936,
  "race_count": 2148,
  "model_dynamic_selected_count": 3737,
  "model_dynamic_non_overlap_count": 361,
  "model_dynamic_selected_race_count": 1172,
  "model_dynamic_zero_selected_race_count": 976,
  "pair_selected_count": 10740,
  "overlap_count": 3376,
  "non_overlap_count": 361,
  "non_overlap_vs_rule_selected_ratio": 0.03361266294227188
}
```

## Daily Stability Summary

```json
{
  "evaluated_date_count": 47.0,
  "comparable_date_count": 47.0,
  "dynamic_positive_days": 8.0,
  "dynamic_negative_days": 26.0,
  "dynamic_zero_or_na_days": 13.0,
  "dynamic_minus_rule_roi_mean": -1.3891019411899643,
  "dynamic_minus_rule_roi_median": -0.7073529411764705,
  "dynamic_minus_rule_roi_total_weighted": -1.529039070749736
}
```

## Conclusion

```json
{
  "evaluation_possible": false,
  "coverage_ok_threshold_0_7": false,
  "dynamic_candidate_scarce": true,
  "non_overlap_vs_rule_selected_ratio": 0.03361266294227188
}
```

## Notes

- 2025 uses evaluation-only rule rebuild (pair_value_score top-5) for missing production artifacts; production logic is unchanged.
- quality_filtered coverage is below 0.7; the strict ?evaluation OK? threshold is not met.
- model_dynamic_non_overlap candidates are much fewer than rule-selected; many days show 0 dynamic non-overlap ROI or sparse samples.
