# Model Dynamic Selection Diagnostics 2025

- input: racing_ai\reports\2025_eval_full\pair_shadow_pair_comparison_expanded_2025_with_results_external_priority.csv

## Summary

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
  "skip_reason_counts_selected_only": {
    "DYNAMIC_BUY_OK": 3737
  },
  "model_dynamic_final_score_quantiles": {
    "min": 0.0291441093986075,
    "p01": 0.03780094003715716,
    "p05": 0.04510853648658089,
    "p10": 0.04994072717947614,
    "p25": 0.06395591313248115,
    "p50": 0.1004237212958782,
    "p75": 0.15463477979479467,
    "p90": 0.21606135460494721,
    "p95": 0.3785636497931488,
    "p99": 0.47580018552418263,
    "max": 0.847766344550797
  },
  "pair_edge_quantiles": {
    "min": -0.375988547586373,
    "p01": -0.07153922157514316,
    "p05": -0.008619175758712365,
    "p10": 0.006174550433436459,
    "p25": 0.02737940379977485,
    "p50": 0.0450869356342721,
    "p75": 0.06480645217431034,
    "p90": 0.09409051366078158,
    "p95": 0.1201694758902098,
    "p99": 0.18464439855853737,
    "max": 0.3323709383817345
  },
  "pair_model_score_quantiles": {
    "min": 0.0283448695386841,
    "p01": 0.0366024492612075,
    "p05": 0.0435059517620938,
    "p10": 0.0482367999545038,
    "p25": 0.0616291122669188,
    "p50": 0.0956018006868595,
    "p75": 0.14790233581825551,
    "p90": 0.20811883035444254,
    "p95": 0.3436022577533458,
    "p99": 0.4158885861699926,
    "max": 0.7063661811476052
  },
  "gap_to_next_quantiles": {}
}
```

## Per-date breakdown

- per_date_csv: racing_ai\reports\model_dynamic_selection_by_date_2025.csv