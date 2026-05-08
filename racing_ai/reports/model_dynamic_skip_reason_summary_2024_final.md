# Model Dynamic Skip Reason Summary

- generated_at: 2026-05-06T15:50:14
- input: racing_ai\reports\2024_eval_full_v4\pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv

## Overall

- model_dynamic_selected_count: 16873
- model_dynamic_selected_race_count: 3423
- model_dynamic_zero_selected_race_count: 31

## Top Skip Reasons (selected rows only)

| skip_reason | selected_row_count | selected_race_count |
|---|---:|---:|
| DYNAMIC_BUY_OK | 16873 | 3423 |

## Top Skip Reasons (race-level; includes zero-selected races)

| skip_reason | race_count |
|---|---:|
| DYNAMIC_BUY_OK | 3423 |
| DYNAMIC_SKIP_EDGE_WEAK | 31 |

## Distributions (all rows)

```json
{
  "model_dynamic_final_score": {
    "min": 0.0430388819452584,
    "p01": 0.0441656373101601,
    "p05": 0.0457026522545956,
    "p10": 0.0458864882913449,
    "p25": 0.0458864882913449,
    "p50": 0.0458864882913449,
    "p75": 0.0458864882913449,
    "p90": 0.0458864882913449,
    "p95": 0.0458864882913449,
    "p99": 0.0468957274998235,
    "max": 0.0468957274998235
  },
  "pair_edge": {
    "min": -0.1202264241771905,
    "p01": -0.1175626538678533,
    "p05": -0.0969706545657979,
    "p10": -0.0691856552157394,
    "p25": -0.0589815735830863,
    "p50": -0.0209924926003747,
    "p75": -0.0209924926003747,
    "p90": 0.0099717503207326,
    "p95": 0.0099717503207326,
    "p99": 0.04093599324184,
    "max": 0.0419452324503185
  },
  "pair_model_score": {
    "min": 0.0430388819452584,
    "p01": 0.0441656373101601,
    "p05": 0.0457026522545956,
    "p10": 0.0458864882913449,
    "p25": 0.0458864882913449,
    "p50": 0.0458864882913449,
    "p75": 0.0458864882913449,
    "p90": 0.0458864882913449,
    "p95": 0.0458864882913449,
    "p99": 0.0468957274998235,
    "max": 0.0468957274998235
  },
  "pair_model_score_gap_to_next": {
    "min": 0.0,
    "p01": 0.0,
    "p05": 0.0,
    "p10": 0.0,
    "p25": 0.0,
    "p50": 0.0,
    "p75": 0.0,
    "p90": 0.0,
    "p95": 0.0004771237423761,
    "p99": 0.0010665072141847,
    "max": 0.0021982649439923
  }
}
```

## Per-date breakdown

- by_date_csv: racing_ai\reports\model_dynamic_skip_reason_by_date_2024_final.csv
