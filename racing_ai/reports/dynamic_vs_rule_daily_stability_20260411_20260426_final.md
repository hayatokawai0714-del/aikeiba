# Dynamic vs Rule Daily Stability (Final) 2026-04-11..2026-04-26

- generated_at: 2026-05-01T20:58:59

## Daily Summary

| race_date | quality_ok_race_count | rule_selected_candidate_count | rule_selected_roi | model_dynamic_non_overlap_candidate_count | model_dynamic_non_overlap_roi | dynamic_minus_rule_roi | exclude_reason |
|---|---:|---:|---:|---:|---:|---:|---|
| 2026-04-11 | 34 | 12 | 0.391667 | 55 | 1.263636 | 0.87197 |  |
| 2026-04-12 | 0 | 0 |  | 0 |  |  | quality_ok_race_count=0 (expected_payout_mismatch) |
| 2026-04-18 | 33 | 165 | 0.776364 | 3 | 3.2 | 2.423636 |  |
| 2026-04-25 | 33 | 165 | 0.832727 | 11 | 0.518182 | -0.314545 |  |
| 2026-04-26 | 0 | 0 |  | 0 |  |  | quality_ok_race_count=0 (expected_payout_mismatch) |

## Notes

- ROI columns are computed on `result_quality_status=ok` races only.
- model_dynamic_non_overlap is defined here as: `model_dynamic_selected_flag==True` AND `pair_selected_flag==False`.
