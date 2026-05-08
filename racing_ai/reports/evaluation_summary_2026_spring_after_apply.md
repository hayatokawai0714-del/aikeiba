# Evaluation Summary 2026_spring (After Apply)

- generated_at: 2026-05-02T15:17:36

## Apply Counts

- results_applied_updates: 0
- payouts_applied_inserts: 0

## Coverage (Pairs CSV)

| metric | before | after |
|---|---:|---:|
| raw_actual_wide_hit_coverage | 0.960638 | 0.960638 |
| quality_filtered_actual_wide_hit_coverage | 0.538656 | 0.538656 |
| hit_rows_payout_coverage | 1.0 | 1.0 |
| quality_ok_race_count | 123 | 123 |

## ROI (quality_ok only)

| group | candidate_count | hit_rate | roi_proxy |
|---|---:|---:|---:|
| rule_selected | 360 | 0.222222 | 0.821667 |
| model_dynamic_non_overlap | 106 | 0.264151 | 1.19717 |

- dynamic_minus_rule_roi: 0.375503

## Daily Stability (quality_ok only)

- dynamic_minus_rule_roi_positive_days: 2
- dynamic_minus_rule_roi_negative_days: 2

## Notes

- `before` coverage uses the previously joined pairs CSV for the smaller spring subset (2026-04-04..2026-04-26).
- `after` coverage uses the joined pairs CSV after applying TARGET backfill into DuckDB for 2026 spring period.
