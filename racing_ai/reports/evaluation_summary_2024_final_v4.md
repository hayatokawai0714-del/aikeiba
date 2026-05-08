# Evaluation Summary 2024 (Final v4)

generated_at: 2026-05-06

## What Broke in 2024 (Root Cause)

2024 evaluation initially failed because:

- `pair_model_score` was **all-NaN** in the expanded candidate pool.
- This was caused by LightGBM inference failing with:
  - `ValueError: train and valid dataset categorical_feature do not match.`

Impact:

- `pair_edge` also became all-NaN.
- Evaluation-only `model_dynamic` postcompute (which relies on `pair_model_score`) resulted in:
  - `DYNAMIC_SKIP_MODEL_SCORE_WEAK` for all races
  - `model_dynamic_selected_flag=True` count = 0
  - `model_dynamic_non_overlap` evaluation was impossible.

## Fix (Evaluation-only; production unchanged)

We fixed the pair_reranker inference in the **evaluation helper path only** by:

- loading `pair_reranker_ts_v4/model.txt` as a LightGBM `Booster`
- building the inference matrix strictly in the feature order from `meta.json`
- forcing a **numeric numpy matrix** (`float32`) to `Booster.predict` (not a DataFrame)
- filling missing/NaN/inf with `0.0`

This avoids the DataFrame→Dataset path that triggers categorical feature mismatch.

Production code unchanged:

- `race_day.py` untouched
- rule selection untouched
- `pair_selected_flag` meaning unchanged

## Confirmation (2024-01-06 sample)

After the fix, for 2024-01-06 expanded pool:

- expanded_rows: 1063
- `pair_model_score_non_null_count`: 1063
- score range roughly: min~0.0430 / median~0.0459 / max~0.0469

## Evaluation (2024, quality_ok subset, full-year combined)

Inputs:

- joined pairs (combined seasons):
  - `racing_ai/reports/2024_eval_full_v4/pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv`
- evaluation (quality_ok only):
  - `racing_ai/reports/2024_eval_full_v4/rule_vs_non_rule_candidate_evaluation_20240106_20241228_external_priority_quality_ok_v4.csv`
  - `racing_ai/reports/2024_eval_full_v4/dynamic_vs_rule_daily_stability_20240106_20241228_v4.csv`

Coverage (quality gate):

- quality_ok_race_count: 2760 / total_race_count: 3454
- quality_filtered_actual_wide_hit_coverage (ok only): 1.0
- quality_ok payout coverage: 1.0

ROI comparison (quality_ok only):

- rule_selected ROI: 0.674638 (candidate_count=13800)
- model_dynamic_non_overlap ROI: 0.455128 (candidate_count=12500)
- dynamic_minus_rule_roi: -0.219510

Daily stability (quality_ok only):

- evaluated_date_count: 106
- rule_comparable_date_count: 106
- dynamic_minus_rule_roi > 0 days: 28
- dynamic_minus_rule_roi < 0 days: 78

Interpretation:

- 2024 evaluation is now **fully runnable** with reliable labels and payouts on the quality_ok subset.
- Under current evaluation-only dynamic rules, **rule is stronger than model_dynamic_non_overlap** in 2024.

## Threshold Grid (2024, quality_ok subset, final)

`racing_ai/reports/model_dynamic_threshold_grid_2024_quality_ok_final.csv`:

- positive conditions (dynamic_minus_rule_roi > 0): 0
- best dynamic_minus_rule_roi: -0.100979 (still negative)

Interpretation:

- Relaxing thresholds within the tested grid did not produce a parameter set that beats rule ROI in 2024.

## Next Step

Proceed to 2023 using the same external-first evaluation contract, but treat 2024/2025 as evidence that:

- dynamic (as currently designed) is not robustly better than rule across years,
- improvements likely require either:
  - different selection logic, or
  - a stronger/value-aware pair model, or
  - changes in candidate pool design (evaluation-only experiments first).
