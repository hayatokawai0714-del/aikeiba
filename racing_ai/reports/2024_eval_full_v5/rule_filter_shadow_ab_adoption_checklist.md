# rule_filter_shadow_ab_adoption_checklist

- window_rows_B: 5
- B_avg_roi_30d: 0.6205357142857143
- B_profit_sum_30d: 1150.0
- A_avg_roi_30d: 0.40393772893772895
- baseline_avg_roi_30d: 0.8557449494949496
- B_median_candidate_count_30d: 1.0
- B_ZERO_AFTER_FILTER_HIGH_days_30d: 4
- B_ONE_DAY_HIT_DEPENDENCY_days_30d: 2

## Checks
- [HOLD] 30日以上の観測
- [HOLD] B filtered_roi > 1.2
- [PASS] B filtered_profit > 0
- [PASS] B ROI > A ROI
- [HOLD] B ROI > baseline ROI
- [HOLD] ZERO_AFTER_FILTER_HIGHが頻発しない
- [PASS] ONE_DAY_HIT_DEPENDENCYが極端でない
- [HOLD] 買い目数が少なすぎない

- decision: SHADOW_CONTINUE
