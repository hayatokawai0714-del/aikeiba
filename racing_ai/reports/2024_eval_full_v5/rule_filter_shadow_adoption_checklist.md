# rule_filter_shadow_adoption_checklist

- evaluation_window_rows: 5
- avg_original_roi_30d: 0.8557449494949496
- avg_filtered_roi_30d: 0.40393772893772895
- original_profit_total_30d: -6420.0
- filtered_profit_total_30d: -1390.0
- zero_after_filter_high_days_30d: 5
- one_day_hit_dependency_days_30d: 0

## Checks
- [HOLD] ROI > 1.2 over additional 30+ days
- [HOLD] Filtered ROI > Original ROI
- [PASS] Filtered profit not excessively lower
- [HOLD] race_count_zero_after_filter not too high
- [PASS] No strong one-day hit dependency

- decision: SHADOW_CONTINUE
