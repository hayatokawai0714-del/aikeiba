# rule_filter_shadow_ab_monitoring_summary

- latest_date: 2026-04-25
- window_days: 30

## fixed_A
- rows_30d: 5
- avg_filtered_roi_30d: 0.40393772893772895
- total_filtered_profit_30d: -1390.0
- avg_buy_reduction_rate_30d: 0.8410101010101011
- warning_days_30d: 5

## risk_adjusted_B
- rows_30d: 5
- avg_filtered_roi_30d: 0.6205357142857143
- total_filtered_profit_30d: 1150.0
- avg_buy_reduction_rate_30d: 0.9043434343434343
- warning_days_30d: 5

## Latest Rows
      date  condition_name  original_rule_candidate_count  filtered_candidate_count  removed_candidate_count  buy_reduction_rate  filtered_roi  filtered_profit  race_count_with_filtered_candidates  race_count_zero_after_filter  baseline_rule_roi  baseline_rule_profit                                                                 warning_flags    date_dt
2026-04-04         fixed_A                             18                         1                       17            0.944444      0.000000           -100.0                                  1.0                           6.0           1.422222                 760.0 FILTERED_CANDIDATE_TOO_FEW|PROFIT_NEGATIVE|ROI_BELOW_1|ZERO_AFTER_FILTER_HIGH 2026-04-04
2026-04-04 risk_adjusted_B                             18                         1                       17            0.944444      0.000000           -100.0                                  1.0                           6.0           1.422222                 760.0 FILTERED_CANDIDATE_TOO_FEW|PROFIT_NEGATIVE|ROI_BELOW_1|ZERO_AFTER_FILTER_HIGH 2026-04-04
2026-04-11         fixed_A                             12                         2                       10            0.833333      0.000000           -200.0                                  2.0                           4.0           0.391667                -730.0 FILTERED_CANDIDATE_TOO_FEW|PROFIT_NEGATIVE|ROI_BELOW_1|ZERO_AFTER_FILTER_HIGH 2026-04-11
2026-04-11 risk_adjusted_B                             12                         1                       11            0.916667      0.000000           -100.0                                  1.0                           5.0           0.391667                -730.0 FILTERED_CANDIDATE_TOO_FEW|PROFIT_NEGATIVE|ROI_BELOW_1|ZERO_AFTER_FILTER_HIGH 2026-04-11
2026-04-12         fixed_A                             12                         2                       10            0.833333           NaN              NaN                                  NaN                           NaN                NaN                   NaN                                                    FILTERED_CANDIDATE_TOO_FEW 2026-04-12
2026-04-12 risk_adjusted_B                             12                         0                       12            1.000000           NaN              NaN                                  NaN                           NaN                NaN                   NaN                                                    FILTERED_CANDIDATE_TOO_FEW 2026-04-12
2026-04-18         fixed_A                            165                        39                      126            0.763636      0.987179            -50.0                                 18.0                          15.0           0.776364               -3690.0                            PROFIT_NEGATIVE|ROI_BELOW_1|ZERO_AFTER_FILTER_HIGH 2026-04-18
2026-04-18 risk_adjusted_B                            165                        28                      137            0.830303      1.128571            360.0                                 19.0                          14.0           0.776364               -3690.0                                 ONE_DAY_HIT_DEPENDENCY|ZERO_AFTER_FILTER_HIGH 2026-04-18
2026-04-25         fixed_A                            165                        28                      137            0.830303      0.628571          -1040.0                                 13.0                          20.0           0.832727               -2760.0                            PROFIT_NEGATIVE|ROI_BELOW_1|ZERO_AFTER_FILTER_HIGH 2026-04-25
2026-04-25 risk_adjusted_B                            165                        28                      137            0.830303      1.353571            990.0                                 15.0                          18.0           0.832727               -2760.0                                 ONE_DAY_HIT_DEPENDENCY|ZERO_AFTER_FILTER_HIGH 2026-04-25
