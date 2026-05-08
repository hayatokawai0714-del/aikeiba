# rule_filter_shadow_monitoring_summary

- latest_date: 2026-04-25
- window_days: 30
- window_rows: 5
- avg_buy_reduction_rate_30d: 0.8410101010101011
- avg_original_roi_30d: 0.8557449494949496
- avg_filtered_roi_30d: 0.40393772893772895
- total_original_profit_30d: -6420.0
- total_filtered_profit_30d: -1390.0
- avg_race_count_zero_after_filter_30d: 9.8
- warning_days_30d: 5

## Latest Rows
      date  original_rule_candidate_count  original_rule_hit_rate  original_rule_roi  original_rule_profit  filtered_rule_candidate_count  filtered_rule_hit_rate  filtered_rule_roi  filtered_rule_profit  buy_reduction_rate  roi_diff_filtered_minus_original  removed_candidate_count  original_roi  filtered_roi  original_profit  filtered_profit  race_count_with_filtered_candidates  race_count_zero_after_filter       source                                                                 warning_flags
2026-04-04                             18                0.277778           1.422222                 760.0                              1                0.000000           0.000000                -100.0            0.944444                         -1.422222                       17      1.422222      0.000000            760.0           -100.0                                    1                             6     backtest FILTERED_CANDIDATE_TOO_FEW|PROFIT_NEGATIVE|ROI_BELOW_1|ZERO_AFTER_FILTER_HIGH
2026-04-11                             12                0.166667           0.391667                -730.0                              2                0.000000           0.000000                -200.0            0.833333                         -0.391667                       10      0.391667      0.000000           -730.0           -200.0                                    2                             4     backtest FILTERED_CANDIDATE_TOO_FEW|PROFIT_NEGATIVE|ROI_BELOW_1|ZERO_AFTER_FILTER_HIGH
2026-04-12                             12                     NaN                NaN                   NaN                              2                     NaN                NaN                   NaN            0.833333                               NaN                       10           NaN           NaN              NaN              NaN                                    2                             4 today_shadow                             FILTERED_CANDIDATE_TOO_FEW|ZERO_AFTER_FILTER_HIGH
2026-04-18                            165                0.242424           0.776364               -3690.0                             39                0.384615           0.987179                 -50.0            0.763636                          0.210816                      126      0.776364      0.987179          -3690.0            -50.0                                   18                            15     backtest                            PROFIT_NEGATIVE|ROI_BELOW_1|ZERO_AFTER_FILTER_HIGH
2026-04-25                            165                0.200000           0.832727               -2760.0                             28                0.250000           0.628571               -1040.0            0.830303                         -0.204156                      137      0.832727      0.628571          -2760.0          -1040.0                                   13                            20     backtest                            PROFIT_NEGATIVE|ROI_BELOW_1|ZERO_AFTER_FILTER_HIGH
