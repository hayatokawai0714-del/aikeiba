# rule_filter_shadow_final_backtest_report

- period: 2025-04-20 to 2026-04-26
- fixed_condition: pair_value_score >= 0.0591 and pair_edge >= 0.02

## Overall
- original_roi: 1.5031403336604514
- filtered_roi: 2.651866801210898
- original_profit: 256350.0
- filtered_profit: 163700.0
- buy_reduction_rate: 0.80549558390579

## Monthly Stability
- months_with_filtered_roi_gt_1: 7/10
  month  filtered_roi  filtered_profit  buy_reduction_rate
2025-04      1.016000             40.0            0.848485
2025-05      3.947143          41260.0            0.820513
2025-06      0.766129          -1450.0            0.860674
2025-07      2.431967          17470.0            0.804800
2025-08      3.583237          44690.0            0.794048
2025-09      1.125000            950.0            0.800000
2025-10      1.717213           8750.0            0.760784
2025-11      5.218939          55690.0            0.760000
2025-12      0.665217          -2310.0            0.843182
2026-04      0.801429          -1390.0            0.805556

## Venue Bias (Top filtered profit)
venue  filtered_roi  filtered_profit
    4      6.918447         121920.0
    3      8.697222          55420.0
    7      1.148611           1070.0
    6      1.092157            470.0
   10      0.736667           -790.0

## Surface Bias
surface  filtered_roi  filtered_profit
      ダ      3.509500         100380.0
      芝      2.155242          57300.0
     54      2.908000           4770.0
     56      2.010000           1010.0
      B      2.025000            820.0
     52      1.780000            780.0
     00      1.044444             40.0
     57      1.075000             30.0
     ?@           NaN              0.0
     @?           NaN              0.0
      C      0.600000           -560.0
      A      0.420000           -870.0

## Big Hit Dependency
- one_day_hit_dependency_month_count: 7
  month  filtered_profit_total    top_day  top_day_profit  top_day_dependency_ratio  one_day_hit_dependency_flag
2025-04                   40.0 2025-04-26            40.0                  1.000000                            1
2025-05                41260.0 2025-05-18         41310.0                  1.001212                            1
2025-06                -1450.0 2025-06-21           310.0                       NaN                            0
2025-07                17470.0 2025-07-27         21050.0                  1.204923                            1
2025-08                44690.0 2025-08-10         24400.0                  0.545983                            1
2025-09                  950.0 2025-09-06           800.0                  0.842105                            1
2025-10                 8750.0 2025-10-19         10460.0                  1.195429                            1
2025-11                55690.0 2025-11-23         58230.0                  1.045610                            1
2025-12                -2310.0 2025-12-20           440.0                       NaN                            0
2026-04                -1390.0 2026-04-18           -50.0                       NaN                            0

## Stability vs Original Rule
- ROI stability: improved
- Profit stability: reduced
- decision: shadow continue (do not switch production yet)
