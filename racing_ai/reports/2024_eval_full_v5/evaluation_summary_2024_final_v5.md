# 2024 v5 Shadow Evaluation Summary (Final)

- generated_at: 2026-05-06T21:23:30
- reports_dir: racing_ai\reports\2024_eval_full_v5
- v4_dir: racing_ai\reports\2024_eval_full_v4

## Scope / Constraints

- Production logic was NOT changed (`race_day.py` untouched).
- `pair_selected_flag` semantics are unchanged (rule-based production selection flag).
- `model_dynamic` remains shadow-evaluation only.
- `actual_wide_hit` / `wide_payout` are evaluation-only signals.

## Critical Caveat (Market Proxy)

- **`market_proxy_source` is 100% `predictions_scaled_low_confidence` for 2024 v5.**
- This is NOT odds-derived; interpret ROI and any edge-related analyses cautiously.

## Headline Numbers (from v5 outputs)

- rule_selected ROI (quality_ok): 0.6746376811594202
- model_dynamic_non_overlap ROI (quality_ok): 0.2333333333333333
- dynamic_minus_rule_roi: -0.44130434782608696

## Evidence / Artifacts

### v4 vs v5 feature recovery
# pair_reranker Required Features Compare (v4 vs v5)

- generated_at: 2026-05-06T21:15:01
- pair_model_dir: racing_ai\data\models_compare\pair_reranker\pair_reranker_ts_v4
- v4_csv: racing_ai\reports\2024_eval_full_v4\pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv
- v5_csv: racing_ai\reports\2024_eval_full_v5\pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv
- required_feature_count: 20

## Highlights

- improved_feature_count: 0
- regressed_feature_count: 0

## Notes

- `filled_with_zero_rate` is a heuristic indicating many rows are zero after numeric coercion/fill; treat as a signal of missingness in the evaluation helper path.


### v4 vs v5 score distribution
# pair_model_score Distribution Compare (v4 vs v5)

- generated_at: 2026-05-06T21:15:21
- v4_csv: racing_ai\reports\2024_eval_full_v4\pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv
- v5_csv: racing_ai\reports\2024_eval_full_v5\pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv

## Summary

See CSV for full quantiles; key signal is `pair_model_score_std` and `gap_p90/p99` increasing in v5.


### Market proxy fallback usage
# Market Proxy Fallback Usage Audit

- generated_at: 2026-05-06T21:17:53
- input: racing_ai\reports\2024_eval_full_v5\pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv
- total_pair_rows: 149424

## Source counts (pair rows)

```json
{
  "predictions_scaled_low_confidence": 149424
}
```

## Score distribution by source

```json
{
  "predictions_scaled_low_confidence": {
    "n": 149424.0,
    "p50": 0.0463464604322446,
    "p90": 0.0463464604322446,
    "p99": 0.0520082457883097,
    "std": 0.005176043181412673
  }
}
```

## model_dynamic performance by source (selected rows only; ROI proxy)

```json
{
  "predictions_scaled_low_confidence": {
    "selected": 136,
    "roi_proxy": 0.7544117647058823
  }
}
```

## Notes

- `predictions_scaled_low_confidence` is NOT odds-derived; interpret ROI comparisons cautiously when it dominates.


### model_dynamic postcompute audit
# model_dynamic Postcompute Audit (from joined pairs CSV)

- generated_at: 2026-05-06T21:21:58
- input: racing_ai\reports\2024_eval_full_v5\pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv
- total_pair_rows: 149424
- race_count: 3454

## Counts

- model_dynamic_selected_count: 136
- model_dynamic_non_overlap_count: 42
- selected_race_count: 68
- zero_selected_race_count: 3386

## Skip reasons (race-level)

```json
{
  "DYNAMIC_SKIP_MODEL_SCORE_WEAK": 3386,
  "DYNAMIC_BUY_OK": 68
}
```

## Feature coverage

- pair_model_score_non_null_count: 149424
- pair_edge_non_null_count: 149424
- pair_model_score_gap_to_next_non_null_count: 149424

## Notes

- This audit reads existing columns; it does not recompute selection logic.


### Evaluation (quality_ok only)
# rule_vs_non_rule_candidate_evaluation

- quality_ok_only: True
- raw_actual_wide_hit_coverage: 0.9814219937894849
- quality_filtered_actual_wide_hit_coverage: 0.7980177213834457
- quality_ok_race_count: 2760
- quality_ng_race_count: 694
- quality_ok_candidate_count: 119243
- quality_ng_candidate_count: 30181

 candidate_count  hit_count  hit_rate  total_payout      cost  roi_proxy  hit_label_coverage_rate  payout_coverage_rate  avg_payout_per_hit  avg_pair_model_score  avg_pair_value_score  avg_pair_market_implied_prob  avg_pair_edge  avg_pair_edge_ratio                     group
           13800     1320.0  0.095652      931000.0 1380000.0   0.674638                      1.0                   1.0           705.30303              0.045201              0.038619                      0.005624       0.039578                  NaN             rule_selected
               0        NaN       NaN           NaN       0.0        NaN                      NaN                   NaN                 NaN                   NaN                   NaN                           NaN            NaN                  NaN       non_rule_model_top1
               0        NaN       NaN           NaN       0.0        NaN                      NaN                   NaN                 NaN                   NaN                   NaN                           NaN            NaN                  NaN       non_rule_model_top3
              33        2.0  0.060606         770.0    3300.0   0.233333                      1.0                   1.0           385.00000              0.106988              0.013586                      0.002333       0.104655                  NaN model_dynamic_non_overlap

### Expanded dynamic conditions (quality_ok only)
# expanded_dynamic_candidate_conditions_with_results

- quality_ok_only: True
- raw_actual_wide_hit_coverage: 0.9814219937894849
- quality_filtered_actual_wide_hit_coverage: 0.7980177213834457
- quality_ok_race_count: 2760
- quality_ng_race_count: 694
- dynamic_candidate_count: 93
- rule_candidate_count: 13800

edge_variant  variant_threshold  min_score  min_gap  default_k  max_k  eval_day_count  selected_pair_count_sum  selected_race_count_sum  rule_overlap_count_sum  rule_non_overlap_dynamic_pair_count_sum  rule_dynamic_overlap_rate_weighted  dynamic_hit_count_sum  dynamic_bet_count_sum  dynamic_total_payout_sum  rule_hit_count_sum  rule_bet_count_sum  rule_total_payout_sum  rule_non_overlap_dynamic_hit_count_sum  rule_non_overlap_dynamic_total_payout_sum  avg_pair_model_score_selected_mean  avg_pair_market_implied_prob_selected_mean  dynamic_cost_sum  rule_cost_sum  dynamic_hit_rate_overall  rule_hit_rate_overall  dynamic_roi_proxy_overall  rule_roi_proxy_overall  dynamic_minus_rule_roi  rule_non_overlap_dynamic_hit_rate_overall  rule_non_overlap_dynamic_roi_proxy_overall                                                                            source_file  selected_pair_count  selected_race_count  avg_selected_pairs_per_race  rule_overlap_count  rule_dynamic_overlap_rate  rule_non_overlap_dynamic_pair_count  rule_non_overlap_dynamic_hit_count  rule_non_overlap_dynamic_hit_rate  rule_non_overlap_dynamic_total_payout  rule_non_overlap_dynamic_roi_proxy  dynamic_hit_count  dynamic_bet_count  dynamic_hit_rate  dynamic_total_payout  dynamic_roi_proxy  rule_hit_count  rule_bet_count  rule_hit_rate  rule_total_payout  rule_roi_proxy  dynamic_minus_rule_hit_rate  avg_variant_score_selected  avg_pair_model_score_selected  avg_pair_market_implied_prob_selected    score_rank
    rank_gap                3.0       0.08    0.005          5      5             NaN                      NaN                      NaN                     NaN                                      NaN                                 NaN                    NaN                    NaN                       NaN                 NaN                 NaN                    NaN                                     NaN                                        NaN                                 NaN                                         NaN               NaN            NaN                       NaN                    NaN                        NaN                     NaN                0.428588                                        NaN                                         NaN racing_ai\reports\model_dynamic_edge_variant_grid_summary_expanded_3d_with_results.csv                194.0                 74.0                     2.621622                 0.0                   0.000000                                194.0                                 7.0                           0.036082                                 3300.0                            0.170103                7.0              194.0           0.11828                3300.0           1.103226             0.0            34.0       0.095652                0.0        0.674638                     0.022627                    7.613402                       0.188201                               0.055650 194101.833582
    rank_gap                1.0       0.10    0.010          5      5             NaN                      NaN                      NaN                     NaN                                      NaN                                 NaN                    NaN                    NaN                       NaN                 NaN                 NaN                    NaN                                     NaN                                        NaN                                 NaN                                         NaN               NaN            NaN                       NaN                    NaN                        NaN                     NaN                0.428588                                        NaN                                         NaN racing_ai\reports\model_dynamic_edge_variant_grid_summary_expanded_3d_with_results.csv                194.0                 87.0                     2.229885                 1.0                   0.005155                                193.0                                 5.0                           0.025907                                 3300.0                            0.170984                5.0              194.0           0.11828                3300.0           1.103226             0.0            34.0       0.095652                0.0        0.674638                     0.022627                    4.731959                       0.239018                               0.101167 193101.323418
    rank_gap                2.0       0.10    0.005          3      3             NaN                      NaN                      NaN                     NaN                                      NaN                                 NaN                    NaN                    NaN                       NaN                 NaN                 NaN                    NaN                                     NaN                                        NaN                                 NaN                                         NaN               NaN            NaN                       NaN                    NaN                        NaN                     NaN                0.428588                                        NaN                                         NaN racing_ai\reports\model_dynamic_edge_variant_grid_summary_expanded_3d_with_results.csv                187.0                 89.0                     2.101124                 0.0                   0.000000                                187.0                                 6.0                           0.032086                                 3300.0                            0.176471                6.0              187.0           0.11828                3300.0           1.103226             0.0            34.0       0.095652                0.0        0.674638                     0.022627                    6.598930                       0.210289                               0.073928 187101.901067
    rank_gap                2.0       0.10    0.005          5      3             NaN                      NaN                      NaN                     NaN                                      NaN                                 NaN                    NaN                    NaN                       NaN                 NaN                 NaN                    NaN                                     NaN                                        NaN                                 NaN                                         NaN               NaN            NaN                       NaN                    NaN                        NaN                     NaN                0.428588                                        NaN                                         NaN racing_ai\reports\model_dynamic_edge_variant_grid_summary_expanded_3d_with_results.csv                187.0                 89.0                     2.101124                 0.0                   0.000000                                187.0                                 6.0                           0.032086                                 3300.0                            0.176471                6.0              187.0           0.11828                3300.0           1.103226             0.0            34.0       0.095652                0.0        0.674638                     0.022627                    6.598930                       0.210289                               0.073928 187101.901067
    rank_gap                2.0       0.10    0.005          3      5             NaN                      NaN                      NaN                     NaN                                      NaN                                 NaN                    NaN                    NaN                       NaN                 NaN                 NaN                    NaN                                     NaN                                        NaN                                 NaN                                         NaN               NaN            NaN                       NaN                    NaN                        NaN                     NaN                0.428588                                        NaN                                         NaN racing_ai\reports\model_dynamic_edge_variant_grid_summary_expanded_3d_with_results.csv                187.0                 89.0                     2.101124                 0.0                   0.000000                                187.0                                 6.0                           0.032086                                 3300.0                            0.176471                6.0              187.0           0.11828                3300.0           1.103226             0.0            34.0       0.095652                0.0        0.674638                     0.022627                    6.598930                       0.210289                               0.073928 187101.901067
     pct_gap                0.2       0.06    0.000          3      3             NaN                      NaN                      NaN                     NaN                                      NaN                                 NaN                    NaN                    NaN                       NaN                 NaN                 NaN                    NaN                                     NaN                                        NaN                                 NaN                                         NaN               NaN            NaN                       NaN                    NaN                        NaN                     NaN                0.428588                                        NaN                                         NaN racing_ai\reports\model_dynamic_edge_variant_grid_summary_expanded_3d_with_results.csv                187.0                 81.0                     2.308642                 0.0                   0.000000                                187.0                                 4.0                           0.021390                                 3300.0                            0.176471                4.0              187.0           0.11828                3300.0           1.103226             0.0            34.0       0.095652                0.0        0.674638                     0.022627                    0.324090                       0.134412                               0.021657 187101.877460
     pct_gap                0.2       0.06    0.000          5      3             NaN                      NaN                      NaN                     NaN                                      NaN                                 NaN                    NaN                    NaN                       NaN                 NaN                 NaN                    NaN                                     NaN                                        NaN                                 NaN                                         NaN               NaN            NaN                       NaN                    NaN                        NaN                     NaN                0.428588                                        NaN                                         NaN racing_ai\reports\model_dynamic_edge_variant_grid_summary_expanded_3d_with_results.csv                187.0                 81.0                     2.308642                 0.0                   0.000000                                187.0                                 4.0                           0.021390                                 3300.0                            0.176471                4.0              187.0           0.11828                3300.0           1.103226             0.0            34.0       0.095652                0.0        0.674638                     0.022627                    0.324090                       0.134412                               0.021657 187101.877460
     pct_gap                0.2       0.06    0.000          3      5             NaN                      NaN                      NaN                     NaN                                      NaN                                 NaN                    NaN                    NaN                       NaN                 NaN                 NaN                    NaN                                     NaN                                        NaN                                 NaN                                         NaN               NaN            NaN                       NaN                    NaN                        NaN                     NaN                0.428588                                        NaN                                         NaN racing_ai\reports\model_dynamic_edge_variant_grid_summary_expanded_3d_with_results.csv                187.0                 81.0                     2.308642                 0.0                   0.000000                                187.0                                 4.0                           0.021390                                 3300.0                            0.176471                4.0              187.0           0.11828                3300.0           1.103226             0.0            34.0       0.095652                0.0        0.674638                     0.022627                    0.324090                       0.134412                               0.021657 187101.877460
    rank_gap                1.0       0.06    0.010          5      3             NaN                      NaN                      NaN                     NaN                                      NaN                                 NaN                    NaN                    NaN                       NaN                 NaN                 NaN                    NaN                                     NaN                                        NaN                                 NaN                                         NaN               NaN            NaN                       NaN                    NaN                        NaN                     NaN                0.428588                                        NaN                                         NaN racing_ai\reports\model_dynamic_edge_variant_grid_summary_expanded_3d_with_results.csv                187.0                 92.0                     2.032609                 1.0                   0.005348                                186.0                                 5.0                           0.026882                                 3300.0                            0.177419                5.0              187.0           0.11828                3300.0           1.103226             0.0            34.0       0.095652                0.0        0.674638                     0.022627                    4.786096                       0.231122                               0.099166 186101.361903
    rank_gap                1.0       0.06    0.010          3      5             NaN                      NaN                      NaN                     NaN                                      NaN                                 NaN                    NaN                    NaN                       NaN                 NaN                 NaN                    NaN                                     NaN                                        NaN                                 NaN                                         NaN               NaN            NaN                       NaN                    NaN                        NaN                     NaN                0.428588                                        NaN                                         NaN racing_ai\reports\model_dynamic_edge_variant_grid_summary_expanded_3d_with_results.csv                187.0                 92.0                     2.032609                 1.0                   0.005348                                186.0                                 5.0                           0.026882                                 3300.0                            0.177419                5.0              187.0           0.11828                3300.0           1.103226             0.0            34.0       0.095652                0.0        0.674638                     0.022627                    4.786096                       0.231122                               0.099166 186101.361903

### Daily stability
# dynamic_vs_rule_daily_stability

- date_range: 2024-01-06 to 2024-12-28
- quality_ok_only: True
- daily_rows: 106
- dynamic_minus_rule_roi_positive_days: 0

 race_date  quality_ok_race_count  rule_selected_candidate_count  rule_selected_hit_count  rule_selected_total_payout  rule_selected_roi  model_dynamic_non_overlap_candidate_count  model_dynamic_non_overlap_hit_count  model_dynamic_non_overlap_total_payout  model_dynamic_non_overlap_roi dynamic_minus_rule_roi
2024-01-06                     21                            105                       14                     10630.0           1.012381                                          0                                    0                                     0.0                            NaN                   <NA>
2024-01-07                     18                             90                       12                      5940.0           0.660000                                          0                                    0                                     0.0                            NaN                   <NA>
2024-01-08                     22                            110                        4                      8510.0           0.773636                                          0                                    0                                     0.0                            NaN                   <NA>
2024-01-13                     34                            170                       15                     12280.0           0.722353                                          0                                    0                                     0.0                            NaN                   <NA>
2024-01-14                     28                            140                       11                     18110.0           1.293571                                          0                                    0                                     0.0                            NaN                   <NA>
2024-01-20                     32                            160                       12                      9140.0           0.571250                                          0                                    0                                     0.0                            NaN                   <NA>
2024-01-21                     32                            160                       17                      8930.0           0.558125                                          0                                    0                                     0.0                            NaN                   <NA>
2024-01-27                     31                            155                       16                     14750.0           0.951613                                          0                                    0                                     0.0                            NaN                   <NA>
2024-01-28                     36                            180                       16                     10840.0           0.602222                                          0                                    0                                     0.0                            NaN                   <NA>
2024-02-03                     30                            150                       14                      8330.0           0.555333                                          0                                    0                                     0.0                            NaN                   <NA>
2024-02-04                     33                            165                       16                      5020.0           0.304242                                          0                                    0                                     0.0                            NaN                   <NA>
2024-02-10                     34                            170                       18                      9310.0           0.547647                                          0                                    0                                     0.0                            NaN                   <NA>
2024-02-11                     32                            160                       12                      6990.0           0.436875                                          0                                    0                                     0.0                            NaN                   <NA>
2024-02-17                     32                            160                        8                      6170.0           0.385625                                          0                                    0                                     0.0                            NaN                   <NA>
2024-02-18                     28                            140                       11                     15200.0           1.085714                                          0                                    0                                     0.0                            NaN                   <NA>
2024-02-24                     33                            165                       13                      8990.0           0.544848                                          0                                    0                                     0.0                            NaN                   <NA>
2024-02-25                     34                            170                        9                      5570.0           0.327647                                          0                                    0                                     0.0                            NaN                   <NA>
2024-03-02                     36                            180                       22                     12130.0           0.673889                                          0                                    0                                     0.0                            NaN                   <NA>
2024-03-03                     33                            165                       19                      9380.0           0.568485                                          0                                    0                                     0.0                            NaN                   <NA>
2024-03-09                     35                            175                       10                     11390.0           0.650857                                          0                                    0                                     0.0                            NaN                   <NA>
2024-03-10                     35                            175                       13                     26710.0           1.526286                                          0                                    0                                     0.0                            NaN                   <NA>
2024-03-16                     33                            165                       22                     12410.0           0.752121                                          0                                    0                                     0.0                            NaN                   <NA>
2024-03-17                     33                            165                       22                     10660.0           0.646061                                          0                                    0                                     0.0                            NaN                   <NA>
2024-03-23                     30                            150                       15                      5740.0           0.382667                                          0                                    0                                     0.0                            NaN                   <NA>
2024-03-24                     32                            160                       17                     12230.0           0.764375                                          0                                    0                                     0.0                            NaN                   <NA>
2024-03-30                     21                            105                        8                      4400.0           0.419048                                          0                                    0                                     0.0                            NaN                   <NA>
2024-03-31                     23                            115                       18                      7320.0           0.636522                                          0                                    0                                     0.0                            NaN                   <NA>
2024-04-06                     32                            160                       13                      5660.0           0.353750                                          0                                    0                                     0.0                            NaN                   <NA>
2024-04-07                     27                            135                        6                      3260.0           0.241481                                          0                                    0                                     0.0                            NaN                   <NA>
2024-04-13                     33                            165                       14                     37980.0           2.301818                                          0                                    0                                     0.0                            NaN                   <NA>
2024-04-14                     31                            155                       18                     10340.0           0.667097                                          0                                    0                                     0.0                            NaN                   <NA>
2024-04-20                     27                            135                       14                     10370.0           0.768148                                          0                                    0                                     0.0                            NaN                   <NA>
2024-04-21                     29                            145                       15                     11100.0           0.765517                                          0                                    0                                     0.0                            NaN                   <NA>
2024-04-27                     23                            115                       17                      8260.0           0.718261                                          0                                    0                                     0.0                            NaN                   <NA>
2024-04-28                     21                            105                        8                      2110.0           0.200952                                          0                                    0                                     0.0                            NaN                   <NA>
2024-05-04                     21                            105                        7                      2410.0           0.229524                                          0                                    0                                     0.0                            NaN                   <NA>
2024-05-05                     20                            100                       13                      4840.0           0.484000                                          0                                    0                                     0.0                            NaN                   <NA>
2024-05-11                     22                            110                        8                     37710.0           3.428182                                          0                                    0                                     0.0                            NaN                   <NA>
2024-05-12                     21                            105                       11                      5790.0           0.551429                                          0                                    0                                     0.0                            NaN                   <NA>
2024-05-18                     23                            115                       10                      6250.0           0.543478                                          0                                    0                                     0.0                            NaN                   <NA>
2024-05-19                     22                            110                       14                     14190.0           1.290000                                         26                                    2                                   770.0                       0.296154              -0.993846
2024-05-25                     20                            100                        8                      4340.0           0.434000                                          0                                    0                                     0.0                            NaN                   <NA>
2024-05-26                     21                            105                       12                      6450.0           0.614286                                          0                                    0                                     0.0                            NaN                   <NA>
2024-06-01                     22                            110                       12                      7840.0           0.712727                                          0                                    0                                     0.0                            NaN                   <NA>
2024-06-02                     22                            110                        9                      7530.0           0.684545                                          0                                    0                                     0.0                            NaN                   <NA>
2024-06-08                     34                            170                       12                     11390.0           0.670000                                          0                                    0                                     0.0                            NaN                   <NA>
2024-06-09                     35                            175                       13                     10870.0           0.621143                                          0                                    0                                     0.0                            NaN                   <NA>
2024-06-15                     30                            150                       18                      6550.0           0.436667                                          0                                    0                                     0.0                            NaN                   <NA>
2024-06-16                     32                            160                       13                      8740.0           0.546250                                          0                                    0                                     0.0                            NaN                   <NA>
2024-06-22                     28                            140                       14                     11500.0           0.821429                                          1                                    0                                     0.0                       0.000000              -0.821429
2024-06-23                     29                            145                       15                      8980.0           0.619310                                          0                                    0                                     0.0                            NaN                   <NA>
2024-06-29                     30                            150                       12                      8320.0           0.554667                                          0                                    0                                     0.0                            NaN                   <NA>
2024-06-30                     32                            160                       17                     10410.0           0.650625                                          0                                    0                                     0.0                            NaN                   <NA>
2024-07-06                     33                            165                       23                     25170.0           1.525455                                          0                                    0                                     0.0                            NaN                   <NA>
2024-07-07                     29                            145                       14                      7850.0           0.541379                                          0                                    0                                     0.0                            NaN                   <NA>
2024-07-13                     32                            160                       19                     15380.0           0.961250                                          0                                    0                                     0.0                            NaN                   <NA>
2024-07-14                     32                            160                       10                      8710.0           0.544375                                          0                                    0                                     0.0                            NaN                   <NA>
2024-07-20                     31                            155                       17                     13150.0           0.848387                                          0                                    0                                     0.0                            NaN                   <NA>
2024-07-21                     31                            155                       15                      8140.0           0.525161                                          0                                    0                                     0.0                            NaN                   <NA>
2024-07-27                     12                             60                        4                      1730.0           0.288333                                          0                                    0                                     0.0                            NaN                   <NA>
2024-07-28                     11                             55                        2                       750.0           0.136364                                          0                                    0                                     0.0                            NaN                   <NA>
2024-08-03                     12                             60                        5                      3630.0           0.605000                                          0                                    0                                     0.0                            NaN                   <NA>
2024-08-04                     10                             50                        5                      2120.0           0.424000                                          0                                    0                                     0.0                            NaN                   <NA>
2024-08-10                     18                             90                        7                      5690.0           0.632222                                          0                                    0                                     0.0                            NaN                   <NA>
2024-08-11                     23                            115                       15                      5180.0           0.450435                                          0                                    0                                     0.0                            NaN                   <NA>
2024-08-17                     23                            115                        6                      4670.0           0.406087                                          0                                    0                                     0.0                            NaN                   <NA>
2024-08-18                     20                            100                        7                      3910.0           0.391000                                          0                                    0                                     0.0                            NaN                   <NA>
2024-08-24                     16                             80                       10                      3620.0           0.452500                                          0                                    0                                     0.0                            NaN                   <NA>
2024-08-25                     18                             90                        5                      2790.0           0.310000                                          0                                    0                                     0.0                            NaN                   <NA>
2024-08-31                     20                            100                       10                      8560.0           0.856000                                          0                                    0                                     0.0                            NaN                   <NA>
2024-09-01                     23                            115                       14                      9370.0           0.814783                                          0                                    0                                     0.0                            NaN                   <NA>
2024-09-07                     21                            105                        9                      1860.0           0.177143                                          0                                    0                                     0.0                            NaN                   <NA>
2024-09-08                     24                            120                       17                      5820.0           0.485000                                          0                                    0                                     0.0                            NaN                   <NA>
2024-09-14                     21                            105                       13                     17810.0           1.696190                                          1                                    0                                     0.0                       0.000000               -1.69619
2024-09-15                     22                            110                       12                      5800.0           0.527273                                          2                                    0                                     0.0                       0.000000              -0.527273
2024-09-16                     19                             95                        7                      2790.0           0.293684                                          0                                    0                                     0.0                            NaN                   <NA>
2024-09-21                     18                             90                       11                      3370.0           0.374444                                          0                                    0                                     0.0                            NaN                   <NA>
2024-09-22                     23                            115                       12                      6990.0           0.607826                                          0                                    0                                     0.0                            NaN                   <NA>
2024-09-28                     19                             95                        7                      3870.0           0.407368                                          0                                    0                                     0.0                            NaN                   <NA>
2024-09-29                     24                            120                        9                      3260.0           0.271667                                          0                                    0                                     0.0                            NaN                   <NA>
2024-10-05                     21                            105                       16                      6490.0           0.618095                                          0                                    0                                     0.0                            NaN                   <NA>
2024-10-06                     22                            110                        9                      5140.0           0.467273                                          0                                    0                                     0.0                            NaN                   <NA>
2024-10-12                     12                             60                        6                      2190.0           0.365000                                          0                                    0                                     0.0                            NaN                   <NA>
2024-10-13                     23                            115                       11                     24980.0           2.172174                                          0                                    0                                     0.0                            NaN                   <NA>
2024-10-14                     11                             55                        6                      5120.0           0.930909                                          0                                    0                                     0.0                            NaN                   <NA>
2024-10-19                     23                            115                       11                      5340.0           0.464348                                          0                                    0                                     0.0                            NaN                   <NA>
2024-10-20                     23                            115                       16                      7690.0           0.668696                                          0                                    0                                     0.0                            NaN                   <NA>
2024-10-26                     23                            115                       18                      6850.0           0.595652                                          0                                    0                                     0.0                            NaN                   <NA>
2024-10-27                     21                            105                       15                      5030.0           0.479048                                          3                                    0                                     0.0                       0.000000              -0.479048
2024-11-02                     30                            150                       24                     13400.0           0.893333                                          0                                    0                                     0.0                            NaN                   <NA>
2024-11-03                     28                            140                       18                     18290.0           1.306429                                          0                                    0                                     0.0                            NaN                   <NA>
2024-11-09                     35                            175                       17                      9380.0           0.536000                                          0                                    0                                     0.0                            NaN                   <NA>
2024-11-10                     32                            160                       11                      3880.0           0.242500                                          0                                    0                                     0.0                            NaN                   <NA>
2024-11-16                     29                            145                       15                      8910.0           0.614483                                          0                                    0                                     0.0                            NaN                   <NA>
2024-11-17                     34                            170                       11                     10890.0           0.640588                                          0                                    0                                     0.0                            NaN                   <NA>
2024-11-23                     19                             95                        5                      1780.0           0.187368                                          0                                    0                                     0.0                            NaN                   <NA>
2024-11-24                     23                            115                        8                      3920.0           0.340870                                          0                                    0                                     0.0                            NaN                   <NA>
2024-11-30                     33                            165                       15                      9030.0           0.547273                                          0                                    0                                     0.0                            NaN                   <NA>
2024-12-01                     34                            170                       19                     12570.0           0.739412                                          0                                    0                                     0.0                            NaN                   <NA>
2024-12-07                     34                            170                       24                     10270.0           0.604118                                          0                                    0                                     0.0                            NaN                   <NA>
2024-12-08                     34                            170                       12                      5920.0           0.348235                                          0                                    0                                     0.0                            NaN                   <NA>
2024-12-14                     32                            160                       11                      7220.0           0.451250                                          0                                    0                                     0.0                            NaN                   <NA>
2024-12-15                     31                            155                       13                     15830.0           1.021290                                          0                                    0                                     0.0                            NaN                   <NA>
2024-12-21                     18                             90                        2                       410.0           0.045556                                          0                                    0                                     0.0                            NaN                   <NA>
2024-12-22                     21                            105                        9                      5030.0           0.479048                                          0                                    0                                     0.0                            NaN                   <NA>
2024-12-28                     21                            105                        6                      3180.0           0.302857                                          0                                    0                                     0.0                            NaN                   <NA>

### Threshold grid (quality_ok only)
# Model Dynamic Threshold Grid 2025 (quality_ok only)

- generated_at: 2026-05-06T21:23:23
- input: racing_ai\reports\2024_eval_full_v5\pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv
- rows(quality_ok pairs): 119243
- races(quality_ok): 2760
- dates(quality_ok): 106

## Output

- csv: racing_ai\reports\2024_eval_full_v5\model_dynamic_threshold_grid_2024_quality_ok_v5.csv

## Top 20 (ranked by non_overlap_count then ROI delta)

| edge_variant | thr | min_score | min_gap | k | max_k | selected | non_overlap | non_overlap_rate | dyn_roi | rule_roi | delta_roi | pos_days | neg_days |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| diff | 0.02 | 0.04 | 0.0 | 5 | 5 | 13800 | 11714 | 0.8488 | 0.569051 | 0.674638 | -0.105587 | 0 | 106 |
| diff | -0.1 | 0.04 | 0.0 | 5 | 5 | 13800 | 11708 | 0.8484 | 0.564159 | 0.674638 | -0.110478 | 0 | 106 |
| diff | -0.05 | 0.04 | 0.0 | 5 | 5 | 13800 | 11708 | 0.8484 | 0.564159 | 0.674638 | -0.110478 | 0 | 106 |
| diff | -0.02 | 0.04 | 0.0 | 5 | 5 | 13800 | 11708 | 0.8484 | 0.564159 | 0.674638 | -0.110478 | 0 | 106 |
| diff | 0.0 | 0.04 | 0.0 | 5 | 5 | 13800 | 11708 | 0.8484 | 0.564159 | 0.674638 | -0.110478 | 0 | 106 |
| diff | 0.02 | 0.04 | 0.0 | 3 | 3 | 8280 | 6460 | 0.7802 | 0.576147 | 0.674638 | -0.09849 | 0 | 106 |
| diff | 0.02 | 0.04 | 0.0 | 3 | 5 | 8280 | 6460 | 0.7802 | 0.576147 | 0.674638 | -0.09849 | 0 | 106 |
| diff | 0.02 | 0.04 | 0.0 | 5 | 3 | 8280 | 6460 | 0.7802 | 0.576147 | 0.674638 | -0.09849 | 0 | 106 |
| diff | 0.0 | 0.04 | 0.0 | 3 | 3 | 8280 | 6457 | 0.7798 | 0.575592 | 0.674638 | -0.099046 | 0 | 106 |
| diff | 0.0 | 0.04 | 0.0 | 3 | 5 | 8280 | 6457 | 0.7798 | 0.575592 | 0.674638 | -0.099046 | 0 | 106 |
| diff | 0.0 | 0.04 | 0.0 | 5 | 3 | 8280 | 6457 | 0.7798 | 0.575592 | 0.674638 | -0.099046 | 0 | 106 |
| diff | -0.1 | 0.04 | 0.0 | 3 | 3 | 8280 | 6456 | 0.7797 | 0.575713 | 0.674638 | -0.098925 | 0 | 106 |
| diff | -0.1 | 0.04 | 0.0 | 3 | 5 | 8280 | 6456 | 0.7797 | 0.575713 | 0.674638 | -0.098925 | 0 | 106 |
| diff | -0.1 | 0.04 | 0.0 | 5 | 3 | 8280 | 6456 | 0.7797 | 0.575713 | 0.674638 | -0.098925 | 0 | 106 |
| diff | -0.05 | 0.04 | 0.0 | 3 | 3 | 8280 | 6456 | 0.7797 | 0.575713 | 0.674638 | -0.098925 | 0 | 106 |
| diff | -0.05 | 0.04 | 0.0 | 3 | 5 | 8280 | 6456 | 0.7797 | 0.575713 | 0.674638 | -0.098925 | 0 | 106 |
| diff | -0.05 | 0.04 | 0.0 | 5 | 3 | 8280 | 6456 | 0.7797 | 0.575713 | 0.674638 | -0.098925 | 0 | 106 |
| diff | -0.02 | 0.04 | 0.0 | 3 | 3 | 8280 | 6456 | 0.7797 | 0.575713 | 0.674638 | -0.098925 | 0 | 106 |
| diff | -0.02 | 0.04 | 0.0 | 3 | 5 | 8280 | 6456 | 0.7797 | 0.575713 | 0.674638 | -0.098925 | 0 | 106 |
| diff | -0.02 | 0.04 | 0.0 | 5 | 3 | 8280 | 6456 | 0.7797 | 0.575713 | 0.674638 | -0.098925 | 0 | 106 |


## Interpretation Checklist

- If v5 score std / gap quantiles increased vs v4: the feature recovery is working.
- If non-overlap count increased and gap gate stops being all-skip: dynamic gating is meaningful again.
- If any ROI improvements are seen, re-check that they are not dominated by low-confidence proxy dates/venues.

## Next Decision

- Given 2024 v5 uses 100% low-confidence proxy, prioritize restoring odds-derived market proxy for 2024 before making go/no-go calls for 2023 expansion.
- If odds cannot be restored historically, consider ROI-oriented retraining that does not rely on odds proxy features, or redesign dynamic selection without edge components.
