# model_dynamic_edge_diagnostics_multi_day

- input_glob: racing_ai/reports/*/model_dynamic_edge_diagnostics.csv
- day_count: 3
- pair_edge all-negative days (max<0): 3
- pair_edge p95<0 days: 3
- positive_edge_pair_rate mean: 0.0
- positive_edge_race_rate mean: 0.0

## Daily Summary

 race_date  row_count  race_count  pair_edge_min  pair_edge_p05  pair_edge_p10  pair_edge_p25  pair_edge_p50  pair_edge_p75  pair_edge_p90  pair_edge_p95  pair_edge_p99  pair_edge_max  pair_edge_mean  positive_edge_pair_count  positive_edge_pair_rate  positive_edge_race_count  positive_edge_race_rate  pass_min_score_rate_current  pass_min_edge_rate_current  pass_min_gap_rate_current  pass_all_rate_current  selected_pair_count_current current_skip_reason_top  current_skip_reason_top_count
2026-04-10        135          27      -0.489684      -0.430843      -0.399519      -0.351109      -0.282822      -0.248581      -0.221984      -0.213876      -0.188180      -0.162979       -0.300275                         0                      0.0                         0                      0.0                     0.955556                         0.0                        0.0                    0.0                            0                     nan                            135
2026-04-11        130          26      -0.475536      -0.388145      -0.357483      -0.288234      -0.233145      -0.181953      -0.152812      -0.136038      -0.113503      -0.107283       -0.246032                         0                      0.0                         0                      0.0                     0.938462                         0.0                        0.0                    0.0                            0                     nan                            130
2026-04-12        130          26      -0.475536      -0.388663      -0.367916      -0.292130      -0.240560      -0.183369      -0.152812      -0.136038      -0.113503      -0.107283       -0.247671                         0                      0.0                         0                      0.0                     0.938462                         0.0                        0.0                    0.0                            0                     nan                            130

## pair_edge max by day

- 2026-04-10: pair_edge_max=-0.1629785830148886
- 2026-04-11: pair_edge_max=-0.1072831927523789
- 2026-04-12: pair_edge_max=-0.1072831927523789

## Comment

- `pair_edge_max < 0` が多い場合は、差分edgeのスケール不一致またはmarket proxy過大の可能性があります。
- 一部日だけ全負の場合は、日別データ品質または当日proxy構成の影響を疑ってください。