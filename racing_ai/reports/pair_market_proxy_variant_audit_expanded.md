# pair_market_proxy_variant_audit

- input_files: 3
- output_csv: racing_ai\reports\pair_market_proxy_variant_audit_expanded.csv

## Proxy Summary

        proxy_name  days  positive_edge_rate_mean  positive_edge_race_rate_mean  model_minus_proxy_p50_mean  model_over_proxy_ratio_p50_mean
calibrated_current     3                 0.000000                      0.000000                   -0.843365                         0.154802
           current     3                 0.000000                      0.000000                   -0.252176                         0.383067
    harmonic_proxy     3                 0.000000                      0.000000                         NaN                              NaN
         min_proxy     3                 0.000000                      0.000000                         NaN                              NaN
normalized_current     3                 0.095916                      0.479582                   -0.440276                         0.266207
     product_proxy     3                 0.000000                      0.000000                         NaN                              NaN
        rank_proxy     3                 0.095916                      0.479582                   -0.440276                         0.266207
sqrt_product_proxy     3                 0.000000                      0.000000                         NaN                              NaN

## Daily × Proxy

 race_date         proxy_name  non_null_count      min      p10      p25      p50      p75      p90      p95      max     mean  pair_model_score_p50  model_minus_proxy_p50  positive_edge_count  positive_edge_rate  positive_edge_race_count  positive_edge_race_rate  model_over_proxy_ratio_p50  model_over_proxy_ratio_p90
2026-04-10 calibrated_current             135 0.817017 0.892550 0.933660 0.993085 1.056384 1.122560 1.139297 1.290310 1.000000              0.153757              -0.831301                    0            0.000000                         0                 0.000000                    0.153716                    0.208572
2026-04-10            current             135 0.291129 0.362321 0.402964 0.452658 0.530003 0.567818 0.600754 0.659085 0.461351              0.153757              -0.282822                    0            0.000000                         0                 0.000000                    0.350016                    0.462092
2026-04-10     harmonic_proxy               0      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN              0.153757                    NaN                    0            0.000000                         0                 0.000000                         NaN                         NaN
2026-04-10          min_proxy               0      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN              0.153757                    NaN                    0            0.000000                         0                 0.000000                         NaN                         NaN
2026-04-10 normalized_current             135 0.200000 0.200000 0.400000 0.600000 0.800000 1.000000 1.000000 1.000000 0.600000              0.153757              -0.446115                   16            0.118519                        16                 0.592593                    0.256475                    1.029960
2026-04-10      product_proxy               0      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN              0.153757                    NaN                    0            0.000000                         0                 0.000000                         NaN                         NaN
2026-04-10         rank_proxy             135 0.200000 0.200000 0.400000 0.600000 0.800000 1.000000 1.000000 1.000000 0.600000              0.153757              -0.446115                   16            0.118519                        16                 0.592593                    0.256475                    1.029960
2026-04-10 sqrt_product_proxy               0      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN              0.153757                    NaN                    0            0.000000                         0                 0.000000                         NaN                         NaN
2026-04-11 calibrated_current             130 0.805967 0.869281 0.922251 1.002623 1.063851 1.131990 1.161650 1.239620 1.000000              0.159629              -0.850340                    0            0.000000                         0                 0.000000                    0.155294                    0.208171
2026-04-11            current             130 0.247915 0.304676 0.330965 0.392748 0.445970 0.533634 0.570924 0.615755 0.401814              0.159629              -0.233145                    0            0.000000                         0                 0.000000                    0.399874                    0.564876
2026-04-11     harmonic_proxy               0      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN              0.159629                    NaN                    0            0.000000                         0                 0.000000                         NaN                         NaN
2026-04-11          min_proxy               0      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN              0.159629                    NaN                    0            0.000000                         0                 0.000000                         NaN                         NaN
2026-04-11 normalized_current             130 0.200000 0.200000 0.400000 0.600000 0.800000 1.000000 1.000000 1.000000 0.600000              0.159629              -0.437356                   11            0.084615                        11                 0.423077                    0.271074                    0.962980
2026-04-11      product_proxy               0      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN              0.159629                    NaN                    0            0.000000                         0                 0.000000                         NaN                         NaN
2026-04-11         rank_proxy             130 0.200000 0.200000 0.400000 0.600000 0.800000 1.000000 1.000000 1.000000 0.600000              0.159629              -0.437356                   11            0.084615                        11                 0.423077                    0.271074                    0.962980
2026-04-11 sqrt_product_proxy               0      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN              0.159629                    NaN                    0            0.000000                         0                 0.000000                         NaN                         NaN
2026-04-12 calibrated_current             130 0.805967 0.866524 0.922251 1.000462 1.067136 1.142717 1.161184 1.239620 1.000000              0.159270              -0.848454                    0            0.000000                         0                 0.000000                    0.155396                    0.208171
2026-04-12            current             130 0.247915 0.304676 0.331638 0.397223 0.449819 0.533634 0.570924 0.615755 0.403344              0.159270              -0.240560                    0            0.000000                         0                 0.000000                    0.399310                    0.563587
2026-04-12     harmonic_proxy               0      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN              0.159270                    NaN                    0            0.000000                         0                 0.000000                         NaN                         NaN
2026-04-12          min_proxy               0      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN              0.159270                    NaN                    0            0.000000                         0                 0.000000                         NaN                         NaN
2026-04-12 normalized_current             130 0.200000 0.200000 0.400000 0.600000 0.800000 1.000000 1.000000 1.000000 0.600000              0.159270              -0.437356                   11            0.084615                        11                 0.423077                    0.271074                    0.963212
2026-04-12      product_proxy               0      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN              0.159270                    NaN                    0            0.000000                         0                 0.000000                         NaN                         NaN
2026-04-12         rank_proxy             130 0.200000 0.200000 0.400000 0.600000 0.800000 1.000000 1.000000 1.000000 0.600000              0.159270              -0.437356                   11            0.084615                        11                 0.423077                    0.271074                    0.963212
2026-04-12 sqrt_product_proxy               0      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN      NaN              0.159270                    NaN                    0            0.000000                         0                 0.000000                         NaN                         NaN

## Notes

- `current` は既存 pair_market_implied_prob です。
- `product/sqrt/min/harmonic` は horse-level確率復元ができた行でのみ有効です。
- 復元不能行は NA になります（候補データの情報不足）。