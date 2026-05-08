# pair_market_proxy_audit

## 1. 生成式（実装確認）
対象: [race_day.py](/C:/Users/HND2205/Documents/git/aikeiba/racing_ai/src/aikeiba/orchestration/race_day.py)

`pair_market_implied_prob` は `_pair_market_implied_prob_from_row` で次順に計算されています。
1. ペアオッズ列 (`pair_wide_odds` / `wide_odds` / `odds_wide` / `pair_odds`) があれば `1/odds`
2. それが無ければ `horse1_market_top3_proxy * horse2_market_top3_proxy`
3. それも無ければ `(pair_fused_prob_sum - pair_ai_market_gap_sum) / 2`

今回3日データでは、比較CSVの実測分布から `pair_market_implied_prob` が `pair_model_score` を恒常的に上回っており、差分edgeが全負になっています。

## 2. 過大化の所見（3日）
対象ファイル: [pair_market_proxy_variant_audit.csv](/C:/Users/HND2205/Documents/git/aikeiba/racing_ai/reports/pair_market_proxy_variant_audit.csv)

- current proxy (`pair_market_implied_prob`) の `model_minus_proxy_p50` は 3日すべて負
- `positive_edge_rate` は 3日すべて `0.0`
- `model_over_proxy_ratio_p50` は約 `0.35〜0.40`

これは current proxy が model score より高尺度である可能性を示します。

## 3. 代替proxyとの比較（監査専用）
- `harmonic_proxy` / `min_proxy` は current より尺度が近く、`positive_edge_rate` が上がる
- `product_proxy` は逆に低すぎ、`positive_edge_rate=1.0`（過補正）
- `normalized_current` / `rank_proxy` は順位系で、差分尺度の歪みを避けやすい

## 4. 結論
- current proxy は少なくともこの3日では過大寄り
- 差分edge (`model - current`) をそのまま gate に使うと全skip化しやすい
- 評価専用では rank/ratio 系指標が有効だが、本番反映は別タスクで検証が必要
