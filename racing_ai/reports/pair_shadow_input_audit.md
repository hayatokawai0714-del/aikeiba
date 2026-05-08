# pair_shadow_input_audit

## 比較CSV生成処理の確認

- 生成スクリプト: [report_pair_shadow_comparison.py](/C:/Users/HND2205/Documents/git/aikeiba/racing_ai/scripts/report_pair_shadow_comparison.py)
- 入力: `candidate_pairs.parquet`（run-race-day成果物）
- 旧挙動/新挙動とも、スクリプト内部で `pair_selected_flag==True` のフィルタは実施していません。
- `model_top5_flag==True` のフィルタも実施していません。

## race_day 側の比較CSV生成

- 実装: [race_day.py](/C:/Users/HND2205/Documents/git/aikeiba/racing_ai/src/aikeiba/orchestration/race_day.py)
- `_write_race_day_artifacts()` で `decision_rows` から比較CSVを書き出し。
- ここでも明示的な `pair_selected_flag` 絞り込みはありません。

## candidate_pairs 実測（3日）

| race_date | candidate_pairs_rows | pair_selected_flag=True | False |
|---|---:|---:|---:|
| 2026-04-10 | 135 | 135 | 0 |
| 2026-04-11 | 130 | 130 | 0 |
| 2026-04-12 | 130 | 130 | 0 |

## 結論

- rule完全重複の主因は「比較CSVの後段フィルタ」ではなく、`candidate_pairs` 母集団自体が全件 `pair_selected_flag=True` であることです。
- そのため、比較CSVを all_candidates にしても、3日実測では non-rule が 0 のままです。
