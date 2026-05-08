# candidate_pair_generation_audit

## 1. 生成経路（コード確認）
対象: [race_day.py](/C:/Users/HND2205/Documents/git/aikeiba/racing_ai/src/aikeiba/orchestration/race_day.py), [wide_rules.py](/C:/Users/HND2205/Documents/git/aikeiba/racing_ai/src/aikeiba/decision/wide_rules.py)

候補は以下の順で生成されます。
1. `generate_wide_candidates_rule_based(...)` で馬ペア候補を作成
   - `axis_k=1`
   - `partner_k=min(6, len(horse_nos))`
2. 候補に `pair_value_score` を付与し、降順ソート
3. `pair_top_n_selected`（実装上は上位5）を `pair_selected_flag=True`
4. その後に pair model score / dynamic指標を付与

## 2. 構造上の制約
- `pair_shadow_pair_comparison.csv` は `pair_selected_flag=True` の集合のみを出力しているため、非ruleペアが含まれない
- その状態での shadow比較は、どのedge variantを使っても rule非重複を作れない

## 3. 実測（3日）
対象: [candidate_pool_rule_dominance.csv](/C:/Users/HND2205/Documents/git/aikeiba/racing_ai/reports/candidate_pool_rule_dominance.csv)

- `model_top1_is_rule` は全レース true
- `non_rule_count` は全レース 0
- `model_top3_non_rule_count` / `model_top5_non_rule_count` / `model_top10_non_rule_count` すべて 0

## 4. 結論
rule完全重複の主因は、選定ロジック以前に「評価入力CSVが rule候補のみ」である点です。
この状態では、model_dynamic の追加価値（rule非重複）を統計的に評価できません。
