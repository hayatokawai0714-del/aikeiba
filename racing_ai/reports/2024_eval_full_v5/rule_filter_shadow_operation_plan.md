# rule_filter_shadow_operation_plan

## 固定条件の根拠
- pair_value_score >= 0.0591
- pair_edge >= 0.02
- 実odds期間でROI改善傾向を確認済み。

## ウォークフォワード結果（要約）
- 固定条件は複数foldでROI>1を維持。
- ただし買い目減少により利益額が細るfoldがある。

## なぜ本番変更ではなくshadow運用か
- ROIは改善余地がある一方、総利益と買い目母数のトレードオフが大きい。
- まず運用データを追加取得して安定性を確認する必要がある。

## 方針
- ROI重視なら有望。
- 利益額重視なら慎重運用。

## 監視すべき指標
- original_rule_candidate_count / filtered_rule_candidate_count
- buy_reduction_rate
- filtered ROI, profit, hit_rate
- race_count_zero_after_filter
- baseline 대비 ROI差・profit差

## 本番採用条件
- 追加30日以上でROI > 1.2
- 現行ruleよりROIが高い
- 利益額が極端に落ちない
- race_count_zero_after_filterが多すぎない
- 特定1日の大当たり依存でない
