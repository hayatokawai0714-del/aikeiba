# rule_filter_shadow_ab_operation_plan

- Bを主候補にする理由: AよりWF指標（ROI/利益/baseline超過fold）で優位だったため。
- Aを対照群として残す理由: 市況変化時の劣化検知ベースラインとして有効だから。
- 本番ruleをまだ変えない理由: 直近30日監視で再現性と依存度低下を確認しきっていないため。

## 30日監視で見る指標
- filtered_roi / filtered_profit（A/B）
- A対BのROI差
- baseline対BのROI差
- buy_reduction_rate
- race_count_zero_after_filter
- ZERO_AFTER_FILTER_HIGH / ONE_DAY_HIT_DEPENDENCY の発生日数
- filtered_candidate_count の中央値

## 本番採用条件
- 追加30日以上
- B filtered_roi > 1.2
- B filtered_profit > 0
- B ROI > A ROI
- B ROI > baseline ROI
- ZERO_AFTER_FILTER_HIGHが頻発しない
- ONE_DAY_HIT_DEPENDENCYが極端でない
- 買い目数が少なすぎない

- decision_now: shadow継続（A/B併走、B主候補）
