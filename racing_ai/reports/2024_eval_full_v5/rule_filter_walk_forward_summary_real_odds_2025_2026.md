# rule_filter_walk_forward_summary_real_odds_2025_2026

## Fixed Condition
- pair_value_score >= 0.0591
- pair_edge >= 0.02

### Fixed Walk-Forward
- test_positive_fold_count: 5
- test_negative_fold_count: 3
- test_roi_mean: 2.043053906273674
- test_roi_median: 1.4211065573770492
- test_profit_total: 122460.0
- baseline_outperform_fold_count: 5
- test_candidate_count_mean: 92.66666666666667
- test_candidate_count_std: 50.21702898420017

### Optimized Walk-Forward (train-only)
- test_positive_fold_count: 5
- test_negative_fold_count: 3
- test_roi_mean: 1.962840547761521
- test_roi_median: 1.4991083400375436
- test_profit_total: 69670.0
- baseline_outperform_fold_count: 8
- test_candidate_count_mean: 54.55555555555556
- test_candidate_count_std: 40.432385259563624

## Judgment
- 固定条件は未来期間でも有効か: Yes (test ROI mean=2.0431)
- 最適化条件は過剰最適化していないか: Likely acceptable (test ROI mean=1.9628)
- ROI重視なら採用候補か: Yes
- 利益額重視なら採用候補か: Conditional
- 本番ruleを変えるべきか: まだ変更せず（shadow継続推奨）
- まずshadow運用すべきか: Yes
