# rule_filter_shadow_ab_walk_forward_report

## A/B Stability
- A roi_mean/median: 2.043053906273674 / 1.4211065573770492
- B roi_mean/median: 2.587837318898934 / 1.8338226157191673
- A profit_sum: 122460.0
- B profit_sum: 163930.0
- A avg_dependency: 0.854920917696783
- B avg_dependency: 0.8360675970653062
- A median_candidate_count: 76.0
- B median_candidate_count: 68.0

- fixed shadow と risk-adjusted のどちらが安定か: B_risk_adjusted
- ROI重視なら: B_risk_adjusted
- 利益額重視なら: B_risk_adjusted
- 大当たり依存は改善したか: Yes
- risk-adjustedは過学習っぽいか: Low-to-moderate
- 本番採用ではなくshadow継続か: Yes
- 次の30日監視の採用候補: B_risk_adjusted
