# rule_filter_shadow_risk_adjusted_report

## Fixed Shadow Stability
- fixed_roi: 2.651866801210898
- fixed_profit: 163700.0
- fixed_monthly_positive_count: 7
- fixed_top_day_dependency_ratio: 0.35571166768478923
- fixed_candidate_count: 991

## Risk-Adjusted Best Candidate
- condition: value>=0.0489266953508144, edge>=0.02, max_per_race=-1, max_per_day=-1, rank_cap=-1, odds=ALL, pop=longshot|semi_long|mid
- roi: 4.118725099601594
- profit: 234840.0
- monthly_positive_count: 7
- top_day_dependency_ratio: 0.2737608584568217
- candidate_count: 753

## Interpretation
- fixed shadow is strong on ROI but not fully stable due to dependency spikes.
- lowering dependency generally reduces ROI and/or candidate_count.
- risk-adjusted candidates can retain positive profit, but often with fewer bets.

## Decision
- production_candidate: not yet
- action: continue shadow monitoring
