# pair_reranker_report

- generated_at: 2026-04-29T12:48:24
- model_version: pair_reranker_v1
- train_rows: 116
- validation_rows: 49
- auc: 0.7166666666666668
- logloss: 0.47758844079243634

## Warnings
- small_sample_warning:rows_less_than_500
- time_split_warning:single_race_date_only

## Hit Rate by Score Decile
- decile=0 cnt=7 hit=1 hit_rate=0.14285714285714285
- decile=1 cnt=3 hit=0 hit_rate=0.0
- decile=2 cnt=5 hit=1 hit_rate=0.2
- decile=3 cnt=5 hit=0 hit_rate=0.0
- decile=4 cnt=5 hit=0 hit_rate=0.0
- decile=5 cnt=4 hit=2 hit_rate=0.5
- decile=6 cnt=5 hit=1 hit_rate=0.2
- decile=7 cnt=5 hit=0 hit_rate=0.0
- decile=8 cnt=8 hit=3 hit_rate=0.375
- decile=9 cnt=2 hit=2 hit_rate=1.0

## TopN Comparison (per race)
- top1: rule_hit_rate=0.5 model_hit_rate=0.5
- top2: rule_hit_rate=0.3 model_hit_rate=0.3
- top3: rule_hit_rate=0.26666666666666666 model_hit_rate=0.23333333333333334

## Feature Importance Top10 (gain)
- pair_prob_naive: gain=157.22017967700958 split=37
- pair_value_score: gain=26.501629948616028 split=10
- pair_value_score_z_in_race: gain=6.197540381923318 split=17
- pair_fused_prob_min: gain=2.0778799057006836 split=1
- pair_prob_naive_z_in_race: gain=1.5763943614438176 split=6
- pair_ai_market_gap_min: gain=1.2740600109100342 split=1
- pair_ai_market_gap_sum: gain=0.6343629956245422 split=1
- pair_ai_market_gap_max: gain=0.6085021756589413 split=2
- distance: gain=0.317095011472702 split=1
- pair_prob_naive_rank_pct: gain=0.05261659994721413 split=1