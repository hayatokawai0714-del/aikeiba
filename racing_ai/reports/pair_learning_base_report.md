# pair_learning_base_report

- 実行日時: 2026-04-29T12:46:09
- 入力: C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\modeling\pair_learning_base.parquet

## 基本統計
- 行数: 165
- actual_wide_hit 件数: 37
- hit_rate: 0.22424242424242424
- wide_payout 欠損件数: 128
- race_id 数: 33

## model_version別件数
- top3_stability_plus_pace_v2: 165

## pair_rank_in_race別 hit_rate
- rank=1.0: cnt=34 hit=14 hit_rate=0.4117647058823529
- rank=2.0: cnt=32 hit=9 hit_rate=0.28125
- rank=3.0: cnt=33 hit=7 hit_rate=0.21212121212121213
- rank=4.0: cnt=33 hit=4 hit_rate=0.12121212121212122
- rank=5.0: cnt=33 hit=3 hit_rate=0.09090909090909091

## pair_value_score 分位点
- q0.00: 0.054435419760598826
- q0.10: 0.06715876998888053
- q0.25: 0.0767730929695873
- q0.50: 0.0902802281958541
- q0.75: 0.1054882185125356
- q0.90: 0.12337386579545606
- q1.00: 0.17956143565179208

## 主要特徴量 欠損率
- pair_prob_naive: 0/165 (0.0)
- pair_value_score: 0/165 (0.0)
- pair_ai_market_gap_sum: 0/165 (0.0)
- pair_ai_market_gap_max: 0/165 (0.0)
- pair_ai_market_gap_min: 0/165 (0.0)
- pair_fused_prob_sum: 0/165 (0.0)
- pair_fused_prob_min: 0/165 (0.0)
- pair_rank_in_race: 0/165 (0.0)
- field_size: 0/165 (0.0)
- venue: 0/165 (0.0)
- surface: 0/165 (0.0)
- distance: 45/165 (0.2727272727272727)
- pair_value_score_rank_pct: 0/165 (0.0)
- pair_value_score_z_in_race: 0/165 (0.0)
- pair_prob_naive_rank_pct: 0/165 (0.0)
- pair_prob_naive_z_in_race: 0/165 (0.0)
- pair_rank_bucket: 0/165 (0.0)
- field_size_bucket: 0/165 (0.0)
- distance_bucket: 0/165 (0.0)