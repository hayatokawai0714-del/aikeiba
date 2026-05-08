# Pair Reranker Required Features Audit

- generated_at: 2026-05-06T19:36:32
- model_dir: racing_ai\data\models_compare\pair_reranker\pair_reranker_ts_v4
- expanded_parquet: racing_ai\data\race_day\2024-01-06\top3_stability_plus_pace_v3\candidate_pairs_expanded.parquet
- required_features: 20
- categorical_features_in_meta: 0

## Summary

- present_count: 9
- missing_count: 11
- all_nan_count: 13

## Top missing/all-NaN features

| feature | present | non_null_rate | dtype | all_nan | recoverable | method |
|---|---:|---:|---|---:|---:|---|
| pair_ai_market_gap_max | True | 0.0000 | object | True | True | compute from horse1/horse2 ai_market_gap |
| pair_ai_market_gap_min | True | 0.0000 | object | True | True | compute from horse1/horse2 ai_market_gap |
| pair_value_score_rank_pct | False | 0.0000 | MISSING | True | True | compute within race_id |
| pair_value_score_z_in_race | False | 0.0000 | MISSING | True | True | compute within race_id |
| pair_prob_naive_rank_pct | False | 0.0000 | MISSING | True | True | compute within race_id |
| pair_prob_naive_z_in_race | False | 0.0000 | MISSING | True | True | compute within race_id |
| field_size | False | 0.0000 | MISSING | True | True | JOIN races by race_id |
| distance | False | 0.0000 | MISSING | True | True | JOIN races by race_id |
| venue | False | 0.0000 | MISSING | True | True | JOIN races by race_id |
| surface | False | 0.0000 | MISSING | True | True | JOIN races by race_id |
| pair_rank_bucket | False | 0.0000 | MISSING | True | True | compute within race_id |
| field_size_bucket | False | 0.0000 | MISSING | True | True | compute within race_id |
| distance_bucket | False | 0.0000 | MISSING | True | True | compute within race_id |
