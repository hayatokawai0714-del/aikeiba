# Pair Reranker Required Features Audit

- generated_at: 2026-05-06T19:37:14
- model_dir: racing_ai\data\models_compare\pair_reranker\pair_reranker_ts_v4
- expanded_parquet: racing_ai\data\race_day\2024-01-06\top3_stability_plus_pace_v3\candidate_pairs_expanded.parquet
- required_features: 20
- categorical_features_in_meta: 0

## Summary

- present_count: 20
- missing_count: 0
- all_nan_count: 6

## Top missing/all-NaN features

| feature | present | non_null_rate | dtype | all_nan | recoverable | method |
|---|---:|---:|---|---:|---:|---|
| pair_ai_market_gap_max | True | 0.0000 | float64 | True | True | compute from horse1/horse2 ai_market_gap |
| pair_ai_market_gap_min | True | 0.0000 | float64 | True | True | compute from horse1/horse2 ai_market_gap |
| venue | True | 0.0000 | object | True | True | JOIN races by race_id |
| surface | True | 0.0000 | object | True | True | JOIN races by race_id |
| field_size_bucket | True | 0.0000 | object | True | True | compute within race_id |
| distance_bucket | True | 0.0000 | object | True | True | compute within race_id |
