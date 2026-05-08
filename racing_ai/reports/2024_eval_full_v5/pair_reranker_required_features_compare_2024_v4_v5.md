# pair_reranker Required Features Compare (v4 vs v5)

- generated_at: 2026-05-06T21:25:00
- pair_model_dir: racing_ai\data\models_compare\pair_reranker\pair_reranker_ts_v4
- v4_csv: racing_ai\reports\2024_eval_full_v4\pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv
- v5_csv: racing_ai\reports\2024_eval_full_v5\pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv
- required_feature_count: 20
- missing_required_feature_rate_v4: 0.950
- missing_required_feature_rate_v5: 0.950

## Highlights

- improved_feature_count: 0
- regressed_feature_count: 0

## Notes

- This script reads the *joined pairs CSV*. If your joined CSV schema does not include the required raw features (common when exporting a minimal evaluation schema), many features will show as missing even if they were present upstream during model inference.
- For a ground-truth audit of feature recovery, run `audit_pair_reranker_required_features.py` against the actual `candidate_pairs_expanded.parquet` used for inference.
- `filled_with_zero_rate` is a heuristic indicating many rows are zero after numeric coercion/fill; treat as a signal of missingness in the evaluation helper path.
