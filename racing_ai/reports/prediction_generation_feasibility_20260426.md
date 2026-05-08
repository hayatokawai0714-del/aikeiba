# Prediction Generation Feasibility (2026-04-26)

```csv
race_date,races_count,entries_count,feature_rows_count,odds_rows_count,predictions_count
2026-04-26,36,509,509,13039,509

```

## Recommended Commands

```bash
py -3.11 -m racing_ai.cli build-features --db-path racing_ai/data/warehouse/aikeiba.duckdb --race-date 2026-04-26 --feature-snapshot-version fs_v1
py -3.11 -m racing_ai.cli infer-top3 --db-path racing_ai/data/warehouse/aikeiba.duckdb --models-root racing_ai/data/models_compare --race-date 2026-04-26 --feature-snapshot-version fs_v1 --model-version top3_stability_plus_pace_v3 --odds-snapshot-version odds_v1
```