# model_dynamic Shadow Validation

## 1. Syntax Check
```bash
python -m py_compile racing_ai/src/aikeiba/orchestration/race_day.py racing_ai/src/aikeiba/cli.py racing_ai/scripts/check_today_outputs.py racing_ai/scripts/report_pair_shadow_comparison.py racing_ai/scripts/grid_search_model_dynamic_thresholds.py racing_ai/scripts/diagnose_model_dynamic_edges.py
```

## 2. 1-day run-race-day (shadow evaluation)
```bash
cd racing_ai/src
python -m aikeiba.cli run-race-day --db-path ../data/warehouse/aikeiba.duckdb --race-date 2026-04-12 --raw-dir ../data/raw/20260412_real_from_jv --snapshot-version manual_20260412 --feature-snapshot-version fs_v1 --odds-snapshot-version odds_v1 --model-version top3_stability_plus_pace_v3 --pair-model-root ../data/models_compare/pair_reranker --pair-model-version pair_reranker_ts_v4 --decision-ai-weight 0.65 --model-dynamic-min-score 0.08 --model-dynamic-min-edge 0.00 --model-dynamic-min-gap 0.01 --model-dynamic-default-k 5 --model-dynamic-min-k 1 --model-dynamic-max-k 5 --probability-gate-mode warn-only --overwrite
```

## 3. Candidate output check
```bash
python racing_ai/scripts/check_today_outputs.py --candidate-pairs racing_ai/data/race_day/2026-04-12/top3_stability_plus_pace_v3/candidate_pairs.parquet --out-json racing_ai/reports/check_today_outputs_2026-04-12.json
```

## 4. Post-race shadow report
```bash
python racing_ai/scripts/report_pair_shadow_comparison.py --candidate-pairs racing_ai/data/race_day/2026-04-12/top3_stability_plus_pace_v3/candidate_pairs.parquet --db-path racing_ai/data/warehouse/aikeiba.duckdb --race-date 2026-04-12 --out-dir racing_ai/reports/2026-04-12
```

## 5. Edge diagnostics
```bash
python racing_ai/scripts/diagnose_model_dynamic_edges.py --input racing_ai/reports/2026-04-12/pair_shadow_pair_comparison.csv --out-csv racing_ai/reports/2026-04-12/model_dynamic_edge_diagnostics.csv --out-md racing_ai/reports/2026-04-12/model_dynamic_edge_diagnostics.md
```

## 6. Threshold grid (include negative edge)
```bash
python racing_ai/scripts/grid_search_model_dynamic_thresholds.py --input racing_ai/reports/2026-04-12/pair_shadow_pair_comparison.csv --out-csv racing_ai/reports/2026-04-12/model_dynamic_threshold_grid_summary.csv --out-md racing_ai/reports/2026-04-12/model_dynamic_threshold_grid_summary.md --min-score-values 0.04,0.06,0.08,0.10 --min-edge-values -0.05,-0.03,-0.02,-0.01,0.00,0.01 --min-gap-values 0.000,0.005,0.010,0.020 --default-k-values 3,5 --max-k-values 3,5
```

## 7. Multi-day aggregate
```bash
python racing_ai/scripts/aggregate_model_dynamic_grid_results.py --glob "racing_ai/reports/*/model_dynamic_threshold_grid_summary.csv" --out-csv racing_ai/reports/model_dynamic_threshold_grid_multi_day_summary.csv --out-md racing_ai/reports/model_dynamic_threshold_grid_multi_day_summary.md
```

## 8. Gate mode policy for shadow
- For shadow comparison, use `--probability-gate-mode warn-only` first.
- `strict` is useful for production-safety checks, but can stop before decision and produce null model_dynamic metrics.
- Always run threshold diagnostics on artifacts generated in warn-only mode.

## 9. Notes
- Keep `pair_selected_flag` unchanged.
- Keep `model_dynamic` shadow-only.
- Do not use `actual_wide_hit` / `wide_payout` as inference features. Use them only for evaluation reports.
## Raw Missing Handling (Operational Note)

- `run-race-day` normally requires `races.csv` and `entries.csv` in `--raw-dir`.
- If either is missing, `missing_raw_file:races.csv` / `missing_raw_file:entries.csv` is raised and pipeline can stop before decision.
- In that case expanded shadow evaluation artifacts are not fully generated from that run.
- Check `run_summary` keys:
  - `expanded_generation_status`
  - `expanded_generation_skipped_reason`
  - `artifacts.*.generated_this_run`
  - `artifacts.*.skipped_reason`

### Standard command (after raw backfill)

```bash
py -3.11 -m racing_ai.cli run-race-day ^
  --db-path racing_ai/data/warehouse/aikeiba.duckdb ^
  --race-date 2026-04-12 ^
  --raw-dir racing_ai/data/raw/2026-04-12 ^
  --snapshot-version manual_20260412 ^
  --feature-snapshot-version fs_v1 ^
  --odds-snapshot-version odds_v1 ^
  --model-version top3_stability_plus_pace_v3 ^
  --pair-model-root racing_ai/data/models_compare/pair_reranker ^
  --pair-model-version pair_reranker_ts_v4 ^
  --probability-gate-mode warn-only ^
  --emit-expanded-candidates ^
  --expanded-top-horse-n 10 ^
  --expanded-ai-gap-horse-n 10 ^
  --expanded-max-pairs-per-race 45 ^
  --overwrite
```

### Fallback for historical verification (raw missing day)

```bash
py -3.11 racing_ai/scripts/build_expanded_candidates_from_artifacts.py ^
  --race-date 2026-04-12 ^
  --db-path racing_ai/data/warehouse/aikeiba.duckdb ^
  --base-dir data/race_day/2026-04-12/top3_stability_plus_pace_v3 ^
  --candidate-pairs data/race_day/2026-04-12/top3_stability_plus_pace_v3/candidate_pairs.parquet ^
  --run-summary data/race_day/2026-04-12/top3_stability_plus_pace_v3/run_summary.json ^
  --model-version top3_stability_plus_pace_v3 ^
  --pair-model-root racing_ai/data/models_compare/pair_reranker ^
  --pair-model-version pair_reranker_ts_v4 ^
  --expanded-top-horse-n 10 ^
  --expanded-ai-gap-horse-n 10 ^
  --expanded-max-pairs-per-race 45 ^
  --out-expanded data/race_day/2026-04-12/top3_stability_plus_pace_v3/candidate_pairs_expanded.parquet ^
  --out-pair-csv racing_ai/reports/2026-04-12/pair_shadow_pair_comparison_expanded.csv ^
  --out-race-csv racing_ai/reports/2026-04-12/pair_shadow_race_comparison_expanded.csv
```
