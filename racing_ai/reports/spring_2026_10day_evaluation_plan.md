# Spring 2026 Shadow Evaluation (10+ Days) Plan

- generated_at: 2026-05-01
- scope: evaluation-only (no production logic changes)
- results source: `external_results` (TARGET-converted) is preferred; DuckDB results is fallback
- payouts source: DuckDB `payouts` (already good for 2026 spring), and/or TARGET-converted external wide payouts as reference

## Current Limitation (Why We Cannot Reach 10 Days Yet)

As of now, the available TARGET-converted external data that *simultaneously* satisfies:

1. DuckDB has `races/entries`
2. DuckDB has `horse_predictions` for `top3_stability_plus_pace_v3`
3. External results CSV includes the `race_date`
4. External wide payouts CSV includes the `race_date`

…is only **7 dates**:

- 2026-04-04
- 2026-04-05
- 2026-04-11
- 2026-04-12
- 2026-04-18
- 2026-04-25
- 2026-04-26

Therefore, to reach **10+ rule-comparable days**, we must **export additional TARGET results/payoffs** (external CSVs) for more central race days.

## Plan Overview (Per Added Date)

For each new central race day `D`:

1. Ensure base data exists in DuckDB:
   - `races` / `entries` present for `D`
2. Ensure predictions exist (if missing):
   - `py -3.11 -m racing_ai.cli build-features --race-date D --feature-snapshot-version fs_v1`
   - `py -3.11 -m racing_ai.cli infer-top3 --race-date D --models-root racing_ai/data/models_compare --model-version top3_stability_plus_pace_v3 --feature-snapshot-version fs_v1 --odds-snapshot-version odds_v1`
3. Ensure rule-selected source exists (if raw artifacts missing):
   - evaluation-only rebuild (keeps semantics: “top 5 by pair_value_score”):
   - `py -3.11 racing_ai/scripts/rebuild_rule_candidate_pairs_from_db.py --race-date D --db-path racing_ai/data/warehouse/aikeiba.duckdb --model-version top3_stability_plus_pace_v3 --out-parquet racing_ai/data/race_day/D/top3_stability_plus_pace_v3/candidate_pairs.parquet --out-audit-md racing_ai/reports/D/rebuild_rule_candidate_pairs_audit_YYYYMMDD.md`
4. Build expanded candidate pool (evaluation-only):
   - `py -3.11 racing_ai/scripts/build_expanded_candidates_from_artifacts.py ... --race-date D ...`
   - outputs:
     - `candidate_pairs_expanded.parquet`
     - `pair_shadow_pair_comparison_expanded_YYYYMMDD.csv`
5. Join evaluation labels with **external-first results**:
   - Use `join_wide_results_to_candidate_pairs.py --external-results-csv ... --results-source-priority external,db`
6. Evaluate:
   - `evaluate_rule_vs_non_rule_candidates.py --quality-ok-only`
   - `build_dynamic_vs_rule_daily_stability.py --quality-ok-only`

## External Data Requirement (What To Export From TARGET)

We need new external CSVs (or an updated spring CSV) that includes new dates:

- results: `race_id, race_date, umaban, finish_position, (optional status/horse_name)`
- wide payouts: `race_id, race_date, bet_type=wide, bet_key(03-07), payout`

After receiving new files:

1. `validate_external_results_payouts.py` (sanity checks)
2. No DB destructive update is required for evaluation, because we can join external results directly during evaluation.

## Pass/Fail Criteria For “Evaluation Possible”

- `quality_ok_race_count >= 50`
- `quality_filtered_actual_wide_hit_coverage >= 0.7`
- `quality_ok payout coverage >= 0.8`
- `rule-comparable days >= 10` (days where `rule_selected_candidate_count > 0`)

## “Promising” Criteria (Shadow Only)

- `model_dynamic_non_overlap ROI > rule_selected ROI`
- `dynamic_minus_rule_roi > 0`
- On the majority of rule-comparable days: `dynamic_minus_rule_roi > 0`

## Notes About Rule Rebuild (Evaluation Only)

For dates where production artifacts are missing, we may rebuild `candidate_pairs.parquet` using:

- `pair_value_score` top-5 selection

This is **NOT** a full reproduction of `run-race-day` and must be treated as **evaluation-only** support.

