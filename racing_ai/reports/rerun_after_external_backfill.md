# rerun_after_external_backfill

1. validate external CSV
`py -3.11 racing_ai/scripts/validate_external_results_payouts.py --results-csv racing_ai/data/external/results_20260410_20260412.csv --wide-payouts-csv racing_ai/data/external/wide_payouts_20260410_20260412.csv --db-path racing_ai/data/warehouse/aikeiba.duckdb --start-date 2026-04-10 --end-date 2026-04-12 --out-csv racing_ai/reports/validate_external_results_payouts.csv --out-md racing_ai/reports/validate_external_results_payouts.md`

2. results backfill dry-run
`py -3.11 racing_ai/scripts/backfill_results_finish_position.py --db-path racing_ai/data/warehouse/aikeiba.duckdb --start-date 2026-04-10 --end-date 2026-04-12 --source-results-csv racing_ai/data/external/results_20260410_20260412.csv --source-name external_results_csv --out-csv racing_ai/reports/backfill_results_finish_position_dryrun.csv --out-md racing_ai/reports/backfill_results_finish_position_dryrun.md`

3. results backfill apply
`py -3.11 racing_ai/scripts/backfill_results_finish_position.py --db-path racing_ai/data/warehouse/aikeiba.duckdb --start-date 2026-04-10 --end-date 2026-04-12 --source-results-csv racing_ai/data/external/results_20260410_20260412.csv --source-name external_results_csv --apply --out-csv racing_ai/reports/backfill_results_finish_position_apply.csv --out-md racing_ai/reports/backfill_results_finish_position_apply.md`

4. payouts backfill dry-run
`py -3.11 racing_ai/scripts/backfill_wide_payouts.py --db-path racing_ai/data/warehouse/aikeiba.duckdb --start-date 2026-04-10 --end-date 2026-04-12 --source-wide-payouts-csv racing_ai/data/external/wide_payouts_20260410_20260412.csv --source-name external_wide_payouts_csv --out-csv racing_ai/reports/backfill_wide_payouts_dryrun.csv --out-md racing_ai/reports/backfill_wide_payouts_dryrun.md`

5. payouts backfill apply
`py -3.11 racing_ai/scripts/backfill_wide_payouts.py --db-path racing_ai/data/warehouse/aikeiba.duckdb --start-date 2026-04-10 --end-date 2026-04-12 --source-wide-payouts-csv racing_ai/data/external/wide_payouts_20260410_20260412.csv --source-name external_wide_payouts_csv --apply --out-csv racing_ai/reports/backfill_wide_payouts_apply.csv --out-md racing_ai/reports/backfill_wide_payouts_apply.md`

6. completeness / join audits
- `py -3.11 racing_ai/scripts/audit_results_finish_position_completeness.py --db-path racing_ai/data/warehouse/aikeiba.duckdb --start-date 2026-04-10 --end-date 2026-04-12 --out-csv racing_ai/reports/results_finish_position_completeness_audit_after.csv --out-md racing_ai/reports/results_finish_position_completeness_audit_after.md`
- `py -3.11 racing_ai/scripts/audit_payouts_join_coverage.py --input-csv racing_ai/reports/pair_shadow_pair_comparison_expanded_3d_with_results.csv --db-path racing_ai/data/warehouse/aikeiba.duckdb --out-csv racing_ai/reports/join_payouts_coverage_audit.csv --out-md racing_ai/reports/join_payouts_coverage_audit.md`

7. rebuild joined evaluation dataset
`py -3.11 racing_ai/scripts/join_wide_results_to_candidate_pairs.py --db-path racing_ai/data/warehouse/aikeiba.duckdb --input-csv racing_ai/reports/pair_shadow_pair_comparison_expanded_3d.csv --out-csv racing_ai/reports/pair_shadow_pair_comparison_expanded_3d_with_results.csv --out-md racing_ai/reports/pair_shadow_pair_comparison_expanded_3d_with_results.md`

8. reevaluate
- `py -3.11 racing_ai/scripts/evaluate_rule_vs_non_rule_candidates.py --input-csv racing_ai/reports/pair_shadow_pair_comparison_expanded_3d_with_results.csv --out-csv racing_ai/reports/rule_vs_non_rule_candidate_evaluation.csv --out-md racing_ai/reports/rule_vs_non_rule_candidate_evaluation.md`
- `py -3.11 racing_ai/scripts/evaluate_expanded_dynamic_conditions_with_results.py --input-csv racing_ai/reports/expanded_dynamic_candidate_conditions.csv --out-csv racing_ai/reports/expanded_dynamic_candidate_conditions_with_results.csv --out-md racing_ai/reports/expanded_dynamic_candidate_conditions_with_results.md`

## 合格基準
- 3日評価の最低基準:
  - actual_wide_hit coverage >= 0.8
  - hit rows payout coverage >= 0.8
- 次段階(10日以上)に進む基準:
  - 3日で coverage >= 0.8
  - model_dynamic_non_overlap ROI > rule ROI
  - 追加日10日以上でも傾向維持
