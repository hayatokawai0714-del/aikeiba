# Expanded Candidate Pool Design Audit

## 1) Root cause confirmation
- `candidate_pairs` is produced in `race_day._decision_preview` only from `generate_wide_candidates_rule_based(...)` output.
- Current generation parameters are narrow (`axis_k=1`, `partner_k=min(6, field_size)`).
- In observed days, generated pool size per race matches selected-top-N behavior, so `pair_selected_flag=True` can saturate all rows.
- Therefore, shadow comparison on this pool cannot produce rule-non-overlap candidates.

## 2) Non-breaking design
- Keep existing `candidate_pairs.parquet` unchanged (production meaning preserved).
- Add evaluation-only artifact `candidate_pairs_expanded.parquet`.
- Expanded pool is union of:
  - `rule_selected` pairs (existing set)
  - `top_horse_pairs` (all combinations from top fused-prob horses)
  - `ai_gap_pairs` (all combinations from union of top fused-prob and top ai-market-gap horses)
- Deduplicate by `race_id + pair_norm`.

## 3) Scoring flow on expanded pool
- Compute pair base features via `simple_pair_value_score`.
- Apply existing shadow scorer `_apply_pair_shadow_scores`.
- Apply existing dynamic enrichment `_enrich_pair_rows_for_shadow_selection`.
- Preserve `pair_selected_flag` as original rule flag (false for added pairs).

## 4) New artifacts
- `candidate_pairs_expanded.parquet`
- `pair_shadow_pair_comparison_expanded.csv`
- `pair_shadow_race_comparison_expanded.csv`
- Existing files remain unchanged.
