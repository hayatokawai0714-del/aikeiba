# Candidate Pool Expansion Experiment Plan (Shadow Only)

generated_at: 2026-05-06

This plan proposes **evaluation-only** experiments to see whether expanding the candidate pool can produce
useful non-overlap opportunities (pairs not already chosen by rule) with acceptable ROI proxy.

No production behavior changes are included.

## Problem

Rule is strong. If candidate pool is narrow (or biased toward rule-like pairs),
even a better model cannot surface new value.

Additionally, "non-overlap" may be small not only due to thresholds,
but due to insufficient coverage of interesting pairs.

## Experiment design

### Baseline pool (current)

- expanded pool from evaluation helper:
  - rule_selected pairs
  - top-horse pairs (top N by p_top3_fused)
  - ai-gap pairs (top by ai_market_gap mixed with top-horse)
  - capped by `expanded_max_pairs_per_race`

### Expansion variants (try one at a time)

1. Increase N and cap:
   - `expanded_top_horse_n`: 10 -> 12 -> 14
   - `expanded_ai_gap_horse_n`: 10 -> 12 -> 14
   - `expanded_max_pairs_per_race`: 45 -> 66 -> 91

2. Add "market underdog" set:
   - include horses with:
     - high model probability but low market proxy (largest positive ai_market_gap)
   - then take all combinations among (top_horses ∪ gap_horses)

3. Add "rank-slice" pool:
   - take top K pairs by `pair_value_score` beyond rule top5
   - keep `pair_selected_flag=False` for these

4. Add "diversity" pool:
   - per race, enforce unique horse coverage (avoid selecting pairs sharing the same horse)
   - evaluation-only candidate generation; keep rule pool unchanged

## Measurements (must report)

For each expansion variant:
- candidate_pair_count per race (avg/p90/max)
- non_rule_candidate_count / rate
- pair_model_score non-null rate
- non-overlap rate vs rule among top-K model selections
- ROI proxy (aggregate + daily)
- instability: ROI driven by 1–2 days? (top day contribution)

## Guardrails

- Memory: do per-date processing, write intermediate parquet/csv (do not load full year into RAM).
- Do not use post-race labels as features.
- Keep expanded pool as separate artifact (do not overwrite production artifacts).

## Recommended next experiment order

1. Increase caps moderately: `max_pairs_per_race=66`, N=12
2. Add market-underdog set
3. Only if needed, go to `max_pairs_per_race=91`

## Expected outcomes / Interpretation

- If non-overlap increases AND ROI proxy improves:
  - candidate pool was a limiting factor
- If non-overlap increases BUT ROI proxy does not improve:
  - model scoring or objective may be the limiting factor (see ROI-oriented retraining plan)
- If non-overlap does not increase:
  - dynamic thresholds/gates are the limiting factor, or rule dominates the score space

