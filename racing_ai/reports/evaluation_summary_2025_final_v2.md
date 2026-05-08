# Evaluation Summary 2025 (Final v2, Diagnostic)

generated_at: 2026-05-06

## Scope

- Year: 2025 (spring/summer/autumn combined)
- Evaluation uses:
  - expanded candidate pool (evaluation-only)
  - **external results preferred** (TARGET converted) with DB fallback
  - wide payouts from DB after backfill (quality_ok payout coverage was 1.0)
- Production behavior unchanged:
  - `pair_selected_flag` meaning unchanged
  - rule-based selection unchanged
  - model_dynamic is shadow-only

## Snapshot Metrics (Given / Confirmed)

- total_race_count: 2148
- quality_ok_race_count: 947
- quality_ng_race_count: 1201
- raw_actual_wide_hit_coverage: 0.989417
- quality_filtered_actual_wide_hit_coverage: 0.442906
- quality_ok_payout_coverage: 1.0

- rule_selected ROI: 1.554952
- model_dynamic_non_overlap ROI: 0.943846
- dynamic_minus_rule_roi: negative

## Step 1: quality_ng Main Causes (Top 3)

Source: `quality_ng_reason_summary_2025.csv`

By pair rows:
1. `expected_payout_mismatch` (row_count=35132, race_count=838)
2. `top3_count_not_3` (row_count=10489, race_count=235)
3. `invalid_finish_position` (row_count=5290, race_count=121)

Interpretation:
- The biggest blocker is **results-derived top3 vs payout-wide-keys mismatch**, not payout availability (payout coverage itself is OK on quality_ok races).
- `top3_count_not_3` and `invalid_finish_position` show that the result-side data has many races where “top3 horses” cannot be reconstructed safely.

## Step 2: expected_payout_mismatch Race Audit (What Kind of Mismatch?)

Source: `expected_vs_payout_mismatch_races_2025.csv`

mismatch_reason counts (race-level rows):
- unknown: 838
- finish_position_wrong: 260
- results_missing: 6

Interpretation:
- A meaningful chunk is clearly explained as `finish_position_wrong` (results-side issue).
- The majority is still `unknown`, which likely means:
  - finish_position is “numeric but wrong” in a way that still yields 3 horses
  - OR race_id / umaban mapping is consistent but the extracted top3 conflicts with official wide keys
  - OR ties/abnormal status handling is not encoded as “top3” deterministically (needs special status accounting).

## Step 3: invalid_finish_position (Where It Comes From)

Source: `invalid_finish_position_rows_2025.csv`

- rows: 5290
- result_source_used:
  - external: 3818
  - db: 1343
  - none: 129
- anomaly_reason:
  - non_1_to_18_or_non_numeric: 5190
  - missing_finish_position: 100

Interpretation:
- Most invalid finish_position comes from **external (TARGET) results** in 2025.
- Even with external-first, we must treat non 1..18 (or non-numeric) as invalid and exclude from quality_ok (already done).
- This is the key reason quality_filtered coverage stays low (0.44).

## Step 4: model_dynamic Non-overlap Scarcity (Why Only 361?)

Source: `model_dynamic_skip_reason_summary_2025.md`

Race-level skip reasons (all races):
- DYNAMIC_BUY_OK: 1172 races
- DYNAMIC_SKIP_GAP_SMALL: 801 races
- DYNAMIC_SKIP_EDGE_WEAK: 151 races
- DYNAMIC_SKIP_MODEL_SCORE_WEAK: 24 races

Key takeaways:
- “dynamic selected = 0” is primarily driven by `DYNAMIC_SKIP_GAP_SMALL`.
- This suggests the current gate `pair_model_score_gap_to_next >= min_gap` (or its equivalent) is too strict under 2025 distributions.
- Non-overlap is inherently limited because:
  - many races never produce a “confident top pair” by gap criterion
  - and when they do, the selected set overlaps heavily with rule.

## Step 5: Evaluation-only Threshold Grid (Quality OK only)

Output:
- `model_dynamic_threshold_grid_2025_quality_ok.csv`
- `model_dynamic_threshold_grid_2025_quality_ok.md`

What changed when thresholds were relaxed:
- non_overlap_count increased materially (e.g., up to ~618–660 in tested grid cells).

But:
- **No tested grid cell achieved `dynamic_minus_rule_roi > 0`**.
- Best (least-bad) cells were still negative (e.g. around `-0.31`), meaning:
  - Increasing non-overlap did not recover ROI relative to rule in 2025.

Interpretation:
- 2025 “rule > dynamic” does not appear to be solely a “too few non-overlap samples” artifact.
- However, because quality_filtered coverage is only 0.44, this is still **not a fully definitive** conclusion for the whole year; it is a conclusion on the **quality_ok subset**.

## Final Re-judgement (2025)

### Coverage insufficiency main cause
- `expected_payout_mismatch` and invalid/unstable finish_position reconstruction (results-side).
- External results itself contains many invalid finish_position values in 2025.

### dynamic non-overlap scarcity main cause
- `DYNAMIC_SKIP_GAP_SMALL` dominates zero-selected races.
- Even when relaxed (evaluation-only grid), ROI remained below rule.

### Decision (A/B)
- **B. 暫定評価（ただし、現状のquality_ok評価ではrule優位が強い）**
  - Coverage is insufficient for a confident year-wide claim.
  - On the quality_ok subset where we *can* evaluate reliably, rule is consistently better than dynamic under many threshold variants.

## Recommendation Before Moving to 2024

Proceed to 2024 evaluation, but treat 2025 findings as:
- “rule is likely strong / dynamic not competitive on quality_ok subset”
- and prioritize:
  1. improve 2025 result quality (reduce expected_payout_mismatch & invalid finish_position)
  2. re-check gap gate design (shadow-only) after we have reliable labels for more races

