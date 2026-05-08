# Pair Reranker Retraining Plan (ROI-oriented, Shadow Only)

generated_at: 2026-05-06

This is a **shadow experiment plan**. It does NOT change production selection.

## Goal

Make pair model outputs useful for ROI improvement (wide betting), not only hit probability.

## Current state (assumed from existing pipeline)

- Current target: `actual_wide_hit` (binary)
- Current evaluation: ROI proxy using `wide_payout` on selected pairs
- Pain points:
  - rule is strong; hit-optimized model tends to overlap with rule or popular pairs
  - even when non-overlap increases, ROI does not necessarily improve

## Proposed shadow objectives (choose 1–2 first)

### Option A: Weighted binary classification (hit, but ROI-aware weights)

Train LightGBM binary with:
- label: `actual_wide_hit` (0/1)
- sample_weight:
  - `w = 1` for non-hit
  - `w = clip(wide_payout / 100, 1, WMAX)` for hit rows

Rationale:
- Keeps stable binary objective (logloss/AUC)
- Encourages the model to prioritize hit types with larger payout (ROI proxy)

Risks:
- payout noise / outliers; need clipping and/or log transform

### Option B: Regression on log payout (two-stage)

Two-stage model:
1. hit model: `P(hit)` (binary)
2. payout model: `E[payout | hit]` (regression on hits only, e.g. `log1p(payout)`)

Final score (shadow only):
- `score_roi = P(hit) * exp(E[log payout])`

Rationale:
- Separates "hit likelihood" from "payout size"
- More aligned with ROI

Risks:
- small number of hits; overfitting likely; requires regularization & time-split CV

### Option C: Direct expected value proxy

Define a per-row target:
- `y = (actual_wide_hit * wide_payout) / 100`

Train regression (e.g. L2, Poisson-ish, Tweedie-like) with strong regularization.

Rationale:
- Directly optimizes expected payout proxy.

Risks:
- zero-inflated; model instability; needs careful evaluation.

## Feature policy (important)

Allowed as features (pre-race available):
- model signals (pair_model_score candidates, horse top3 probs, their relations)
- market proxies available pre-race (odds-derived implied probabilities)

NOT allowed as features:
- `actual_wide_hit`, `wide_payout` (labels) or post-race derived columns

## Training protocol

1. Build training base:
   - use `pair_learning_base.parquet` or expanded-candidate training table
   - must include race_date for time split
2. Time-series validation:
   - rolling / blocked split (no leakage)
   - report fold AUC/logloss (for hit model), plus ROI proxy metrics
3. Safety gate (shadow-only):
   - min validation days >= 10
   - min rows >= 3000 (pairs)
   - ROI proxy improvement on aggregate AND not driven by 1 day

## Evaluation metrics (shadow)

Always report:
- overall ROI proxy (total_payout / cost)
- daily ROI distribution (median, p25/p75, worst day)
- non-overlap ROI proxy vs rule
- overlap ratio vs rule

## Implementation plan (minimal diffs)

1. Add new training script:
   - `scripts/train_pair_reranker_roi.py`
2. Output model to new version:
   - `data/models_compare/pair_reranker/<pair_reranker_roi_vX>/`
3. Shadow scoring:
   - load the ROI-model alongside existing one
   - write columns:
     - `pair_model_score_roi`
     - `pair_model_version_roi`
   - do not affect production selection

## Recommendation for first experiment

Start with **Option A (weighted binary)**:
- smallest change
- uses existing infrastructure
- tends to be more stable than direct EV regression

