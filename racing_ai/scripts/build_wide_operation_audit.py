from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd

from aikeiba.db.duckdb import DuckDb
from aikeiba.decision.pair_scoring import simple_pair_value_score
from aikeiba.decision.skip_reasoning import SkipReasonConfig, decide_skip_reason
from aikeiba.decision.value_model import blend_ai_market_prob, compute_market_top3_prob_from_place_odds
from aikeiba.decision.wide_rules import generate_wide_candidates_rule_based
from aikeiba.inference.top3 import _shrink_race_sum_top3
from aikeiba.modeling.registry import load_model_bundle


def _latest_predictions(db: DuckDb, race_date: str, model_version: str) -> pd.DataFrame:
    return db.query_df(
        """
        WITH latest AS (
          SELECT race_id, horse_no, model_version, max(inference_timestamp) AS ts
          FROM horse_predictions
          WHERE model_version = ?
          GROUP BY race_id, horse_no, model_version
        )
        SELECT hp.*
        FROM horse_predictions hp
        JOIN latest l
          ON l.race_id=hp.race_id
         AND l.horse_no=hp.horse_no
         AND l.model_version=hp.model_version
         AND l.ts=hp.inference_timestamp
        JOIN races r ON r.race_id=hp.race_id
        WHERE r.race_date = cast(? as DATE)
        """,
        (model_version, race_date),
    )


def _market_proxy(db: DuckDb, race_date: str) -> pd.DataFrame:
    rows = db.query_df(
        """
        WITH ranked AS (
          SELECT o.race_id, o.horse_no, lower(o.odds_type) AS odds_type, o.odds_value, o.captured_at,
                 row_number() OVER (
                   PARTITION BY o.race_id, o.horse_no, lower(o.odds_type)
                   ORDER BY o.captured_at DESC NULLS LAST, o.odds_snapshot_version DESC
                 ) AS rn
          FROM odds o
          JOIN races r ON r.race_id=o.race_id
          WHERE r.race_date = cast(? as DATE)
            AND lower(o.odds_type) IN ('place', 'place_max')
            AND o.horse_no > 0
        )
        SELECT race_id, horse_no,
               max(CASE WHEN odds_type='place' THEN odds_value END) AS odds_place,
               max(CASE WHEN odds_type='place_max' THEN odds_value END) AS odds_place_max,
               max(captured_at) AS odds_timestamp
        FROM ranked
        WHERE rn=1
        GROUP BY race_id, horse_no
        """,
        (race_date,),
    )
    if len(rows) == 0:
        return rows
    rows["market_top3_proxy"] = rows.apply(
        lambda r: compute_market_top3_prob_from_place_odds(r["odds_place"], r["odds_place_max"]),
        axis=1,
    )
    # race-wise normalize to sum=3
    out = []
    for rid, g in rows.groupby("race_id"):
        gg = g.copy()
        s = gg["market_top3_proxy"].dropna().sum()
        if s and s > 0:
            gg["market_top3_proxy"] = gg["market_top3_proxy"].apply(lambda v: min(1.0, max(0.0, (v * 3.0 / s) if pd.notna(v) else v)))
        out.append(gg)
    return pd.concat(out, ignore_index=True) if out else rows


def build_outputs(
    *,
    db: DuckDb,
    models_root: Path,
    race_date: str,
    model_version: str,
    feature_snapshot_version: str,
    decision_ai_weight: float,
    out_root: Path,
) -> dict[str, str]:
    generated_at = dt.datetime.now().isoformat(timespec="seconds")

    entries = db.query_df(
        """
        SELECT r.race_id, cast(r.race_date as VARCHAR) AS race_date, r.venue AS course, r.distance, r.surface,
               e.horse_no AS umaban, e.horse_id, e.horse_name
        FROM entries e JOIN races r ON r.race_id=e.race_id
        WHERE r.race_date = cast(? as DATE)
        """,
        (race_date,),
    )
    feats = db.query_df(
        """
        SELECT * FROM feature_store
        WHERE race_date = cast(? as DATE)
          AND feature_snapshot_version = ?
        """,
        (race_date, feature_snapshot_version),
    )
    model, calibrator, _ = load_model_bundle(root=models_root, task="top3", model_version=model_version)
    p_raw = model.predict(feats)
    p_cal = calibrator.predict(p_raw)
    pred = feats[["race_id", "horse_no"]].copy()
    pred["p_top3_raw"] = p_raw
    pred["p_top3_calibrated"] = p_cal
    pred = pred.rename(columns={"horse_no": "umaban"})
    shr = pred[["race_id", "umaban", "p_top3_calibrated"]].rename(columns={"p_top3_calibrated": "p_top3"})
    shr = _shrink_race_sum_top3(shr, race_col="race_id", prob_col="p_top3", target_sum=3.0).rename(columns={"p_top3": "p_top3_shrunk"})

    mkt = _market_proxy(db, race_date)
    merged = entries.merge(pred, on=["race_id", "umaban"], how="left").merge(shr, on=["race_id", "umaban"], how="left")
    merged = merged.merge(mkt.rename(columns={"horse_no": "umaban"}), on=["race_id", "umaban"], how="left")
    merged["decision_ai_weight"] = float(decision_ai_weight)
    merged["p_top3_fused"] = merged.apply(
        lambda r: blend_ai_market_prob(
            p_ai=r["p_top3_shrunk"] if pd.notna(r["p_top3_shrunk"]) else None,
            p_market=r["market_top3_proxy"] if pd.notna(r["market_top3_proxy"]) else None,
            ai_weight=decision_ai_weight,
        ),
        axis=1,
    )
    merged["ai_market_gap"] = merged["p_top3_fused"] - merged["market_top3_proxy"]
    merged["ai_market_gap_rank"] = merged.groupby("race_id")["ai_market_gap"].rank(ascending=False, method="min")
    merged["prediction_created_at"] = generated_at
    merged["model_version"] = model_version
    merged["feature_set_version"] = feature_snapshot_version

    race_stats = merged.groupby("race_id").apply(
        lambda g: pd.Series(
            {
                "density_top3": g["p_top3_fused"].nlargest(3).sum(skipna=True),
                "gap12": (lambda s: (s.iloc[0] - s.iloc[1]) if len(s) >= 2 else None)(g["p_top3_fused"].dropna().sort_values(ascending=False).reset_index(drop=True)),
                "positive_ai_gap_count": int((g["ai_market_gap"] > 0).sum()),
                "market_top1": g["market_top3_proxy"].max(skipna=True),
                "race_density_top3_ai": g["p_top3_shrunk"].nlargest(3).sum(skipna=True),
                "race_density_top3_market": g["market_top3_proxy"].nlargest(3).sum(skipna=True),
                "race_density_top3_fused": g["p_top3_fused"].nlargest(3).sum(skipna=True),
            }
        )
    ).reset_index()
    merged = merged.merge(race_stats, on="race_id", how="left")
    cfg = SkipReasonConfig(density_top3_max=1.35, gap12_min=0.003)
    race_skip = race_stats.copy()
    race_skip["skip_reason"] = race_skip.apply(
        lambda r: decide_skip_reason(
            density_top3=r["density_top3"],
            gap12=r["gap12"],
            positive_ai_gap_count=int(r["positive_ai_gap_count"]),
            candidate_count=0,
            market_top1=r["market_top1"],
            top_pair_value_score=None,
            config=cfg,
        ),
        axis=1,
    )
    race_skip["buy_skip"] = race_skip["skip_reason"].apply(lambda x: "BUY" if x == "BUY_OK" else "SKIP")
    merged = merged.merge(race_skip[["race_id", "buy_skip", "skip_reason"]], on="race_id", how="left")

    # Pair candidates + score
    bet_rows = []
    top_n_pairs = 5
    for rid, g in merged.groupby("race_id"):
        if g["buy_skip"].iloc[0] != "BUY":
            continue
        horse_nos = [int(x) for x in g["umaban"].dropna().tolist()]
        pmap = {int(r.umaban): float(r.p_top3_fused) for r in g.itertuples() if pd.notna(r.p_top3_fused)}
        cands = generate_wide_candidates_rule_based(race_id=rid, horse_nos=horse_nos, p_top3=pmap, axis_k=1, partner_k=min(6, len(horse_nos)))
        for c in cands:
            h1 = g[g["umaban"] == c.axis_horse_no].iloc[0]
            h2 = g[g["umaban"] == c.partner_horse_no].iloc[0]
            pair_prob_naive, pair_score, miss = simple_pair_value_score(
                p1=h1["p_top3_fused"] if pd.notna(h1["p_top3_fused"]) else None,
                p2=h2["p_top3_fused"] if pd.notna(h2["p_top3_fused"]) else None,
                gap1=h1["ai_market_gap"] if pd.notna(h1["ai_market_gap"]) else None,
                gap2=h2["ai_market_gap"] if pd.notna(h2["ai_market_gap"]) else None,
            )
            bet_rows.append(
                {
                    "race_id": rid,
                    "pair": c.pair,
                    "pair_norm": c.pair,
                    "pair_value_score_version": "v1_simple_gap_bonus",
                    "horse1_umaban": int(c.axis_horse_no),
                    "horse2_umaban": int(c.partner_horse_no),
                    "horse1_p_top3_fused": h1["p_top3_fused"],
                    "horse2_p_top3_fused": h2["p_top3_fused"],
                    "pair_prob_naive": pair_prob_naive,
                    "horse1_market_top3_proxy": h1["market_top3_proxy"],
                    "horse2_market_top3_proxy": h2["market_top3_proxy"],
                    "horse1_ai_market_gap": h1["ai_market_gap"],
                    "horse2_ai_market_gap": h2["ai_market_gap"],
                    "pair_ai_market_gap_sum": (
                        (float(h1["ai_market_gap"]) if pd.notna(h1["ai_market_gap"]) else 0.0)
                        + (float(h2["ai_market_gap"]) if pd.notna(h2["ai_market_gap"]) else 0.0)
                    ),
                    "pair_ai_market_gap_max": max(
                        float(h1["ai_market_gap"]) if pd.notna(h1["ai_market_gap"]) else float("-inf"),
                        float(h2["ai_market_gap"]) if pd.notna(h2["ai_market_gap"]) else float("-inf"),
                    ),
                    "pair_ai_market_gap_min": min(
                        float(h1["ai_market_gap"]) if pd.notna(h1["ai_market_gap"]) else float("inf"),
                        float(h2["ai_market_gap"]) if pd.notna(h2["ai_market_gap"]) else float("inf"),
                    ),
                    "pair_fused_prob_sum": (
                        (float(h1["p_top3_fused"]) if pd.notna(h1["p_top3_fused"]) else 0.0)
                        + (float(h2["p_top3_fused"]) if pd.notna(h2["p_top3_fused"]) else 0.0)
                    ),
                    "pair_fused_prob_min": min(
                        float(h1["p_top3_fused"]) if pd.notna(h1["p_top3_fused"]) else float("inf"),
                        float(h2["p_top3_fused"]) if pd.notna(h2["p_top3_fused"]) else float("inf"),
                    ),
                    "pair_value_score": pair_score,
                    "pair_missing_flag": miss,
                    "selected_stage": c.selected_stage,
                    "generated_at": generated_at,
                }
            )
    bets = pd.DataFrame(bet_rows)
    if len(bets) > 0:
        bets["pair_ai_market_gap_max"] = bets["pair_ai_market_gap_max"].replace([float("-inf"), float("inf")], pd.NA)
        bets["pair_ai_market_gap_min"] = bets["pair_ai_market_gap_min"].replace([float("-inf"), float("inf")], pd.NA)
        bets["pair_fused_prob_min"] = bets["pair_fused_prob_min"].replace([float("inf"), float("-inf")], pd.NA)
        bets["pair_rank_in_race"] = bets.groupby("race_id")["pair_value_score"].rank(ascending=False, method="min")
        bets["pair_selected_flag"] = bets["pair_rank_in_race"] <= float(top_n_pairs)
        bets["pair_selection_reason"] = bets["pair_selected_flag"].apply(
            lambda x: "SELECT_TOP_PAIR_SCORE" if bool(x) else "NOT_SELECTED_LOW_PAIR_SCORE"
        )

    # Training-base dataset for future market-blend learning
    results = db.query_df(
        """
        SELECT res.race_id, res.horse_no AS umaban,
               CASE WHEN res.finish_position BETWEEN 1 AND 3 THEN 1 ELSE 0 END AS actual_top3,
               res.pop_rank AS popularity_rank
        FROM results res JOIN races r ON r.race_id=res.race_id
        WHERE r.race_date = cast(? as DATE)
        """,
        (race_date,),
    )
    blend_base = merged.merge(results, on=["race_id", "umaban"], how="left")
    blend_base["field_size"] = blend_base.groupby("race_id")["umaban"].transform("count")
    eps = 1e-6
    blend_base["p_top3_logit"] = blend_base["p_top3_shrunk"].clip(eps, 1 - eps).apply(lambda p: float(np.log(p / (1.0 - p))) if pd.notna(p) else None)
    blend_base["market_top3_logit"] = blend_base["market_top3_proxy"].clip(eps, 1 - eps).apply(lambda p: float(np.log(p / (1.0 - p))) if pd.notna(p) else None)
    blend_base["ai_market_gap_rank"] = blend_base.groupby("race_id")["ai_market_gap"].rank(ascending=False, method="min")
    blend_base["popularity_bucket"] = blend_base["popularity_rank"].apply(
        lambda v: "unknown"
        if pd.isna(v)
        else ("fav_1_3" if float(v) <= 3 else ("mid_4_6" if float(v) <= 6 else ("long_7_10" if float(v) <= 10 else "deep_11_plus")))
    )
    blend_base["odds_place_bucket"] = blend_base["odds_place"].apply(
        lambda v: "unknown"
        if pd.isna(v)
        else ("low" if float(v) < 2.0 else ("mid" if float(v) < 5.0 else "high"))
    )
    blend_base["field_size_bucket"] = blend_base["field_size"].apply(
        lambda v: "unknown"
        if pd.isna(v)
        else ("small" if int(v) <= 10 else ("medium" if int(v) <= 14 else "large"))
    )

    pred_dir = out_root / "predictions"
    bet_dir = out_root / "bets"
    log_dir = out_root / "logs"
    modeling_dir = out_root / "modeling"
    for d in (pred_dir, bet_dir, log_dir, modeling_dir):
        d.mkdir(parents=True, exist_ok=True)

    pred_path = pred_dir / f"wide_prediction_audit_{race_date}_{model_version}.parquet"
    bet_path = bet_dir / f"wide_pair_candidates_{race_date}_{model_version}.parquet"
    log_path = log_dir / f"wide_skip_log_{race_date}_{model_version}.parquet"
    blend_path = modeling_dir / "market_blend_training_base.parquet"

    merged.to_parquet(pred_path, index=False)
    (bets if len(bets) > 0 else pd.DataFrame()).to_parquet(bet_path, index=False)
    race_skip.assign(generated_at=generated_at).to_parquet(log_path, index=False)
    blend_cols = [
        "race_id",
        "horse_id",
        "p_top3_calibrated",
        "p_top3_shrunk",
        "market_top3_proxy",
        "p_top3_fused",
        "actual_top3",
        "popularity_rank",
        "odds_place",
        "odds_place_max",
        "ai_market_gap",
        "ai_market_gap_rank",
        "race_date",
        "course",
        "distance",
        "surface",
        "field_size",
        "p_top3_logit",
        "market_top3_logit",
        "race_density_top3_ai",
        "race_density_top3_market",
        "race_density_top3_fused",
        "popularity_bucket",
        "odds_place_bucket",
        "field_size_bucket",
    ]
    keep = [c for c in blend_cols if c in blend_base.columns]
    blend_base.assign(generated_at=generated_at)[keep + ["generated_at"]].to_parquet(blend_path, index=False)
    return {
        "prediction_path": str(pred_path),
        "bet_path": str(bet_path),
        "log_path": str(log_path),
        "blend_path": str(blend_path),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--models-root", required=True)
    ap.add_argument("--race-date", required=True)
    ap.add_argument("--model-version", required=True)
    ap.add_argument("--feature-set-version", default="fs_v1")
    ap.add_argument("--decision-ai-weight", type=float, default=0.65)
    ap.add_argument("--out-root", default="data")
    args = ap.parse_args()

    db = DuckDb.connect(Path(args.db_path))
    out = build_outputs(
        db=db,
        models_root=Path(args.models_root),
        race_date=args.race_date,
        model_version=args.model_version,
        feature_snapshot_version=args.feature_set_version,
        decision_ai_weight=float(args.decision_ai_weight),
        out_root=Path(args.out_root),
    )
    print(out)


if __name__ == "__main__":
    main()
