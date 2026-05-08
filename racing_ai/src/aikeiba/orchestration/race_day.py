from __future__ import annotations

import datetime as dt
import json
import math
import uuid
from itertools import combinations
from pathlib import Path
from typing import Any
import pandas as pd
import numpy as np

from aikeiba.checks.doctor_structured import build_doctor_structured_result
from aikeiba.checks.data_quality import run_doctor
from aikeiba.checks.race_metadata import validate_race_metadata, write_race_metadata_validation_report
from aikeiba.common.hashing import stable_fingerprint
from aikeiba.common.run_summary import normalize_run_summary, validate_run_summary
from aikeiba.common.run_log import write_race_day_run_log
from aikeiba.datalab.ingest_csv import ingest_from_csv_dir
from aikeiba.datalab.raw_pipeline import normalize_raw_jv_to_normalized
from aikeiba.db.duckdb import DuckDb
from aikeiba.decision.skip_rules import decide_buy_or_skip
from aikeiba.decision.skip_reasoning import SkipReasonConfig, decide_skip_reason
from aikeiba.decision.pair_scoring import simple_pair_value_score
from aikeiba.decision.value_model import blend_ai_market_prob, compute_market_top3_prob_from_place_odds
from aikeiba.decision.wide_rules import generate_wide_candidates_rule_based
from aikeiba.export.to_static import export_for_dashboard
from aikeiba.features.assemble import build_feature_store_snapshot
from aikeiba.inference.top3 import infer_top3_for_date


PAIR_RERANKER_FEATURES = [
    "pair_prob_naive",
    "pair_value_score",
    "pair_ai_market_gap_sum",
    "pair_ai_market_gap_max",
    "pair_ai_market_gap_min",
    "pair_fused_prob_sum",
    "pair_fused_prob_min",
    "pair_rank_in_race",
    "pair_value_score_rank_pct",
    "pair_value_score_z_in_race",
    "pair_prob_naive_rank_pct",
    "pair_prob_naive_z_in_race",
    "field_size",
    "distance",
    "venue",
    "surface",
    "pair_rank_bucket",
    "field_size_bucket",
    "distance_bucket",
    "pair_missing_flag",
]


def _bucket_rank(v: object) -> str:
    if v is None or pd.isna(v):
        return "UNKNOWN"
    x = float(v)
    if x <= 3:
        return "TOP3"
    if x <= 6:
        return "TOP6"
    if x <= 10:
        return "TOP10"
    return "LOW"


def _bucket_field_size(v: object) -> str:
    if v is None or pd.isna(v):
        return "UNKNOWN"
    n = int(v)
    if n <= 10:
        return "small"
    if n <= 14:
        return "medium"
    return "large"


def _bucket_distance(v: object) -> str:
    if v is None or pd.isna(v):
        return "UNKNOWN"
    d = int(v)
    if d < 1400:
        return "sprint"
    if d < 2000:
        return "mile"
    if d < 2600:
        return "middle"
    return "long"


def _safe_float(v: object) -> float | None:
    if v is None or pd.isna(v):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _pair_market_implied_prob_from_row(row: dict[str, Any]) -> float | None:
    # Prefer explicit pair odds if present.
    for key in ("pair_wide_odds", "wide_odds", "odds_wide", "pair_odds"):
        ov = _safe_float(row.get(key))
        if ov is not None and ov > 1.0:
            return max(min(1.0 / ov, 0.999999), 1e-6)
    # Fallback: combine horse-level market proxy probabilities.
    m1 = _safe_float(row.get("horse1_market_top3_proxy"))
    m2 = _safe_float(row.get("horse2_market_top3_proxy"))
    if m1 is not None and m2 is not None:
        p = m1 * m2
        return max(min(p, 0.999999), 1e-6)
    # Secondary fallback: infer from fused probs minus AI-market gaps when possible.
    fsum = _safe_float(row.get("pair_fused_prob_sum"))
    gsum = _safe_float(row.get("pair_ai_market_gap_sum"))
    if fsum is not None and gsum is not None:
        p = max((fsum - gsum) / 2.0, 0.0)
        return max(min(p, 0.999999), 1e-6)
    return None


def _enrich_pair_rows_for_shadow_selection(
    *,
    pair_rows: list[dict[str, Any]],
    base_top_n: int,
    score_threshold: float = 0.08,
    edge_threshold: float = 0.0,
    gap_threshold: float = 0.01,
    default_k: int | None = None,
    min_k: int = 1,
    max_k: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if len(pair_rows) == 0:
        return pair_rows, {
            "buy_race_count": 0,
            "skip_race_count": 0,
            "selected_pair_count": 0,
            "avg_pair_edge_selected": None,
            "skip_reason_counts": {},
            "rule_model_overlap_avg": None,
        }

    df = pd.DataFrame(pair_rows).copy()
    if len(df) == 0:
        return pair_rows, {
            "buy_race_count": 0,
            "skip_race_count": 0,
            "selected_pair_count": 0,
            "avg_pair_edge_selected": None,
            "skip_reason_counts": {},
            "rule_model_overlap_avg": None,
        }

    # Pair relation features
    pmin = df["pair_fused_prob_min"].astype(float)
    psum = df["pair_fused_prob_sum"].astype(float)
    pmax = psum - pmin
    df["p_top3_fused_min"] = pmin
    df["p_top3_fused_max"] = pmax
    df["p_top3_fused_hmean"] = np.where((pmin > 0) & (pmax > 0), 2.0 / ((1.0 / pmin) + (1.0 / pmax)), np.nan)
    df["p_top3_fused_abs_diff"] = (pmax - pmin).abs()
    df["p_top3_fused_ratio"] = np.where((pmin > 0), pmax / pmin, np.nan)
    gmin = pd.to_numeric(df.get("pair_ai_market_gap_min"), errors="coerce")
    gmax = pd.to_numeric(df.get("pair_ai_market_gap_max"), errors="coerce")
    df["ai_market_gap_min"] = gmin
    df["ai_market_gap_max"] = gmax
    df["ai_market_gap_abs_diff"] = (gmax - gmin).abs()
    df["both_positive_gap_flag"] = ((gmin > 0) & (gmax > 0)).astype(int)
    df["one_side_positive_gap_flag"] = (((gmin > 0) ^ (gmax > 0))).astype(int)

    # Edge features
    df["pair_market_implied_prob"] = [
        _pair_market_implied_prob_from_row(r)
        for r in df.to_dict("records")
    ]
    df["pair_edge"] = pd.to_numeric(df.get("pair_model_score"), errors="coerce") - pd.to_numeric(df["pair_market_implied_prob"], errors="coerce")

    # Rank/gap features in race
    df["pair_model_score_rank_in_race"] = df.groupby("race_id")["pair_model_score"].rank(method="min", ascending=False)
    df["pair_model_score_rank_pct_in_race"] = df.groupby("race_id")["pair_model_score"].rank(method="average", pct=True, ascending=False)
    top_model = df.groupby("race_id")["pair_model_score"].transform("max")
    df["pair_model_score_gap_to_top"] = top_model - pd.to_numeric(df["pair_model_score"], errors="coerce")
    sort_model = df.sort_values(["race_id", "pair_model_score"], ascending=[True, False]).copy()
    sort_model["pair_model_score_gap_to_next"] = pd.to_numeric(sort_model["pair_model_score"], errors="coerce") - pd.to_numeric(sort_model.groupby("race_id")["pair_model_score"].shift(-1), errors="coerce")
    sort_model["pair_model_score_gap_from_prev"] = pd.to_numeric(sort_model.groupby("race_id")["pair_model_score"].shift(1), errors="coerce") - pd.to_numeric(sort_model["pair_model_score"], errors="coerce")
    df = df.merge(
        sort_model[["race_id", "pair", "pair_model_score_gap_to_next", "pair_model_score_gap_from_prev"]],
        on=["race_id", "pair"],
        how="left",
    )
    df["pair_value_score_rank_in_race"] = df.groupby("race_id")["pair_value_score"].rank(method="min", ascending=False)
    top_value = df.groupby("race_id")["pair_value_score"].transform("max")
    df["pair_value_score_gap_to_top"] = top_value - pd.to_numeric(df["pair_value_score"], errors="coerce")
    sort_value = df.sort_values(["race_id", "pair_value_score"], ascending=[True, False]).copy()
    sort_value["pair_value_score_gap_to_next"] = pd.to_numeric(sort_value["pair_value_score"], errors="coerce") - pd.to_numeric(sort_value.groupby("race_id")["pair_value_score"].shift(-1), errors="coerce")
    df = df.merge(
        sort_value[["race_id", "pair", "pair_value_score_gap_to_next"]],
        on=["race_id", "pair"],
        how="left",
    )
    df["pair_edge_rank_in_race"] = df.groupby("race_id")["pair_edge"].rank(method="min", ascending=False)
    sort_edge = df.sort_values(["race_id", "pair_edge"], ascending=[True, False]).copy()
    sort_edge["pair_edge_gap_to_next"] = pd.to_numeric(sort_edge["pair_edge"], errors="coerce") - pd.to_numeric(sort_edge.groupby("race_id")["pair_edge"].shift(-1), errors="coerce")
    df = df.merge(sort_edge[["race_id", "pair", "pair_edge_gap_to_next"]], on=["race_id", "pair"], how="left")
    df["both_positive_edge_flag"] = (pd.to_numeric(df["pair_edge"], errors="coerce") > 0).astype(int)
    df["one_side_positive_edge_flag"] = (pd.to_numeric(df.get("pair_ai_market_gap_sum"), errors="coerce") > 0).astype(int)

    # Dynamic score and selection (shadow only)
    edge_pos = pd.to_numeric(df["pair_edge"], errors="coerce").fillna(0.0).clip(lower=0.0)
    model_score = pd.to_numeric(df["pair_model_score"], errors="coerce")
    df["model_hit_score"] = model_score
    df["model_value_score"] = pd.to_numeric(df["pair_edge"], errors="coerce")
    df["model_dynamic_final_score"] = np.where(model_score.notna(), model_score * (1.0 + edge_pos), model_score)

    skip_reason_counts: dict[str, int] = {}
    selected_edges: list[float] = []
    overlap_rates: list[float] = []
    buy_race_count = 0
    skip_race_count = 0
    selected_pair_count = 0
    out_chunks: list[pd.DataFrame] = []
    base_k = max(1, int(default_k if default_k is not None else base_top_n))
    min_k_eff = max(1, int(min_k))
    max_k_eff = int(max_k) if max_k is not None else max(1, int(base_top_n))
    max_k_eff = max(min_k_eff, max_k_eff)
    for rid, g in df.groupby("race_id", sort=False):
        gg = g.sort_values("model_dynamic_final_score", ascending=False).copy()
        top1 = _safe_float(gg["model_dynamic_final_score"].iloc[0]) if len(gg) > 0 else None
        top2 = _safe_float(gg["model_dynamic_final_score"].iloc[1]) if len(gg) > 1 else None
        top_edge = _safe_float(gg["pair_edge"].iloc[0]) if len(gg) > 0 else None
        gap = None if top1 is None or top2 is None else (top1 - top2)
        reason = "DYNAMIC_BUY_OK"
        if top1 is None or top1 < score_threshold:
            reason = "DYNAMIC_SKIP_MODEL_SCORE_WEAK"
        elif top_edge is None or top_edge < edge_threshold:
            reason = "DYNAMIC_SKIP_EDGE_WEAK"
        elif gap is None or gap < gap_threshold:
            reason = "DYNAMIC_SKIP_GAP_SMALL"
        skip_reason_counts[reason] = int(skip_reason_counts.get(reason, 0) + 1)

        if reason.startswith("DYNAMIC_SKIP"):
            skip_race_count += 1
            k = 0
        else:
            buy_race_count += 1
            # dynamic K: strong edge => fewer concentrated pairs, otherwise up to base top-n
            if top_edge is not None and top_edge >= 0.08:
                k = min(3, base_k)
            elif top_edge is not None and top_edge >= 0.03:
                k = min(4, base_k)
            else:
                k = base_k
            k = max(min_k_eff, min(max_k_eff, int(k)))
        gg["model_dynamic_k"] = int(k)
        gg["model_dynamic_skip_reason"] = reason
        gg["model_dynamic_rank"] = range(1, len(gg) + 1)
        gg["model_dynamic_selected_flag"] = gg["model_dynamic_rank"] <= int(k)
        selected = gg[gg["model_dynamic_selected_flag"]]
        selected_pair_count += int(len(selected))
        if len(selected) > 0:
            selected_edges.extend([float(x) for x in pd.to_numeric(selected["pair_edge"], errors="coerce").dropna().tolist()])

        rule_selected = set(gg.loc[gg.get("pair_selected_flag", False) == True, "pair"].astype(str).tolist())
        model_selected = set(selected["pair"].astype(str).tolist())
        if len(model_selected) > 0:
            overlap_rates.append(len(rule_selected.intersection(model_selected)) / float(len(model_selected)))
        out_chunks.append(gg)

    out_df = pd.concat(out_chunks, ignore_index=True) if len(out_chunks) > 0 else df
    stable_reason_keys = [
        "DYNAMIC_BUY_OK",
        "DYNAMIC_SKIP_MODEL_SCORE_WEAK",
        "DYNAMIC_SKIP_EDGE_WEAK",
        "DYNAMIC_SKIP_GAP_SMALL",
    ]
    stable_counts = {k: int(skip_reason_counts.get(k, 0)) for k in stable_reason_keys}
    other_count = int(sum(v for k, v in skip_reason_counts.items() if k not in stable_reason_keys))
    if other_count > 0:
        stable_counts["DYNAMIC_SKIP_OTHER"] = other_count

    metrics = {
        "buy_race_count": int(buy_race_count),
        "skip_race_count": int(skip_race_count),
        "selected_pair_count": int(selected_pair_count),
        "avg_pair_edge_selected": (float(sum(selected_edges) / len(selected_edges)) if len(selected_edges) > 0 else None),
        "skip_reason_counts": stable_counts,
        "rule_model_overlap_avg": (float(sum(overlap_rates) / len(overlap_rates)) if len(overlap_rates) > 0 else None),
    }
    return out_df.to_dict("records"), metrics


def _apply_pair_shadow_scores(
    *,
    pair_rows: list[dict[str, Any]],
    pair_model_root: Path,
    pair_model_version: str | None,
    race_meta_map: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str], bool]:
    warnings: list[str] = []
    if len(pair_rows) == 0:
        return pair_rows, warnings, False
    if pair_model_version is None or str(pair_model_version).strip() == "":
        for r in pair_rows:
            r["pair_model_score"] = None
            r["pair_model_rank_in_race"] = None
            r["pair_model_version"] = pair_model_version
            r["pair_model_available"] = False
        warnings.append("pair_shadow_model_version_missing")
        return pair_rows, warnings, False

    model_path = pair_model_root / pair_model_version / "model.txt"
    if not model_path.exists():
        for r in pair_rows:
            r["pair_model_score"] = None
            r["pair_model_rank_in_race"] = None
            r["pair_model_version"] = pair_model_version
            r["pair_model_available"] = False
        warnings.append(f"pair_shadow_model_not_found:{model_path}")
        return pair_rows, warnings, False

    try:
        import lightgbm as lgb

        booster = lgb.Booster(model_file=str(model_path))
        df = pd.DataFrame(pair_rows).copy()
        if len(df) == 0:
            return pair_rows, warnings, False
        meta_df = pd.DataFrame(
            [
                {
                    "race_id": rid,
                    "venue": m.get("venue"),
                    "surface": m.get("surface"),
                    "distance": m.get("distance"),
                    "field_size": m.get("field_size"),
                }
                for rid, m in race_meta_map.items()
            ]
        )
        if len(meta_df) > 0:
            df = df.merge(meta_df, on="race_id", how="left")
        df["pair_value_score_rank_pct"] = df.groupby("race_id")["pair_value_score"].rank(method="average", pct=True)
        g1 = df.groupby("race_id")["pair_value_score"]
        df["pair_value_score_z_in_race"] = (df["pair_value_score"] - g1.transform("mean")) / g1.transform("std").replace(0, pd.NA)
        df["pair_prob_naive_rank_pct"] = df.groupby("race_id")["pair_prob_naive"].rank(method="average", pct=True)
        g2 = df.groupby("race_id")["pair_prob_naive"]
        df["pair_prob_naive_z_in_race"] = (df["pair_prob_naive"] - g2.transform("mean")) / g2.transform("std").replace(0, pd.NA)
        df["pair_rank_bucket"] = df["pair_rank_in_race"].apply(_bucket_rank)
        df["field_size_bucket"] = df["field_size"].apply(_bucket_field_size)
        df["distance_bucket"] = df["distance"].apply(_bucket_distance)

        feature_cols = [c for c in PAIR_RERANKER_FEATURES if c in df.columns]
        for c in PAIR_RERANKER_FEATURES:
            if c not in df.columns:
                df[c] = pd.NA
        x = df[PAIR_RERANKER_FEATURES].copy()
        cat_cols = [c for c in x.columns if str(x[c].dtype) in ("object", "bool", "category")]
        for c in cat_cols:
            x[c] = x[c].astype("category")
        pred = booster.predict(x)
        df["pair_model_score"] = pred
        df["pair_model_rank_in_race"] = df.groupby("race_id")["pair_model_score"].rank(method="min", ascending=False)
        df["pair_model_version"] = pair_model_version
        df["pair_model_available"] = True

        out_rows = df.to_dict("records")
        return out_rows, warnings, True
    except Exception as exc:
        for r in pair_rows:
            r["pair_model_score"] = None
            r["pair_model_rank_in_race"] = None
            r["pair_model_version"] = pair_model_version
            r["pair_model_available"] = False
        warnings.append(f"pair_shadow_model_load_failed:{exc.__class__.__name__}:{exc}")
        return pair_rows, warnings, False


def _latest_top3_predictions_for_date(db: DuckDb, race_date: str, model_version: str) -> list[dict[str, Any]]:
    return db.query_df(
        """
        WITH latest AS (
          SELECT race_id, horse_no, model_version, max(inference_timestamp) AS ts
          FROM horse_predictions
          WHERE model_version = ?
          GROUP BY race_id, horse_no, model_version
        )
        SELECT hp.race_id, hp.horse_no, hp.p_top3
        FROM horse_predictions hp
        JOIN latest l
          ON l.race_id=hp.race_id
         AND l.horse_no=hp.horse_no
         AND l.model_version=hp.model_version
         AND l.ts=hp.inference_timestamp
        JOIN races r ON r.race_id = hp.race_id
        WHERE r.race_date = cast(? as DATE)
        ORDER BY hp.race_id, hp.horse_no
        """,
        (model_version, race_date),
    ).to_dict("records")


def _decision_preview(
    db: DuckDb,
    race_date: str,
    model_version: str,
    *,
    density_top3_max: float = 1.35,
    gap12_min: float = 0.003,
    ai_weight: float = 0.65,
    skip_reason_config: SkipReasonConfig | None = None,
    pair_top_n_selected: int = 5,
    model_dynamic_min_score: float = 0.08,
    model_dynamic_min_edge: float = 0.0,
    model_dynamic_min_gap: float = 0.01,
    model_dynamic_default_k: int = 5,
    model_dynamic_min_k: int = 1,
    model_dynamic_max_k: int = 5,
    pair_model_root: Path | None = None,
    pair_model_version: str | None = "pair_reranker_ts_v4",
    excluded_race_ids: set[str] | None = None,
    emit_expanded_candidates: bool = False,
    expanded_top_horse_n: int = 10,
    expanded_ai_gap_horse_n: int = 10,
    expanded_max_pairs_per_race: int = 45,
) -> dict[str, Any]:
    cfg = skip_reason_config or SkipReasonConfig(
        density_top3_max=density_top3_max,
        gap12_min=gap12_min,
    )
    races = db.query_df("SELECT race_id FROM races WHERE race_date = cast(? as DATE)", (race_date,)).to_dict("records")
    excluded = set(str(x) for x in (excluded_race_ids or set()))
    if excluded:
        races = [r for r in races if str(r.get("race_id")) not in excluded]
    entries = db.query_df(
        """
        SELECT r.race_id, e.horse_no, e.horse_id
        FROM entries e
        JOIN races r ON r.race_id=e.race_id
        WHERE r.race_date = cast(? as DATE)
          AND (e.is_scratched IS NULL OR e.is_scratched=FALSE)
        """,
        (race_date,),
    ).to_dict("records")
    preds = _latest_top3_predictions_for_date(db, race_date, model_version)
    pred_map = {(p["race_id"], int(p["horse_no"])): p.get("p_top3") for p in preds}
    market_probs = _latest_market_top3_probs_for_date(db=db, race_date=race_date)

    by_race_horse: dict[str, list[int]] = {}
    horse_id_map: dict[tuple[str, int], str | None] = {}
    for e in entries:
        rid0 = str(e["race_id"])
        hn0 = int(e["horse_no"])
        by_race_horse.setdefault(rid0, []).append(hn0)
        horse_id_map[(rid0, hn0)] = e.get("horse_id")

    buy_races = 0
    total_candidates = 0
    candidate_pairs: list[dict[str, Any]] = []
    candidate_pairs_expanded: list[dict[str, Any]] = []
    race_flags: list[dict[str, Any]] = []
    shadow_warnings: list[str] = []
    shadow_available_any = False
    dynamic_metrics = {
        "buy_race_count": 0,
        "skip_race_count": 0,
        "selected_pair_count": 0,
        "avg_pair_edge_selected": None,
        "skip_reason_counts": {},
        "rule_model_overlap_avg": None,
    }
    race_meta_df = db.query_df(
        """
        SELECT race_id, venue, surface, distance,
               field_size_expected AS field_size
        FROM races
        WHERE race_date = cast(? as DATE)
        """,
        (race_date,),
    )
    race_meta_map: dict[str, dict[str, Any]] = {
        str(r["race_id"]): {
            "venue": r.get("venue"),
            "surface": r.get("surface"),
            "distance": r.get("distance"),
            "field_size": r.get("field_size"),
        }
        for r in race_meta_df.to_dict("records")
    }
    for r in races:
        rid = r["race_id"]
        horse_nos = by_race_horse.get(rid, [])
        p_top3_ai = {hn: float(pred_map[(rid, hn)]) for hn in horse_nos if pred_map.get((rid, hn)) is not None}
        p_top3_dict: dict[int, float] = {}
        for hn in horse_nos:
            p_ai = p_top3_ai.get(hn)
            if p_ai is None:
                continue
            p_mkt = market_probs.get((rid, hn))
            p_fused = blend_ai_market_prob(p_ai=p_ai, p_market=p_mkt, ai_weight=ai_weight)
            if p_fused is not None:
                p_top3_dict[hn] = float(p_fused)
        decision = decide_buy_or_skip(
            p_top3=list(p_top3_dict.values()),
            density_top3_max=density_top3_max,
            gap12_min=gap12_min,
        )
        positive_ai_gap_count = 0
        gap_items: list[tuple[int, float, float]] = []  # (horse_no, p_fused, gap)
        for hn, p_fused in p_top3_dict.items():
            p_mkt = market_probs.get((rid, hn))
            if p_mkt is None:
                continue
            gap = float(p_fused - p_mkt)
            gap_items.append((hn, float(p_fused), gap))
            if gap > 0:
                positive_ai_gap_count += 1
        market_top1 = max([float(v) for (rr, _), v in market_probs.items() if rr == rid], default=None)
        top3_gap_items = sorted(gap_items, key=lambda x: x[1], reverse=True)[:3]
        market_overrated_top_count = int(sum(1 for _, _, g in top3_gap_items if g < 0))
        race_ai_market_gap_max = max([g for _, _, g in gap_items]) if len(gap_items) > 0 else None
        skip_reason = decide_skip_reason(
            density_top3=decision.density_top3,
            gap12=decision.gap12,
            positive_ai_gap_count=positive_ai_gap_count,
            candidate_count=0,
            market_top1=market_top1,
            top_pair_value_score=None,
            race_ai_market_gap_max=race_ai_market_gap_max,
            market_overrated_top_count=market_overrated_top_count,
            config=cfg,
        )
        buy_flag = skip_reason == "BUY_OK"
        if buy_flag:
            buy_races += 1
            cands = generate_wide_candidates_rule_based(
                race_id=rid,
                horse_nos=horse_nos,
                p_top3=p_top3_dict,
                axis_k=1,
                partner_k=min(6, len(horse_nos)),
            )
            pair_rows: list[dict[str, Any]] = []
            for c in cands:
                h1, h2 = sorted((int(c.axis_horse_no), int(c.partner_horse_no)))
                p1 = p_top3_dict.get(h1)
                p2 = p_top3_dict.get(h2)
                g1 = None
                g2 = None
                m1 = market_probs.get((rid, h1))
                m2 = market_probs.get((rid, h2))
                if p1 is not None and m1 is not None:
                    g1 = float(p1 - m1)
                if p2 is not None and m2 is not None:
                    g2 = float(p2 - m2)
                pair_prob_naive, pair_value_score, pair_missing_flag = simple_pair_value_score(
                    p1=p1, p2=p2, gap1=g1, gap2=g2
                )
                pair_rows.append(
                    {
                        "race_id": c.race_id,
                        "pair": c.pair,
                        "pair_norm": c.pair,
                        "horse1_umaban": h1,
                        "horse2_umaban": h2,
                        "selected_stage": c.selected_stage,
                        "pair_value_score_version": "v1_simple_gap_bonus",
                        "pair_prob_naive": pair_prob_naive,
                        "pair_ai_market_gap_sum": ((g1 or 0.0) + (g2 or 0.0)),
                        "pair_ai_market_gap_max": max([x for x in [g1, g2] if x is not None], default=None),
                        "pair_ai_market_gap_min": min([x for x in [g1, g2] if x is not None], default=None),
                        "pair_fused_prob_sum": ((p1 or 0.0) + (p2 or 0.0)),
                        "pair_fused_prob_min": min([x for x in [p1, p2] if x is not None], default=None),
                        "pair_value_score": pair_value_score,
                        "pair_missing_flag": pair_missing_flag,
                    }
                )
            pair_rows = sorted(
                pair_rows,
                key=lambda x: float("-inf") if x.get("pair_value_score") is None else float(x["pair_value_score"]),
                reverse=True,
            )
            for idx, row in enumerate(pair_rows, start=1):
                row["pair_rank_in_race"] = idx
                row["pair_selected_flag"] = bool(idx <= max(1, int(pair_top_n_selected)))
                row["pair_selection_reason"] = "SELECT_TOP_PAIR_SCORE" if row["pair_selected_flag"] else "NOT_SELECTED_LOW_PAIR_SCORE"
            total_candidates += len(cands)
            top_pair_value_score = pair_rows[0]["pair_value_score"] if len(pair_rows) > 0 else None
            # Re-evaluate with pair/candidate dependent rules
            skip_reason_after = decide_skip_reason(
                density_top3=decision.density_top3,
                gap12=decision.gap12,
                positive_ai_gap_count=positive_ai_gap_count,
                candidate_count=len(pair_rows),
                market_top1=market_top1,
                top_pair_value_score=top_pair_value_score,
                race_ai_market_gap_max=race_ai_market_gap_max,
                market_overrated_top_count=market_overrated_top_count,
                config=cfg,
            )
            if skip_reason_after != "BUY_OK":
                buy_races -= 1
                skip_reason = skip_reason_after
            else:
                scored_rows, sw, sa = _apply_pair_shadow_scores(
                    pair_rows=pair_rows,
                    pair_model_root=pair_model_root or Path("racing_ai/data/models_compare/pair_reranker"),
                    pair_model_version=pair_model_version,
                    race_meta_map=race_meta_map,
                )
                scored_rows, dm = _enrich_pair_rows_for_shadow_selection(
                    pair_rows=scored_rows,
                    base_top_n=max(1, int(pair_top_n_selected)),
                    score_threshold=float(model_dynamic_min_score),
                    edge_threshold=float(model_dynamic_min_edge),
                    gap_threshold=float(model_dynamic_min_gap),
                    default_k=max(1, int(model_dynamic_default_k)),
                    min_k=max(1, int(model_dynamic_min_k)),
                    max_k=max(1, int(model_dynamic_max_k)),
                )
                dynamic_metrics["buy_race_count"] += int(dm.get("buy_race_count", 0))
                dynamic_metrics["skip_race_count"] += int(dm.get("skip_race_count", 0))
                dynamic_metrics["selected_pair_count"] += int(dm.get("selected_pair_count", 0))
                s = dynamic_metrics.get("skip_reason_counts", {})
                for k, v in (dm.get("skip_reason_counts", {}) or {}).items():
                    s[k] = int(s.get(k, 0) + int(v))
                dynamic_metrics["skip_reason_counts"] = s
                # keep simple weighted aggregate for averages
                if dm.get("avg_pair_edge_selected") is not None:
                    cur = dynamic_metrics.get("avg_pair_edge_selected")
                    if cur is None:
                        dynamic_metrics["avg_pair_edge_selected"] = float(dm["avg_pair_edge_selected"])
                    else:
                        dynamic_metrics["avg_pair_edge_selected"] = float((float(cur) + float(dm["avg_pair_edge_selected"])) / 2.0)
                if dm.get("rule_model_overlap_avg") is not None:
                    cur2 = dynamic_metrics.get("rule_model_overlap_avg")
                    if cur2 is None:
                        dynamic_metrics["rule_model_overlap_avg"] = float(dm["rule_model_overlap_avg"])
                    else:
                        dynamic_metrics["rule_model_overlap_avg"] = float((float(cur2) + float(dm["rule_model_overlap_avg"])) / 2.0)
                shadow_warnings.extend(sw)
                shadow_available_any = shadow_available_any or sa
                for row in scored_rows:
                    candidate_pairs.append(row)
                if emit_expanded_candidates:
                    base_rows_by_pair = {str(rw.get("pair_norm") or rw.get("pair")): rw for rw in scored_rows}
                    expanded_rows = _build_expanded_pair_rows(
                        race_id=str(rid),
                        horse_nos=[int(x) for x in horse_nos],
                        p_top3_dict=p_top3_dict,
                        market_probs=market_probs,
                        horse_id_map=horse_id_map,
                        base_rows_by_pair=base_rows_by_pair,
                        expanded_top_horse_n=max(2, int(expanded_top_horse_n)),
                        expanded_ai_gap_horse_n=max(2, int(expanded_ai_gap_horse_n)),
                        expanded_max_pairs_per_race=max(5, int(expanded_max_pairs_per_race)),
                    )
                    exp_rows_scored, sw2, sa2 = _apply_pair_shadow_scores(
                        pair_rows=expanded_rows,
                        pair_model_root=pair_model_root or Path("racing_ai/data/models_compare/pair_reranker"),
                        pair_model_version=pair_model_version,
                        race_meta_map=race_meta_map,
                    )
                    exp_rows_scored, _ = _enrich_pair_rows_for_shadow_selection(
                        pair_rows=exp_rows_scored,
                        base_top_n=max(1, int(pair_top_n_selected)),
                        score_threshold=float(model_dynamic_min_score),
                        edge_threshold=float(model_dynamic_min_edge),
                        gap_threshold=float(model_dynamic_min_gap),
                        default_k=max(1, int(model_dynamic_default_k)),
                        min_k=max(1, int(model_dynamic_min_k)),
                        max_k=max(1, int(model_dynamic_max_k)),
                    )
                    shadow_warnings.extend(sw2)
                    shadow_available_any = shadow_available_any or sa2
                    for row in exp_rows_scored:
                        row.setdefault("expanded_candidate_flag", bool(not row.get("pair_selected_flag", False)))
                        row.setdefault("expanded_source", "rule_selected" if bool(row.get("pair_selected_flag", False)) else "expanded")
                        candidate_pairs_expanded.append(row)
        race_flags.append(
            {
                "race_id": rid,
                "buy_flag": buy_flag,
                "reason": skip_reason,
                "density_top3": decision.density_top3,
                "gap12": decision.gap12,
                "race_ai_market_gap_max": (max([g for _, _, g in gap_items]) if len(gap_items) > 0 else None),
                "race_ai_market_gap_mean_top3": (
                    sum([g for _, _, g in sorted(gap_items, key=lambda x: x[1], reverse=True)[:3]]) / len(sorted(gap_items, key=lambda x: x[1], reverse=True)[:3])
                    if len(gap_items) > 0
                    else None
                ),
                "race_ai_market_gap_mean_top5": (
                    sum([g for _, _, g in sorted(gap_items, key=lambda x: x[1], reverse=True)[:5]]) / len(sorted(gap_items, key=lambda x: x[1], reverse=True)[:5])
                    if len(gap_items) > 0
                    else None
                ),
                "race_value_horse_count": int(sum(1 for _, _, g in gap_items if g > 0)),
                "race_market_overrated_top_count": market_overrated_top_count,
                "race_density_top3": decision.density_top3,
                "race_gap12": decision.gap12,
                "race_skip_reason": skip_reason,
                "decision_ai_weight": ai_weight,
            }
        )
    return {
        "buy_races": buy_races,
        "total_candidates": total_candidates,
        "candidate_pairs": candidate_pairs,
        "race_flags": race_flags,
        "pair_model_version": pair_model_version,
        "pair_shadow_mode": True,
        "pair_model_available": shadow_available_any,
        "shadow_warnings": sorted(set(shadow_warnings)),
        "model_dynamic_buy_race_count": int(dynamic_metrics.get("buy_race_count", 0)),
        "model_dynamic_skip_race_count": int(dynamic_metrics.get("skip_race_count", 0)),
        "model_dynamic_selected_pair_count": int(dynamic_metrics.get("selected_pair_count", 0)),
        "avg_pair_edge_selected": dynamic_metrics.get("avg_pair_edge_selected"),
        "rule_model_overlap_avg": dynamic_metrics.get("rule_model_overlap_avg"),
        "model_dynamic_skip_reason_counts": dynamic_metrics.get("skip_reason_counts", {}),
        "candidate_pairs_expanded": candidate_pairs_expanded,
    }


def _latest_market_top3_probs_for_date(
    *,
    db: DuckDb,
    race_date: str,
) -> dict[tuple[str, int], float]:
    rows = db.query_df(
        """
        WITH ranked AS (
          SELECT
            o.race_id,
            o.horse_no,
            lower(o.odds_type) AS odds_type,
            o.odds_value,
            o.captured_at,
            row_number() OVER (
              PARTITION BY o.race_id, o.horse_no, lower(o.odds_type)
              ORDER BY o.captured_at DESC NULLS LAST, o.odds_snapshot_version DESC
            ) AS rn
          FROM odds o
          JOIN races r ON r.race_id = o.race_id
          WHERE r.race_date = cast(? as DATE)
            AND lower(o.odds_type) IN ('place', 'place_max')
            AND o.horse_no > 0
        ),
        latest AS (
          SELECT race_id, horse_no,
                 max(CASE WHEN odds_type='place' THEN odds_value END) AS place_min,
                 max(CASE WHEN odds_type='place_max' THEN odds_value END) AS place_max
          FROM ranked
          WHERE rn = 1
          GROUP BY race_id, horse_no
        )
        SELECT race_id, horse_no, place_min, place_max
        FROM latest
        """,
        (race_date,),
    ).to_dict("records")
    by_race: dict[str, list[tuple[int, float]]] = {}
    for r in rows:
        race_id = str(r["race_id"])
        horse_no = int(r["horse_no"])
        p = compute_market_top3_prob_from_place_odds(r.get("place_min"), r.get("place_max"))
        if p is None:
            continue
        by_race.setdefault(race_id, []).append((horse_no, float(p)))

    out: dict[tuple[str, int], float] = {}
    for race_id, vals in by_race.items():
        s = float(sum(v for _, v in vals))
        if s <= 0:
            continue
        scale = 3.0 / s
        for horse_no, v in vals:
            out[(race_id, horse_no)] = min(1.0, max(0.0, v * scale))
    return out


def _normalize_pair_key(pair: str | None) -> str | None:
    if pair is None:
        return None
    parts = str(pair).strip().split("-")
    if len(parts) != 2:
        return None
    try:
        a = int(parts[0])
        b = int(parts[1])
    except Exception:
        return None
    x, y = sorted((a, b))
    return f"{x:02d}-{y:02d}"


def _build_expanded_pair_rows(
    *,
    race_id: str,
    horse_nos: list[int],
    p_top3_dict: dict[int, float],
    market_probs: dict[tuple[str, int], float],
    horse_id_map: dict[tuple[str, int], str | None],
    base_rows_by_pair: dict[str, dict[str, Any]],
    expanded_top_horse_n: int,
    expanded_ai_gap_horse_n: int,
    expanded_max_pairs_per_race: int,
) -> list[dict[str, Any]]:
    if not horse_nos:
        return []
    pairs: dict[str, tuple[int, int, str]] = {}

    def _add_pair(h1: int, h2: int, source: str) -> None:
        a, b = sorted((int(h1), int(h2)))
        if a == b:
            return
        key = f"{a:02d}-{b:02d}"
        if key not in pairs:
            pairs[key] = (a, b, source)

    for key, row in base_rows_by_pair.items():
        try:
            a = int(row.get("horse1_umaban"))
            b = int(row.get("horse2_umaban"))
            _add_pair(a, b, "rule_selected")
        except Exception:
            continue

    ranked_horses = sorted(
        [(hn, float(p_top3_dict.get(hn, 0.0))) for hn in horse_nos],
        key=lambda x: x[1],
        reverse=True,
    )
    top_horses = [hn for hn, _ in ranked_horses[: max(2, int(expanded_top_horse_n))]]
    for h1, h2 in combinations(top_horses, 2):
        _add_pair(h1, h2, "top_horse_pairs")

    gap_rank = []
    for hn in horse_nos:
        p_fused = p_top3_dict.get(hn)
        p_mkt = market_probs.get((race_id, hn))
        if p_fused is None or p_mkt is None:
            continue
        gap_rank.append((hn, float(p_fused - p_mkt)))
    gap_horses = [hn for hn, _ in sorted(gap_rank, key=lambda x: x[1], reverse=True)[: max(2, int(expanded_ai_gap_horse_n))]]
    mix_pool = sorted(set(top_horses).union(gap_horses))
    for h1, h2 in combinations(mix_pool, 2):
        _add_pair(h1, h2, "ai_gap_pairs")

    out_rows: list[dict[str, Any]] = []
    max_pairs = max(1, int(expanded_max_pairs_per_race))
    for key, (h1, h2, source) in sorted(pairs.items()):
        p1 = p_top3_dict.get(h1)
        p2 = p_top3_dict.get(h2)
        m1 = market_probs.get((race_id, h1))
        m2 = market_probs.get((race_id, h2))
        g1 = float(p1 - m1) if (p1 is not None and m1 is not None) else None
        g2 = float(p2 - m2) if (p2 is not None and m2 is not None) else None
        pair_prob_naive, pair_value_score, pair_missing_flag = simple_pair_value_score(
            p1=p1, p2=p2, gap1=g1, gap2=g2
        )
        base = base_rows_by_pair.get(key, {})
        row = {
            "race_id": race_id,
            "pair": key,
            "pair_norm": key,
            "horse1_umaban": h1,
            "horse2_umaban": h2,
            "horse1_horse_id": horse_id_map.get((race_id, h1)),
            "horse2_horse_id": horse_id_map.get((race_id, h2)),
            "horse1_p_top3_fused": p1,
            "horse2_p_top3_fused": p2,
            "horse1_ai_market_gap": g1,
            "horse2_ai_market_gap": g2,
            "horse1_market_top3_proxy": m1,
            "horse2_market_top3_proxy": m2,
            "expanded_source": source if source != "rule_selected" else "rule_selected",
            "expanded_candidate_flag": bool(source != "rule_selected"),
            "pair_selected_flag": bool(base.get("pair_selected_flag", False)),
            "selected_stage": base.get("selected_stage", source),
            "pair_value_score_version": base.get("pair_value_score_version", "v1_simple_gap_bonus"),
            "pair_prob_naive": pair_prob_naive,
            "pair_ai_market_gap_sum": ((g1 or 0.0) + (g2 or 0.0)),
            "pair_ai_market_gap_max": max([x for x in [g1, g2] if x is not None], default=None),
            "pair_ai_market_gap_min": min([x for x in [g1, g2] if x is not None], default=None),
            "pair_fused_prob_sum": ((p1 or 0.0) + (p2 or 0.0)),
            "pair_fused_prob_min": min([x for x in [p1, p2] if x is not None], default=None),
            "pair_value_score": pair_value_score,
            "pair_missing_flag": pair_missing_flag,
            "pair_selection_reason": base.get("pair_selection_reason", ("SELECT_TOP_PAIR_SCORE" if bool(base.get("pair_selected_flag", False)) else "NOT_SELECTED_LOW_PAIR_SCORE")),
        }
        out_rows.append(row)

    out_rows = sorted(
        out_rows,
        key=lambda x: float("-inf") if x.get("pair_value_score") is None else float(x["pair_value_score"]),
        reverse=True,
    )[:max_pairs]
    for idx, row in enumerate(out_rows, start=1):
        row["pair_rank_in_race"] = idx
    return out_rows


def _max_losing_streak(flags: list[int]) -> int:
    streak = 0
    max_streak = 0
    for f in flags:
        if f == 0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak


def _compute_betting_metrics(
    *,
    db: DuckDb,
    race_date: str,
    decision: dict[str, Any],
) -> dict[str, Any]:
    candidates = decision.get("candidate_pairs", []) if isinstance(decision, dict) else []
    total_bets = int(len(candidates))
    if total_bets <= 0:
        return {
            "roi": None,
            "hit_rate": None,
            "total_bets": 0.0,
            "hit_bets": None,
            "total_return_yen": None,
            "total_bet_yen": 0.0,
            "max_losing_streak": None,
            "metric_warnings": ["roi_unavailable_no_bets"],
        }

    results_rows = db.query_df(
        "SELECT count(*) AS c FROM results res JOIN races r ON r.race_id=res.race_id WHERE r.race_date = cast(? as DATE)",
        (race_date,),
    ).to_dict("records")
    if not results_rows or int(results_rows[0].get("c", 0)) == 0:
        return {
            "roi": None,
            "hit_rate": None,
            "total_bets": float(total_bets),
            "hit_bets": None,
            "total_return_yen": None,
            "total_bet_yen": float(total_bets * 100),
            "max_losing_streak": None,
            "metric_warnings": ["results_missing_for_roi"],
        }

    payout_rows = db.query_df(
        """
        SELECT p.race_id, p.bet_key, p.payout
        FROM payouts p
        JOIN races r ON r.race_id = p.race_id
        WHERE r.race_date = cast(? as DATE)
          AND (lower(p.bet_type) = 'wide' OR p.bet_type = 'ワイド')
        """,
        (race_date,),
    ).to_dict("records")

    payout_map: dict[tuple[str, str], float] = {}
    for row in payout_rows:
        norm = _normalize_pair_key(row.get("bet_key"))
        if norm is None:
            continue
        payout = row.get("payout")
        if payout is None:
            continue
        payout_map[(str(row.get("race_id")), norm)] = float(payout)

    hit_flags: list[int] = []
    total_return = 0.0
    hit_bets = 0
    for c in candidates:
        rid = str(c.get("race_id"))
        pair = _normalize_pair_key(c.get("pair"))
        payout = payout_map.get((rid, pair)) if pair is not None else None
        if payout is not None:
            hit_flags.append(1)
            hit_bets += 1
            total_return += float(payout)
        else:
            hit_flags.append(0)

    total_bet_yen = float(total_bets * 100)
    roi = float(total_return / total_bet_yen) if total_bet_yen > 0 else None
    hit_rate = float(hit_bets / total_bets) if total_bets > 0 else None
    warnings: list[str] = []
    if len(payout_map) == 0:
        warnings.append("payouts_missing_for_roi")

    return {
        "roi": roi,
        "hit_rate": hit_rate,
        "total_bets": float(total_bets),
        "hit_bets": float(hit_bets),
        "total_return_yen": float(total_return),
        "total_bet_yen": total_bet_yen,
        "max_losing_streak": float(_max_losing_streak(hit_flags)),
        "metric_warnings": warnings,
    }


def _post_infer_probability_gate(
    db: DuckDb,
    race_date: str,
    model_version: str,
    *,
    mode: str = "strict",
    excluded_race_ids: set[str] | None = None,
) -> dict[str, Any]:
    rows = _latest_top3_predictions_for_date(db, race_date, model_version)
    stop_reasons: list[str] = []
    warn_reasons: list[str] = []

    if len(rows) == 0:
        stop_reasons.append("no_top3_predictions")
        return {"status": "stop", "stop_reasons": stop_reasons, "warn_reasons": warn_reasons, "race_sums": []}

    pvals = [r["p_top3"] for r in rows]

    def is_nullish(v: Any) -> bool:
        return v is None or (isinstance(v, float) and math.isnan(v))

    if all(is_nullish(v) for v in pvals):
        stop_reasons.append("top3_all_null")
        return {"status": "stop", "stop_reasons": stop_reasons, "warn_reasons": warn_reasons, "race_sums": []}

    sums = db.query_df(
        """
        WITH latest AS (
          SELECT race_id, horse_no, model_version, max(inference_timestamp) AS ts
          FROM horse_predictions
          WHERE model_version = ?
          GROUP BY race_id, horse_no, model_version
        )
        SELECT hp.race_id, sum(hp.p_top3) AS sum_top3, count(*) AS n
        FROM horse_predictions hp
        JOIN latest l
          ON l.race_id=hp.race_id
         AND l.horse_no=hp.horse_no
         AND l.model_version=hp.model_version
         AND l.ts=hp.inference_timestamp
        JOIN races r ON r.race_id=hp.race_id
        WHERE r.race_date = cast(? as DATE)
        GROUP BY hp.race_id
        ORDER BY hp.race_id
        """,
        (model_version, race_date),
    ).to_dict("records")
    excluded = set(str(x) for x in (excluded_race_ids or set()))
    if excluded:
        sums = [s for s in sums if str(s.get("race_id")) not in excluded]

    for s in sums:
        race_id = s["race_id"]
        sum_top3 = s["sum_top3"]
        if sum_top3 is None or (isinstance(sum_top3, float) and math.isnan(sum_top3)):
            stop_reasons.append(f"sum_top3_null:{race_id}")
            continue
        v = float(sum_top3)
        # Soft/hard bounds as sanity gates (config化は次段で可能)
        if v < 0.5 or v > 6.0:
            if mode == "warn-only":
                warn_reasons.append(f"sum_top3_extreme:{race_id}:{v:.3f}")
            else:
                stop_reasons.append(f"sum_top3_extreme:{race_id}:{v:.3f}")
        elif v < 1.2 or v > 4.8:
            warn_reasons.append(f"sum_top3_unusual:{race_id}:{v:.3f}")

    status = "stop" if len(stop_reasons) > 0 else ("warn" if len(warn_reasons) > 0 else "ok")
    return {
        "status": status,
        "mode": mode,
        "stop_reasons": stop_reasons,
        "warn_reasons": warn_reasons,
        "stop_count": int(len(stop_reasons)),
        "warn_count": int(len(warn_reasons)),
        "all_reasons": [*stop_reasons, *warn_reasons],
        "race_sums": sums,
    }


def _write_run_summary(run_summary_path: Path, payload: dict[str, Any]) -> None:
    run_summary_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = run_summary_path.with_suffix(run_summary_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    tmp.replace(run_summary_path)


def _resolve_race_day_output_dir(
    *,
    race_day_out_root: Path,
    race_date: str,
    model_version: str,
    overwrite: bool,
) -> Path:
    base = race_day_out_root / race_date / model_version
    if overwrite:
        return base
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return base / stamp


def _write_race_day_artifacts(
    *,
    db: DuckDb,
    race_date: str,
    model_version: str,
    out_dir: Path,
    decision_rows: list[dict[str, Any]],
    decision_rows_expanded: list[dict[str, Any]] | None,
    race_flags: list[dict[str, Any]],
    run_summary_path: Path,
) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    predictions_path = out_dir / "predictions.parquet"
    candidate_pairs_path = out_dir / "candidate_pairs.parquet"
    candidate_pairs_expanded_path = out_dir / "candidate_pairs_expanded.parquet"
    race_flags_path = out_dir / "race_flags.parquet"
    skip_log_path = out_dir / "skip_log.parquet"
    run_summary_out_path = out_dir / "run_summary.json"
    pair_shadow_report_path = out_dir / "pair_shadow_compare_report.md"
    pair_race_cmp_path = out_dir / "pair_shadow_race_comparison.csv"
    pair_pair_cmp_path = out_dir / "pair_shadow_pair_comparison.csv"
    pair_race_cmp_all_path = out_dir / "pair_shadow_race_comparison_all_candidates.csv"
    pair_pair_cmp_all_path = out_dir / "pair_shadow_pair_comparison_all_candidates.csv"
    pair_race_cmp_expanded_path = out_dir / "pair_shadow_race_comparison_expanded.csv"
    pair_pair_cmp_expanded_path = out_dir / "pair_shadow_pair_comparison_expanded.csv"

    pd_decision = None
    try:
        import pandas as _pd

        pd_decision = _pd.DataFrame(decision_rows)
        pd_race_flags = _pd.DataFrame(race_flags)
        pred_rows = _latest_top3_predictions_for_date(db=db, race_date=race_date, model_version=model_version)
        _pd.DataFrame(pred_rows).to_parquet(predictions_path, index=False)
        pd_decision.to_parquet(candidate_pairs_path, index=False)
        if decision_rows_expanded is not None:
            _pd.DataFrame(decision_rows_expanded).to_parquet(candidate_pairs_expanded_path, index=False)
        pd_race_flags.to_parquet(race_flags_path, index=False)
        pd_race_flags.to_parquet(skip_log_path, index=False)
        if len(pd_decision) > 0:
            tmp = pd_decision.copy()
            tmp_all = _pd.DataFrame(decision_rows_expanded).copy() if decision_rows_expanded is not None and len(decision_rows_expanded) > 0 else tmp.copy()
            if "pair_norm" not in tmp.columns and {"horse1_umaban", "horse2_umaban"}.issubset(tmp.columns):
                tmp["pair_norm"] = tmp.apply(
                    lambda r: f"{min(int(r['horse1_umaban']), int(r['horse2_umaban'])):02d}-{max(int(r['horse1_umaban']), int(r['horse2_umaban'])):02d}",
                    axis=1,
                )
            if "pair_norm" not in tmp_all.columns and {"horse1_umaban", "horse2_umaban"}.issubset(tmp_all.columns):
                tmp_all["pair_norm"] = tmp_all.apply(
                    lambda r: f"{min(int(r['horse1_umaban']), int(r['horse2_umaban'])):02d}-{max(int(r['horse1_umaban']), int(r['horse2_umaban'])):02d}",
                    axis=1,
                )
            if "model_top5_flag" not in tmp.columns:
                tmp["model_top5_flag"] = tmp.get("pair_model_rank_in_race", pd.Series([None] * len(tmp))).apply(
                    lambda x: bool(pd.notna(x) and float(x) <= 5)
                )
            if "model_top5_flag" not in tmp_all.columns:
                tmp_all["model_top5_flag"] = tmp_all.get("pair_model_rank_in_race", pd.Series([None] * len(tmp_all))).apply(
                    lambda x: bool(pd.notna(x) and float(x) <= 5)
                )
            for c in ("actual_wide_hit", "wide_payout"):
                if c not in tmp.columns:
                    tmp[c] = pd.NA
                if c not in tmp_all.columns:
                    tmp_all[c] = pd.NA
            for c in (
                "model_dynamic_rank",
                "model_dynamic_skip_reason",
                "top_dynamic_score",
                "avg_pair_edge_selected",
                "max_pair_edge_selected",
            ):
                if c not in tmp.columns:
                    tmp[c] = pd.NA
                if c not in tmp_all.columns:
                    tmp_all[c] = pd.NA
            pair_view = tmp[
                [
                    "race_id",
                    "pair_norm",
                    "pair_value_score",
                    "pair_model_score",
                    "pair_edge",
                    "model_dynamic_final_score",
                    "pair_selected_flag",
                    "model_top5_flag",
                    "model_dynamic_selected_flag",
                    "model_dynamic_rank",
                    "model_dynamic_skip_reason",
                    "actual_wide_hit",
                    "wide_payout",
                ]
            ].copy()
            pair_view.to_csv(pair_pair_cmp_path, index=False, encoding="utf-8")

            all_pair_cols = [
                "race_id",
                "pair_norm",
                "horse1_umaban",
                "horse2_umaban",
                "pair_selected_flag",
                "model_top5_flag",
                "model_dynamic_selected_flag",
                "pair_value_score",
                "pair_model_score",
                "pair_market_implied_prob",
                "pair_edge",
                "pair_edge_ratio",
                "pair_edge_log_ratio",
                "pair_edge_rank_gap",
                "pair_edge_pct_gap",
                "model_dynamic_final_score",
                "model_dynamic_rank",
                "model_dynamic_skip_reason",
                "actual_wide_hit",
                "wide_payout",
                "p_top3_fused_hmean",
                "p_top3_fused_min",
                "p_top3_fused_max",
                "p_top3_fused_abs_diff",
                "ai_market_gap_min",
                "ai_market_gap_max",
                "both_positive_gap_flag",
                "one_side_positive_gap_flag",
                "pair_model_score_rank_in_race",
                "pair_model_score_gap_to_next",
                "pair_value_score_rank_in_race",
                "pair_value_score_gap_to_next",
            ]
            for c in all_pair_cols:
                if c not in tmp_all.columns:
                    tmp_all[c] = pd.NA
            tmp_all[all_pair_cols].to_csv(pair_pair_cmp_all_path, index=False, encoding="utf-8")
            tmp_all[all_pair_cols].to_csv(pair_pair_cmp_expanded_path, index=False, encoding="utf-8")

            race_rows: list[dict[str, Any]] = []
            race_rows_all: list[dict[str, Any]] = []
            for rid, g in tmp_all.groupby("race_id", sort=False):
                rule_set = set(g.loc[g["pair_selected_flag"] == True, "pair_norm"].astype(str).tolist()) if "pair_selected_flag" in g.columns else set()
                model5_set = set(g.loc[g["model_top5_flag"] == True, "pair_norm"].astype(str).tolist()) if "model_top5_flag" in g.columns else set()
                model_dyn_set = set(g.loc[g["model_dynamic_selected_flag"] == True, "pair_norm"].astype(str).tolist()) if "model_dynamic_selected_flag" in g.columns else set()
                top_rule = pd.to_numeric(g.get("pair_value_score"), errors="coerce").max() if "pair_value_score" in g.columns else None
                top_model = pd.to_numeric(g.get("pair_model_score"), errors="coerce").max() if "pair_model_score" in g.columns else None
                top_dynamic = pd.to_numeric(g.get("model_dynamic_final_score"), errors="coerce").max() if "model_dynamic_final_score" in g.columns else None
                top_edge = pd.to_numeric(g.get("pair_edge"), errors="coerce").max() if "pair_edge" in g.columns else None
                model_dyn_selected_edges = pd.to_numeric(g.loc[g.get("model_dynamic_selected_flag", False) == True, "pair_edge"], errors="coerce")
                skip_reason = g.get("model_dynamic_skip_reason", pd.Series([None])).dropna().astype(str).head(1)
                race_rows.append(
                    {
                        "race_id": rid,
                        "rule_selected_count": int(len(rule_set)),
                        "model_top5_selected_count": int(len(model5_set)),
                        "model_dynamic_selected_count": int(len(model_dyn_set)),
                        "model_dynamic_skip_reason": (skip_reason.iloc[0] if len(skip_reason) > 0 else None),
                        "top_rule_score": (None if pd.isna(top_rule) else float(top_rule)),
                        "top_model_score": (None if pd.isna(top_model) else float(top_model)),
                        "top_dynamic_score": (None if pd.isna(top_dynamic) else float(top_dynamic)),
                        "top_edge": (None if pd.isna(top_edge) else float(top_edge)),
                        "overlap_rule_model_top5": int(len(rule_set.intersection(model5_set))),
                        "overlap_rule_model_dynamic": int(len(rule_set.intersection(model_dyn_set))),
                        "avg_pair_edge_selected": (
                            None
                            if len(model_dyn_selected_edges.dropna()) == 0
                            else float(model_dyn_selected_edges.dropna().mean())
                        ),
                        "max_pair_edge_selected": (
                            None
                            if len(model_dyn_selected_edges.dropna()) == 0
                            else float(model_dyn_selected_edges.dropna().max())
                        ),
                    }
                )

                rule_mask = g.get("pair_selected_flag", False) == True
                top5_mask = g.get("model_top5_flag", False) == True
                dyn_mask = g.get("model_dynamic_selected_flag", False) == True
                non_rule_mask = ~rule_mask
                top_non_rule = g.loc[non_rule_mask].sort_values("pair_model_score", ascending=False).head(1)
                race_rows_all.append(
                    {
                        "race_id": rid,
                        "candidate_pair_count": int(len(g)),
                        "rule_selected_count": int(rule_mask.sum()),
                        "non_rule_candidate_count": int(non_rule_mask.sum()),
                        "model_top5_selected_count": int(top5_mask.sum()),
                        "model_dynamic_selected_count": int(dyn_mask.sum()),
                        "model_top5_non_rule_count": int((top5_mask & non_rule_mask).sum()),
                        "model_dynamic_non_rule_count": int((dyn_mask & non_rule_mask).sum()),
                        "top_rule_score": (None if pd.to_numeric(g.loc[rule_mask, "pair_value_score"], errors="coerce").dropna().empty else float(pd.to_numeric(g.loc[rule_mask, "pair_value_score"], errors="coerce").max())),
                        "top_model_score": (None if pd.to_numeric(g.get("pair_model_score"), errors="coerce").dropna().empty else float(pd.to_numeric(g.get("pair_model_score"), errors="coerce").max())),
                        "top_non_rule_model_score": (None if pd.to_numeric(g.loc[non_rule_mask, "pair_model_score"], errors="coerce").dropna().empty else float(pd.to_numeric(g.loc[non_rule_mask, "pair_model_score"], errors="coerce").max())),
                        "best_non_rule_pair_norm": (top_non_rule["pair_norm"].iloc[0] if len(top_non_rule) > 0 else None),
                        "best_non_rule_pair_value_score": (top_non_rule["pair_value_score"].iloc[0] if len(top_non_rule) > 0 else None),
                        "best_non_rule_market_proxy": (top_non_rule["pair_market_implied_prob"].iloc[0] if len(top_non_rule) > 0 else None),
                        "best_non_rule_actual_wide_hit": (top_non_rule["actual_wide_hit"].iloc[0] if len(top_non_rule) > 0 else None),
                        "best_non_rule_wide_payout": (top_non_rule["wide_payout"].iloc[0] if len(top_non_rule) > 0 else None),
                        "overlap_rule_model_top5": int(len(set(g.loc[rule_mask, "pair_norm"].astype(str)).intersection(set(g.loc[top5_mask, "pair_norm"].astype(str))))),
                        "overlap_rule_model_dynamic": int(len(set(g.loc[rule_mask, "pair_norm"].astype(str)).intersection(set(g.loc[dyn_mask, "pair_norm"].astype(str))))),
                        "model_top5_non_rule_hit_count": int(pd.to_numeric(g.loc[top5_mask & non_rule_mask, "actual_wide_hit"], errors="coerce").fillna(0).sum()),
                        "model_dynamic_non_rule_hit_count": int(pd.to_numeric(g.loc[dyn_mask & non_rule_mask, "actual_wide_hit"], errors="coerce").fillna(0).sum()),
                    }
                )
            pd.DataFrame(race_rows).to_csv(pair_race_cmp_path, index=False, encoding="utf-8")
            pd.DataFrame(race_rows_all).to_csv(pair_race_cmp_all_path, index=False, encoding="utf-8")
            pd.DataFrame(race_rows_all).to_csv(pair_race_cmp_expanded_path, index=False, encoding="utf-8")
        if len(pd_decision) > 0 and "pair_model_rank_in_race" in pd_decision.columns and "pair_rank_in_race" in pd_decision.columns:
            rep_lines = [
                "# pair_shadow_compare_report",
                "",
                f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
                f"- rows: {len(pd_decision)}",
            ]
            avail = pd_decision["pair_model_available"].fillna(False).astype(bool) if "pair_model_available" in pd_decision.columns else pd.Series([False] * len(pd_decision))
            rep_lines.append(f"- pair_model_available_rows: {int(avail.sum())}")
            if avail.any():
                dd = pd_decision.loc[avail].copy()
                dd["rank_diff_model_minus_rule"] = dd["pair_model_rank_in_race"] - dd["pair_rank_in_race"]
                rep_lines.append("")
                rep_lines.append("## Rank Diff Summary")
                rep_lines.append(f"- mean_diff: {float(dd['rank_diff_model_minus_rule'].mean())}")
                rep_lines.append(f"- median_diff: {float(dd['rank_diff_model_minus_rule'].median())}")
                rep_lines.append(f"- abs_mean_diff: {float(dd['rank_diff_model_minus_rule'].abs().mean())}")
                rep_lines.append("")
                rep_lines.append("## Top Rank Gap By Race (head)")
                rep_lines.append("| race_id | rule_top_pair | model_top_pair | same_top |")
                rep_lines.append("|---|---|---|---|")
                for rid, g in dd.groupby("race_id"):
                    rg = g.sort_values("pair_rank_in_race").head(1)
                    mg = g.sort_values("pair_model_rank_in_race").head(1)
                    rp = rg["pair"].iloc[0] if len(rg) else None
                    mp = mg["pair"].iloc[0] if len(mg) else None
                    rep_lines.append(f"| {rid} | {rp} | {mp} | {str(rp == mp)} |")
            else:
                rep_lines.extend(["", "## Rank Diff Summary", "- pair model unavailable"])
            pair_shadow_report_path.write_text("\n".join(rep_lines), encoding="utf-8")
    except Exception:
        # keep run robust even if optional parquet writing fails
        pass

    # always write a colocated run_summary copy
    if run_summary_path.exists():
        run_summary_out_path.write_text(run_summary_path.read_text(encoding="utf-8"), encoding="utf-8")

    return {
        "predictions_path": str(predictions_path),
        "candidate_pairs_path": str(candidate_pairs_path),
        "candidate_pairs_expanded_path": str(candidate_pairs_expanded_path),
        "race_flags_path": str(race_flags_path),
        "skip_log_path": str(skip_log_path),
        "run_summary_path": str(run_summary_out_path),
        "pair_shadow_report_path": str(pair_shadow_report_path),
        "pair_shadow_race_comparison_path": str(pair_race_cmp_path),
        "pair_shadow_pair_comparison_path": str(pair_pair_cmp_path),
        "pair_shadow_race_comparison_all_candidates_path": str(pair_race_cmp_all_path),
        "pair_shadow_pair_comparison_all_candidates_path": str(pair_pair_cmp_all_path),
        "pair_shadow_race_comparison_expanded_path": str(pair_race_cmp_expanded_path),
        "pair_shadow_pair_comparison_expanded_path": str(pair_pair_cmp_expanded_path),
    }


def _artifact_status(path_str: str | None, *, generated_this_run: bool, skipped_reason: str | None = None) -> dict[str, Any]:
    p = Path(path_str) if path_str else None
    exists = bool(p and p.exists())
    row_count = None
    if exists and p is not None:
        try:
            if p.suffix.lower() == ".parquet":
                row_count = int(len(pd.read_parquet(p)))
            elif p.suffix.lower() == ".csv":
                row_count = int(len(pd.read_csv(p)))
        except Exception:
            row_count = None
    out = {
        "path": path_str,
        "exists": bool(exists),
        "generated_this_run": bool(generated_this_run),
        "row_count": row_count,
    }
    if skipped_reason:
        out["skipped_reason"] = str(skipped_reason)
    return out


def run_race_day_pipeline(
    *,
    db: DuckDb,
    raw_dir: Path,
    normalized_root: Path,
    race_date: str,
    snapshot_version: str,
    feature_snapshot_version: str,
    model_version: str,
    odds_snapshot_version: str,
    models_root: Path,
    export_out_dir: Path,
    run_summary_path: Path,
    auto_run_summary_dir: Path | None,
    allow_no_wide_odds: bool,
    experiment_name: str | None = None,
    calibration_summary_path: str | None = None,
    feature_importance_summary_path: str | None = None,
    decision_density_top3_max: float = 1.35,
    decision_gap12_min: float = 0.003,
    decision_ai_weight: float = 0.65,
    skip_reason_config: SkipReasonConfig | None = None,
    race_day_out_root: Path | None = None,
    overwrite_race_day_outputs: bool = False,
    force_null_top3_for_test: bool = False,
    force_overlap_guard_fail_for_test: bool = False,
    skip_post_infer_gate: bool = False,
    skip_doctor_structured_stop: bool = False,
    pair_model_root: Path = Path("racing_ai/data/models_compare/pair_reranker"),
    pair_model_version: str | None = "pair_reranker_ts_v4",
    model_dynamic_min_score: float = 0.08,
    model_dynamic_min_edge: float = 0.0,
    model_dynamic_min_gap: float = 0.01,
    model_dynamic_default_k: int = 5,
    model_dynamic_min_k: int = 1,
    model_dynamic_max_k: int = 5,
    emit_expanded_candidates: bool = False,
    expanded_top_horse_n: int = 10,
    expanded_ai_gap_horse_n: int = 10,
    expanded_max_pairs_per_race: int = 45,
    probability_gate_mode: str = "strict",
    race_meta_policy: str = "skip",
) -> dict[str, Any]:
    run_id = str(uuid.uuid4())
    warnings: list[str] = []
    gate_mode = str(probability_gate_mode or "strict").strip().lower()
    if gate_mode not in {"strict", "warn-only"}:
        warnings.append(f"invalid_probability_gate_mode_fallback_to_strict:{probability_gate_mode}")
        gate_mode = "strict"
    meta_policy = str(race_meta_policy or "skip").strip().lower()
    if meta_policy not in {"skip", "warn-only"}:
        warnings.append(f"invalid_race_meta_policy_fallback_to_skip:{race_meta_policy}")
        meta_policy = "skip"
    step_results: dict[str, Any] = {}
    invalid_race_rows: list[dict] = []
    invalid_race_ids: set[str] = set()
    race_meta_report_path = Path("racing_ai/reports/race_metadata_validation_report.md")
    status = "ok"
    stop_reason: str | None = None
    doctor_overall_status: str | None = None
    stop_check_codes: list[str] = []
    try:
        # 1) jv-file-pipeline (normalize + ingest)
        normalize = normalize_raw_jv_to_normalized(
            raw_dir=raw_dir,
            normalized_root=normalized_root,
            target_race_date=race_date,
            snapshot_version=snapshot_version,
            db=db,
        )
        step_results["normalize"] = normalize
        if normalize["status"] == "stop":
            status = "stop"
            stop_reason = ";".join(normalize.get("stop_reasons", [])) or "normalize_stop"
        else:
            warnings.extend(normalize.get("warn_reasons", []))
            normalized_dir = Path(normalize["normalized_dir"])
            ingest = ingest_from_csv_dir(db=db, in_dir=normalized_dir)
            step_results["ingest"] = ingest

        # 2) build-features
        if status != "stop":
            step_results["build_features"] = build_feature_store_snapshot(
                db=db,
                race_date=race_date,
                feature_snapshot_version=feature_snapshot_version,
            )
            # 2.5) race metadata validation before inference
            invalid_race_rows = validate_race_metadata(db=db, race_date=race_date)
            invalid_race_ids = set(str(r.get("race_id")) for r in invalid_race_rows)
            step_results["race_metadata_validation"] = {
                "policy": meta_policy,
                "invalid_count": int(len(invalid_race_rows)),
                "invalid_race_ids": sorted(list(invalid_race_ids)),
            }
            step_results["race_metadata_validation"]["report_path"] = write_race_metadata_validation_report(
                race_date=race_date,
                invalid_rows=invalid_race_rows,
                policy=meta_policy,
                out_path=race_meta_report_path,
            )
            if len(invalid_race_rows) > 0:
                warnings.append(f"race_meta_invalid_count:{len(invalid_race_rows)}")

        # 3) doctor
        if status != "stop":
            doctor = run_doctor(db, race_date=race_date)
            step_results["doctor"] = doctor
            if doctor["should_stop"]:
                status = "stop"
                stop_reason = ";".join(doctor.get("stop_reasons", [])) or "doctor_stop"
            warnings.extend(doctor.get("warn_reasons", []))

        # 4) infer-top3
        if status != "stop":
            dataset_fingerprint = stable_fingerprint(
                {
                    "race_date": race_date,
                    "snapshot_version": snapshot_version,
                    "feature_snapshot_version": feature_snapshot_version,
                    "odds_snapshot_version": odds_snapshot_version,
                }
            )
            infer = infer_top3_for_date(
                db=db,
                models_root=models_root,
                race_date=race_date,
                feature_snapshot_version=feature_snapshot_version,
                model_version=model_version,
                odds_snapshot_version=odds_snapshot_version,
                dataset_fingerprint=dataset_fingerprint,
                excluded_race_ids=(invalid_race_ids if meta_policy == "skip" else None),
            )
            step_results["infer_top3"] = infer
            if force_null_top3_for_test:
                db.execute(
                    """
                    UPDATE horse_predictions
                    SET p_top3 = NULL
                    WHERE model_version = ?
                      AND race_id IN (SELECT race_id FROM races WHERE race_date = cast(? as DATE))
                    """,
                    (model_version, race_date),
                )
                step_results["infer_top3"]["forced_null_top3"] = True

        # post-infer gates
        if status != "stop":
            prob_gate = _post_infer_probability_gate(
                db=db,
                race_date=race_date,
                model_version=model_version,
                mode=gate_mode,
                excluded_race_ids=(invalid_race_ids if meta_policy == "skip" else None),
            )
            step_results["post_infer_gate"] = prob_gate
            if prob_gate["status"] == "stop":
                if skip_post_infer_gate:
                    warnings.append("post_infer_gate_skipped")
                    warnings.extend([f"post_infer_gate_stop_reason:{r}" for r in prob_gate.get("stop_reasons", [])])
                else:
                    status = "stop"
                    stop_reason = ";".join(prob_gate.get("stop_reasons", [])) or "post_infer_stop"
            warnings.extend(prob_gate.get("warn_reasons", []))

        # 5) decision
        if status != "stop":
            decision = _decision_preview(
                db=db,
                race_date=race_date,
                model_version=model_version,
                density_top3_max=decision_density_top3_max,
                gap12_min=decision_gap12_min,
                ai_weight=decision_ai_weight,
                skip_reason_config=skip_reason_config,
                model_dynamic_min_score=model_dynamic_min_score,
                model_dynamic_min_edge=model_dynamic_min_edge,
                model_dynamic_min_gap=model_dynamic_min_gap,
                model_dynamic_default_k=model_dynamic_default_k,
                model_dynamic_min_k=model_dynamic_min_k,
                model_dynamic_max_k=model_dynamic_max_k,
                pair_model_root=pair_model_root,
                pair_model_version=pair_model_version,
                excluded_race_ids=(invalid_race_ids if meta_policy == "skip" else None),
                emit_expanded_candidates=emit_expanded_candidates,
                expanded_top_horse_n=expanded_top_horse_n,
                expanded_ai_gap_horse_n=expanded_ai_gap_horse_n,
                expanded_max_pairs_per_race=expanded_max_pairs_per_race,
            )
            step_results["decision"] = decision
            warnings.extend(list(decision.get("shadow_warnings", [])))
            if int(decision.get("buy_races", 0)) == 0:
                warnings.append("buy_races_zero")
            bet_metrics = _compute_betting_metrics(db=db, race_date=race_date, decision=decision)
            step_results["betting_metrics"] = {k: v for k, v in bet_metrics.items() if k != "metric_warnings"}
            warnings.extend(list(bet_metrics.get("metric_warnings", [])))

        # 6) export-static
        if status != "stop":
            export = export_for_dashboard(
                db=db,
                race_date=race_date,
                out_dir=export_out_dir,
                feature_snapshot_version=feature_snapshot_version,
                model_version=model_version,
                odds_snapshot_version=odds_snapshot_version,
                allow_no_wide_odds=allow_no_wide_odds,
                decision_density_top3_max=decision_density_top3_max,
                decision_gap12_min=decision_gap12_min,
            )
            step_results["export_static"] = export
    except Exception as exc:
        status = "stop"
        stop_reason = f"exception:{exc.__class__.__name__}:{exc}"
        step_results["exception"] = {"type": exc.__class__.__name__, "message": str(exc)}

    # status to warn if non-stop and warnings exist
    if status != "stop" and len(warnings) > 0:
        status = "warn"

    warning_count = len(warnings)
    dataset_fingerprint = stable_fingerprint(
        {
            "race_date": race_date,
            "snapshot_version": snapshot_version,
            "feature_snapshot_version": feature_snapshot_version,
            "odds_snapshot_version": odds_snapshot_version,
            # Keep dataset_fingerprint stable for the same inputs/configs.
            # Do not include runtime status/warnings here; those can vary by run.
        }
    )

    doctor_json_path = export_out_dir / "doctor_result.json"
    doctor_csv_path = export_out_dir / "doctor_result.csv"
    doctor_result = build_doctor_structured_result(
        db=db,
        run_id=run_id,
        race_date=race_date,
        snapshot_version=snapshot_version,
        feature_snapshot_version=feature_snapshot_version,
        model_version=model_version,
        step_results=step_results,
        warnings=warnings,
        json_path=doctor_json_path,
        csv_path=doctor_csv_path,
        force_overlap_guard_fail_for_test=force_overlap_guard_fail_for_test,
    )
    doctor_overall_status = doctor_result.get("overall_status")
    stop_check_codes = doctor_result.get("stop_check_codes", [])
    warn_check_codes = doctor_result.get("warn_check_codes", [])
    warning_count = max(warning_count, len(warn_check_codes))
    if status != "stop" and doctor_overall_status == "stop" and not skip_doctor_structured_stop:
        status = "stop"
        stop_reason = stop_reason or "doctor_structured_stop"
    elif status == "ok" and doctor_overall_status == "warn":
        status = "warn"

    decision_summary = step_results.get("decision", {}) if isinstance(step_results.get("decision", {}), dict) else {}
    betting_metrics = step_results.get("betting_metrics", {}) if isinstance(step_results.get("betting_metrics", {}), dict) else {}
    summary = normalize_run_summary(
        {
            "run_id": run_id,
            "race_date": race_date,
            "snapshot_version": snapshot_version,
            "feature_snapshot_version": feature_snapshot_version,
            "model_version": model_version,
            "odds_snapshot_version": odds_snapshot_version,
            "status": status,
            "stop_reason": stop_reason,
            "doctor_overall_status": doctor_overall_status,
            "stop_check_codes": stop_check_codes,
            "warning_count": warning_count,
            "warnings": warnings,
            "dataset_fingerprint": dataset_fingerprint,
            "experiment_name": experiment_name,
            "roi": betting_metrics.get("roi"),
            "hit_rate": betting_metrics.get("hit_rate"),
            "buy_races": decision_summary.get("buy_races"),
            "model_dynamic_buy_race_count": decision_summary.get("model_dynamic_buy_race_count"),
            "model_dynamic_skip_race_count": decision_summary.get("model_dynamic_skip_race_count"),
            "model_dynamic_selected_pair_count": decision_summary.get("model_dynamic_selected_pair_count"),
            "avg_pair_edge_selected": decision_summary.get("avg_pair_edge_selected"),
            "rule_model_overlap_avg": decision_summary.get("rule_model_overlap_avg"),
            "model_dynamic_skip_reason_counts": decision_summary.get("model_dynamic_skip_reason_counts"),
            "total_bets": betting_metrics.get("total_bets", decision_summary.get("total_candidates")),
            "hit_bets": betting_metrics.get("hit_bets"),
            "total_return_yen": betting_metrics.get("total_return_yen"),
            "total_bet_yen": betting_metrics.get("total_bet_yen"),
            "max_losing_streak": betting_metrics.get("max_losing_streak"),
            "calibration_summary_path": calibration_summary_path,
            "feature_importance_summary_path": feature_importance_summary_path,
            "created_at": dt.datetime.now().isoformat(timespec="seconds"),
            "doctor_result_paths": {"json": str(doctor_json_path), "csv": str(doctor_csv_path)},
            "decision_settings": {
                "enable_value_skip": bool(skip_reason_config.enforce_no_value_horse) if skip_reason_config else False,
                "min_ai_market_gap": (skip_reason_config.min_ai_market_gap if skip_reason_config else None),
                "enable_market_overrated_skip": bool(skip_reason_config.enforce_market_overrated_top_count) if skip_reason_config else False,
                "max_market_overrated_top_count": (skip_reason_config.max_market_overrated_top_count if skip_reason_config else None),
                "enable_pair_ev_skip": bool(skip_reason_config.enforce_pair_value) if skip_reason_config else False,
                "min_pair_value_score": (skip_reason_config.min_pair_value_score if skip_reason_config else None),
                "decision_ai_weight": decision_ai_weight,
                "density_top3_threshold": decision_density_top3_max,
                "gap12_threshold": decision_gap12_min,
                "pair_score_version": "v1_simple_gap_bonus",
                "pair_model_version": pair_model_version,
                "shadow_mode": True,
                "model_dynamic_min_score": model_dynamic_min_score,
                "model_dynamic_min_edge": model_dynamic_min_edge,
                "model_dynamic_min_gap": model_dynamic_min_gap,
                "model_dynamic_default_k": model_dynamic_default_k,
                "model_dynamic_min_k": model_dynamic_min_k,
                "model_dynamic_max_k": model_dynamic_max_k,
                "emit_expanded_candidates": bool(emit_expanded_candidates),
                "expanded_top_horse_n": int(expanded_top_horse_n),
                "expanded_ai_gap_horse_n": int(expanded_ai_gap_horse_n),
                "expanded_max_pairs_per_race": int(expanded_max_pairs_per_race),
                "probability_gate_mode": gate_mode,
                "race_meta_policy": meta_policy,
                "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
            },
            "probability_gate_mode": gate_mode,
            "probability_gate_stop_count": int(
                (step_results.get("post_infer_gate", {}).get("stop_count", 0))
                if isinstance(step_results.get("post_infer_gate", {}), dict)
                else 0
            ),
            "probability_gate_warn_count": int(
                (step_results.get("post_infer_gate", {}).get("warn_count", 0))
                if isinstance(step_results.get("post_infer_gate", {}), dict)
                else 0
            ),
            "probability_gate_reasons": (
                step_results.get("post_infer_gate", {}).get("all_reasons", [])
                if isinstance(step_results.get("post_infer_gate", {}), dict)
                else []
            ),
            "race_meta_invalid_count": int(len(invalid_race_rows)),
            "race_meta_invalid_race_ids": sorted(list(invalid_race_ids)),
            "race_meta_invalid_reasons": [
                {"race_id": str(r.get("race_id")), "reasons": list(r.get("reasons", []))}
                for r in invalid_race_rows
            ],
            "race_metadata_validation_report_path": str(race_meta_report_path),
            "raw_required_files_check": {
                "required_files": ["races.csv", "entries.csv"],
                "status": "fail" if (status == "stop" and isinstance(step_results.get("normalize"), dict) and any("missing_raw_file:" in str(x) for x in step_results["normalize"].get("stop_reasons", []))) else "pass",
                "missing_files": [
                    str(x).split("missing_raw_file:")[-1]
                    for x in (step_results.get("normalize", {}).get("stop_reasons", []) if isinstance(step_results.get("normalize"), dict) else [])
                    if "missing_raw_file:" in str(x)
                ],
            },
            "steps": step_results,
            "expanded_generation_attempted": bool(emit_expanded_candidates),
            "expanded_generation_status": ("skipped" if status == "stop" else ("pending" if emit_expanded_candidates else "disabled")),
            "expanded_generation_skipped_reason": ("pipeline_stopped_before_decision" if (emit_expanded_candidates and status == "stop") else None),
            "expanded_candidate_pairs_exists": False,
            "expanded_pair_comparison_exists": False,
            "expanded_race_comparison_exists": False,
            "expanded_candidate_pair_count": None,
            "expanded_non_rule_candidate_count": None,
        },
        default_race_date=race_date,
        default_run_id=run_id,
        default_experiment_name=experiment_name or model_version,
        default_model_version=model_version,
        default_feature_snapshot_version=feature_snapshot_version,
        default_dataset_fingerprint=dataset_fingerprint,
        default_status=status,
        default_calibration_summary_path=calibration_summary_path,
        default_feature_importance_summary_path=feature_importance_summary_path,
    )
    validation = validate_run_summary(summary, strict=True)
    if validation["warnings"]:
        summary["warnings"] = sorted(set([*summary.get("warnings", []), *validation["warnings"]]))
    if validation["errors"]:
        status = "stop"
        stop_reason = stop_reason or "run_summary_schema_error"
        summary["status"] = "stop"
        summary["stop_reason"] = stop_reason
        summary["warnings"] = sorted(set([*summary.get("warnings", []), *validation["errors"]]))
    warnings = list(summary.get("warnings", []))
    warning_count = len(warnings)
    status = str(summary.get("status", status))
    stop_reason = summary.get("stop_reason")
    _write_run_summary(run_summary_path, summary)
    resolved_auto_dir = auto_run_summary_dir or (run_summary_path.parent / "run_summary_v1")
    auto_run_summary_path = resolved_auto_dir / race_date / f"run_summary_{run_id}.json"
    _write_run_summary(auto_run_summary_path, summary)
    summary["auto_run_summary_path"] = str(auto_run_summary_path)
    _write_run_summary(run_summary_path, summary)

    if race_day_out_root is not None:
        out_dir = _resolve_race_day_output_dir(
            race_day_out_root=race_day_out_root,
            race_date=race_date,
            model_version=model_version,
            overwrite=overwrite_race_day_outputs,
        )
        decision_rows = decision_summary.get("candidate_pairs", []) if isinstance(decision_summary, dict) else []
        decision_rows_expanded = decision_summary.get("candidate_pairs_expanded", []) if isinstance(decision_summary, dict) else []
        race_flags = decision_summary.get("race_flags", []) if isinstance(decision_summary, dict) else []
        paths = _write_race_day_artifacts(
            db=db,
            race_date=race_date,
            model_version=model_version,
            out_dir=out_dir,
            decision_rows=decision_rows,
            decision_rows_expanded=decision_rows_expanded,
            race_flags=race_flags,
            run_summary_path=run_summary_path,
        )
        summary.update(paths)
        decision_present = isinstance(decision_summary, dict) and len(decision_summary.get("candidate_pairs", [])) > 0
        expanded_present = isinstance(decision_summary, dict) and len(decision_summary.get("candidate_pairs_expanded", [])) > 0
        skipped_reason = None
        if not decision_present:
            skipped_reason = "pipeline_stopped_before_decision"
        elif emit_expanded_candidates and not expanded_present:
            skipped_reason = "expanded_not_built_from_decision"
        summary["artifacts"] = {
            "candidate_pairs": _artifact_status(paths.get("candidate_pairs_path"), generated_this_run=decision_present, skipped_reason=(None if decision_present else skipped_reason)),
            "candidate_pairs_expanded": _artifact_status(paths.get("candidate_pairs_expanded_path"), generated_this_run=expanded_present, skipped_reason=(None if expanded_present else skipped_reason)),
            "pair_shadow_pair_comparison_expanded": _artifact_status(paths.get("pair_shadow_pair_comparison_expanded_path"), generated_this_run=expanded_present, skipped_reason=(None if expanded_present else skipped_reason)),
            "pair_shadow_race_comparison_expanded": _artifact_status(paths.get("pair_shadow_race_comparison_expanded_path"), generated_this_run=expanded_present, skipped_reason=(None if expanded_present else skipped_reason)),
        }
        summary["expanded_generation_attempted"] = bool(emit_expanded_candidates)
        if not emit_expanded_candidates:
            summary["expanded_generation_status"] = "disabled"
            summary["expanded_generation_skipped_reason"] = "emit_expanded_candidates_false"
        elif expanded_present:
            summary["expanded_generation_status"] = "generated"
            summary["expanded_generation_skipped_reason"] = None
        else:
            summary["expanded_generation_status"] = "skipped"
            summary["expanded_generation_skipped_reason"] = skipped_reason or "unknown"
        summary["expanded_candidate_pairs_exists"] = bool(Path(paths.get("candidate_pairs_expanded_path", "")).exists())
        summary["expanded_pair_comparison_exists"] = bool(Path(paths.get("pair_shadow_pair_comparison_expanded_path", "")).exists())
        summary["expanded_race_comparison_exists"] = bool(Path(paths.get("pair_shadow_race_comparison_expanded_path", "")).exists())
        try:
            import pandas as _pd
            expanded_path = Path(paths.get("candidate_pairs_expanded_path", ""))
            if expanded_path.exists():
                _exp = _pd.read_parquet(expanded_path)
                _exp_rule = _pd.to_numeric(_exp.get("pair_selected_flag"), errors="coerce").fillna(0).astype(int)
                _exp_top5 = _pd.to_numeric(_exp.get("model_top5_flag"), errors="coerce").fillna(0).astype(int)
                summary["expanded_candidate_pair_count"] = int(len(_exp))
                summary["expanded_rule_selected_pair_count"] = int(_exp_rule.sum())
                summary["expanded_non_rule_candidate_count"] = int((1 - _exp_rule).sum())
                summary["expanded_non_rule_candidate_rate"] = float(((1 - _exp_rule).sum() / len(_exp))) if len(_exp) > 0 else 0.0
                summary["expanded_model_top5_non_rule_count"] = int(((_exp_top5 == 1) & (_exp_rule == 0)).sum())
                summary["expanded_pair_model_score_non_null_count"] = int(_exp.get("pair_model_score", _pd.Series(dtype=float)).notna().sum())
            all_pair_path = Path(paths["pair_shadow_pair_comparison_all_candidates_path"])
            if all_pair_path.exists():
                _all = _pd.read_csv(all_pair_path)
                _rule = _pd.to_numeric(_all.get("pair_selected_flag"), errors="coerce").fillna(0).astype(int)
                _top5 = _pd.to_numeric(_all.get("model_top5_flag"), errors="coerce").fillna(0).astype(int)
                _dyn = _pd.to_numeric(_all.get("model_dynamic_selected_flag"), errors="coerce").fillna(0).astype(int)
                summary["all_candidate_pair_count"] = int(len(_all))
                summary["all_candidate_rule_selected_pair_count"] = int(_rule.sum())
                summary["all_candidate_non_rule_pair_count"] = int((1 - _rule).sum())
                summary["all_candidate_model_top5_non_rule_count"] = int(((_top5 == 1) & (_rule == 0)).sum())
                summary["all_candidate_model_dynamic_non_rule_count"] = int(((_dyn == 1) & (_rule == 0)).sum())
        except Exception:
            pass
        summary["feature_set_version"] = feature_snapshot_version
        summary["generated_at"] = dt.datetime.now().isoformat(timespec="seconds")
        _write_run_summary(run_summary_path, summary)
        out_summary_path = Path(paths["run_summary_path"])
        _write_run_summary(out_summary_path, summary)

    # mandatory logging:
    # 1) dedicated race-day log
    write_race_day_run_log(
        db=db,
        run_id=run_id,
        race_date=race_date,
        snapshot_version=snapshot_version,
        feature_snapshot_version=feature_snapshot_version,
        model_version=model_version,
        odds_snapshot_version=odds_snapshot_version,
        status=status,
        doctor_overall_status=doctor_overall_status,
        stop_reason=stop_reason,
        warning_count=warning_count,
        warnings=warnings,
        run_summary_path=str(run_summary_path),
    )

    # 2) inference_log compatibility
    decision = step_results.get("decision", {})
    db.execute(
        """
        INSERT INTO inference_log(
          inference_id, race_date, inference_timestamp, feature_snapshot_version, model_version,
          odds_snapshot_version, dataset_fingerprint, stop_reason, buy_races, total_candidates, warnings_json
        ) VALUES (?, cast(? as DATE), cast(? as TIMESTAMP), ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            race_date,
            dt.datetime.now().isoformat(timespec="seconds"),
            feature_snapshot_version,
            model_version,
            odds_snapshot_version,
            dataset_fingerprint,
            stop_reason,
            int(decision.get("buy_races", 0)) if isinstance(decision, dict) else 0,
            int(decision.get("total_candidates", 0)) if isinstance(decision, dict) else 0,
            json.dumps({"warnings": warnings, "warning_count": warning_count}, ensure_ascii=False),
        ),
    )

    return summary
