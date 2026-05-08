from __future__ import annotations

import argparse
import datetime as dt
import json
from itertools import combinations
from pathlib import Path
from typing import Any
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from aikeiba.db.duckdb import DuckDb
from aikeiba.decision.pair_scoring import simple_pair_value_score
from aikeiba.decision.value_model import blend_ai_market_prob, compute_market_top3_prob_from_place_odds
from aikeiba.orchestration.race_day import _enrich_pair_rows_for_shadow_selection


def _safe_apply_pair_model_score(
    df: pd.DataFrame,
    *,
    pair_model_root: Path,
    pair_model_version: str,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Evaluation-only scorer for pair_reranker that avoids LightGBM Dataset categorical_feature mismatch
    by using Booster.predict directly with a feature list from meta.json.

    Returns (df_with_pair_model_score, warnings).
    """
    warnings: list[str] = []
    try:
        import lightgbm as lgb
    except Exception as e:
        warnings.append(f"pair_model_score_unavailable:missing_lightgbm:{type(e).__name__}:{e}")
        if "pair_model_score" not in df.columns:
            df["pair_model_score"] = pd.NA
        return df, warnings

    meta_path = pair_model_root / pair_model_version / "meta.json"
    model_path = pair_model_root / pair_model_version / "model.txt"
    if not meta_path.exists() or not model_path.exists():
        warnings.append("pair_model_score_unavailable:missing_pair_model_files")
        if "pair_model_score" not in df.columns:
            df["pair_model_score"] = pd.NA
        return df, warnings

    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        features = list(meta.get("features") or [])
        categorical = set(meta.get("categorical_features") or [])
        if not features:
            warnings.append("pair_model_score_unavailable:meta_missing_features")
            if "pair_model_score" not in df.columns:
                df["pair_model_score"] = pd.NA
            return df, warnings

        X = df.copy()
        for c in features:
            if c not in X.columns:
                X[c] = 0.0
        X = X[features]
        # Force numeric matrix to avoid LightGBM categorical_feature mismatch on some historical inputs.
        for c in features:
            X[c] = pd.to_numeric(X[c], errors="coerce")
        X = X.replace([float("inf"), float("-inf")], pd.NA).fillna(0.0)

        booster = lgb.Booster(model_file=str(model_path))
        # IMPORTANT: pass numpy array, not DataFrame. DataFrame can trigger internal Dataset creation
        # with categorical_feature checks, which fail when train/valid schema differs.
        pred = booster.predict(
            X.to_numpy(dtype="float32", copy=False),
            num_iteration=getattr(booster, "best_iteration", None) or booster.current_iteration(),
        )
        df = df.copy()
        df["pair_model_score"] = pd.to_numeric(pd.Series(pred), errors="coerce")
        return df, warnings
    except Exception as e:
        warnings.append(f"pair_shadow_model_load_failed:{type(e).__name__}:{e}")
        if "pair_model_score" not in df.columns:
            df["pair_model_score"] = pd.NA
        return df, warnings


def _latest_market_top3_probs_for_date(db: DuckDb, race_date: str) -> dict[tuple[str, int], float]:
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
        p = compute_market_top3_prob_from_place_odds(r.get("place_min"), r.get("place_max"))
        if p is None:
            continue
        by_race.setdefault(str(r["race_id"]), []).append((int(r["horse_no"]), float(p)))
    out: dict[tuple[str, int], float] = {}
    for rid, vals in by_race.items():
        s = sum(v for _, v in vals)
        if s <= 0:
            continue
        scale = 3.0 / s
        for hn, v in vals:
            out[(rid, hn)] = max(0.0, min(1.0, v * scale))
    return out


def _fallback_market_top3_probs_from_predictions(
    db: DuckDb, race_date: str, model_version: str
) -> dict[tuple[str, int], float]:
    """
    When odds(place/place_max) are missing historically, create a low-confidence market proxy from predictions.
    This is evaluation-only: we just need a non-null proxy to form ai_market_gap features.
    """
    rows = db.query_df(
        """
        select hp.race_id, hp.horse_no, hp.p_top3
        from horse_predictions hp
        join races r on r.race_id=hp.race_id
        where r.race_date = cast(? as date)
          and hp.model_version = ?
          and hp.horse_no > 0
        """,
        (race_date, model_version),
    ).to_dict("records")
    by_race: dict[str, list[tuple[int, float]]] = {}
    for r in rows:
        p = r.get("p_top3")
        if p is None:
            continue
        try:
            pv = float(p)
        except Exception:
            continue
        by_race.setdefault(str(r["race_id"]), []).append((int(r["horse_no"]), pv))
    out: dict[tuple[str, int], float] = {}
    for rid, vals in by_race.items():
        s = sum(v for _, v in vals)
        if s <= 0:
            continue
        scale = 3.0 / s
        for hn, v in vals:
            out[(rid, hn)] = max(0.0, min(1.0, v * scale))
    return out


def _build_for_race(
    *,
    rid: str,
    horse_nos: list[int],
    horse_id_map: dict[int, str | None],
    p_top3: dict[int, float],
    market: dict[tuple[str, int], float],
    market_proxy_source: str,
    rule_pair_source: dict[str, str],
    top_horse_n: int,
    ai_gap_horse_n: int,
    max_pairs: int,
) -> list[dict[str, Any]]:
    pair_map: dict[str, tuple[int, int, str]] = {}

    def add_pair(a: int, b: int, src: str) -> None:
        x, y = sorted((int(a), int(b)))
        if x == y:
            return
        key = f"{x:02d}-{y:02d}"
        if key not in pair_map:
            pair_map[key] = (x, y, src)

    for key in rule_pair_source.keys():
        try:
            a, b = [int(x) for x in str(key).split("-")]
            add_pair(a, b, "rule_selected")
        except Exception:
            pass

    ranked = sorted([(hn, float(p_top3.get(hn, 0.0))) for hn in horse_nos], key=lambda x: x[1], reverse=True)
    top_horses = [hn for hn, _ in ranked[: max(2, int(top_horse_n))]]
    for a, b in combinations(top_horses, 2):
        add_pair(a, b, "top_horse_pairs")

    gaps = []
    for hn in horse_nos:
        pa = p_top3.get(hn)
        pm = market.get((rid, hn))
        if pa is None or pm is None:
            continue
        gaps.append((hn, float(pa - pm)))
    gap_horses = [hn for hn, _ in sorted(gaps, key=lambda x: x[1], reverse=True)[: max(2, int(ai_gap_horse_n))]]
    mixed = sorted(set(top_horses).union(gap_horses))
    for a, b in combinations(mixed, 2):
        add_pair(a, b, "ai_gap_pairs")

    rows: list[dict[str, Any]] = []
    for key, (h1, h2, src) in pair_map.items():
        p1 = p_top3.get(h1)
        p2 = p_top3.get(h2)
        m1 = market.get((rid, h1))
        m2 = market.get((rid, h2))
        g1 = float(p1 - m1) if (p1 is not None and m1 is not None) else None
        g2 = float(p2 - m2) if (p2 is not None and m2 is not None) else None
        pair_prob_naive, pair_value_score, pair_missing_flag = simple_pair_value_score(p1=p1, p2=p2, gap1=g1, gap2=g2)
        src_for_rule = rule_pair_source.get(key)
        rule_selected = src_for_rule is not None
        rows.append(
            {
                "race_id": rid,
                "pair": key,
                "pair_norm": key,
                "horse1_umaban": h1,
                "horse2_umaban": h2,
                "horse1_horse_id": horse_id_map.get(h1),
                "horse2_horse_id": horse_id_map.get(h2),
                "expanded_source": src,
                "expanded_candidate_flag": bool(src != "rule_selected"),
                "pair_selected_flag": bool(rule_selected),
                "rule_selected_source": (src_for_rule if src_for_rule is not None else "unavailable"),
                "rule_flag_recovered": bool(rule_selected),
                "pair_prob_naive": pair_prob_naive,
                "pair_value_score": pair_value_score,
                "pair_missing_flag": pair_missing_flag,
                "pair_ai_market_gap_sum": ((g1 or 0.0) + (g2 or 0.0)),
                "pair_ai_market_gap_max": max([x for x in [g1, g2] if x is not None], default=None),
                "pair_ai_market_gap_min": min([x for x in [g1, g2] if x is not None], default=None),
                "pair_fused_prob_sum": ((p1 or 0.0) + (p2 or 0.0)),
                "pair_fused_prob_min": min([x for x in [p1, p2] if x is not None], default=None),
                "horse1_p_top3_fused": p1,
                "horse2_p_top3_fused": p2,
                "horse1_market_top3_proxy": m1,
                "horse2_market_top3_proxy": m2,
                "horse1_ai_market_gap": g1,
                "horse2_ai_market_gap": g2,
                "market_proxy_source": market_proxy_source,
            }
        )
    rows = sorted(rows, key=lambda r: float("-inf") if r.get("pair_value_score") is None else float(r["pair_value_score"]), reverse=True)[: max(1, int(max_pairs))]
    for i, r in enumerate(rows, start=1):
        r["pair_rank_in_race"] = i
    return rows


def _zscore_in_group(s: pd.Series) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    mu = x.mean(skipna=True)
    sd = x.std(skipna=True)
    if sd is None or sd == 0 or pd.isna(sd):
        return pd.Series([0.0] * len(s), index=s.index)
    return (x - mu) / sd


def _add_pair_reranker_features(
    df: pd.DataFrame,
    *,
    race_meta_map: dict[str, dict[str, Any]],
    required_features: list[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Evaluation-only feature recovery for pair_reranker inference.
    We only add columns; we do not change semantics of existing ones.
    """
    out = df.copy()
    out["race_id"] = out["race_id"].astype(str)

    # Race meta
    meta_df = pd.DataFrame(
        [
            {
                "race_id": rid,
                # DuckDB races uses field_size_expected (central).
                "field_size": m.get("field_size_expected") if m.get("field_size_expected") is not None else m.get("field_size"),
                "distance": m.get("distance"),
                "venue": m.get("venue"),
                "surface": m.get("surface"),
            }
            for rid, m in race_meta_map.items()
        ]
    )
    if len(meta_df) > 0:
        out = out.merge(meta_df, on="race_id", how="left")

    # Encode venue/surface to numeric codes for model stability.
    if "venue" in out.columns:
        venue_map = {
            "札幌": 1, "函館": 2, "福島": 3, "新潟": 4, "東京": 5, "中山": 6,
            "中京": 7, "京都": 8, "阪神": 9, "小倉": 10,
        }
        out["venue"] = out["venue"].map(venue_map)
    if "surface" in out.columns:
        surface_map = {"芝": 1, "ダ": 2, "ダート": 2, "障": 3}
        out["surface"] = out["surface"].map(surface_map)

    # Pair gap min/max/abs_diff
    for c in ["horse1_ai_market_gap", "horse2_ai_market_gap"]:
        if c not in out.columns:
            out[c] = pd.NA
    g1 = pd.to_numeric(out["horse1_ai_market_gap"], errors="coerce")
    g2 = pd.to_numeric(out["horse2_ai_market_gap"], errors="coerce")
    out["pair_ai_market_gap_min"] = pd.concat([g1, g2], axis=1).min(axis=1, skipna=True)
    out["pair_ai_market_gap_max"] = pd.concat([g1, g2], axis=1).max(axis=1, skipna=True)
    out["pair_ai_market_gap_abs_diff"] = (g1 - g2).abs()

    # Rank pct / z-in-race
    if "pair_value_score" in out.columns:
        out["pair_value_score_rank_pct"] = out.groupby("race_id")["pair_value_score"].rank(pct=True, ascending=False, method="first")
        out["pair_value_score_z_in_race"] = out.groupby("race_id")["pair_value_score"].transform(_zscore_in_group)
    if "pair_prob_naive" in out.columns:
        out["pair_prob_naive_rank_pct"] = out.groupby("race_id")["pair_prob_naive"].rank(pct=True, ascending=False, method="first")
        out["pair_prob_naive_z_in_race"] = out.groupby("race_id")["pair_prob_naive"].transform(_zscore_in_group)

    # Buckets (simple, stable)
    if "pair_rank_in_race" in out.columns:
        r = pd.to_numeric(out["pair_rank_in_race"], errors="coerce")
        out["pair_rank_bucket"] = pd.cut(r, bins=[0, 1, 3, 5, 10, 9999], labels=["1", "2-3", "4-5", "6-10", "11+"], right=True).astype(str)
    if "field_size" in out.columns:
        fs = pd.to_numeric(out["field_size"], errors="coerce")
        out["field_size_bucket"] = pd.cut(fs, bins=[0, 10, 14, 18, 99], labels=[1, 2, 3, 4], right=True).astype("float")
    if "distance" in out.columns:
        dist = pd.to_numeric(out["distance"], errors="coerce")
        out["distance_bucket"] = pd.cut(dist, bins=[0, 1400, 1800, 2200, 2600, 9999], labels=[1, 2, 3, 4, 5], right=True).astype("float")

    audit: dict[str, Any] = {}
    if required_features:
        present = set(out.columns)
        rows = []
        for f in required_features:
            s = out[f] if f in out.columns else pd.Series([pd.NA] * len(out))
            non_null_rate = float(pd.to_numeric(s, errors="ignore").notna().mean()) if len(out) > 0 else 0.0
            rows.append(
                {
                    "required_feature": f,
                    "present_in_expanded": f in present,
                    "non_null_rate": non_null_rate,
                    "dtype": str(s.dtype),
                    "all_nan": bool(pd.to_numeric(s, errors="coerce").notna().sum() == 0),
                }
            )
        audit["required_feature_rows"] = rows
    return out, audit


def _normalize_pair_norm(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "pair_norm" not in out.columns:
        if {"horse1_umaban", "horse2_umaban"}.issubset(out.columns):
            out["pair_norm"] = out.apply(
                lambda r: f"{min(int(r['horse1_umaban']), int(r['horse2_umaban'])):02d}-{max(int(r['horse1_umaban']), int(r['horse2_umaban'])):02d}",
                axis=1,
            )
        elif "pair" in out.columns:
            out["pair_norm"] = out["pair"].astype(str)
    if "pair_norm" in out.columns:
        out["pair_norm"] = out["pair_norm"].astype(str)
    if "race_id" in out.columns:
        out["race_id"] = out["race_id"].astype(str)
    return out


def _pair_norm_from_row(row: pd.Series) -> str | None:
    if "pair_norm" in row and pd.notna(row.get("pair_norm")):
        return str(row.get("pair_norm"))
    if "pair" in row and pd.notna(row.get("pair")):
        return str(row.get("pair"))
    if {"horse1_umaban", "horse2_umaban"}.issubset(set(row.index)):
        try:
            a = int(float(row.get("horse1_umaban")))
            b = int(float(row.get("horse2_umaban")))
            x, y = sorted((a, b))
            return f"{x:02d}-{y:02d}"
        except Exception:
            return None
    return None


def _load_rule_pairs_with_source(
    *,
    race_id: str,
    candidate_pairs_path: Path,
    pair_shadow_csv_path: Path,
    run_summary_path: Path,
    today_pipeline_bets_path: Path | None = None,
    today_wide_candidates_final_path: Path | None = None,
) -> dict[str, str]:
    pair_source: dict[str, str] = {}

    if candidate_pairs_path.exists():
        try:
            cdf = pd.read_parquet(candidate_pairs_path) if candidate_pairs_path.suffix.lower() == ".parquet" else pd.read_csv(candidate_pairs_path)
            cdf = _normalize_pair_norm(cdf)
            if "pair_norm" in cdf.columns and "race_id" in cdf.columns:
                r = cdf[cdf["race_id"] == str(race_id)]
                if "pair_selected_flag" in r.columns:
                    mask = pd.to_numeric(r["pair_selected_flag"], errors="coerce").fillna(0).astype(int) == 1
                    for key in r.loc[mask, "pair_norm"].astype(str).tolist():
                        pair_source[key] = "candidate_pairs"
        except Exception:
            pass

    if pair_shadow_csv_path.exists():
        try:
            sdf = pd.read_csv(pair_shadow_csv_path)
            sdf = _normalize_pair_norm(sdf)
            if "pair_norm" in sdf.columns and "race_id" in sdf.columns:
                r = sdf[sdf["race_id"].astype(str) == str(race_id)]
                if "pair_selected_flag" in r.columns:
                    mask = pd.to_numeric(r["pair_selected_flag"], errors="coerce").fillna(0).astype(int) == 1
                    for key in r.loc[mask, "pair_norm"].astype(str).tolist():
                        pair_source.setdefault(key, "pair_shadow_pair_comparison")
        except Exception:
            pass

    if today_pipeline_bets_path is not None and today_pipeline_bets_path.exists():
        try:
            tdf = pd.read_csv(today_pipeline_bets_path)
            tdf = _normalize_pair_norm(tdf)
            if "race_id" in tdf.columns:
                tdf = tdf[tdf["race_id"].astype(str) == str(race_id)]
            for _, rr in tdf.iterrows():
                pn = _pair_norm_from_row(rr)
                if pn:
                    pair_source.setdefault(pn, "today_pipeline_bets")
        except Exception:
            pass

    if today_wide_candidates_final_path is not None and today_wide_candidates_final_path.exists():
        try:
            wdf = pd.read_csv(today_wide_candidates_final_path)
            wdf = _normalize_pair_norm(wdf)
            if "race_id" in wdf.columns:
                wdf = wdf[wdf["race_id"].astype(str) == str(race_id)]
            for _, rr in wdf.iterrows():
                pn = _pair_norm_from_row(rr)
                if pn:
                    pair_source.setdefault(pn, "today_wide_candidates_final")
        except Exception:
            pass

    if run_summary_path.exists():
        try:
            js = json.loads(run_summary_path.read_text(encoding="utf-8"))
            # Optional: if future schema stores explicit rule pair list.
            pairs = []
            if isinstance(js.get("decision_pairs"), list):
                pairs = js.get("decision_pairs", [])
            for p in pairs:
                if isinstance(p, dict):
                    rid = str(p.get("race_id"))
                    pn = str(p.get("pair_norm"))
                    if rid == str(race_id) and pn:
                        pair_source.setdefault(pn, "run_summary")
        except Exception:
            pass

    return pair_source


def main() -> None:
    ap = argparse.ArgumentParser(description="Build expanded candidate pool from DB + existing artifacts for shadow evaluation.")
    ap.add_argument("--race-date", required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--base-dir", type=Path, required=True)
    ap.add_argument("--candidate-pairs", type=Path, required=True)
    ap.add_argument("--run-summary", type=Path, required=True)
    ap.add_argument("--pair-shadow-pair-csv", type=Path, default=None, help="Optional pair_shadow_pair_comparison.csv path for rule recovery")
    ap.add_argument("--model-version", default="top3_stability_plus_pace_v3")
    ap.add_argument("--pair-model-root", type=Path, default=Path("racing_ai/data/models_compare/pair_reranker"))
    ap.add_argument("--pair-model-version", default="pair_reranker_ts_v4")
    ap.add_argument("--expanded-top-horse-n", type=int, default=10)
    ap.add_argument("--expanded-ai-gap-horse-n", type=int, default=10)
    ap.add_argument("--expanded-max-pairs-per-race", type=int, default=45)
    ap.add_argument("--out-expanded", type=Path, required=True)
    ap.add_argument("--out-pair-csv", type=Path, required=True)
    ap.add_argument("--out-race-csv", type=Path, required=True)
    ap.add_argument("--out-audit-md", type=Path, default=Path("racing_ai/reports/expanded_candidate_pool_audit.md"))
    ap.add_argument("--today-pipeline-bets", type=Path, default=None)
    ap.add_argument("--today-wide-candidates-final", type=Path, default=None)
    args = ap.parse_args()

    db = DuckDb.connect(args.db_path)
    race_date = args.race_date
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
    preds = db.query_df(
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
        JOIN races r ON r.race_id=hp.race_id
        WHERE r.race_date = cast(? as DATE)
          AND hp.model_version = ?
        """,
        (args.model_version, race_date, args.model_version),
    ).to_dict("records")
    market = _latest_market_top3_probs_for_date(db=db, race_date=race_date)
    market_proxy_source = "odds_place"
    if len(market) == 0:
        market = _fallback_market_top3_probs_from_predictions(db, race_date, args.model_version)
        market_proxy_source = "predictions_scaled_low_confidence"

    base_df = pd.read_parquet(args.candidate_pairs) if args.candidate_pairs.exists() else pd.DataFrame()
    base_df = _normalize_pair_norm(base_df)
    pair_shadow_pair_csv = args.pair_shadow_pair_csv or (args.base_dir / "pair_shadow_pair_comparison.csv")
    today_pipeline_bets_path = args.today_pipeline_bets or (args.base_dir / "today_pipeline_bets.csv")
    today_wide_candidates_final_path = args.today_wide_candidates_final or (args.base_dir / "today_wide_candidates_final.csv")

    pred_map = {(str(r["race_id"]), int(r["horse_no"])): (None if r.get("p_top3") is None else float(r["p_top3"])) for r in preds}
    by_race_hn: dict[str, list[int]] = {}
    by_race_hid: dict[str, dict[int, str | None]] = {}
    for e in entries:
        rid = str(e["race_id"])
        hn = int(e["horse_no"])
        by_race_hn.setdefault(rid, []).append(hn)
        by_race_hid.setdefault(rid, {})[hn] = e.get("horse_id")

    expanded_rows: list[dict[str, Any]] = []
    race_meta_df = db.query_df(
        """
        SELECT race_id, venue, surface, distance, field_size_expected AS field_size
        FROM races
        WHERE race_date = cast(? as DATE)
        """,
        (race_date,),
    )
    race_meta_map = {str(r["race_id"]): {"venue": r.get("venue"), "surface": r.get("surface"), "distance": r.get("distance"), "field_size": r.get("field_size")} for r in race_meta_df.to_dict("records")}

    for rid, hns in by_race_hn.items():
        p_top3: dict[int, float] = {}
        for hn in hns:
            p_ai = pred_map.get((rid, hn))
            if p_ai is None:
                continue
            p_mkt = market.get((rid, hn))
            p_fused = blend_ai_market_prob(p_ai=p_ai, p_market=p_mkt, ai_weight=0.65)
            if p_fused is not None:
                p_top3[hn] = float(p_fused)
        if not p_top3:
            continue
        rule_pair_source = _load_rule_pairs_with_source(
            race_id=rid,
            candidate_pairs_path=args.candidate_pairs,
            pair_shadow_csv_path=pair_shadow_pair_csv,
            run_summary_path=args.run_summary,
            today_pipeline_bets_path=today_pipeline_bets_path,
            today_wide_candidates_final_path=today_wide_candidates_final_path,
        )
        if len(rule_pair_source) == 0 and len(base_df) > 0 and "race_id" in base_df.columns and "pair_norm" in base_df.columns:
            for key in base_df.loc[base_df["race_id"].astype(str) == rid, "pair_norm"].astype(str).tolist():
                rule_pair_source.setdefault(key, "candidate_pairs")
        rows = _build_for_race(
            rid=rid,
            horse_nos=sorted(set(hns)),
            horse_id_map=by_race_hid.get(rid, {}),
            p_top3=p_top3,
            market=market,
            market_proxy_source=market_proxy_source,
            rule_pair_source=rule_pair_source,
            top_horse_n=args.expanded_top_horse_n,
            ai_gap_horse_n=args.expanded_ai_gap_horse_n,
            max_pairs=args.expanded_max_pairs_per_race,
        )
        expanded_rows.extend(rows)

    # Try the shared shadow scorer first (may fail for older years due to feature schema drift).
    warnings: list[str] = []
    try:
        from aikeiba.orchestration.race_day import _apply_pair_shadow_scores  # local import to keep eval-only fallback isolated

        expanded_rows, warnings, _ = _apply_pair_shadow_scores(
            pair_rows=expanded_rows,
            pair_model_root=args.pair_model_root,
            pair_model_version=args.pair_model_version,
            race_meta_map=race_meta_map,
        )
    except Exception as e:
        expanded_rows = expanded_rows
        warnings = [f"pair_shadow_model_load_failed:{type(e).__name__}:{e}"]

    # If model scoring didn't produce any non-null pair_model_score, fall back to Booster.predict-based scorer.
    tmp_df = pd.DataFrame(expanded_rows)
    if ("pair_model_score" not in tmp_df.columns) or (pd.to_numeric(tmp_df.get("pair_model_score"), errors="coerce").notna().sum() == 0):
        tmp_df, w2 = _safe_apply_pair_model_score(tmp_df, pair_model_root=args.pair_model_root, pair_model_version=args.pair_model_version)
        warnings.extend(w2)
        expanded_rows = tmp_df.to_dict("records")
    expanded_rows, _ = _enrich_pair_rows_for_shadow_selection(
        pair_rows=expanded_rows,
        base_top_n=5,
        score_threshold=0.08,
        edge_threshold=0.0,
        gap_threshold=0.01,
        default_k=5,
        min_k=1,
        max_k=5,
    )

    out_df = pd.DataFrame(expanded_rows)

    # Recover missing pair_reranker features (evaluation-only).
    required_features: list[str] = []
    meta_path = args.pair_model_root / args.pair_model_version / "meta.json"
    if meta_path.exists():
        try:
            required_features = list(json.loads(meta_path.read_text(encoding="utf-8")).get("features") or [])
        except Exception:
            required_features = []
    out_df, recovery_audit = _add_pair_reranker_features(out_df, race_meta_map=race_meta_map, required_features=required_features)
    if len(out_df) > 0:
        out_df = out_df.drop_duplicates(subset=["race_id", "pair_norm"], keep="first")
        conf_map = {
            "candidate_pairs": "high",
            "pair_shadow_pair_comparison": "high",
            "today_pipeline_bets": "medium",
            "today_wide_candidates_final": "medium",
            "run_summary": "low",
            "unavailable": "none",
        }
        out_df["rule_recovery_confidence"] = out_df.get("rule_selected_source", pd.Series(["unavailable"] * len(out_df))).map(conf_map).fillna("none")
    args.out_expanded.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(args.out_expanded, index=False)

    # Build expanded comparison CSVs (reusing report script behavior in-place)
    pair_cols = [
        "race_id", "pair_norm", "horse1_umaban", "horse2_umaban", "pair_selected_flag", "model_top5_flag",
        "model_dynamic_selected_flag", "pair_value_score", "pair_model_score", "pair_market_implied_prob",
        "pair_edge", "pair_edge_ratio", "pair_edge_log_ratio", "pair_edge_rank_gap", "pair_edge_pct_gap",
        # Evaluation-only audit column: tells whether market proxy came from odds or fallback.
        "market_proxy_source",
        "model_dynamic_final_score", "model_dynamic_rank", "model_dynamic_skip_reason", "actual_wide_hit", "wide_payout"
    ]
    for c in pair_cols:
        if c not in out_df.columns:
            out_df[c] = pd.NA
    args.out_pair_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df[pair_cols].to_csv(args.out_pair_csv, index=False, encoding="utf-8")

    race_rows = []
    for rid, g in out_df.groupby("race_id", sort=False):
        rule = pd.to_numeric(g.get("pair_selected_flag"), errors="coerce").fillna(0).astype(int)
        top5 = pd.to_numeric(g.get("model_top5_flag"), errors="coerce").fillna(0).astype(int)
        dyn = pd.to_numeric(g.get("model_dynamic_selected_flag"), errors="coerce").fillna(0).astype(int)
        non_rule = (rule == 0)
        race_rows.append(
            {
                "race_id": rid,
                "candidate_pair_count": int(len(g)),
                "rule_selected_count": int(rule.sum()),
                "non_rule_candidate_count": int(non_rule.sum()),
                "model_top5_selected_count": int(top5.sum()),
                "model_dynamic_selected_count": int(dyn.sum()),
                "model_top5_non_rule_count": int(((top5 == 1) & non_rule).sum()),
                "model_dynamic_non_rule_count": int(((dyn == 1) & non_rule).sum()),
                "top_non_rule_model_score": (None if pd.to_numeric(g.loc[non_rule, "pair_model_score"], errors="coerce").dropna().empty else float(pd.to_numeric(g.loc[non_rule, "pair_model_score"], errors="coerce").max())),
            }
        )
    pd.DataFrame(race_rows).to_csv(args.out_race_csv, index=False, encoding="utf-8")

    recovered_rule_pair_count = int(pd.to_numeric(out_df.get("pair_selected_flag"), errors="coerce").fillna(0).astype(int).sum()) if len(out_df) > 0 else 0
    expanded_non_rule_pair_count = int((pd.to_numeric(out_df.get("pair_selected_flag"), errors="coerce").fillna(0).astype(int) == 0).sum()) if len(out_df) > 0 else 0
    source_counts = out_df.get("rule_selected_source", pd.Series(dtype=str)).fillna("unavailable").value_counts().to_dict() if len(out_df) > 0 else {}
    races_with_recovered_rule_count = int(out_df.groupby("race_id")["pair_selected_flag"].apply(lambda s: pd.to_numeric(s, errors="coerce").fillna(0).astype(int).sum() > 0).sum()) if len(out_df) > 0 else 0
    races_without_recovered_rule_count = int(out_df["race_id"].nunique()) - races_with_recovered_rule_count if len(out_df) > 0 else 0

    audit = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "race_date": race_date,
        "model_version": args.model_version,
        "expanded_top_horse_n": args.expanded_top_horse_n,
        "expanded_ai_gap_horse_n": args.expanded_ai_gap_horse_n,
        "expanded_max_pairs_per_race": args.expanded_max_pairs_per_race,
        "expanded_rows": int(len(out_df)),
        "expanded_source_counts": (out_df["expanded_source"].fillna("NA").value_counts().to_dict() if "expanded_source" in out_df.columns else {}),
        "pair_model_score_non_null_count": int(out_df.get("pair_model_score", pd.Series(dtype=float)).notna().sum()),
        "rule_selected_source": source_counts,
        "recovered_rule_pair_count": recovered_rule_pair_count,
        "expanded_total_pair_count": int(len(out_df)),
        "expanded_non_rule_pair_count": expanded_non_rule_pair_count,
        "expanded_non_rule_pair_rate": (float(expanded_non_rule_pair_count / len(out_df)) if len(out_df) > 0 else None),
        "races_with_recovered_rule_count": races_with_recovered_rule_count,
        "races_without_recovered_rule_count": races_without_recovered_rule_count,
        "rule_flag_recovered": bool(recovered_rule_pair_count > 0),
        "rule_source_unavailable_warning": bool(source_counts.get("unavailable", 0) > 0),
        "warnings": warnings,
    }
    args.out_audit_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_audit_md.write_text(
        "\n".join(
            [
                "# Expanded Candidate Pool Audit",
                "",
                f"- generated_at: {audit['generated_at']}",
                f"- race_date: {race_date}",
                f"- expanded_rows: {audit['expanded_rows']}",
                f"- expanded_source_counts: {json.dumps(audit['expanded_source_counts'], ensure_ascii=False)}",
                f"- pair_model_score_non_null_count: {audit['pair_model_score_non_null_count']}",
                f"- rule_selected_source: {json.dumps(audit['rule_selected_source'], ensure_ascii=False)}",
                f"- recovered_rule_pair_count: {audit['recovered_rule_pair_count']}",
                f"- expanded_total_pair_count: {audit['expanded_total_pair_count']}",
                f"- expanded_non_rule_pair_count: {audit['expanded_non_rule_pair_count']}",
                f"- expanded_non_rule_pair_rate: {audit['expanded_non_rule_pair_rate']}",
                f"- races_with_recovered_rule_count: {audit['races_with_recovered_rule_count']}",
                f"- races_without_recovered_rule_count: {audit['races_without_recovered_rule_count']}",
                f"- rule_flag_recovered: {audit['rule_flag_recovered']}",
                f"- warning_rule_source_unavailable: {audit['rule_source_unavailable_warning']}",
                f"- warnings: {json.dumps(audit['warnings'], ensure_ascii=False)}",
            ]
        ),
        encoding="utf-8",
    )
    print(str(args.out_expanded))
    print(str(args.out_pair_csv))
    print(str(args.out_race_csv))
    print(str(args.out_audit_md))


if __name__ == "__main__":
    main()
