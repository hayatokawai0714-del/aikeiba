from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any

import pandas as pd

from aikeiba.db.duckdb import DuckDb
from aikeiba.decision.skip_reasoning import SkipReasonConfig, decide_skip_reason
from aikeiba.decision.skip_rules import decide_buy_or_skip
from aikeiba.decision.value_model import blend_ai_market_prob, compute_market_top3_prob_from_place_odds
from aikeiba.decision.wide_rules import generate_wide_candidates_rule_based


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


def _bucket_distance(v: Any) -> str:
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


def _bucket_field_size(v: Any) -> str:
    if v is None or pd.isna(v):
        return "UNKNOWN"
    n = int(v)
    if n <= 10:
        return "small"
    if n <= 14:
        return "medium"
    return "large"


def _bucket_popularity(v: Any) -> str:
    if v is None or pd.isna(v):
        return "UNKNOWN"
    p = float(v)
    if p <= 1:
        return "1人気"
    if p <= 3:
        return "2-3人気"
    if p <= 5:
        return "4-5人気"
    if p <= 9:
        return "6-9人気"
    return "10人気以下"


def _bucket_gap(v: Any) -> str:
    if v is None or pd.isna(v):
        return "UNKNOWN"
    x = float(v)
    if x < -0.05:
        return "neg_large"
    if x < 0.0:
        return "neg_small"
    if x < 0.05:
        return "pos_small"
    return "pos_large"


_DEFAULT_NORMALIZATION = {
    "venue": {
        "05": "東京",
        "TOKYO": "東京",
        "東京": "東京",
        "06": "中山",
        "NAKAYAMA": "中山",
        "中山": "中山",
        "08": "京都",
        "KYOTO": "京都",
        "京都": "京都",
        "09": "阪神",
        "HANSHIN": "阪神",
        "阪神": "阪神",
        "10": "小倉",
        "KOKURA": "小倉",
        "小倉": "小倉",
    },
    "surface": {
        "turf": "芝",
        "芝": "芝",
        "80": "芝",
        "dirt": "ダート",
        "ダート": "ダート",
        "00": "ダート",
        "jump": "障害",
        "障害": "障害",
    },
}


def _load_normalization_maps() -> dict[str, dict[str, str]]:
    cfg_path = Path(__file__).resolve().parents[1] / "config" / "normalization_maps.json"
    if cfg_path.exists():
        try:
            obj = json.loads(cfg_path.read_text(encoding="utf-8"))
            return {
                "venue": {str(k): str(v) for k, v in obj.get("venue", {}).items()},
                "surface": {str(k): str(v) for k, v in obj.get("surface", {}).items()},
            }
        except Exception:
            pass
    return _DEFAULT_NORMALIZATION


def normalize_venue(value: Any, maps: dict[str, dict[str, str]]) -> str:
    if value is None or pd.isna(value):
        return "UNKNOWN"
    s = str(value).strip().upper()
    mapping = {str(k).strip().upper(): v for k, v in maps.get("venue", {}).items()}
    return mapping.get(s, "UNKNOWN")


def normalize_surface(value: Any, maps: dict[str, dict[str, str]]) -> str:
    if value is None or pd.isna(value):
        return "UNKNOWN"
    s = str(value).strip().lower()
    mapping = {str(k).strip().lower(): v for k, v in maps.get("surface", {}).items()}
    return mapping.get(s, "UNKNOWN")


def _ratio_dict(df: pd.DataFrame, key_col: str) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    if len(df) == 0:
        return out
    for k, g in df.groupby(key_col):
        stake = float(g["stake"].sum())
        ret = float(g["ret"].sum())
        out[str(k)] = (ret / stake) if stake > 0 else None
    return out


def _max_drawdown(pnl_series: list[float]) -> float:
    if len(pnl_series) == 0:
        return 0.0
    equity = 0.0
    peak = 0.0
    mdd = 0.0
    for p in pnl_series:
        equity += float(p)
        peak = max(peak, equity)
        mdd = min(mdd, equity - peak)
    return float(mdd)


def _load_base_tables(db: DuckDb, start_date: str, end_date: str, model_version: str) -> dict[str, pd.DataFrame]:
    races = db.query_df(
        """
        SELECT race_id, cast(race_date as VARCHAR) AS race_date, venue, distance, surface
        FROM races
        WHERE race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
        """,
        (start_date, end_date),
    )
    entries = db.query_df(
        """
        SELECT e.race_id, e.horse_no, e.horse_id, e.horse_name, e.pop_rank,
               (e.is_scratched IS NULL OR e.is_scratched=FALSE) AS is_active
        FROM entries e
        JOIN races r ON r.race_id=e.race_id
        WHERE r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
        """,
        (start_date, end_date),
    )
    results_pop = db.query_df(
        """
        SELECT res.race_id, res.horse_no, res.pop_rank
        FROM results res
        JOIN races r ON r.race_id=res.race_id
        WHERE r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
        """,
        (start_date, end_date),
    )
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
        WHERE r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
        """,
        (model_version, start_date, end_date),
    )
    odds = db.query_df(
        """
        WITH ranked AS (
          SELECT o.race_id, o.horse_no, lower(o.odds_type) AS odds_type, o.odds_value, o.captured_at,
                 row_number() OVER (
                   PARTITION BY o.race_id, o.horse_no, lower(o.odds_type)
                   ORDER BY o.captured_at DESC NULLS LAST, o.odds_snapshot_version DESC
                 ) AS rn
          FROM odds o
          JOIN races r ON r.race_id=o.race_id
          WHERE r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
            AND lower(o.odds_type) IN ('place','place_max')
            AND o.horse_no > 0
        )
        SELECT race_id, horse_no,
               max(CASE WHEN odds_type='place' THEN odds_value END) AS odds_place,
               max(CASE WHEN odds_type='place_max' THEN odds_value END) AS odds_place_max
        FROM ranked
        WHERE rn=1
        GROUP BY race_id, horse_no
        """,
        (start_date, end_date),
    )
    payouts = db.query_df(
        """
        SELECT p.race_id, p.bet_key, p.payout
        FROM payouts p
        JOIN races r ON r.race_id=p.race_id
        WHERE r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
          AND lower(p.bet_type)='wide'
        """,
        (start_date, end_date),
    )
    return {
        "races": races,
        "entries": entries,
        "results_pop": results_pop,
        "preds": preds,
        "odds": odds,
        "payouts": payouts,
    }


def _resolve_feature_set_version(
    *,
    model_version: str,
    feature_set_version_arg: str | None,
) -> tuple[str | None, str]:
    if feature_set_version_arg and str(feature_set_version_arg).strip():
        return str(feature_set_version_arg), "cli"
    meta_path = Path("racing_ai/data/models_compare") / "top3" / str(model_version) / "meta.json"
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            v = meta.get("feature_set_version") or meta.get("feature_snapshot_version")
            if v:
                return str(v), "model_meta"
        except Exception:
            pass
    return None, "unknown"


def _build_market_proxy(odds: pd.DataFrame) -> pd.DataFrame:
    if len(odds) == 0:
        return pd.DataFrame(columns=["race_id", "horse_no", "market_top3_proxy"])
    o = odds.copy()
    o["market_top3_proxy"] = o.apply(
        lambda r: compute_market_top3_prob_from_place_odds(r["odds_place"], r["odds_place_max"]),
        axis=1,
    )
    parts: list[pd.DataFrame] = []
    for _, g in o.groupby("race_id"):
        gg = g.copy()
        s = gg["market_top3_proxy"].dropna().sum()
        if s and s > 0:
            gg["market_top3_proxy"] = gg["market_top3_proxy"].apply(
                lambda v: min(1.0, max(0.0, (float(v) * 3.0 / float(s)) if pd.notna(v) else v))
            )
        parts.append(gg[["race_id", "horse_no", "market_top3_proxy", "odds_place", "odds_place_max"]])
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["race_id", "horse_no", "market_top3_proxy"])


def _simulate_for_params(
    *,
    races: pd.DataFrame,
    entries: pd.DataFrame,
    preds: pd.DataFrame,
    market: pd.DataFrame,
    odds: pd.DataFrame,
    results_pop: pd.DataFrame,
    payouts: pd.DataFrame,
    density_top3_max: float,
    gap12_min: float,
    ai_weight: float,
) -> tuple[dict[str, Any], dict[str, Any], pd.DataFrame]:
    pred_map = {(str(r.race_id), int(r.horse_no)): float(r.p_top3) for r in preds.itertuples() if pd.notna(r.p_top3)}
    mkt_map = {(str(r.race_id), int(r.horse_no)): float(r.market_top3_proxy) for r in market.itertuples() if pd.notna(r.market_top3_proxy)}
    pop_map_entries = {(str(r.race_id), int(r.horse_no)): r.pop_rank for r in entries.itertuples()}
    pop_map_results = {(str(r.race_id), int(r.horse_no)): r.pop_rank for r in results_pop.itertuples()}
    odds_rank_map: dict[tuple[str, int], float] = {}
    if len(odds) > 0:
        oo = odds.copy().dropna(subset=["odds_place"])
        if len(oo) > 0:
            oo["odds_place_rank_estimated"] = oo.groupby("race_id")["odds_place"].rank(ascending=True, method="min")
            odds_rank_map = {(str(r.race_id), int(r.horse_no)): float(r.odds_place_rank_estimated) for r in oo.itertuples()}

    payout_map = {}
    for r in payouts.itertuples():
        k = _normalize_pair_key(r.bet_key)
        if k is None or pd.isna(r.payout):
            continue
        payout_map[(str(r.race_id), k)] = float(r.payout)

    race_meta = races.set_index("race_id")[["race_date", "venue", "distance", "surface"]].to_dict("index")
    norm_maps = _load_normalization_maps()

    def _get_popularity(race_id: str, horse_no: int) -> tuple[float | None, str]:
        key = (race_id, horse_no)
        v_res = pop_map_results.get(key)
        if v_res is not None and not pd.isna(v_res):
            return float(v_res), "results.pop_rank"
        v_ent = pop_map_entries.get(key)
        if v_ent is not None and not pd.isna(v_ent):
            return float(v_ent), "entries.pop_rank"
        v_odds = odds_rank_map.get(key)
        if v_odds is not None and not pd.isna(v_odds):
            return float(v_odds), "odds.estimated_rank"
        return None, "UNKNOWN"

    active_entries = entries[entries["is_active"] == True]
    by_race = active_entries.groupby("race_id")["horse_no"].apply(list).to_dict()

    rows: list[dict[str, Any]] = []
    pop_audit_rows: list[dict[str, Any]] = []
    skip_counts: dict[str, int] = {}
    for rid, horse_nos_raw in by_race.items():
        rid = str(rid)
        horse_nos = [int(h) for h in horse_nos_raw]
        fused: dict[int, float] = {}
        gaps: dict[int, float] = {}
        markets: dict[int, float] = {}

        meta = race_meta.get(rid, {})
        venue_norm = normalize_venue(meta.get("venue"), norm_maps)
        surface_norm = normalize_surface(meta.get("surface"), norm_maps)

        for hn in horse_nos:
            p_ai = pred_map.get((rid, hn))
            if p_ai is None:
                continue
            p_mkt = mkt_map.get((rid, hn))
            pf = blend_ai_market_prob(p_ai=p_ai, p_market=p_mkt, ai_weight=ai_weight)
            if pf is None:
                continue
            fused[hn] = float(pf)
            if p_mkt is not None:
                gaps[hn] = float(pf - p_mkt)
                markets[hn] = float(p_mkt)

            pop_rank, pop_source = _get_popularity(rid, hn)
            pop_audit_rows.append(
                {
                    "race_id": rid,
                    "horse_no": hn,
                    "horse_id": entries.loc[(entries["race_id"] == rid) & (entries["horse_no"] == hn), "horse_id"].iloc[0]
                    if len(entries.loc[(entries["race_id"] == rid) & (entries["horse_no"] == hn)]) > 0
                    else None,
                    "model_version": None,
                    "race_date": meta.get("race_date"),
                    "pop_rank": pop_rank,
                    "popularity_bucket": _bucket_popularity(pop_rank),
                    "popularity_source": pop_source,
                    "odds_place": odds.loc[(odds["race_id"] == rid) & (odds["horse_no"] == hn), "odds_place"].iloc[0]
                    if len(odds.loc[(odds["race_id"] == rid) & (odds["horse_no"] == hn)]) > 0
                    else None,
                    "odds_place_rank_estimated": odds_rank_map.get((rid, hn)),
                    "raw_venue": meta.get("venue"),
                    "venue": venue_norm,
                    "raw_surface": meta.get("surface"),
                    "surface": surface_norm,
                    "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
                }
            )

        dec = decide_buy_or_skip(p_top3=list(fused.values()), density_top3_max=density_top3_max, gap12_min=gap12_min)
        skip_reason = decide_skip_reason(
            density_top3=dec.density_top3,
            gap12=dec.gap12,
            positive_ai_gap_count=sum(1 for v in gaps.values() if v > 0),
            candidate_count=0,
            market_top1=max(markets.values()) if len(markets) > 0 else None,
            top_pair_value_score=None,
            config=SkipReasonConfig(density_top3_max=density_top3_max, gap12_min=gap12_min),
        )
        skip_counts[skip_reason] = skip_counts.get(skip_reason, 0) + 1
        if skip_reason != "BUY_OK":
            continue

        cands = generate_wide_candidates_rule_based(
            race_id=rid,
            horse_nos=horse_nos,
            p_top3=fused,
            axis_k=1,
            partner_k=min(6, len(horse_nos)),
        )
        for c in cands:
            k = _normalize_pair_key(c.pair)
            if k is None:
                continue
            ret = float(payout_map.get((rid, k), 0.0))
            hit = 1 if ret > 0 else 0
            h1, h2 = sorted((int(c.axis_horse_no), int(c.partner_horse_no)))
            pop1, src1 = _get_popularity(rid, h1)
            pop2, src2 = _get_popularity(rid, h2)
            pair_pop = ((float(pop1) + float(pop2)) / 2.0) if (pop1 is not None and pop2 is not None) else None
            pair_src = src1 if src1 == src2 else f"mixed:{src1}+{src2}"
            g1 = gaps.get(h1)
            g2 = gaps.get(h2)
            pair_gap = (float(g1 + g2) if (g1 is not None and g2 is not None) else None)

            rows.append(
                {
                    "race_id": rid,
                    "race_date": meta.get("race_date"),
                    "raw_venue": meta.get("venue"),
                    "venue": venue_norm,
                    "distance": meta.get("distance"),
                    "raw_surface": meta.get("surface"),
                    "surface": surface_norm,
                    "field_size": len(horse_nos),
                    "skip_reason": skip_reason,
                    "pair": k,
                    "stake": 100.0,
                    "ret": ret,
                    "hit": hit,
                    "pair_popularity_mean": pair_pop,
                    "popularity_bucket": _bucket_popularity(pair_pop),
                    "popularity_source": pair_src,
                    "ai_market_gap_sum": pair_gap,
                    "ai_market_gap_bucket": _bucket_gap(pair_gap),
                }
            )

    sim = pd.DataFrame(rows)
    pop_audit = pd.DataFrame(pop_audit_rows)

    total_stake = float(sim["stake"].sum()) if len(sim) > 0 else 0.0
    total_return = float(sim["ret"].sum()) if len(sim) > 0 else 0.0
    profit = total_return - total_stake
    hit_count = int(sim["hit"].sum()) if len(sim) > 0 else 0
    bet_pair_count = int(len(sim))
    bet_race_count = int(sim["race_id"].nunique()) if len(sim) > 0 else 0
    hit_rate = float(hit_count / bet_pair_count) if bet_pair_count > 0 else None
    roi = float(total_return / total_stake) if total_stake > 0 else None

    monthly_roi = {}
    if len(sim) > 0:
        tmp = sim.copy()
        tmp["month"] = tmp["race_date"].astype(str).str.slice(0, 7)
        monthly_roi = _ratio_dict(tmp, "month")

    pop_roi = _ratio_dict(sim, "popularity_bucket") if len(sim) > 0 else {}
    pop_source_counts = sim["popularity_source"].value_counts(dropna=False).to_dict() if len(sim) > 0 else {}
    venue_roi = _ratio_dict(sim, "venue") if len(sim) > 0 else {}
    surface_roi = _ratio_dict(sim, "surface") if len(sim) > 0 else {}

    distance_roi = {}
    if len(sim) > 0:
        tmp = sim.copy()
        tmp["distance_bucket"] = tmp["distance"].apply(_bucket_distance)
        distance_roi = _ratio_dict(tmp, "distance_bucket")

    field_size_roi = {}
    if len(sim) > 0:
        tmp = sim.copy()
        tmp["field_size_bucket"] = tmp["field_size"].apply(_bucket_field_size)
        field_size_roi = _ratio_dict(tmp, "field_size_bucket")

    gap_roi = _ratio_dict(sim, "ai_market_gap_bucket") if len(sim) > 0 else {}

    unknown_venue_raw = (
        sorted(sim.loc[sim["venue"] == "UNKNOWN", "raw_venue"].dropna().astype(str).unique().tolist()) if len(sim) > 0 else []
    )
    unknown_surface_raw = (
        sorted(sim.loc[sim["surface"] == "UNKNOWN", "raw_surface"].dropna().astype(str).unique().tolist()) if len(sim) > 0 else []
    )

    mdd = 0.0
    if len(sim) > 0:
        race_pnl_df = (
            sim.groupby(["race_date", "race_id"], as_index=False)
            .agg(ret=("ret", "sum"), stake=("stake", "sum"))
            .sort_values(["race_date", "race_id"], ascending=[True, True])
        )
        race_pnl = [float(r.ret - r.stake) for r in race_pnl_df.itertuples()]
        mdd = _max_drawdown(race_pnl)

    row = {
        "ai_weight": float(ai_weight),
        "density_top3_max": float(density_top3_max),
        "gap12_min": float(gap12_min),
        "bet_race_count": bet_race_count,
        "bet_pair_count": bet_pair_count,
        "hit_count": hit_count,
        "hit_rate": hit_rate,
        "total_stake": total_stake,
        "total_return": total_return,
        "roi": roi,
        "profit": profit,
        "avg_return_per_bet": (total_return / bet_pair_count) if bet_pair_count > 0 else None,
        "max_drawdown": mdd,
        "monthly_roi": json.dumps(monthly_roi, ensure_ascii=False),
        "popularity_bucket_roi": json.dumps(pop_roi, ensure_ascii=False),
        "venue_roi": json.dumps(venue_roi, ensure_ascii=False),
        "distance_bucket_roi": json.dumps(distance_roi, ensure_ascii=False),
        "surface_roi": json.dumps(surface_roi, ensure_ascii=False),
        "field_size_bucket_roi": json.dumps(field_size_roi, ensure_ascii=False),
        "ai_market_gap_bucket_roi": json.dumps(gap_roi, ensure_ascii=False),
        "race_count_by_skip_reason": json.dumps(skip_counts, ensure_ascii=False),
        "max_drawdown_method": "race_date_race_id_ordered_race_aggregate_pnl",
        "popularity_source_counts": json.dumps(pop_source_counts, ensure_ascii=False),
    }

    detail = {
        "monthly_roi": monthly_roi,
        "popularity_bucket_roi": pop_roi,
        "popularity_source_counts": pop_source_counts,
        "venue_roi": venue_roi,
        "distance_bucket_roi": distance_roi,
        "surface_roi": surface_roi,
        "field_size_bucket_roi": field_size_roi,
        "ai_market_gap_bucket_roi": gap_roi,
        "race_count_by_skip_reason": skip_counts,
        "unknown_raw_venue_values": unknown_venue_raw,
        "unknown_raw_surface_values": unknown_surface_raw,
    }
    return row, detail, pop_audit


def run(
    *,
    db: DuckDb,
    start_date: str,
    end_date: str,
    model_version: str,
    density_values: list[float],
    gap12_values: list[float],
    ai_weight_values: list[float],
    out_dir: Path,
    apply_start_date: str | None = None,
    apply_end_date: str | None = None,
    feature_set_version: str | None = None,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    tables = _load_base_tables(db, start_date, end_date, model_version)
    market = _build_market_proxy(tables["odds"])

    rows: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []
    pop_audits: list[pd.DataFrame] = []
    warnings: list[str] = []

    if len(tables["payouts"]) == 0:
        warnings.append("payouts_missing_wide_only_hit_may_be_underestimated")
    resolved_feature_set_version, fs_source = _resolve_feature_set_version(
        model_version=model_version,
        feature_set_version_arg=feature_set_version,
    )
    if resolved_feature_set_version is None:
        warnings.append("feature_set_version_unresolved")

    for aiw in ai_weight_values:
        for dmax in density_values:
            for gmin in gap12_values:
                row, detail, pop_audit = _simulate_for_params(
                    races=tables["races"],
                    entries=tables["entries"],
                    preds=tables["preds"],
                    market=market,
                    odds=tables["odds"],
                    results_pop=tables["results_pop"],
                    payouts=tables["payouts"],
                    density_top3_max=float(dmax),
                    gap12_min=float(gmin),
                    ai_weight=float(aiw),
                )
                rows.append(row)
                detail_rows.append({"ai_weight": float(aiw), "density_top3_max": float(dmax), "gap12_min": float(gmin), **detail})
                if len(pop_audit) > 0:
                    pop_audit = pop_audit.copy()
                    pop_audit["model_version"] = model_version
                    pop_audits.append(pop_audit)

    grid = pd.DataFrame(rows)

    if len(grid) == 0:
        payload = {
            "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
            "start_date": start_date,
            "end_date": end_date,
            "model_version": model_version,
            "grid_rows": 0,
            "warnings": warnings,
        }
        (out_dir / "wide_grid_enhanced.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    best = grid.sort_values(["roi", "bet_race_count"], ascending=[False, False]).head(1).to_dict("records")[0]

    detail_json_path = out_dir / "wide_grid_enhanced_detail.json"
    detail_json = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "start_date": start_date,
        "end_date": end_date,
        "model_version": model_version,
        "details": detail_rows,
    }

    grid_parquet_path = out_dir / "wide_grid_enhanced.parquet"
    grid_json_path = out_dir / "wide_grid_enhanced.json"
    pop_audit_path = out_dir / "popularity_source_audit.parquet"
    grid_metadata_path = out_dir / "grid_search_metadata.json"

    payload = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "start_date": start_date,
        "end_date": end_date,
        "model_version": model_version,
        "feature_set_version": resolved_feature_set_version,
        "best": best,
        "grid_rows": int(len(grid)),
        "warnings": warnings,
        "detail_json_path": str(detail_json_path),
        "popularity_source_audit_path": str(pop_audit_path),
        "grid_search_metadata_path": str(grid_metadata_path),
        "grid_param_application_path": str(out_dir / "grid_param_application.json"),
        "venue_surface_normalization": {"enabled": True, "raw_columns_preserved": ["raw_venue", "raw_surface"]},
        "skip_reason_extra_settings": {
            "enable_value_skip": False,
            "min_ai_market_gap": None,
            "enable_market_overrated_skip": False,
            "max_market_overrated_top_count": None,
            "enable_pair_ev_skip": False,
            "min_pair_value_score": None,
        },
        "max_drawdown_method": "race_date_race_id_ordered_race_aggregate_pnl",
        "pair_score_in_production_candidates": True,
    }

    grid_metadata = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "grid_start_date": start_date,
        "grid_end_date": end_date,
        "model_version": model_version,
        "feature_set_version": resolved_feature_set_version,
        "feature_set_version_source": fs_source,
        "density_values": density_values,
        "gap12_values": gap12_values,
        "ai_weight_values": ai_weight_values,
        "selected_best_params": {
            "ai_weight": best.get("ai_weight"),
            "density_top3_max": best.get("density_top3_max"),
            "gap12_min": best.get("gap12_min"),
        },
        "selection_metric": "roi",
        "warning_if_used_for_same_period_prediction": "Do not apply best params to predictions evaluated on the same grid period.",
        "apply_start_date": apply_start_date,
        "apply_end_date": apply_end_date,
    }
    is_safe_temporal_application = None
    if apply_start_date:
        try:
            is_safe_temporal_application = pd.to_datetime(apply_start_date).date() > pd.to_datetime(end_date).date()
        except Exception:
            is_safe_temporal_application = None
    grid_param_application = {
        "grid_start_date": start_date,
        "grid_end_date": end_date,
        "selected_best_params": grid_metadata["selected_best_params"],
        "selection_metric": "roi",
        "apply_start_date": apply_start_date,
        "apply_end_date": apply_end_date,
        "applied_model_version": model_version,
        "feature_set_version": resolved_feature_set_version,
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "is_safe_temporal_application": is_safe_temporal_application,
        "manual_check_required": bool(apply_start_date is None),
    }

    grid.to_parquet(grid_parquet_path, index=False)
    grid_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    detail_json_path.write_text(json.dumps(detail_json, ensure_ascii=False, indent=2), encoding="utf-8")
    grid_metadata_path.write_text(json.dumps(grid_metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "grid_param_application.json").write_text(
        json.dumps(grid_param_application, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    if len(pop_audits) > 0:
        pd.concat(pop_audits, ignore_index=True).drop_duplicates(
            subset=["race_id", "horse_no", "model_version", "race_date", "popularity_source"], keep="last"
        ).to_parquet(pop_audit_path, index=False)

    return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--model-version", required=True)
    ap.add_argument("--density-values", default="1.2,1.35,1.5,1.8,2.1,2.4")
    ap.add_argument("--gap12-values", default="0.0,0.003,0.005,0.01,0.02")
    ap.add_argument("--ai-weight-values", default="0.5,0.65,0.8")
    ap.add_argument("--out-dir", default="reports")
    ap.add_argument("--apply-start-date", default="")
    ap.add_argument("--apply-end-date", default="")
    ap.add_argument("--feature-set-version", default="")
    args = ap.parse_args()

    db = DuckDb.connect(Path(args.db_path))
    payload = run(
        db=db,
        start_date=args.start_date,
        end_date=args.end_date,
        model_version=args.model_version,
        density_values=[float(x) for x in args.density_values.split(",") if x.strip()],
        gap12_values=[float(x) for x in args.gap12_values.split(",") if x.strip()],
        ai_weight_values=[float(x) for x in args.ai_weight_values.split(",") if x.strip()],
        out_dir=Path(args.out_dir),
        apply_start_date=args.apply_start_date if str(args.apply_start_date).strip() else None,
        apply_end_date=args.apply_end_date if str(args.apply_end_date).strip() else None,
        feature_set_version=args.feature_set_version if str(args.feature_set_version).strip() else None,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
