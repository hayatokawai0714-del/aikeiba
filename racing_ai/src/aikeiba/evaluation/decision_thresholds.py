from __future__ import annotations

from typing import Any

from aikeiba.db.duckdb import DuckDb
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


def search_decision_thresholds(
    *,
    db: DuckDb,
    start_date: str,
    end_date: str,
    model_version: str,
    density_values: list[float],
    gap12_values: list[float],
    ai_weight_values: list[float] | None = None,
) -> dict[str, Any]:
    if ai_weight_values is None or len(ai_weight_values) == 0:
        ai_weight_values = [0.65]
    entries = db.query_df(
        """
        SELECT r.race_id, e.horse_no
        FROM entries e
        JOIN races r ON r.race_id=e.race_id
        WHERE r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
          AND (e.is_scratched IS NULL OR e.is_scratched=FALSE)
        """,
        (start_date, end_date),
    ).to_dict("records")
    by_race_horse: dict[str, list[int]] = {}
    for e in entries:
        by_race_horse.setdefault(str(e["race_id"]), []).append(int(e["horse_no"]))

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
    ).to_dict("records")
    pred_map = {(str(p["race_id"]), int(p["horse_no"])): p.get("p_top3") for p in preds}
    market_map = _latest_market_top3_probs_for_range(db=db, start_date=start_date, end_date=end_date)

    payouts = db.query_df(
        """
        SELECT p.race_id, p.bet_key, p.payout
        FROM payouts p
        JOIN races r ON r.race_id=p.race_id
        WHERE r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
          AND lower(p.bet_type)='wide'
        """,
        (start_date, end_date),
    ).to_dict("records")
    payout_map: dict[tuple[str, str], float] = {}
    for row in payouts:
        key = _normalize_pair_key(row.get("bet_key"))
        if key is None:
            continue
        if row.get("payout") is None:
            continue
        payout_map[(str(row["race_id"]), key)] = float(row["payout"])

    race_ids = sorted(by_race_horse.keys())
    grid_results: list[dict[str, Any]] = []
    best: dict[str, Any] | None = None
    for ai_weight in ai_weight_values:
        for dmax in density_values:
            for gmin in gap12_values:
                total_bets = 0
                total_return = 0.0
                buy_races = 0
                for rid in race_ids:
                    horse_nos = by_race_horse.get(rid, [])
                    p_top3: dict[int, float] = {}
                    for hn in horse_nos:
                        p_ai = pred_map.get((rid, hn))
                        if p_ai is None:
                            continue
                        p_mkt = market_map.get((rid, hn))
                        p_fused = blend_ai_market_prob(
                            p_ai=float(p_ai),
                            p_market=p_mkt,
                            ai_weight=float(ai_weight),
                        )
                        if p_fused is not None:
                            p_top3[hn] = float(p_fused)
                    decision = decide_buy_or_skip(
                        p_top3=list(p_top3.values()),
                        density_top3_max=float(dmax),
                        gap12_min=float(gmin),
                    )
                    if not decision.buy_flag:
                        continue
                    buy_races += 1
                    cands = generate_wide_candidates_rule_based(
                        race_id=rid,
                        horse_nos=horse_nos,
                        p_top3=p_top3,
                        axis_k=1,
                        partner_k=min(6, len(horse_nos)),
                    )
                    for c in cands:
                        total_bets += 1
                        pair_key = _normalize_pair_key(c.pair)
                        if pair_key is None:
                            continue
                        total_return += float(payout_map.get((rid, pair_key), 0.0))
                total_bet_yen = float(total_bets * 100)
                roi = (total_return / total_bet_yen) if total_bet_yen > 0 else None
                row = {
                    "ai_weight": float(ai_weight),
                    "density_top3_max": float(dmax),
                    "gap12_min": float(gmin),
                    "buy_races": int(buy_races),
                    "total_bets": int(total_bets),
                    "total_return_yen": float(total_return),
                    "total_bet_yen": total_bet_yen,
                    "roi": roi,
                }
                grid_results.append(row)
                score = (roi if roi is not None else -1.0, buy_races, -abs(float(dmax) - 1.35), -abs(float(ai_weight) - 0.65))
                if best is None:
                    best = row
                    best["_score"] = score
                else:
                    if score > best["_score"]:
                        best = row
                        best["_score"] = score
    if best is None:
        best = {"ai_weight": None, "density_top3_max": None, "gap12_min": None, "roi": None, "buy_races": 0, "total_bets": 0}
    best = {k: v for k, v in best.items() if k != "_score"}
    return {
        "start_date": start_date,
        "end_date": end_date,
        "model_version": model_version,
        "grid_results": grid_results,
        "best": best,
    }


def _latest_market_top3_probs_for_range(
    *,
    db: DuckDb,
    start_date: str,
    end_date: str,
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
          WHERE r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
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
        (start_date, end_date),
    ).to_dict("records")

    by_race: dict[str, list[tuple[int, float]]] = {}
    for r in rows:
        rid = str(r["race_id"])
        hn = int(r["horse_no"])
        p = compute_market_top3_prob_from_place_odds(r.get("place_min"), r.get("place_max"))
        if p is None:
            continue
        by_race.setdefault(rid, []).append((hn, float(p)))

    out: dict[tuple[str, int], float] = {}
    for rid, vals in by_race.items():
        s = float(sum(v for _, v in vals))
        if s <= 0:
            continue
        scale = 3.0 / s
        for hn, v in vals:
            out[(rid, hn)] = min(1.0, max(0.0, v * scale))
    return out
