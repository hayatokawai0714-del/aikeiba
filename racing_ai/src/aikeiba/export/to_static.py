from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from aikeiba.db.duckdb import DuckDb
from aikeiba.checks.data_quality import run_doctor
from aikeiba.decision.skip_rules import decide_buy_or_skip
from aikeiba.decision.wide_rules import generate_wide_candidates_rule_based


def _write_json(path: Path, obj: Any) -> None:
    def sanitize(value: Any) -> Any:
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
            return value
        if isinstance(value, dict):
            return {k: sanitize(v) for k, v in value.items()}
        if isinstance(value, list):
            return [sanitize(v) for v in value]
        return value

    safe_obj = sanitize(obj)
    path.write_text(json.dumps(safe_obj, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


def export_for_dashboard(
    *,
    db: DuckDb,
    race_date: str,
    out_dir: Path,
    feature_snapshot_version: str,
    model_version: str,
    odds_snapshot_version: str,
    allow_no_wide_odds: bool,
) -> dict[str, object]:
    """
    MVP export:
    - races_today.json (from races)
    - horse_predictions.json (placeholder fields; to be filled after inference pipeline exists)
    - today_pipeline_bets.json (generated from rule-based wide candidates, no EV)
    - race_summary.json (skip reasons placeholder)
    """
    races = db.query_df("SELECT * FROM races WHERE race_date = ? ORDER BY venue, race_no", (race_date,)).to_dict("records")
    if len(races) == 0:
        raise ValueError(f"no races for {race_date}")

    doctor = run_doctor(db, race_date=race_date)

    entries = db.query_df(
        """
        SELECT r.race_id, r.venue, r.race_no, e.waku, e.horse_no, e.horse_name, e.horse_id
        FROM entries e
        JOIN races r ON r.race_id=e.race_id
        WHERE r.race_date = ?
        ORDER BY r.venue, r.race_no, e.horse_no
        """,
        (race_date,),
    ).to_dict("records")

    # Load latest predictions for the date/model_version.
    preds = db.query_df(
        """
        WITH latest AS (
          SELECT
            race_id, horse_no, model_version,
            max(inference_timestamp) AS ts
          FROM horse_predictions hp
          WHERE hp.model_version = ?
          GROUP BY race_id, horse_no, model_version
        )
        SELECT hp.*
        FROM horse_predictions hp
        JOIN latest l
          ON l.race_id=hp.race_id
         AND l.horse_no=hp.horse_no
         AND l.model_version=hp.model_version
         AND l.ts=hp.inference_timestamp
        JOIN races r ON r.race_id = hp.race_id
        WHERE r.race_date = cast(? as DATE)
        """,
        (model_version, race_date),
    ).to_dict("records")
    pred_map: dict[tuple[str, int], dict[str, Any]] = {(p["race_id"], int(p["horse_no"])): p for p in preds}

    # Build per-horse export (ties entries + predictions).
    horse_predictions = []
    for e in entries:
        p = pred_map.get((e["race_id"], int(e["horse_no"])), {})
        horse_predictions.append(
            {
                "race_id": e["race_id"],
                "waku": e["waku"],
                "horse_no": int(e["horse_no"]),
                "horse_name": e["horse_name"],
                "pop_rank": None,
                "ai_rank": p.get("ai_rank"),
                "win_rate": None,
                "top3_rate": p.get("p_top3"),
                "ability": p.get("ability"),
                "stability": p.get("stability"),
                "value_score": None,
                "course_waku_final_multi": None,
                "role": p.get("role"),
                "market_gap": None,
            }
        )

    # Race-level decisions + wide candidates
    today_pipeline_bets: list[dict[str, Any]] = []
    race_summary: list[dict[str, Any]] = []
    races_today_export: list[dict[str, Any]] = []

    # Group entries by race for generation.
    by_race: dict[str, list[dict[str, Any]]] = {}
    for e in entries:
        by_race.setdefault(e["race_id"], []).append(e)

    for r in races:
        rid = r["race_id"]
        horses = by_race.get(rid, [])
        horse_nos = [int(x["horse_no"]) for x in horses]
        p_top3_dict = {hn: float(pred_map.get((rid, hn), {}).get("p_top3")) for hn in horse_nos if pred_map.get((rid, hn), {}).get("p_top3") is not None}

        if doctor.get("should_stop"):
            decision = decide_buy_or_skip(p_top3=[])
            buy_flag = False
            reason = f"doctor_stop: {','.join(doctor.get('stop_reasons', []))}"
        elif len(p_top3_dict) == 0:
            decision = decide_buy_or_skip(p_top3=[])
            buy_flag = False
            reason = "no_predictions"
        else:
            decision = decide_buy_or_skip(p_top3=list(p_top3_dict.values()))
            buy_flag = decision.buy_flag
            reason = decision.reason

        if not buy_flag:
            race_summary.append(
                {
                    "race_id": rid,
                    "reason": reason,
                    "popular_concentration": None,
                    "small_ai_market_gap": None,
                    "data_shortage": None,
                    "density_top3_excess": "density_top3_excess" in reason,
                    "gap12_shortage": "gap12_shortage" in reason,
                    "odds_unstable": None,
                    "horse_id_missing": None,
                }
            )

        candidates = []
        if buy_flag and len(p_top3_dict) > 0:
            candidates = generate_wide_candidates_rule_based(
                race_id=rid,
                horse_nos=horse_nos,
                p_top3=p_top3_dict,
                axis_k=1,
                partner_k=min(6, len(horse_nos)),
            )

        # Export race card fields expected by the static site.
        field_size = len(horses)
        races_today_export.append(
            {
                "race_id": rid,
                "venue": r["venue"],
                "race_no": r["race_no"],
                "post_time": r.get("post_time"),
                "condition": f"{r.get('surface') or ''}{r.get('distance') or ''}m {r.get('race_class') or ''}".strip(),
                "field_size": field_size,
                "buy_flag": buy_flag,
                "recommendation": 80 if buy_flag else 40,
                "candidate_pairs": len(candidates),
                "expected_roi": None,
                "ai_market_gap": None,
                "density_top3": decision.density_top3,
                "gap12": decision.gap12,
                "chaos_index": None,
                "track": r.get("track_condition"),
                "surface": r.get("surface"),
            }
        )

        for c in candidates:
            # MVP: no EV, no wide odds required.
            pair_score = None
            if c.axis_horse_no in p_top3_dict and c.partner_horse_no in p_top3_dict:
                pair_score = int(round((p_top3_dict[c.axis_horse_no] + p_top3_dict[c.partner_horse_no]) * 100))
            today_pipeline_bets.append(
                {
                    "race_id": rid,
                    "race_no": r["race_no"],
                    "venue": r["venue"],
                    "pair": c.pair,
                    "axis_horse": c.axis_horse_no,
                    "partner_horse": c.partner_horse_no,
                    "pair_score": pair_score,
                    "expected_value": None,
                    "top3_rate_pair": None,
                    "value_label": None,
                    "wide_odds_est": None,
                    "recommendation": None,
                    "selected_stage": c.selected_stage,
                }
            )

    out_dir.mkdir(parents=True, exist_ok=True)
    _write_json(out_dir / "races_today.json", races_today_export)
    _write_json(out_dir / "horse_predictions.json", horse_predictions)
    _write_json(out_dir / "today_pipeline_bets.json", today_pipeline_bets)
    _write_json(out_dir / "race_summary.json", race_summary)

    return {
        "race_date": race_date,
        "out_dir": str(out_dir),
        "feature_snapshot_version": feature_snapshot_version,
        "model_version": model_version,
        "odds_snapshot_version": odds_snapshot_version,
        "allow_no_wide_odds": allow_no_wide_odds,
        "doctor": doctor,
        "files": [
            "races_today.json",
            "horse_predictions.json",
            "today_pipeline_bets.json",
            "race_summary.json",
        ],
    }
