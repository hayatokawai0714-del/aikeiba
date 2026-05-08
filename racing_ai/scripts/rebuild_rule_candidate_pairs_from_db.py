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
        # Normalize to sum≈3 per race to match our historical "market_top3_proxy" scale.
        scale = 3.0 / s
        for hn, v in vals:
            out[(rid, hn)] = max(0.0, min(1.0, v * scale))
    return out


def _latest_ai_p_top3_for_date(db: DuckDb, race_date: str, model_version: str) -> dict[tuple[str, int], float]:
    rows = db.query_df(
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
        (model_version, race_date, model_version),
    ).to_dict("records")
    out: dict[tuple[str, int], float] = {}
    for r in rows:
        rid = str(r.get("race_id"))
        hn = int(r.get("horse_no"))
        p = r.get("p_top3")
        if p is None:
            continue
        out[(rid, hn)] = float(p)
    return out


def build_rule_candidate_pairs(
    *,
    db: DuckDb,
    race_date: str,
    model_version: str,
    ai_weight: float,
    top_n_selected: int,
) -> pd.DataFrame:
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
    if not entries:
        return pd.DataFrame()

    market = _latest_market_top3_probs_for_date(db=db, race_date=race_date)
    ai = _latest_ai_p_top3_for_date(db=db, race_date=race_date, model_version=model_version)

    by_race: dict[str, list[int]] = {}
    by_race_hid: dict[str, dict[int, str | None]] = {}
    for e in entries:
        rid = str(e["race_id"])
        hn = int(e["horse_no"])
        by_race.setdefault(rid, []).append(hn)
        by_race_hid.setdefault(rid, {})[hn] = e.get("horse_id")

    rows: list[dict[str, Any]] = []
    for rid, hns in by_race.items():
        # Precompute fused p_top3 + gaps per horse.
        fused: dict[int, float] = {}
        gaps: dict[int, float | None] = {}
        for hn in hns:
            p_ai = ai.get((rid, hn))
            p_mkt = market.get((rid, hn))
            if p_ai is None:
                continue
            p_fused = blend_ai_market_prob(p_ai=float(p_ai), p_market=(None if p_mkt is None else float(p_mkt)), ai_weight=ai_weight)
            if p_fused is None:
                continue
            fused[hn] = float(p_fused)
            gaps[hn] = (None if p_mkt is None else float(p_fused) - float(p_mkt))

        if len(fused) < 2:
            continue

        pair_rows: list[dict[str, Any]] = []
        for a, b in combinations(sorted(fused.keys()), 2):
            p1 = fused.get(a)
            p2 = fused.get(b)
            g1 = gaps.get(a)
            g2 = gaps.get(b)
            pair_prob_naive, pair_value_score, pair_missing_flag = simple_pair_value_score(p1=p1, p2=p2, gap1=g1, gap2=g2)
            key = f"{int(a):02d}-{int(b):02d}"
            pair_rows.append(
                {
                    "race_id": rid,
                    "pair": key,
                    "pair_norm": key,
                    "horse1_umaban": int(a),
                    "horse2_umaban": int(b),
                    "horse1_horse_id": by_race_hid.get(rid, {}).get(int(a)),
                    "horse2_horse_id": by_race_hid.get(rid, {}).get(int(b)),
                    "horse1_p_top3_fused": (None if p1 is None else float(p1)),
                    "horse2_p_top3_fused": (None if p2 is None else float(p2)),
                    "horse1_market_top3_proxy": (None if market.get((rid, int(a))) is None else float(market[(rid, int(a))])),
                    "horse2_market_top3_proxy": (None if market.get((rid, int(b))) is None else float(market[(rid, int(b))])),
                    "horse1_ai_market_gap": (None if g1 is None else float(g1)),
                    "horse2_ai_market_gap": (None if g2 is None else float(g2)),
                    "pair_prob_naive": (None if pair_prob_naive is None else float(pair_prob_naive)),
                    "pair_value_score_version": "v1_simple_gap_bonus",
                    "pair_value_score": (None if pair_value_score is None else float(pair_value_score)),
                    "pair_missing_flag": bool(pair_missing_flag),
                }
            )

        pair_rows.sort(key=lambda x: float("-inf") if x.get("pair_value_score") is None else float(x["pair_value_score"]), reverse=True)
        for idx, pr in enumerate(pair_rows, start=1):
            pr["pair_rank_in_race"] = int(idx)
            pr["pair_selected_flag"] = bool(idx <= max(1, int(top_n_selected)))
            pr["pair_selection_reason"] = "SELECT_TOP_PAIR_SCORE" if pr["pair_selected_flag"] else "NOT_SELECTED_LOW_PAIR_SCORE"

        # Keep only selected rows, matching current production `candidate_pairs.parquet` semantics.
        rows.extend([r for r in pair_rows if bool(r.get("pair_selected_flag"))])

    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Rebuild rule-selected candidate_pairs.parquet from DuckDB only (evaluation support).")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--race-date", required=True)
    ap.add_argument("--model-version", default="top3_stability_plus_pace_v3")
    ap.add_argument("--ai-weight", type=float, default=0.65)
    ap.add_argument("--pair-top-n-selected", type=int, default=5)
    ap.add_argument("--out-parquet", type=Path, required=True)
    ap.add_argument("--out-audit-md", type=Path, required=True)
    args = ap.parse_args()

    db = DuckDb.connect(args.db_path)
    df = build_rule_candidate_pairs(
        db=db,
        race_date=args.race_date,
        model_version=args.model_version,
        ai_weight=float(args.ai_weight),
        top_n_selected=int(args.pair_top_n_selected),
    )

    args.out_parquet.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.out_parquet, index=False)

    audit = {
        "race_date": args.race_date,
        "model_version": args.model_version,
        "ai_weight": float(args.ai_weight),
        "pair_top_n_selected": int(args.pair_top_n_selected),
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "rows": int(len(df)),
        "race_count": int(df["race_id"].nunique()) if len(df) > 0 and "race_id" in df.columns else 0,
        "note": "This is evaluation support output (DB-only rebuild). It should not be treated as production run output.",
    }
    args.out_audit_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_audit_md.write_text(
        "\n".join(
            [
                "# Rebuilt Rule Candidate Pairs (DB-only)\n",
                "```json",
                json.dumps(audit, ensure_ascii=False, indent=2),
                "```",
                "",
            ]
        ),
        encoding="utf-8",
    )

    print(str(args.out_parquet))
    print(str(args.out_audit_md))


if __name__ == "__main__":
    main()

