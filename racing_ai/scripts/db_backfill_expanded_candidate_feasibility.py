from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import duckdb
import pandas as pd


def _count(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> int:
    return int(con.execute(sql, params or []).fetchone()[0])


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit feasibility of building expanded candidates from DB/artifacts.")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--race-date", required=True)
    ap.add_argument("--model-version", default="top3_stability_plus_pace_v3")
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/db_backfill_expanded_candidate_feasibility.md"))
    args = ap.parse_args()

    con = duckdb.connect(str(args.db_path))
    race_date = args.race_date
    model_version = args.model_version
    rows: list[str] = []
    rows.append("# DB Backfill Expanded Candidate Feasibility")
    rows.append("")
    rows.append(f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}")
    rows.append(f"- race_date: {race_date}")
    rows.append(f"- model_version: {model_version}")
    rows.append("")

    checks = [
        ("races", "SELECT COUNT(*) FROM races WHERE race_date = cast(? as DATE)", [race_date]),
        ("entries", "SELECT COUNT(*) FROM entries e JOIN races r ON r.race_id=e.race_id WHERE r.race_date = cast(? as DATE)", [race_date]),
        ("horse_predictions", "SELECT COUNT(*) FROM horse_predictions hp JOIN races r ON r.race_id=hp.race_id WHERE r.race_date = cast(? as DATE) AND hp.model_version = ?", [race_date, model_version]),
        ("odds(place/place_max)", "SELECT COUNT(*) FROM odds o JOIN races r ON r.race_id=o.race_id WHERE r.race_date = cast(? as DATE) AND lower(o.odds_type) IN ('place','place_max')", [race_date]),
        ("results", "SELECT COUNT(*) FROM results rs JOIN races r ON r.race_id=rs.race_id WHERE r.race_date = cast(? as DATE)", [race_date]),
        ("payouts(wide)", "SELECT COUNT(*) FROM payouts p JOIN races r ON r.race_id=p.race_id WHERE r.race_date = cast(? as DATE) AND lower(p.bet_type)='wide'", [race_date]),
    ]
    rows.append("## Table Coverage")
    rows.append("| source | row_count |")
    rows.append("|---|---:|")
    cov: dict[str, int] = {}
    for name, sql, params in checks:
        c = _count(con, sql, params)
        cov[name] = c
        rows.append(f"| {name} | {c} |")
    rows.append("")

    rows.append("## Feasibility")
    can_build_pool = cov["races"] > 0 and cov["entries"] > 0 and cov["horse_predictions"] > 0
    can_market_proxy = cov["odds(place/place_max)"] > 0
    can_eval_hit = cov["results"] > 0
    can_eval_roi = cov["payouts(wide)"] > 0
    rows.append(f"- expanded_pool_from_db: {'YES' if can_build_pool else 'NO'}")
    rows.append(f"- market_proxy_from_db: {'YES' if can_market_proxy else 'NO'}")
    rows.append(f"- actual_wide_hit_join: {'YES' if can_eval_hit else 'NO'}")
    rows.append(f"- wide_payout_join: {'YES' if can_eval_roi else 'NO'}")
    rows.append("")
    rows.append("## Missing / Risk")
    if not can_build_pool:
        rows.append("- Missing one or more of races/entries/horse_predictions for the date.")
    if not can_market_proxy:
        rows.append("- place/place_max odds missing; market proxy may fall back to NA.")
    if not can_eval_hit:
        rows.append("- results missing; actual_wide_hit will be NA.")
    if not can_eval_roi:
        rows.append("- wide payouts missing; ROI proxy will be NA.")
    if can_build_pool and can_market_proxy and can_eval_hit:
        rows.append("- Core backfill path is feasible for expanded shadow evaluation.")

    con.close()
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(rows), encoding="utf-8")
    print(str(args.out_md))


if __name__ == "__main__":
    main()

