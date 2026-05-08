from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import duckdb
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit market proxy / ai_market_gap sources for a given date.")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--race-date", required=True)
    ap.add_argument("--model-version", default="top3_stability_plus_pace_v3")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.db_path), read_only=True)
    # predictions availability
    preds = con.execute(
        """
        select count(*) as n
        from horse_predictions hp
        join races r on r.race_id=hp.race_id
        where r.race_date=cast(? as date)
          and hp.model_version = ?
        """,
        [args.race_date, args.model_version],
    ).fetchdf()["n"].iloc[0]

    # odds availability (place / place_max)
    odds = con.execute(
        """
        select
          lower(odds_type) as odds_type,
          count(*) as n
        from odds o
        join races r on r.race_id=o.race_id
        where r.race_date=cast(? as date)
        group by 1
        order by 2 desc
        """,
        [args.race_date],
    ).fetchdf()

    # odds place coverage
    place = con.execute(
        """
        select
          count(*) as place_rows
        from odds o
        join races r on r.race_id=o.race_id
        where r.race_date=cast(? as date)
          and lower(o.odds_type) in ('place','place_max')
        """,
        [args.race_date],
    ).fetchdf()["place_rows"].iloc[0]

    con.close()

    out = pd.DataFrame(
        [
            {
                "race_date": args.race_date,
                "horse_predictions_rows": int(preds),
                "odds_rows_total_by_type_json": odds.to_json(orient="records", force_ascii=False),
                "odds_place_rows": int(place),
            }
        ]
    )
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    md = [
        "# Market gap source audit",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- race_date: {args.race_date}",
        f"- model_version: {args.model_version}",
        "",
        "## Summary",
        "",
        f"- horse_predictions_rows: {int(preds)}",
        f"- odds_place_rows (place/place_max): {int(place)}",
        "",
        "## odds_type breakdown (JSON)",
        "",
        "```json",
        odds.to_json(orient='records', force_ascii=False, indent=2),
        "```",
        "",
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

