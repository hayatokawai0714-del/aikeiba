from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import duckdb
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit races table schema/values for venue/surface recovery.")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--race-date", required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.db_path), read_only=True)
    cols = con.execute("PRAGMA table_info('races')").fetchdf()
    df = con.execute("select * from races where race_date=cast(? as date) order by race_id", [args.race_date]).fetchdf()
    con.close()

    # Candidate columns
    venue_cols = [c for c in df.columns if c.lower() in ["venue", "jyo", "place", "track", "racecourse", "venue_code"]]
    surface_cols = [c for c in df.columns if c.lower() in ["surface", "track_type", "course_type", "turf_dirt", "surface_code"]]
    meta_cols = [c for c in df.columns if c.lower() in ["distance", "field_size_expected", "field_size", "race_no", "race_date", "race_id"]]

    out_rows = []
    for c in df.columns:
        out_rows.append(
            {
                "column": c,
                "dtype": str(df[c].dtype),
                "non_null_count": int(df[c].notna().sum()),
                "sample_values": ", ".join([str(x) for x in df[c].dropna().astype(str).unique().tolist()[:8]]),
            }
        )
    out = pd.DataFrame(out_rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    md = [
        "# races schema audit",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- db: {args.db_path}",
        f"- race_date: {args.race_date}",
        f"- race_row_count: {len(df)}",
        "",
        "## Candidate columns",
        "",
        f"- venue_candidates: {venue_cols}",
        f"- surface_candidates: {surface_cols}",
        f"- meta_candidates: {meta_cols}",
        "",
        "## Table columns (non-null + samples)",
        "",
        f"- out_csv: {args.out_csv}",
        "",
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

