from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def _source_row(
    source_name: str,
    exists: bool,
    row_count: int | None,
    race_count: int | None,
    finish_position_available: bool,
    available_dates: str,
    key_columns: str,
    can_backfill: bool,
    notes: str,
) -> dict[str, object]:
    return {
        "source_name": source_name,
        "exists": exists,
        "row_count": row_count,
        "race_count": race_count,
        "finish_position_available": finish_position_available,
        "available_dates": available_dates,
        "key_columns": key_columns,
        "can_backfill_finish_position": can_backfill,
        "notes": notes,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit candidate sources for results.finish_position backfill.")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.db_path))
    tables = set(con.execute("show tables").fetchdf()["name"].astype(str).tolist())

    rows: list[dict[str, object]] = []

    if "results" in tables:
        d = con.execute(
            """
            select
              count(*) as row_count,
              count(distinct rs.race_id) as race_count,
              min(r.race_date)::VARCHAR as min_date,
              max(r.race_date)::VARCHAR as max_date,
              sum(case when finish_position is not null then 1 else 0 end) as non_null_finish
            from results rs
            left join races r on r.race_id=rs.race_id
            """
        ).fetchdf().iloc[0]
        rows.append(
            _source_row(
                "db.results",
                True,
                int(d["row_count"]),
                int(d["race_count"]),
                int(d["non_null_finish"]) > 0,
                f"{d['min_date']}..{d['max_date']}",
                "race_id,horse_no,finish_position",
                True,
                "Primary source. Current quality issue suspected.",
            )
        )
    else:
        rows.append(_source_row("db.results", False, None, None, False, "", "", False, "Table missing"))

    if "feature_store" in tables:
        cols = con.execute("describe feature_store").fetchdf()["column_name"].astype(str).tolist()
        has_fp = "finish_position" in cols
        cnt = con.execute("select count(*) as c from feature_store").fetchdf().iloc[0]["c"]
        rows.append(
            _source_row(
                "db.feature_store",
                True,
                int(cnt),
                None,
                has_fp,
                "",
                "various",
                has_fp,
                "Only usable if finish_position exists in this table.",
            )
        )

    # raw CSV candidates
    raw_root = Path("racing_ai/data/raw")
    result_csvs = list(raw_root.rglob("results.csv")) if raw_root.exists() else []
    sample_dates: list[str] = []
    for p in result_csvs[:200]:
        parent = p.parent.name
        if len(parent) >= 8 and parent[:8].isdigit():
            sample_dates.append(f"{parent[:4]}-{parent[4:6]}-{parent[6:8]}")
    sample_dates = sorted(set(sample_dates))
    rows.append(
        _source_row(
            "raw.results.csv",
            len(result_csvs) > 0,
            len(result_csvs),
            None,
            True,
            f"{sample_dates[0]}..{sample_dates[-1]}" if sample_dates else "",
            "race_id,umaban,finish_pos",
            len(result_csvs) > 0,
            "Needs validation because some exports may be malformed placeholders.",
        )
    )

    # pair learning base
    plb = Path("racing_ai/data/modeling/pair_learning_base.parquet")
    if plb.exists():
        try:
            d = pd.read_parquet(plb, columns=["race_id", "actual_wide_hit"])
            rows.append(
                _source_row(
                    "modeling.pair_learning_base",
                    True,
                    int(len(d)),
                    int(d["race_id"].nunique()),
                    "actual_wide_hit" in d.columns,
                    "",
                    "race_id,pair labels",
                    False,
                    "Pair-level label only; cannot directly reconstruct finish_position.",
                )
            )
        except Exception as e:
            rows.append(_source_row("modeling.pair_learning_base", True, None, None, False, "", "", False, f"read error: {e}"))
    else:
        rows.append(_source_row("modeling.pair_learning_base", False, None, None, False, "", "", False, "File missing"))

    con.close()

    out = pd.DataFrame(rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    try:
        tbl = out.to_markdown(index=False)
    except Exception:
        tbl = out.to_string(index=False)
    args.out_md.write_text("# results_backfill_source_audit\n\n" + tbl, encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
