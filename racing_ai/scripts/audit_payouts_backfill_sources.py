from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def _row(
    source_name: str,
    exists: bool,
    row_count: int | None,
    wide_row_count: int | None,
    available_dates: str,
    key_columns: str,
    payout_amount_column: str,
    can_backfill: bool,
    notes: str,
) -> dict[str, object]:
    return {
        "source_name": source_name,
        "exists": exists,
        "row_count": row_count,
        "wide_row_count": wide_row_count,
        "available_dates": available_dates,
        "key_columns": key_columns,
        "payout_amount_column": payout_amount_column,
        "can_backfill_wide_payout": can_backfill,
        "notes": notes,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit candidate sources for wide payout backfill.")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.db_path))
    tables = set(con.execute("show tables").fetchdf()["name"].astype(str).tolist())
    rows: list[dict[str, object]] = []

    if "payouts" in tables:
        d = con.execute(
            """
            select
              count(*) as row_count,
              sum(case when upper(cast(bet_type as varchar))='WIDE' or cast(bet_type as varchar) like '%ワイド%' then 1 else 0 end) as wide_row_count,
              min(r.race_date)::VARCHAR as min_date,
              max(r.race_date)::VARCHAR as max_date
            from payouts p
            left join races r on r.race_id=p.race_id
            """
        ).fetchdf().iloc[0]
        rows.append(
            _row(
                "db.payouts",
                True,
                int(d["row_count"]),
                int(d["wide_row_count"] or 0),
                f"{d['min_date']}..{d['max_date']}",
                "race_id,bet_type,bet_key",
                "payout",
                True,
                "Primary source. Contains mixed meta/system rows too.",
            )
        )
    else:
        rows.append(_row("db.payouts", False, None, None, "", "", "", False, "Table missing"))

    raw_root = Path("racing_ai/data/raw")
    payout_csvs = list(raw_root.rglob("payouts.csv")) if raw_root.exists() else []
    sample_dates: list[str] = []
    for p in payout_csvs[:500]:
        parent = p.parent.name
        if len(parent) >= 8 and parent[:8].isdigit():
            sample_dates.append(f"{parent[:4]}-{parent[4:6]}-{parent[6:8]}")
    sample_dates = sorted(set(sample_dates))
    rows.append(
        _row(
            "raw.payouts.csv",
            len(payout_csvs) > 0,
            len(payout_csvs),
            None,
            f"{sample_dates[0]}..{sample_dates[-1]}" if sample_dates else "",
            "race_id,bet_type,winning_combination",
            "payout_yen",
            len(payout_csvs) > 0,
            "Needs per-file validation (some exports include meta rows).",
        )
    )

    # external import candidates
    ext = Path("racing_ai/data/external")
    ext_csvs = list(ext.rglob("*.csv")) if ext.exists() else []
    maybe_wide = [p for p in ext_csvs if "wide" in p.name.lower() or "payout" in p.name.lower()]
    rows.append(
        _row(
            "external.csv(wide/payout*)",
            len(maybe_wide) > 0,
            len(maybe_wide),
            None,
            "",
            "varies",
            "varies",
            len(maybe_wide) > 0,
            "Potential backfill source; inspect schema before loading.",
        )
    )

    con.close()

    out = pd.DataFrame(rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    try:
        tbl = out.to_markdown(index=False)
    except Exception:
        tbl = out.to_string(index=False)
    args.out_md.write_text("# payouts_backfill_source_audit\n\n" + tbl, encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

