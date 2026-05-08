from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit results.finish_position completeness by race.")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.db_path))
    races = con.execute(
        """
        select race_id::VARCHAR as race_id, race_date::VARCHAR as race_date
        from races
        where race_date::VARCHAR between ? and ?
        order by race_date, race_id
        """,
        [args.start_date, args.end_date],
    ).fetchdf()
    entries = con.execute(
        """
        select race_id::VARCHAR as race_id, horse_no, horse_id::VARCHAR as horse_id
        from entries
        where race_id in (select race_id from races where race_date::VARCHAR between ? and ?)
        """,
        [args.start_date, args.end_date],
    ).fetchdf()
    results = con.execute(
        """
        select race_id::VARCHAR as race_id, horse_no, finish_position
        from results
        where race_id in (select race_id from races where race_date::VARCHAR between ? and ?)
        """,
        [args.start_date, args.end_date],
    ).fetchdf()
    con.close()

    entries["horse_no"] = pd.to_numeric(entries["horse_no"], errors="coerce").astype("Int64")
    results["horse_no"] = pd.to_numeric(results["horse_no"], errors="coerce").astype("Int64")

    rows: list[dict[str, object]] = []
    for race in races.itertuples(index=False):
        rid = str(race.race_id)
        rdate = str(race.race_date)
        e = entries[entries["race_id"] == rid].copy()
        rs = results[results["race_id"] == rid].copy()

        expected = int(len(e))
        result_rows = int(len(rs))
        fnum = pd.to_numeric(rs["finish_position"], errors="coerce")
        non_null = int(fnum.notna().sum())
        null_count = int(result_rows - non_null)
        non_numeric_count = int(rs["finish_position"].notna().sum() - non_null)

        e_no = set(e["horse_no"].dropna().astype(int).tolist())
        r_no = set(rs["horse_no"].dropna().astype(int).tolist())
        missing_umaban = sorted(e_no - r_no)
        extra_umaban = sorted(r_no - e_no)

        horse_id_mismatch = None

        rows.append(
            {
                "race_date": rdate,
                "race_id": rid,
                "expected_runner_count": expected,
                "results_row_count": result_rows,
                "finish_position_non_null_count": non_null,
                "finish_position_null_count": null_count,
                "finish_position_coverage_rate": (non_null / expected) if expected else None,
                "missing_umaban_list": ",".join(f"{x:02d}" for x in missing_umaban),
                "extra_results_umaban_list": ",".join(f"{x:02d}" for x in extra_umaban),
                "non_numeric_finish_position_count": non_numeric_count,
                "sample_non_numeric_finish_position_values": ",".join(
                    rs[pd.to_numeric(rs["finish_position"], errors="coerce").isna()]["finish_position"]
                    .dropna()
                    .astype(str)
                    .head(5)
                    .tolist()
                ),
                "entries_not_in_results_count": len(missing_umaban),
                "results_not_in_entries_count": len(extra_umaban),
                "horse_id_mismatch_count": horse_id_mismatch,
            }
        )

    out = pd.DataFrame(rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    summary = pd.DataFrame(
        [
            {
                "race_count": int(len(out)),
                "mean_finish_position_coverage_rate": float(out["finish_position_coverage_rate"].mean()) if len(out) else None,
                "races_with_any_missing_finish_position": int((out["finish_position_null_count"] > 0).sum()),
            }
        ]
    )
    try:
        detail_md = out.head(80).to_markdown(index=False)
        summary_md = summary.to_markdown(index=False)
    except Exception:
        detail_md = out.head(80).to_string(index=False)
        summary_md = summary.to_string(index=False)

    args.out_md.write_text(
        "\n".join(
            [
                "# results_finish_position_completeness_audit",
                "",
                "## Summary",
                summary_md,
                "",
                "## Detail (top 80 races)",
                detail_md,
            ]
        ),
        encoding="utf-8",
    )
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
