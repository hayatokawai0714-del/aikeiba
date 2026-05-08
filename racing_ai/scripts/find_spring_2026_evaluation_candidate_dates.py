from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description="Find 2026 spring evaluation candidate dates based on DB + external coverage.")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--model-version", default="top3_stability_plus_pace_v3")
    ap.add_argument("--external-results-csv", type=Path, required=True)
    ap.add_argument("--external-wide-payouts-csv", type=Path, required=True)
    ap.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    import duckdb

    con = duckdb.connect(str(args.db_path), read_only=True)

    dates_df = con.execute(
        "select distinct cast(race_date as varchar) as race_date from races where race_date between cast(? as date) and cast(? as date) order by race_date",
        [args.start_date, args.end_date],
    ).fetchdf()
    dates = dates_df["race_date"].astype(str).tolist()

    # DB counts
    races_cnt = con.execute(
        "select cast(race_date as varchar) as race_date, count(*) as races_count from races where race_date between cast(? as date) and cast(? as date) group by 1",
        [args.start_date, args.end_date],
    ).fetchdf()
    entries_cnt = con.execute(
        """
        select cast(r.race_date as varchar) as race_date, count(*) as entries_count
        from entries e join races r on r.race_id=e.race_id
        where r.race_date between cast(? as date) and cast(? as date)
        group by 1
        """,
        [args.start_date, args.end_date],
    ).fetchdf()
    preds_cnt = con.execute(
        """
        select cast(r.race_date as varchar) as race_date, count(*) as predictions_count
        from horse_predictions hp join races r on r.race_id=hp.race_id
        where r.race_date between cast(? as date) and cast(? as date)
          and hp.model_version = ?
        group by 1
        """,
        [args.start_date, args.end_date, args.model_version],
    ).fetchdf()

    # External coverage
    ext_res = pd.read_csv(args.external_results_csv)
    ext_pay = pd.read_csv(args.external_wide_payouts_csv)
    for df in (ext_res, ext_pay):
        df["race_date"] = df.get("race_date", "").astype(str)

    ext_res_cnt = ext_res[(ext_res["race_date"] >= args.start_date) & (ext_res["race_date"] <= args.end_date)].groupby("race_date").size().rename("external_results_count").reset_index()
    ext_pay_cnt = ext_pay[(ext_pay["race_date"] >= args.start_date) & (ext_pay["race_date"] <= args.end_date)].groupby("race_date").size().rename("external_payout_wide_count").reset_index()

    # Merge
    out = pd.DataFrame({"race_date": dates})
    for t in (races_cnt, entries_cnt, preds_cnt, ext_res_cnt, ext_pay_cnt):
        out = out.merge(t, on="race_date", how="left")
    for c in ["races_count", "entries_count", "predictions_count", "external_results_count", "external_payout_wide_count"]:
        if c in out.columns:
            out[c] = out[c].fillna(0).astype(int)

    out["can_build_expanded"] = (out["entries_count"] > 0) & (out["predictions_count"] > 0)
    out["can_external_join"] = (out["external_results_count"] > 0) & (out["external_payout_wide_count"] > 0)
    out["recommended_include"] = (out["races_count"] > 0) & out["can_build_expanded"] & out["can_external_join"]

    def _missing_reason(r) -> str:
        reasons = []
        if int(r["races_count"]) == 0:
            reasons.append("no_races_in_db")
        if int(r["entries_count"]) == 0:
            reasons.append("missing_entries")
        if int(r["predictions_count"]) == 0:
            reasons.append("missing_predictions")
        if int(r["external_results_count"]) == 0:
            reasons.append("missing_external_results")
        if int(r["external_payout_wide_count"]) == 0:
            reasons.append("missing_external_wide_payouts")
        return ",".join(reasons) if reasons else ""

    out["missing_reason"] = out.apply(_missing_reason, axis=1)

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    # Markdown summary
    rec = out[out["recommended_include"] == True].copy()  # noqa: E712
    lines = [
        f"# Spring 2026 Evaluation Candidate Dates ({args.start_date}..{args.end_date})",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- db_path: {args.db_path}",
        f"- model_version(predictions): {args.model_version}",
        f"- external_results_csv: {args.external_results_csv}",
        f"- external_wide_payouts_csv: {args.external_wide_payouts_csv}",
        "",
        "## Output",
        "",
        f"- csv: {args.out_csv}",
        "",
        "## Summary",
        "",
        f"- date_count: {len(out)}",
        f"- recommended_include_count: {len(rec)}",
        "",
    ]
    if len(rec) > 0:
        lines += ["## Recommended Dates", "", "| race_date | races | entries | predictions | ext_results | ext_wide_payouts |", "|---|---:|---:|---:|---:|---:|"]
        for _, r in rec.iterrows():
            lines.append(
                f"| {r['race_date']} | {int(r['races_count'])} | {int(r['entries_count'])} | {int(r['predictions_count'])} | {int(r['external_results_count'])} | {int(r['external_payout_wide_count'])} |"
            )
        lines.append("")

    args.out_md.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

