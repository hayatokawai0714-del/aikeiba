from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description="Summarize result_quality_status reasons (counts + race counts), overall and by date.")
    ap.add_argument("--pairs-csv", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    ap.add_argument("--out-by-date-csv", type=Path, required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.pairs_csv)
    df["race_date"] = df.get("race_date", "").astype(str)
    if "result_quality_status" not in df.columns:
        df["result_quality_status"] = pd.NA

    # Overall row counts
    row_counts = df["result_quality_status"].value_counts(dropna=False).rename_axis("result_quality_status").reset_index(name="row_count")

    # Race counts: how many distinct races have at least one row with the status.
    race_counts = (
        df.groupby("result_quality_status", dropna=False)["race_id"]
        .nunique()
        .rename("race_count")
        .reset_index()
    )

    out = row_counts.merge(race_counts, on="result_quality_status", how="left")
    out["race_count"] = out["race_count"].fillna(0).astype(int)

    # Per-date ok/ng race counts
    by_date_rows = []
    for d, g in df.groupby("race_date"):
        all_r = set(g["race_id"].astype(str).unique())
        ok_r = set(g.loc[g["result_quality_status"] == "ok", "race_id"].astype(str).unique())
        by_date_rows.append(
            {
                "race_date": str(d),
                "race_count": int(len(all_r)),
                "quality_ok_race_count": int(len(ok_r)),
                "quality_ng_race_count": int(len(all_r) - len(ok_r)),
                "quality_ok_rate": (float(len(ok_r) / len(all_r)) if len(all_r) > 0 else None),
            }
        )
    by_date = pd.DataFrame(by_date_rows).sort_values("race_date")

    # Low ok-rate days (top 20)
    low_ok = by_date.sort_values(["quality_ok_rate", "race_date"], ascending=[True, True]).head(20).copy()

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_by_date_csv.parent.mkdir(parents=True, exist_ok=True)

    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    by_date.to_csv(args.out_by_date_csv, index=False, encoding="utf-8")

    md_lines = [
        "# Quality NG Reason Summary",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- input: {args.pairs_csv}",
        "",
        "## Outputs",
        "",
        f"- summary_csv: {args.out_csv}",
        f"- by_date_csv: {args.out_by_date_csv}",
        "",
        "## Top Reasons (by row_count)",
        "",
    ]
    if len(out) > 0:
        md_lines += ["| result_quality_status | row_count | race_count |", "|---|---:|---:|"]
        for _, r in out.head(12).iterrows():
            md_lines.append(f"| {r['result_quality_status']} | {int(r['row_count'])} | {int(r['race_count'])} |")
        md_lines.append("")

    md_lines += ["## Lowest Quality OK Rate Days (top 20)", ""]
    if len(low_ok) > 0:
        md_lines += ["| race_date | race_count | quality_ok_race_count | quality_ng_race_count | quality_ok_rate |", "|---|---:|---:|---:|---:|"]
        for _, r in low_ok.iterrows():
            rate = "" if pd.isna(r.get("quality_ok_rate")) else f"{float(r['quality_ok_rate']):.4f}"
            md_lines.append(
                f"| {r['race_date']} | {int(r['race_count'])} | {int(r['quality_ok_race_count'])} | {int(r['quality_ng_race_count'])} | {rate} |"
            )
        md_lines.append("")

    args.out_md.write_text("\n".join(md_lines), encoding="utf-8")

    print(str(args.out_csv))
    print(str(args.out_md))
    print(str(args.out_by_date_csv))


if __name__ == "__main__":
    main()

