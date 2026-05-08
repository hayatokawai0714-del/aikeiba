from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description="Summarize quality gate coverage from joined pairs CSV.")
    ap.add_argument("--pairs-csv", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    ap.add_argument("--out-by-date-csv", type=Path, required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.pairs_csv)
    df["race_date"] = df.get("race_date", "").astype(str)

    status_col = "result_quality_status" if "result_quality_status" in df.columns else None
    if status_col is None:
        raise RuntimeError("missing required column: result_quality_status")

    total_race_count = int(df["race_id"].nunique()) if "race_id" in df.columns else 0
    ok_race_count = int(df.loc[df[status_col] == "ok", "race_id"].nunique()) if "race_id" in df.columns else 0
    ng_race_count = int(total_race_count - ok_race_count)

    raw_cov = None
    if "raw_actual_wide_hit" in df.columns:
        raw_cov = float(pd.to_numeric(df["raw_actual_wide_hit"], errors="coerce").notna().mean())
    elif "actual_wide_hit" in df.columns:
        raw_cov = float(pd.to_numeric(df["actual_wide_hit"], errors="coerce").notna().mean())

    qcov = None
    ok_df = df[df[status_col] == "ok"].copy()
    if len(ok_df) > 0 and "actual_wide_hit" in ok_df.columns:
        qcov = float(pd.to_numeric(ok_df["actual_wide_hit"], errors="coerce").notna().mean())

    payout_cov = None
    if len(ok_df) > 0 and "wide_payout" in ok_df.columns and "actual_wide_hit" in ok_df.columns:
        hit = pd.to_numeric(ok_df["actual_wide_hit"], errors="coerce") == 1
        if bool(hit.any()):
            payout_cov = float(pd.to_numeric(ok_df.loc[hit, "wide_payout"], errors="coerce").notna().mean())

    status_counts = df[status_col].value_counts(dropna=False).to_dict()

    # By-date race counts
    by_date = (
        df.groupby("race_date", dropna=False)
        .agg(
            race_count=("race_id", "nunique"),
            quality_ok_race_count=("race_id", lambda s: int(df.loc[s.index, status_col].eq("ok").groupby(df.loc[s.index, "race_id"]).any().sum())),
        )
        .reset_index()
    )
    by_date["quality_ng_race_count"] = by_date["race_count"] - by_date["quality_ok_race_count"]

    out = pd.DataFrame(
        [
            {
                "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
                "pairs_csv": str(args.pairs_csv),
                "total_race_count": total_race_count,
                "quality_ok_race_count": ok_race_count,
                "quality_ng_race_count": ng_race_count,
                "raw_actual_wide_hit_coverage": raw_cov,
                "quality_filtered_actual_wide_hit_coverage": qcov,
                "quality_ok_payout_coverage": payout_cov,
                "result_quality_status_counts_json": json.dumps(status_counts, ensure_ascii=False),
            }
        ]
    )

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_by_date_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    by_date.to_csv(args.out_by_date_csv, index=False, encoding="utf-8")

    md = [
        "# Quality Coverage Summary",
        "",
        f"- generated_at: {out.loc[0, 'generated_at']}",
        f"- input: {args.pairs_csv}",
        "",
        "## Overall",
        "",
        f"- total_race_count: {total_race_count}",
        f"- quality_ok_race_count: {ok_race_count}",
        f"- quality_ng_race_count: {ng_race_count}",
        f"- raw_actual_wide_hit_coverage: {raw_cov}",
        f"- quality_filtered_actual_wide_hit_coverage: {qcov}",
        f"- quality_ok_payout_coverage: {payout_cov}",
        "",
        "## result_quality_status counts (pair rows)",
        "",
        "```json",
        json.dumps(status_counts, ensure_ascii=False, indent=2),
        "```",
        "",
        "## Per-date",
        "",
        f"- by_date_csv: {args.out_by_date_csv}",
        "",
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")

    print(str(args.out_csv))
    print(str(args.out_md))
    print(str(args.out_by_date_csv))


if __name__ == "__main__":
    main()

