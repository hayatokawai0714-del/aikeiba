from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description="Break down result_quality_status for a single race_date from joined pair CSV.")
    ap.add_argument("--pairs-csv", type=Path, required=True)
    ap.add_argument("--race-date", required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.pairs_csv)
    if "race_date" not in df.columns:
        df["race_date"] = ""
    df["race_date"] = df["race_date"].astype(str)
    sub = df[df["race_date"] == str(args.race_date)].copy()

    def _count_true(frame: pd.DataFrame, col: str) -> int:
        if col not in frame.columns:
            return 0
        s = frame[col]
        if s.dtype == bool:
            return int(s.sum())
        return int((s == True).sum())  # noqa: E712

    total_pair_rows = int(len(sub))
    race_count = int(sub["race_id"].nunique()) if len(sub) > 0 and "race_id" in sub.columns else 0

    # Race-level ok count (requires at least one ok row for race).
    ok_race_count = 0
    if len(sub) > 0 and "result_quality_status" in sub.columns and "race_id" in sub.columns:
        ok_race_count = int(sub[sub["result_quality_status"] == "ok"]["race_id"].nunique())

    breakdown = (
        sub["result_quality_status"].value_counts(dropna=False).rename_axis("result_quality_status").reset_index(name="row_count")
        if "result_quality_status" in sub.columns
        else pd.DataFrame(columns=["result_quality_status", "row_count"])
    )
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    breakdown.to_csv(args.out_csv, index=False, encoding="utf-8")

    payload = {
        "pairs_csv": str(args.pairs_csv),
        "race_date": str(args.race_date),
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "total_pair_rows": total_pair_rows,
        "race_count": race_count,
        "quality_ok_race_count": ok_race_count,
        "actual_wide_hit_non_null": int(sub["actual_wide_hit"].notna().sum()) if "actual_wide_hit" in sub.columns else 0,
        "wide_payout_non_null": int(sub["wide_payout"].notna().sum()) if "wide_payout" in sub.columns else 0,
        "rule_selected_count": _count_true(sub, "pair_selected_flag"),
        "model_dynamic_non_overlap_count": _count_true(sub, "model_dynamic_non_overlap_flag") or _count_true(sub, "model_dynamic_non_overlap"),
        "missing_reason_hint": "If quality_ok_race_count==0 but races_count>0, check results/payout coverage and key mismatches; also confirm race_date column correctness.",
    }

    lines = [
        f"# Quality Gate Breakdown ({args.race_date})",
        "",
        "## Summary",
        "",
        "```json",
        json.dumps(payload, ensure_ascii=False, indent=2),
        "```",
        "",
        "## Breakdown (result_quality_status)",
        "",
        f"- csv: {args.out_csv}",
        "",
    ]
    if len(breakdown) > 0:
        lines += ["| result_quality_status | row_count |", "|---|---:|"]
        for _, r in breakdown.iterrows():
            lines.append(f"| {r['result_quality_status']} | {int(r['row_count'])} |")
        lines.append("")
    args.out_md.write_text("\n".join(lines), encoding="utf-8")

    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

