from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit why a specific race_date is missing from daily stability outputs.")
    ap.add_argument("--pairs-csv", type=Path, required=True, help="Joined pair-level CSV (after results/payout join).")
    ap.add_argument("--race-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.pairs_csv)
    # Normalize race_date string just in case.
    if "race_date" in df.columns:
        df["race_date"] = df["race_date"].astype(str)
    else:
        # If missing, we cannot slice properly; keep empty subset.
        df["race_date"] = ""

    sub = df[df["race_date"] == str(args.race_date)].copy()

    def _count_true(col: str) -> int:
        if col not in sub.columns:
            return 0
        s = sub[col]
        if s.dtype == bool:
            return int(s.sum())
        return int((s == True).sum())  # noqa: E712

    audit = {
        "pairs_csv": str(args.pairs_csv),
        "race_date": str(args.race_date),
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "total_rows_in_pairs_csv": int(len(df)),
        "total_race_dates_in_pairs_csv": int(df["race_date"].nunique()) if "race_date" in df.columns else None,
        "rows_for_race_date": int(len(sub)),
        "race_count_for_race_date": int(sub["race_id"].nunique()) if len(sub) > 0 and "race_id" in sub.columns else 0,
        "columns_present": sorted(list(sub.columns)),
        "result_source_used_counts": (sub["result_source_used"].value_counts(dropna=False).to_dict() if "result_source_used" in sub.columns else {}),
        "result_quality_status_counts": (sub["result_quality_status"].value_counts(dropna=False).to_dict() if "result_quality_status" in sub.columns else {}),
        "actual_wide_hit_non_null": int(sub["actual_wide_hit"].notna().sum()) if "actual_wide_hit" in sub.columns else 0,
        "raw_actual_wide_hit_non_null": int(sub["raw_actual_wide_hit"].notna().sum()) if "raw_actual_wide_hit" in sub.columns else 0,
        "wide_payout_non_null": int(sub["wide_payout"].notna().sum()) if "wide_payout" in sub.columns else 0,
        "rule_selected_row_count": _count_true("pair_selected_flag"),
        "model_dynamic_non_overlap_row_count": _count_true("model_dynamic_non_overlap_flag") or _count_true("model_dynamic_non_overlap"),
        "model_dynamic_selected_row_count": _count_true("model_dynamic_selected_flag"),
    }

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([audit]).to_csv(args.out_csv, index=False, encoding="utf-8")

    lines = [
        f"# Daily Stability Missing Date Audit ({args.race_date})",
        "",
        "```json",
        json.dumps(audit, ensure_ascii=False, indent=2),
        "```",
        "",
        "## Notes",
        "",
        "- If `rows_for_race_date` is 0, the upstream candidate union CSV likely did not include this date (or `race_date` is missing/incorrect in the input rows).",
        "- If `rows_for_race_date` > 0 but `result_quality_status=ok` race count is 0, daily stability may exclude it when `--quality-ok-only` is used.",
        "",
    ]
    args.out_md.write_text("\n".join(lines), encoding="utf-8")

    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

