from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit external results/payout coverage for a specific race_date.")
    ap.add_argument("--race-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--external-results-csv", type=Path, required=True)
    ap.add_argument("--external-wide-payouts-csv", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    d = str(args.race_date)
    res_df = pd.read_csv(args.external_results_csv)
    pay_df = pd.read_csv(args.external_wide_payouts_csv)

    for df in (res_df, pay_df):
        if "race_date" in df.columns:
            df["race_date"] = df["race_date"].astype(str)
        else:
            df["race_date"] = ""

    res_sub = res_df[res_df["race_date"] == d].copy()
    pay_sub = pay_df[pay_df["race_date"] == d].copy()

    out = {
        "race_date": d,
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "external_results_path": str(args.external_results_csv),
        "external_wide_payouts_path": str(args.external_wide_payouts_csv),
        "external_results_rows": int(len(res_sub)),
        "external_results_race_count": int(res_sub["race_id"].nunique()) if "race_id" in res_sub.columns else None,
        "external_results_finish_position_non_null": int(res_sub["finish_position"].notna().sum()) if "finish_position" in res_sub.columns else None,
        "external_results_status_values": (res_sub["status"].value_counts(dropna=False).head(10).to_dict() if "status" in res_sub.columns else {}),
        "external_wide_payout_rows": int(len(pay_sub)),
        "external_wide_payout_race_count": int(pay_sub["race_id"].nunique()) if "race_id" in pay_sub.columns else None,
        "external_wide_payout_bet_type_values": (pay_sub["bet_type"].value_counts(dropna=False).head(10).to_dict() if "bet_type" in pay_sub.columns else {}),
        "external_wide_payout_key_samples": (pay_sub["bet_key"].astype(str).dropna().head(10).tolist() if "bet_key" in pay_sub.columns else []),
    }

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([out]).to_csv(args.out_csv, index=False, encoding="utf-8")

    lines = [
        f"# External Data Coverage Audit ({d})",
        "",
        "```json",
        json.dumps(out, ensure_ascii=False, indent=2),
        "```",
        "",
    ]
    args.out_md.write_text("\n".join(lines), encoding="utf-8")

    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

