from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract invalid_finish_position rows for specific dates from joined pair CSV.")
    ap.add_argument("--pairs-csv", type=Path, required=True)
    ap.add_argument("--dates", required=True, help="Comma-separated YYYY-MM-DD list")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    dates = [d.strip() for d in str(args.dates).split(",") if d.strip()]
    df = pd.read_csv(args.pairs_csv)
    df["race_date"] = df.get("race_date", "").astype(str)
    sub = df[df["race_date"].isin(dates)].copy()

    # Keep only invalid_finish_position rows.
    sub = sub[sub.get("result_quality_status", "") == "invalid_finish_position"].copy()

    # Determine which source was used per horse (external/db/none may be stored in result_source_used)
    # The join script stores per-row `result_source_used` (external/db/none) and per-horse finish positions.
    out_cols = [
        "race_date",
        "race_id",
        "horse1_umaban",
        "horse2_umaban",
        "horse1_name",
        "horse2_name",
        "external_finish_position_h1",
        "external_finish_position_h2",
        "db_finish_position_h1",
        "db_finish_position_h2",
        "external_status_h1",
        "external_status_h2",
        "db_status_h1",
        "db_status_h2",
        "result_source_used",
        "result_source_conflict",
        "result_quality_status",
        "result_join_status",
    ]
    # Back-compat: older outputs may store horse_name columns as horse1_horse_name / horse2_horse_name
    if "horse1_horse_name" in sub.columns and "horse1_name" not in sub.columns:
        sub["horse1_name"] = sub["horse1_horse_name"]
    if "horse2_horse_name" in sub.columns and "horse2_name" not in sub.columns:
        sub["horse2_name"] = sub["horse2_horse_name"]

    # Provide a single-row-per-horse view would be heavier; for now output pair rows, with both horses.
    for c in out_cols:
        if c not in sub.columns:
            sub[c] = pd.NA

    out = sub[out_cols].copy()

    # Add anomaly_reason derived from the used source values (best-effort).
    def _reason(r) -> str:
        # Prefer external if used.
        used = str(r.get("result_source_used") or "")
        vals = []
        if used == "external":
            vals = [r.get("external_finish_position_h1"), r.get("external_finish_position_h2")]
        elif used == "db":
            vals = [r.get("db_finish_position_h1"), r.get("db_finish_position_h2")]
        else:
            vals = [r.get("external_finish_position_h1"), r.get("external_finish_position_h2"), r.get("db_finish_position_h1"), r.get("db_finish_position_h2")]
        bad = [v for v in vals if v is not None and not (isinstance(v, float) and pd.isna(v))]
        if not bad:
            return "missing_finish_position"
        return "non_1_to_18_or_non_numeric"

    out["anomaly_reason"] = out.apply(_reason, axis=1)

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    lines = [
        f"# Invalid Finish Position Rows ({','.join(dates)})",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- input_pairs_csv: {args.pairs_csv}",
        f"- row_count: {len(out)}",
        "",
        "## Output",
        "",
        f"- csv: {args.out_csv}",
        "",
        "## Quick Counts",
        "",
    ]
    if len(out) > 0:
        vc = out["result_source_used"].value_counts(dropna=False).to_dict()
        lines.append("```json")
        lines.append(json.dumps({"result_source_used_counts": vc}, ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append("")
    args.out_md.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

