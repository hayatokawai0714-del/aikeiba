from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def _parse_key_list(v) -> list[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return []
    s = str(v).strip()
    if not s:
        return []
    # Our join script stores lists as JSON-ish string or comma-separated string.
    # Try JSON first.
    try:
        obj = json.loads(s)
        if isinstance(obj, list):
            return [str(x) for x in obj]
    except Exception:
        pass
    # Fallback: split by comma
    if "," in s:
        return [x.strip() for x in s.split(",") if x.strip()]
    return [s]


def main() -> None:
    ap = argparse.ArgumentParser(description="Break down quality_ng reasons per race_id for specific dates.")
    ap.add_argument("--pairs-csv", type=Path, required=True, help="Joined pair-level CSV (external priority).")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--dates", required=True, help="Comma-separated YYYY-MM-DD list (e.g. 2026-04-12,2026-04-26)")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    import duckdb

    dates = [d.strip() for d in str(args.dates).split(",") if d.strip()]
    df = pd.read_csv(args.pairs_csv)
    df["race_date"] = df.get("race_date", "").astype(str)
    df = df[df["race_date"].isin(dates)].copy()

    con = duckdb.connect(str(args.db_path), read_only=True)
    race_meta = con.execute(
        "select race_id, cast(race_date as varchar) as race_date, venue, race_no from races where race_date in (%s)"
        % ",".join(["?"] * len(dates)),
        dates,
    ).fetchdf()
    race_meta["race_id"] = race_meta["race_id"].astype(str)
    race_meta["race_date"] = race_meta["race_date"].astype(str)

    # Aggregate counts per race_id.
    def _count_status(g: pd.DataFrame, status: str) -> int:
        if "result_quality_status" not in g.columns:
            return 0
        return int((g["result_quality_status"] == status).sum())

    out_rows = []
    for (race_date, race_id), g in df.groupby(["race_date", "race_id"], dropna=False):
        race_id = str(race_id)
        total = int(len(g))
        vc = g["result_quality_status"].value_counts(dropna=False).to_dict() if "result_quality_status" in g.columns else {}
        expected_mismatch = int(vc.get("expected_payout_mismatch", 0))
        invalid_fp = int(vc.get("invalid_finish_position", 0))
        top3_not3 = int(vc.get("top3_count_not_3", 0))
        results_missing = int(vc.get("results_missing", 0))

        # Pull a few samples.
        sample_invalid = []
        if "horse1_finish_position" in g.columns and "horse2_finish_position" in g.columns:
            bad = g[(g["result_quality_status"] == "invalid_finish_position")].copy()
            if len(bad) > 0:
                # Take finish positions from either side.
                for _, r in bad.head(5).iterrows():
                    sample_invalid.append(
                        f"{r.get('horse1_finish_position')}/{r.get('horse2_finish_position')}"
                    )

        sample_mismatch_keys = []
        mm = g[g.get("result_quality_status", "") == "expected_payout_mismatch"].head(1)
        if len(mm) > 0:
            r = mm.iloc[0].to_dict()
            sample_mismatch_keys = list(
                set(_parse_key_list(r.get("result_expected_wide_keys")) + _parse_key_list(r.get("payout_wide_keys")))
            )[:10]

        # race-level fields from one row (string)
        result_top3_count = None
        payout_wide_keys = None
        expected_wide_keys = None
        expected_vs_payout_match = None
        if len(g) > 0:
            rr = g.iloc[0].to_dict()
            result_top3_count = rr.get("result_top3_count")
            expected_wide_keys = rr.get("result_expected_wide_keys")
            payout_wide_keys = rr.get("payout_wide_keys")
            expected_vs_payout_match = rr.get("expected_vs_payout_match")

        out_rows.append(
            {
                "race_date": str(race_date),
                "race_id": race_id,
                "total_pair_rows": total,
                "result_quality_status_counts": json.dumps(vc, ensure_ascii=False),
                "expected_payout_mismatch_count": expected_mismatch,
                "invalid_finish_position_count": invalid_fp,
                "top3_count_not_3_count": top3_not3,
                "results_missing_count": results_missing,
                "result_top3_count": result_top3_count,
                "result_expected_wide_keys": expected_wide_keys,
                "payout_wide_keys": payout_wide_keys,
                "expected_vs_payout_match": expected_vs_payout_match,
                "sample_invalid_finish_positions": json.dumps(sample_invalid, ensure_ascii=False),
                "sample_mismatch_keys": json.dumps(sample_mismatch_keys, ensure_ascii=False),
            }
        )

    out_df = pd.DataFrame(out_rows)
    out_df["race_id"] = out_df["race_id"].astype(str)
    out_df = out_df.merge(race_meta[["race_id", "venue", "race_no"]], on="race_id", how="left")
    # Keep stable column order
    cols = [
        "race_date",
        "race_id",
        "venue",
        "race_no",
        "total_pair_rows",
        "result_quality_status_counts",
        "expected_payout_mismatch_count",
        "invalid_finish_position_count",
        "top3_count_not_3_count",
        "results_missing_count",
        "result_top3_count",
        "result_expected_wide_keys",
        "payout_wide_keys",
        "expected_vs_payout_match",
        "sample_invalid_finish_positions",
        "sample_mismatch_keys",
    ]
    out_df = out_df[[c for c in cols if c in out_df.columns]]

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.out_csv, index=False, encoding="utf-8")

    # Summary counts per date
    lines = [
        f"# Quality NG Race Breakdown ({','.join(dates)})",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- input_pairs_csv: {args.pairs_csv}",
        "",
        "## Output",
        "",
        f"- csv: {args.out_csv}",
        "",
        "## Per-date Summary",
        "",
    ]
    if len(out_df) == 0:
        lines.append("- no rows for the specified dates")
    else:
        for d in dates:
            sub = out_df[out_df["race_date"] == d].copy()
            lines.append(f"### {d}")
            lines.append(f"- race_count: {int(sub['race_id'].nunique())}")
            lines.append(f"- total_pair_rows: {int(sub['total_pair_rows'].sum())}")
            lines.append(f"- expected_payout_mismatch_pair_rows: {int(sub['expected_payout_mismatch_count'].sum())}")
            lines.append(f"- invalid_finish_position_pair_rows: {int(sub['invalid_finish_position_count'].sum())}")
            lines.append(f"- top3_count_not_3_pair_rows: {int(sub['top3_count_not_3_count'].sum())}")
            lines.append(f"- results_missing_pair_rows: {int(sub['results_missing_count'].sum())}")
            lines.append("")

    args.out_md.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

