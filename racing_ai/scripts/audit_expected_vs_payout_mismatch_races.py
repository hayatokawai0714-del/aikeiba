from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def _parse_list(v) -> list[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return []
    s = str(v).strip()
    if not s:
        return []
    try:
        obj = json.loads(s)
        if isinstance(obj, list):
            return [str(x) for x in obj]
    except Exception:
        pass
    if "," in s:
        return [x.strip() for x in s.split(",") if x.strip()]
    return [s]


def main() -> None:
    ap = argparse.ArgumentParser(description="Build expected vs payout mismatch table per race for specific dates.")
    ap.add_argument("--pairs-csv", type=Path, required=True)
    ap.add_argument("--dates", required=True, help="Comma-separated YYYY-MM-DD list")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    import duckdb

    dates = [d.strip() for d in str(args.dates).split(",") if d.strip()]
    df = pd.read_csv(args.pairs_csv)
    df["race_date"] = df.get("race_date", "").astype(str)
    df = df[df["race_date"].isin(dates)].copy()

    # Race meta
    con = duckdb.connect(str(args.db_path), read_only=True)
    race_meta = con.execute(
        "select race_id, cast(race_date as varchar) as race_date, venue, race_no from races where race_date in (%s)"
        % ",".join(["?"] * len(dates)),
        dates,
    ).fetchdf()
    race_meta["race_id"] = race_meta["race_id"].astype(str)

    rows = []
    for (race_date, race_id), g in df.groupby(["race_date", "race_id"], dropna=False):
        race_id = str(race_id)
        # Take one representative row for expected/payout keys and top3 list if present.
        r = g.iloc[0].to_dict()
        expected = set(_parse_list(r.get("result_expected_wide_keys")))
        payout = set(_parse_list(r.get("payout_wide_keys")))
        if not expected and not payout:
            continue
        match = bool(r.get("expected_vs_payout_match")) if "expected_vs_payout_match" in g.columns else (expected == payout)
        if match:
            continue
        missing = sorted(list(expected - payout))
        extra = sorted(list(payout - expected))

        status_counts = g["result_quality_status"].value_counts(dropna=False).to_dict() if "result_quality_status" in g.columns else {}
        mismatch_reason = "unknown"
        if int(status_counts.get("invalid_finish_position", 0)) > 0 or int(status_counts.get("top3_count_not_3", 0)) > 0:
            mismatch_reason = "finish_position_wrong"
        elif int(status_counts.get("payout_wide_missing", 0)) > 0:
            mismatch_reason = "payout_key_wrong"
        elif int(status_counts.get("results_missing", 0)) > 0:
            mismatch_reason = "results_missing"

        rows.append(
            {
                "race_date": str(race_date),
                "race_id": race_id,
                "venue": None,
                "race_no": None,
                "result_top3_umaban_list": r.get("result_top3_umaban_list"),
                "result_expected_wide_keys": json.dumps(sorted(list(expected)), ensure_ascii=False),
                "payout_wide_keys": json.dumps(sorted(list(payout)), ensure_ascii=False),
                "missing_from_payout": json.dumps(missing, ensure_ascii=False),
                "extra_in_payout": json.dumps(extra, ensure_ascii=False),
                "mismatch_reason": mismatch_reason,
            }
        )

    out_df = pd.DataFrame(rows)
    if len(out_df) > 0:
        out_df["race_id"] = out_df["race_id"].astype(str)
        out_df = out_df.merge(race_meta[["race_id", "venue", "race_no"]], on="race_id", how="left")

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.out_csv, index=False, encoding="utf-8")

    lines = [
        f"# Expected vs Payout Mismatch Races ({','.join(dates)})",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- input_pairs_csv: {args.pairs_csv}",
        f"- mismatch_race_count: {int(out_df['race_id'].nunique()) if len(out_df)>0 else 0}",
        "",
        "## Output",
        "",
        f"- csv: {args.out_csv}",
        "",
    ]
    args.out_md.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

