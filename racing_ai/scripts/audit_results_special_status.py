from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit special result statuses and suspicious finish values.")
    ap.add_argument("--external-results-csv", type=Path, required=True)
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.external_results_csv)
    if "race_date" not in df.columns:
        raise SystemExit("missing race_date")
    if "finish_position" not in df.columns:
        raise SystemExit("missing finish_position")
    if "status" not in df.columns:
        df["status"] = ""
    df["race_date"] = df["race_date"].astype(str)
    df = df[(df["race_date"] >= args.start_date) & (df["race_date"] <= args.end_date)].copy()

    fin = _to_num(df["finish_position"])
    df["finish_position_num"] = fin
    df["is_special_numeric"] = fin.isin([0, 44, 99])
    df["is_finish_invalid"] = fin.isna() | (~fin.between(1, 18, inclusive="both"))
    s = df["status"].astype(str).fillna("")
    kw_map = {
        "同着": s.str.contains("同着", na=False),
        "失格": s.str.contains("失格", na=False),
        "降着": s.str.contains("降着", na=False),
        "取消": s.str.contains("取消", na=False),
        "除外": s.str.contains("除外", na=False),
        "競走中止": s.str.contains("中止", na=False),
    }

    rows = []
    rows.append({"category": "rows_total", "count": int(len(df))})
    rows.append({"category": "finish_invalid_rows", "count": int(df["is_finish_invalid"].sum())})
    rows.append({"category": "finish_eq_0", "count": int((fin == 0).sum())})
    rows.append({"category": "finish_eq_44", "count": int((fin == 44).sum())})
    rows.append({"category": "finish_eq_99", "count": int((fin == 99).sum())})
    for k, m in kw_map.items():
        rows.append({"category": f"status_{k}", "count": int(m.sum())})

    by_date = df.groupby("race_date").agg(
        rows=("race_id", "count"),
        finish_invalid_rows=("is_finish_invalid", "sum"),
        finish_eq_0=("finish_position_num", lambda x: int((x == 0).sum())),
        finish_eq_44=("finish_position_num", lambda x: int((x == 44).sum())),
        finish_eq_99=("finish_position_num", lambda x: int((x == 99).sum())),
    ).reset_index()
    out_main = pd.DataFrame(rows)
    out = out_main.copy()
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    sample = df[df["is_finish_invalid"] | df["is_special_numeric"]][
        ["race_date", "race_id", "umaban", "horse_name", "finish_position", "status"]
    ].head(200)
    try:
        t1 = out_main.to_markdown(index=False)
        t2 = by_date.to_markdown(index=False)
        t3 = sample.to_markdown(index=False)
    except Exception:
        t1 = out_main.to_string(index=False)
        t2 = by_date.to_string(index=False)
        t3 = sample.to_string(index=False)
    md = [
        "# results_special_status_audit",
        "",
        f"- date_range: {args.start_date} to {args.end_date}",
        "",
        "## Totals",
        t1,
        "",
        "## By Date",
        t2,
        "",
        "## Invalid/Special Samples",
        t3,
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

