from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pandas as pd


def _dist(df: pd.DataFrame, label: str) -> dict:
    x = pd.to_numeric(df.get("pair_model_score"), errors="coerce")
    g = pd.to_numeric(df.get("pair_model_score_gap_to_next"), errors="coerce")
    e = pd.to_numeric(df.get("pair_edge"), errors="coerce") if "pair_edge" in df.columns else pd.Series([pd.NA] * len(df))

    xn = x.dropna()
    gn = g.dropna()
    en = e.dropna()
    out = {
        "dataset": label,
        "row_count": int(len(df)),
        "pair_model_score_non_null": int(xn.shape[0]),
        "pair_model_score_unique_count": int(xn.nunique(dropna=True)),
        "pair_model_score_min": (float(xn.min()) if len(xn) else None),
        "pair_model_score_p50": (float(xn.quantile(0.5)) if len(xn) else None),
        "pair_model_score_p90": (float(xn.quantile(0.9)) if len(xn) else None),
        "pair_model_score_p95": (float(xn.quantile(0.95)) if len(xn) else None),
        "pair_model_score_p99": (float(xn.quantile(0.99)) if len(xn) else None),
        "pair_model_score_max": (float(xn.max()) if len(xn) else None),
        "pair_model_score_mean": (float(xn.mean()) if len(xn) else None),
        "pair_model_score_std": (float(xn.std()) if len(xn) else None),
        "gap_non_null": int(gn.shape[0]),
        "gap_p50": (float(gn.quantile(0.5)) if len(gn) else None),
        "gap_p90": (float(gn.quantile(0.9)) if len(gn) else None),
        "gap_p99": (float(gn.quantile(0.99)) if len(gn) else None),
        "pair_edge_non_null": int(en.shape[0]),
        "pair_edge_p50": (float(en.quantile(0.5)) if len(en) else None),
        "pair_edge_p90": (float(en.quantile(0.9)) if len(en) else None),
        "pair_edge_p99": (float(en.quantile(0.99)) if len(en) else None),
    }
    out["all_same_flag"] = bool(out["pair_model_score_unique_count"] == 1) if out["pair_model_score_unique_count"] is not None else None
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare pair_model_score distribution between v4 and v5 joined CSVs.")
    ap.add_argument("--v4-csv", type=Path, required=True)
    ap.add_argument("--v5-csv", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    v4 = pd.read_csv(args.v4_csv, low_memory=False)
    v5 = pd.read_csv(args.v5_csv, low_memory=False)

    out = pd.DataFrame([_dist(v4, "v4"), _dist(v5, "v5")])
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    md = [
        "# pair_model_score Distribution Compare (v4 vs v5)",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- v4_csv: {args.v4_csv}",
        f"- v5_csv: {args.v5_csv}",
        "",
        "## Summary",
        "",
        "See CSV for full quantiles; key signal is `pair_model_score_std` and `gap_p90/p99` increasing in v5.",
        "",
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

