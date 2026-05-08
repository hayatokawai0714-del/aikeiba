from __future__ import annotations

import argparse
import datetime as dt
import json
import csv
from pathlib import Path

import pandas as pd


def _read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        return next(csv.reader(f))


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit pair_reranker required features vs expanded candidate pool.")
    ap.add_argument("--pair-model-root", type=Path, required=True)
    ap.add_argument("--pair-model-version", required=True)
    ap.add_argument("--expanded-parquet", type=Path, required=True, help="candidate_pairs_expanded.parquet")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    meta_path = args.pair_model_root / args.pair_model_version / "meta.json"
    model_path = args.pair_model_root / args.pair_model_version / "model.txt"
    if not meta_path.exists():
        raise FileNotFoundError(str(meta_path))

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    required_features = list(meta.get("features") or [])
    categorical = list(meta.get("categorical_features") or [])

    df = pd.read_parquet(args.expanded_parquet)
    present = set(df.columns)

    rows = []
    for f in required_features:
        if f in df.columns:
            s = df[f]
            non_null_rate = float(pd.to_numeric(s, errors="coerce").notna().mean()) if len(df) > 0 else 0.0
            all_nan = bool(pd.to_numeric(s, errors="coerce").notna().sum() == 0)
            dtype = str(s.dtype)
            # crude "filled_with_zero_rate": only for numeric-like
            x = pd.to_numeric(s, errors="coerce")
            filled_zero_rate = float((x.fillna(0.0) == 0.0).mean()) if len(x) > 0 else 0.0
        else:
            non_null_rate = 0.0
            all_nan = True
            dtype = "MISSING"
            filled_zero_rate = 1.0

        # Recoverability heuristic
        recoverable = f in [
            "field_size",
            "distance",
            "venue",
            "surface",
            "pair_value_score_rank_pct",
            "pair_value_score_z_in_race",
            "pair_prob_naive_rank_pct",
            "pair_prob_naive_z_in_race",
            "pair_rank_bucket",
            "field_size_bucket",
            "distance_bucket",
            "pair_ai_market_gap_min",
            "pair_ai_market_gap_max",
            "pair_ai_market_gap_abs_diff",
        ]
        recovery_method = ""
        if f in ["field_size", "distance", "venue", "surface"]:
            recovery_method = "JOIN races by race_id"
        elif f.endswith("_rank_pct") or f.endswith("_z_in_race") or f.endswith("_bucket"):
            recovery_method = "compute within race_id"
        elif f.startswith("pair_ai_market_gap_"):
            recovery_method = "compute from horse1/horse2 ai_market_gap"

        rows.append(
            {
                "required_feature": f,
                "present_in_expanded": f in present,
                "non_null_rate": non_null_rate,
                "dtype": dtype,
                "all_nan": all_nan,
                "filled_with_zero_rate": filled_zero_rate,
                "source_candidate": recovery_method,
                "recoverable": recoverable,
                "recovery_method": recovery_method if recoverable else "",
                "is_categorical_in_meta": f in categorical,
            }
        )

    out_df = pd.DataFrame(rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.out_csv, index=False, encoding="utf-8")

    md = [
        "# Pair Reranker Required Features Audit",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- model_dir: {args.pair_model_root / args.pair_model_version}",
        f"- expanded_parquet: {args.expanded_parquet}",
        f"- required_features: {len(required_features)}",
        f"- categorical_features_in_meta: {len(categorical)}",
        "",
        "## Summary",
        "",
        f"- present_count: {int(out_df['present_in_expanded'].sum())}",
        f"- missing_count: {int((~out_df['present_in_expanded']).sum())}",
        f"- all_nan_count: {int(out_df['all_nan'].sum())}",
        "",
        "## Top missing/all-NaN features",
        "",
    ]
    top_bad = out_df[(~out_df["present_in_expanded"]) | (out_df["all_nan"])].head(30)
    if len(top_bad) > 0:
        md += ["| feature | present | non_null_rate | dtype | all_nan | recoverable | method |", "|---|---:|---:|---|---:|---:|---|"]
        for _, r in top_bad.iterrows():
            md.append(
                f"| {r['required_feature']} | {bool(r['present_in_expanded'])} | {r['non_null_rate']:.4f} | {r['dtype']} | {bool(r['all_nan'])} | {bool(r['recoverable'])} | {r['recovery_method']} |"
            )
        md.append("")

    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

