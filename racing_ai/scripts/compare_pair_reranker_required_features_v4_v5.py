from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def _load_required_features(pair_model_dir: Path) -> list[str]:
    meta = pair_model_dir / "meta.json"
    if not meta.exists():
        return []
    try:
        obj = json.loads(meta.read_text(encoding="utf-8"))
        feats = obj.get("features") or []
        return [str(x) for x in feats]
    except Exception:
        return []


def _audit(df: pd.DataFrame, required: list[str], label: str) -> pd.DataFrame:
    rows: list[dict] = []
    n = int(len(df))
    for f in required:
        present = f in df.columns
        s = df[f] if present else pd.Series([pd.NA] * n)
        x = pd.to_numeric(s, errors="coerce")
        non_null = int(x.notna().sum())
        all_nan = bool(non_null == 0)
        # "filled_with_zero" is a useful proxy for "missing got zero-filled".
        filled_zero = int((x.fillna(0.0) == 0.0).sum()) if present else n
        rows.append(
            {
                "dataset": label,
                "required_feature": f,
                "present_in_dataset": bool(present),
                "row_count": n,
                "non_null_count": non_null,
                "non_null_rate": (float(non_null / n) if n > 0 else None),
                "dtype": (str(df[f].dtype) if present else None),
                "all_nan": all_nan,
                "filled_with_zero_rate": (float(filled_zero / n) if n > 0 else None),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare pair_reranker required feature availability between v4 and v5 joined CSVs.")
    ap.add_argument("--pair-model-dir", type=Path, required=True, help="e.g. racing_ai/data/models_compare/pair_reranker/pair_reranker_ts_v4")
    ap.add_argument("--v4-csv", type=Path, required=True)
    ap.add_argument("--v5-csv", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    required = _load_required_features(args.pair_model_dir)
    if not required:
        raise SystemExit(f"meta.json features not found: {args.pair_model_dir}")

    v4 = pd.read_csv(args.v4_csv, low_memory=False)
    v5 = pd.read_csv(args.v5_csv, low_memory=False)

    a4 = _audit(v4, required, "v4")
    a5 = _audit(v5, required, "v5")
    out = pd.concat([a4, a5], axis=0, ignore_index=True)

    # Pivot for quick comparisons
    pivot = (
        out.pivot_table(
            index="required_feature",
            columns="dataset",
            values=["present_in_dataset", "non_null_rate", "filled_with_zero_rate", "dtype"],
            aggfunc="first",
        )
        .reset_index()
    )
    # Flatten columns
    pivot.columns = ["_".join([c for c in col if c]).rstrip("_") for col in pivot.columns.to_list()]

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    pivot.to_csv(args.out_csv, index=False, encoding="utf-8")

    improved = pivot[
        (pivot.get("non_null_rate_v5").fillna(0.0) > pivot.get("non_null_rate_v4").fillna(0.0))
    ].sort_values("non_null_rate_v5", ascending=False)
    regressed = pivot[
        (pivot.get("non_null_rate_v5").fillna(0.0) < pivot.get("non_null_rate_v4").fillna(0.0))
    ].sort_values("non_null_rate_v5", ascending=True)

    missing_rate_v4 = float((pivot.get("present_in_dataset_v4") != True).mean()) if len(pivot) else 1.0
    missing_rate_v5 = float((pivot.get("present_in_dataset_v5") != True).mean()) if len(pivot) else 1.0

    md = [
        "# pair_reranker Required Features Compare (v4 vs v5)",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- pair_model_dir: {args.pair_model_dir}",
        f"- v4_csv: {args.v4_csv}",
        f"- v5_csv: {args.v5_csv}",
        f"- required_feature_count: {len(required)}",
        f"- missing_required_feature_rate_v4: {missing_rate_v4:.3f}",
        f"- missing_required_feature_rate_v5: {missing_rate_v5:.3f}",
        "",
        "## Highlights",
        "",
        f"- improved_feature_count: {int(len(improved))}",
        f"- regressed_feature_count: {int(len(regressed))}",
        "",
        "## Notes",
        "",
        "- This script reads the *joined pairs CSV*. If your joined CSV schema does not include the required raw features (common when exporting a minimal evaluation schema), many features will show as missing even if they were present upstream during model inference.",
        "- For a ground-truth audit of feature recovery, run `audit_pair_reranker_required_features.py` against the actual `candidate_pairs_expanded.parquet` used for inference.",
        "- `filled_with_zero_rate` is a heuristic indicating many rows are zero after numeric coercion/fill; treat as a signal of missingness in the evaluation helper path.",
        "",
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
