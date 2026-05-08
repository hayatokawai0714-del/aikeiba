from __future__ import annotations

import argparse
import json
from pathlib import Path

REQUIRED_DYNAMIC_COLUMNS = [
    "pair_edge",
    "model_dynamic_final_score",
    "model_dynamic_selected_flag",
    "model_dynamic_skip_reason",
    "model_dynamic_rank",
    "pair_model_score_rank_in_race",
    "pair_model_score_gap_to_next",
]


def _load_df(path: Path) -> pd.DataFrame:
    import pandas as pd
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"unsupported file extension: {path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Check race-day candidate output columns.")
    ap.add_argument("--candidate-pairs", type=Path, required=True, help="candidate_pairs.parquet or csv")
    ap.add_argument("--out-json", type=Path, default=None, help="Optional output report json")
    args = ap.parse_args()
    import pandas as pd

    if not args.candidate_pairs.exists():
        raise SystemExit(f"missing file: {args.candidate_pairs}")

    df = _load_df(args.candidate_pairs)
    missing = [c for c in REQUIRED_DYNAMIC_COLUMNS if c not in df.columns]
    warnings: list[str] = []
    checks: dict[str, object] = {}
    if "model_dynamic_selected_flag" in df.columns:
        s = df["model_dynamic_selected_flag"]
        as_num = pd.to_numeric(s, errors="coerce")
        is_bool_like = bool(
            str(s.dtype) == "bool"
            or set(as_num.dropna().unique().tolist()).issubset({0.0, 1.0})
        )
        if not is_bool_like:
            warnings.append("model_dynamic_selected_flag_not_bool_like")
        checks["model_dynamic_selected_flag_bool_like"] = is_bool_like
    if "model_dynamic_rank" in df.columns and "model_dynamic_selected_flag" in df.columns:
        selected = df[df["model_dynamic_selected_flag"].astype(bool)]
        rank_missing = int(selected["model_dynamic_rank"].isna().sum()) if len(selected) > 0 else 0
        if rank_missing > 0:
            warnings.append(f"selected_rows_missing_model_dynamic_rank:{rank_missing}")
        checks["selected_rows_missing_model_dynamic_rank"] = rank_missing
    if "model_dynamic_skip_reason" in df.columns:
        by_race = (
            df.groupby("race_id")["model_dynamic_skip_reason"]
            .apply(lambda x: x.notna().any())
            .astype(bool)
        ) if "race_id" in df.columns else pd.Series([], dtype=bool)
        missing_race_count = int((~by_race).sum()) if len(by_race) > 0 else 0
        if missing_race_count > 0:
            warnings.append(f"races_missing_model_dynamic_skip_reason:{missing_race_count}")
        checks["races_missing_model_dynamic_skip_reason"] = missing_race_count
    if "pair_edge" in df.columns:
        nonnull_edge = int(pd.to_numeric(df["pair_edge"], errors="coerce").notna().sum())
        if nonnull_edge == 0:
            warnings.append("pair_edge_all_null")
        checks["pair_edge_non_null_count"] = nonnull_edge
    if "model_dynamic_final_score" in df.columns:
        nonnull_score = int(pd.to_numeric(df["model_dynamic_final_score"], errors="coerce").notna().sum())
        if nonnull_score == 0:
            warnings.append("model_dynamic_final_score_all_null")
        checks["model_dynamic_final_score_non_null_count"] = nonnull_score

    report = {
        "candidate_pairs_path": str(args.candidate_pairs),
        "row_count": int(len(df)),
        "required_dynamic_columns": REQUIRED_DYNAMIC_COLUMNS,
        "missing_columns": missing,
        "checks": checks,
        "warnings": warnings,
        "status": ("ok" if len(missing) == 0 and len(warnings) == 0 else "warn"),
    }
    if args.out_json is not None:
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
