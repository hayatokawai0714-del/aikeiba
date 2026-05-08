from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def _as_bool(s: pd.Series) -> pd.Series:
    return s.astype(str).str.lower().isin(["1", "true", "t", "yes", "y"])


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit model_dynamic postcompute outputs from joined pairs CSV (evaluation helper).")
    ap.add_argument("--pairs-csv", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.pairs_csv, low_memory=False)
    total_pair_rows = int(len(df))
    race_count = int(df.get("race_id", pd.Series(dtype=str)).nunique())

    # Selected / non-overlap counts (pair level)
    sel = _as_bool(df.get("model_dynamic_selected_flag", pd.Series([False] * len(df))))
    rule = _as_bool(df.get("pair_selected_flag", pd.Series([False] * len(df))))
    selected_count = int(sel.sum())
    non_overlap_count = int((sel & (~rule)).sum())

    # Race-level selected presence
    selected_race_count = int(df.loc[sel, "race_id"].nunique()) if "race_id" in df.columns else 0
    zero_selected_race_count = int(race_count - selected_race_count) if race_count is not None else None

    # Skip reason counts (race-level; prefer per-race unique reason)
    if "model_dynamic_skip_reason" in df.columns and "race_id" in df.columns:
        race_reason = (
            df[["race_id", "model_dynamic_skip_reason"]]
            .drop_duplicates(subset=["race_id"])
            .fillna({"model_dynamic_skip_reason": "NA"})
        )
        skip_reason_counts = race_reason["model_dynamic_skip_reason"].astype(str).value_counts().to_dict()
    else:
        skip_reason_counts = {}

    # Feature coverage counts
    pair_model_score_non_null = int(pd.to_numeric(df.get("pair_model_score"), errors="coerce").notna().sum()) if "pair_model_score" in df.columns else 0
    pair_edge_non_null = int(pd.to_numeric(df.get("pair_edge"), errors="coerce").notna().sum()) if "pair_edge" in df.columns else 0
    gap_non_null = int(pd.to_numeric(df.get("pair_model_score_gap_to_next"), errors="coerce").notna().sum()) if "pair_model_score_gap_to_next" in df.columns else 0

    out = pd.DataFrame(
        [
            {
                "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
                "pairs_csv": str(args.pairs_csv),
                "total_pair_rows": total_pair_rows,
                "race_count": race_count,
                "after_model_dynamic_selected_count": selected_count,
                "after_model_dynamic_non_overlap_count": non_overlap_count,
                "selected_race_count": selected_race_count,
                "zero_selected_race_count": zero_selected_race_count,
                "skip_reason_counts_json": json.dumps(skip_reason_counts, ensure_ascii=False),
                "pair_model_score_non_null_count": pair_model_score_non_null,
                "pair_edge_non_null_count": pair_edge_non_null,
                "gap_non_null_count": gap_non_null,
            }
        ]
    )

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    md = [
        "# model_dynamic Postcompute Audit (from joined pairs CSV)",
        "",
        f"- generated_at: {out.loc[0, 'generated_at']}",
        f"- input: {args.pairs_csv}",
        f"- total_pair_rows: {total_pair_rows}",
        f"- race_count: {race_count}",
        "",
        "## Counts",
        "",
        f"- model_dynamic_selected_count: {selected_count}",
        f"- model_dynamic_non_overlap_count: {non_overlap_count}",
        f"- selected_race_count: {selected_race_count}",
        f"- zero_selected_race_count: {zero_selected_race_count}",
        "",
        "## Skip reasons (race-level)",
        "",
        "```json",
        json.dumps(skip_reason_counts, ensure_ascii=False, indent=2),
        "```",
        "",
        "## Feature coverage",
        "",
        f"- pair_model_score_non_null_count: {pair_model_score_non_null}",
        f"- pair_edge_non_null_count: {pair_edge_non_null}",
        f"- pair_model_score_gap_to_next_non_null_count: {gap_non_null}",
        "",
        "## Notes",
        "",
        "- This audit reads existing columns; it does not recompute selection logic.",
        "",
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

