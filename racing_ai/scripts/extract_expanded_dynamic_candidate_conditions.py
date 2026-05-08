from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract promising dynamic conditions from expanded edge-variant grid.")
    ap.add_argument("--inputs", required=True, help="Comma-separated grid summary csv paths")
    ap.add_argument("--out-csv", type=Path, default=Path("racing_ai/reports/expanded_dynamic_candidate_conditions.csv"))
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/expanded_dynamic_candidate_conditions.md"))
    ap.add_argument("--top-n", type=int, default=10)
    args = ap.parse_args()

    rows = []
    for p in [Path(x.strip()) for x in args.inputs.split(",") if x.strip()]:
        if not p.exists():
            continue
        d = pd.read_csv(p)
        d["source_file"] = str(p)
        rows.append(d)
    if not rows:
        raise SystemExit("no input grid files")

    df = pd.concat(rows, ignore_index=True)
    for c in ["rule_non_overlap_dynamic_pair_count", "selected_pair_count", "rule_dynamic_overlap_rate", "dynamic_roi_proxy", "rule_roi_proxy", "avg_pair_model_score_selected", "avg_pair_market_implied_prob_selected"]:
        if c not in df.columns:
            df[c] = pd.NA

    cond = _to_num(df["rule_non_overlap_dynamic_pair_count"]).fillna(0) > 0
    cond &= _to_num(df["selected_pair_count"]).fillna(0) <= 200
    cond &= _to_num(df["rule_dynamic_overlap_rate"]).fillna(1.0) < 1.0
    cand = df[cond].copy()
    cand["dynamic_minus_rule_roi"] = _to_num(cand["dynamic_roi_proxy"]) - _to_num(cand["rule_roi_proxy"])
    cand["score_rank"] = (
        _to_num(cand["rule_non_overlap_dynamic_pair_count"]).fillna(0) * 1000
        + (1.0 - _to_num(cand["rule_dynamic_overlap_rate"]).fillna(1.0)) * 100
        + _to_num(cand["dynamic_minus_rule_roi"]).fillna(0) * 10
        + _to_num(cand["avg_pair_model_score_selected"]).fillna(0)
        - _to_num(cand["avg_pair_market_implied_prob_selected"]).fillna(0)
    )
    cand = cand.sort_values("score_rank", ascending=False)
    out = cand.head(max(1, int(args.top_n))).copy()
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    try:
        tbl = out.to_markdown(index=False)
    except Exception:
        tbl = out.to_string(index=False)
    md = [
        "# expanded_dynamic_candidate_conditions",
        "",
        f"- total_candidates_after_filter: {len(cand)}",
        f"- top_n: {int(args.top_n)}",
        "",
        tbl,
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

