from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


KEY_COLS = ["edge_variant", "variant_threshold", "min_score", "min_gap", "default_k", "max_k"]


def _parse_inputs(inputs: str, glob_pattern: str | None):
    paths: list[Path] = []
    if inputs.strip():
        for x in inputs.split(","):
            p = Path(x.strip())
            if p.as_posix():
                paths.append(p)
    if glob_pattern:
        for p in Path(".").glob(glob_pattern):
            paths.append(p)
    uniq: list[Path] = []
    seen = set()
    for p in paths:
        rp = str(p.resolve())
        if rp in seen:
            continue
        seen.add(rp)
        uniq.append(p)
    return uniq


def main() -> None:
    ap = argparse.ArgumentParser(description="Aggregate multi-day edge variant grid results.")
    ap.add_argument("--inputs", default="", help="Comma-separated day summary CSV paths")
    ap.add_argument("--glob", dest="glob_pattern", default="", help="Glob pattern for day summaries")
    ap.add_argument("--out-csv", type=Path, default=Path("racing_ai/reports/model_dynamic_edge_variant_grid_multi_day_summary.csv"))
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/model_dynamic_edge_variant_grid_multi_day_summary.md"))
    args = ap.parse_args()

    paths = _parse_inputs(args.inputs, args.glob_pattern if args.glob_pattern else None)
    if not paths:
        raise SystemExit("no input files. use --inputs or --glob")
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        raise SystemExit("missing input files:\n- " + "\n- ".join(missing))

    frames = []
    for p in paths:
        d = pd.read_csv(p)
        d["source_file"] = str(p)
        frames.append(d)
    all_df = pd.concat(frames, ignore_index=True)

    need_num = [
        "selected_pair_count",
        "selected_race_count",
        "rule_overlap_count",
        "rule_dynamic_overlap_rate",
        "rule_non_overlap_dynamic_pair_count",
        "rule_non_overlap_dynamic_hit_count",
        "rule_non_overlap_dynamic_total_payout",
        "dynamic_hit_count",
        "dynamic_bet_count",
        "dynamic_total_payout",
        "rule_hit_count",
        "rule_bet_count",
        "rule_total_payout",
        "avg_pair_model_score_selected",
        "avg_pair_market_implied_prob_selected",
    ]
    for c in need_num:
        if c not in all_df.columns:
            all_df[c] = pd.NA
        all_df[c] = pd.to_numeric(all_df[c], errors="coerce")

    grp = all_df.groupby(KEY_COLS, dropna=False)
    out = grp.agg(
        eval_day_count=("source_file", "nunique"),
        selected_pair_count_sum=("selected_pair_count", "sum"),
        selected_race_count_sum=("selected_race_count", "sum"),
        rule_overlap_count_sum=("rule_overlap_count", "sum"),
        rule_non_overlap_dynamic_pair_count_sum=("rule_non_overlap_dynamic_pair_count", "sum"),
        rule_dynamic_overlap_rate_weighted=("rule_dynamic_overlap_rate", "mean"),
        dynamic_hit_count_sum=("dynamic_hit_count", "sum"),
        dynamic_bet_count_sum=("dynamic_bet_count", "sum"),
        dynamic_total_payout_sum=("dynamic_total_payout", "sum"),
        rule_hit_count_sum=("rule_hit_count", "sum"),
        rule_bet_count_sum=("rule_bet_count", "sum"),
        rule_total_payout_sum=("rule_total_payout", "sum"),
        rule_non_overlap_dynamic_hit_count_sum=("rule_non_overlap_dynamic_hit_count", "sum"),
        rule_non_overlap_dynamic_total_payout_sum=("rule_non_overlap_dynamic_total_payout", "sum"),
        avg_pair_model_score_selected_mean=("avg_pair_model_score_selected", "mean"),
        avg_pair_market_implied_prob_selected_mean=("avg_pair_market_implied_prob_selected", "mean"),
    ).reset_index()

    out["dynamic_cost_sum"] = out["dynamic_bet_count_sum"] * 100.0
    out["rule_cost_sum"] = out["rule_bet_count_sum"] * 100.0
    out["dynamic_hit_rate_overall"] = out["dynamic_hit_count_sum"] / out["dynamic_bet_count_sum"].replace(0, pd.NA)
    out["rule_hit_rate_overall"] = out["rule_hit_count_sum"] / out["rule_bet_count_sum"].replace(0, pd.NA)
    out["dynamic_roi_proxy_overall"] = out["dynamic_total_payout_sum"] / out["dynamic_cost_sum"].replace(0, pd.NA)
    out["rule_roi_proxy_overall"] = out["rule_total_payout_sum"] / out["rule_cost_sum"].replace(0, pd.NA)
    out["dynamic_minus_rule_roi"] = out["dynamic_roi_proxy_overall"] - out["rule_roi_proxy_overall"]
    out["rule_non_overlap_dynamic_hit_rate_overall"] = out["rule_non_overlap_dynamic_hit_count_sum"] / out[
        "rule_non_overlap_dynamic_pair_count_sum"
    ].replace(0, pd.NA)
    out["rule_non_overlap_dynamic_roi_proxy_overall"] = out["rule_non_overlap_dynamic_total_payout_sum"] / (
        out["rule_non_overlap_dynamic_pair_count_sum"].replace(0, pd.NA) * 100.0
    )

    out = out.sort_values(
        ["rule_non_overlap_dynamic_pair_count_sum", "rule_dynamic_overlap_rate_weighted", "dynamic_minus_rule_roi", "selected_pair_count_sum"],
        ascending=[False, True, False, False],
        na_position="last",
    )

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    top = out.head(20)
    try:
        top_table = top.to_markdown(index=False)
    except Exception:
        top_table = top.to_string(index=False)

    md_lines = [
        "# model_dynamic_edge_variant_grid_multi_day_summary",
        "",
        f"- input_files: {len(paths)}",
        f"- output_csv: {args.out_csv}",
        "",
        "## Ranking Priority",
        "1. rule_non_overlap_dynamic_pair_count_sum larger",
        "2. rule_dynamic_overlap_rate_weighted lower",
        "3. selected_pair_count_sum not too small",
        "4. dynamic_roi_proxy_overall higher",
        "",
        "## Top 20",
        "",
        top_table,
    ]
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(md_lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
