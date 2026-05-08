from __future__ import annotations

import argparse
from pathlib import Path


KEY_COLS = ["min_score", "min_edge", "min_gap", "default_k", "max_k"]


def _parse_inputs(inputs: str, glob_pattern: str | None):
    paths: list[Path] = []
    if inputs.strip():
        for x in inputs.split(","):
            p = Path(x.strip())
            if p.as_posix():
                paths.append(p)
    if glob_pattern:
        root = Path(".")
        for p in root.glob(glob_pattern):
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
    ap = argparse.ArgumentParser(description="Aggregate multi-day model_dynamic threshold grid results.")
    ap.add_argument("--inputs", default="", help="Comma-separated daily grid summary CSV paths")
    ap.add_argument("--glob", dest="glob_pattern", default="", help="Optional glob pattern (e.g. racing_ai/reports/**/model_dynamic_threshold_grid_summary.csv)")
    ap.add_argument("--out-csv", type=Path, default=Path("racing_ai/reports/model_dynamic_threshold_grid_multi_day_summary.csv"))
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/model_dynamic_threshold_grid_multi_day_summary.md"))
    args = ap.parse_args()

    import pandas as pd

    paths = _parse_inputs(args.inputs, args.glob_pattern if args.glob_pattern else None)
    if not paths:
        raise SystemExit("no input files. use --inputs or --glob")
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        raise SystemExit("missing input files:\n- " + "\n- ".join(missing))

    frames = []
    for p in paths:
        df = pd.read_csv(p)
        df["source_file"] = str(p)
        frames.append(df)
    all_df = pd.concat(frames, ignore_index=True)

    required = KEY_COLS + [
        "buy_race_count",
        "skip_race_count",
        "selected_pair_count",
        "avg_selected_pairs_per_buy_race",
        "avg_pair_edge_selected",
        "rule_model_overlap_avg",
        "dynamic_hit_count",
        "dynamic_bet_count",
        "dynamic_total_payout",
        "rule_hit_count",
        "rule_bet_count",
        "rule_total_payout",
        "dynamic_non_overlap_count",
        "dynamic_non_overlap_hit_rate",
        "dynamic_non_overlap_roi_proxy",
    ]
    for c in required:
        if c not in all_df.columns:
            all_df[c] = pd.NA

    num_cols = [c for c in required if c not in KEY_COLS]
    for c in num_cols:
        all_df[c] = pd.to_numeric(all_df[c], errors="coerce")

    grouped = all_df.groupby(KEY_COLS, dropna=False)
    out = grouped.agg(
        eval_day_count=("source_file", "nunique"),
        total_buy_race_count=("buy_race_count", "sum"),
        total_skip_race_count=("skip_race_count", "sum"),
        total_selected_pair_count=("selected_pair_count", "sum"),
        avg_selected_pairs_per_buy_race=("avg_selected_pairs_per_buy_race", "mean"),
        avg_pair_edge_selected_mean=("avg_pair_edge_selected", "mean"),
        rule_model_overlap_avg_mean=("rule_model_overlap_avg", "mean"),
        dynamic_hit_count_sum=("dynamic_hit_count", "sum"),
        dynamic_bet_count_sum=("dynamic_bet_count", "sum"),
        dynamic_total_payout_sum=("dynamic_total_payout", "sum"),
        rule_hit_count_sum=("rule_hit_count", "sum"),
        rule_bet_count_sum=("rule_bet_count", "sum"),
        rule_total_payout_sum=("rule_total_payout", "sum"),
        rule_non_overlap_dynamic_count_sum=("dynamic_non_overlap_count", "sum"),
    ).reset_index()

    out["dynamic_cost_sum"] = out["dynamic_bet_count_sum"] * 100.0
    out["rule_cost_sum"] = out["rule_bet_count_sum"] * 100.0
    out["dynamic_hit_rate_overall"] = out["dynamic_hit_count_sum"] / out["dynamic_bet_count_sum"].replace(0, pd.NA)
    out["rule_hit_rate_overall"] = out["rule_hit_count_sum"] / out["rule_bet_count_sum"].replace(0, pd.NA)
    out["dynamic_roi_proxy_overall"] = out["dynamic_total_payout_sum"] / out["dynamic_cost_sum"].replace(0, pd.NA)
    out["rule_roi_proxy_overall"] = out["rule_total_payout_sum"] / out["rule_cost_sum"].replace(0, pd.NA)
    out["dynamic_minus_rule_roi"] = out["dynamic_roi_proxy_overall"] - out["rule_roi_proxy_overall"]
    out["dynamic_minus_rule_hit_rate"] = out["dynamic_hit_rate_overall"] - out["rule_hit_rate_overall"]

    # non-overlap aggregated by sum of implied payouts/cost from daily proxy when available
    no_df = all_df.copy()
    no_df["dynamic_non_overlap_count"] = pd.to_numeric(no_df["dynamic_non_overlap_count"], errors="coerce").fillna(0)
    no_df["dynamic_non_overlap_hit_rate"] = pd.to_numeric(no_df["dynamic_non_overlap_hit_rate"], errors="coerce")
    no_df["dynamic_non_overlap_roi_proxy"] = pd.to_numeric(no_df["dynamic_non_overlap_roi_proxy"], errors="coerce")
    no_df["no_hit_count_proxy"] = no_df["dynamic_non_overlap_count"] * no_df["dynamic_non_overlap_hit_rate"]
    no_df["no_cost_proxy"] = no_df["dynamic_non_overlap_count"] * 100.0
    no_df["no_payout_proxy"] = no_df["dynamic_non_overlap_roi_proxy"] * no_df["no_cost_proxy"]
    no_grp = no_df.groupby(KEY_COLS, dropna=False).agg(
        no_hit_count_proxy_sum=("no_hit_count_proxy", "sum"),
        no_count_sum=("dynamic_non_overlap_count", "sum"),
        no_cost_proxy_sum=("no_cost_proxy", "sum"),
        no_payout_proxy_sum=("no_payout_proxy", "sum"),
    ).reset_index()
    out = out.merge(no_grp, on=KEY_COLS, how="left")
    out["rule_non_overlap_dynamic_hit_rate_overall"] = out["no_hit_count_proxy_sum"] / out["no_count_sum"].replace(0, pd.NA)
    out["rule_non_overlap_dynamic_roi_proxy_overall"] = out["no_payout_proxy_sum"] / out["no_cost_proxy_sum"].replace(0, pd.NA)

    out = out.sort_values(
        ["dynamic_minus_rule_roi", "dynamic_roi_proxy_overall", "eval_day_count", "dynamic_bet_count_sum"],
        ascending=[False, False, False, False],
        na_position="last",
    )

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    top = out.head(10)
    try:
        top_table = top.to_markdown(index=False)
    except Exception:
        top_table = top.to_string(index=False)
    md_lines = [
        "# model_dynamic_threshold_grid_multi_day_summary",
        "",
        f"- input_files: {len(paths)}",
        f"- output_csv: {args.out_csv}",
        "",
        "## Ranking Rule",
        "1. dynamic_roi_proxy_overall > rule_roi_proxy_overall",
        "2. dynamic_bet_count_sum not too small",
        "3. eval_day_count larger",
        "4. dynamic_hit_rate_overall not too low",
        "5. rule_model_overlap_avg_mean not too high",
        "6. avg_pair_edge_selected_mean positive",
        "",
        "## Top 10",
        "",
        top_table,
    ]
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(md_lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
