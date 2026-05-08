from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _to_bool(s: pd.Series) -> pd.Series:
    return _to_num(s).fillna(0).astype(int).astype(bool)


def main() -> None:
    ap = argparse.ArgumentParser(description="Summarize expanded dynamic conditions with result-aware metrics.")
    ap.add_argument("--input-csv", type=Path, required=True, help="expanded_dynamic_candidate_conditions.csv")
    ap.add_argument("--pairs-csv", type=Path, default=None, help="pair-level csv with actual_wide_hit/wide_payout and flags")
    ap.add_argument("--quality-ok-only", action="store_true", help="aggregate using result_quality_status=ok rows only")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.input_csv)
    needed = [
        "rule_non_overlap_dynamic_pair_count",
        "rule_non_overlap_dynamic_hit_count",
        "rule_non_overlap_dynamic_hit_rate",
        "rule_non_overlap_dynamic_total_payout",
        "rule_non_overlap_dynamic_roi_proxy",
        "dynamic_hit_rate",
        "dynamic_roi_proxy",
        "rule_hit_rate",
        "rule_roi_proxy",
        "dynamic_minus_rule_roi",
        "dynamic_minus_rule_hit_rate",
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = pd.NA

    extra_lines: list[str] = []
    if args.pairs_csv is not None and args.pairs_csv.exists():
        p = pd.read_csv(args.pairs_csv)
        for c in ["actual_wide_hit", "wide_payout", "pair_selected_flag", "model_dynamic_selected_flag", "result_quality_status", "raw_actual_wide_hit"]:
            if c not in p.columns:
                p[c] = pd.NA
        raw_cov = float(_to_num(p["raw_actual_wide_hit"]).notna().mean()) if len(p) else None
        q_cov = float(_to_num(p["actual_wide_hit"]).notna().mean()) if len(p) else None
        qok_races = int(p.loc[p["result_quality_status"].astype(str) == "ok", "race_id"].nunique()) if "race_id" in p.columns else 0
        qng_races = int(p.loc[p["result_quality_status"].astype(str) != "ok", "race_id"].nunique()) if "race_id" in p.columns else 0
        if args.quality_ok_only:
            p = p[p["result_quality_status"].astype(str) == "ok"].copy()
        dyn = p[_to_bool(p["model_dynamic_selected_flag"])]
        rule = p[_to_bool(p["pair_selected_flag"])]
        dyn_h = _to_num(dyn["actual_wide_hit"])
        rule_h = _to_num(rule["actual_wide_hit"])
        dyn_p = _to_num(dyn["wide_payout"])
        rule_p = _to_num(rule["wide_payout"])
        dyn_bets = int(len(dyn))
        rule_bets = int(len(rule))
        dyn_hit = int(dyn_h.fillna(0).sum()) if dyn_bets else 0
        rule_hit = int(rule_h.fillna(0).sum()) if rule_bets else 0
        dyn_ret = float((dyn_p.fillna(0) * (dyn_h.fillna(0) > 0).astype(float)).sum()) if dyn_bets else 0.0
        rule_ret = float((rule_p.fillna(0) * (rule_h.fillna(0) > 0).astype(float)).sum()) if rule_bets else 0.0
        dyn_roi = (dyn_ret / (dyn_bets * 100.0)) if dyn_bets else None
        rule_roi = (rule_ret / (rule_bets * 100.0)) if rule_bets else None
        # stamp summary metrics into all rows (evaluation helper)
        df["dynamic_hit_rate"] = (dyn_hit / dyn_bets) if dyn_bets else pd.NA
        df["dynamic_roi_proxy"] = dyn_roi
        df["rule_hit_rate"] = (rule_hit / rule_bets) if rule_bets else pd.NA
        df["rule_roi_proxy"] = rule_roi
        df["dynamic_minus_rule_roi"] = (dyn_roi - rule_roi) if dyn_roi is not None and rule_roi is not None else pd.NA
        df["dynamic_minus_rule_hit_rate"] = ((dyn_hit / dyn_bets) - (rule_hit / rule_bets)) if dyn_bets and rule_bets else pd.NA
        extra_lines.extend(
            [
                f"- quality_ok_only: {args.quality_ok_only}",
                f"- raw_actual_wide_hit_coverage: {raw_cov}",
                f"- quality_filtered_actual_wide_hit_coverage: {q_cov}",
                f"- quality_ok_race_count: {qok_races}",
                f"- quality_ng_race_count: {qng_races}",
                f"- dynamic_candidate_count: {dyn_bets}",
                f"- rule_candidate_count: {rule_bets}",
            ]
        )

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out_csv, index=False, encoding="utf-8")
    try:
        tbl = df[needed + [c for c in ["edge_variant", "variant_threshold", "min_score", "min_gap", "default_k", "max_k"] if c in df.columns]].to_markdown(index=False)
    except Exception:
        tbl = df.to_string(index=False)
    lines = ["# expanded_dynamic_candidate_conditions_with_results", ""]
    if extra_lines:
        lines.extend(extra_lines)
        lines.append("")
    lines.append(tbl)
    args.out_md.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
