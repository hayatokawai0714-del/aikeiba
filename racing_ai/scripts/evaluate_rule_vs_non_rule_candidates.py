from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _to_bool(s: pd.Series) -> pd.Series:
    n = _to_num(s)
    return n.fillna(0).astype(int).astype(bool)


def _metrics(df: pd.DataFrame) -> dict:
    if len(df) == 0:
        return {"candidate_count": 0, "hit_count": None, "hit_rate": None, "total_payout": None, "cost": 0.0, "roi_proxy": None}
    hit = _to_num(df.get("actual_wide_hit", pd.Series([None] * len(df))))
    pay = _to_num(df.get("wide_payout", pd.Series([None] * len(df))))
    h = int(hit.fillna(0).sum()) if hit.notna().any() else None
    payout = float((pay.fillna(0) * (hit.fillna(0) > 0).astype(float)).sum()) if pay.notna().any() else None
    cost = float(len(df) * 100.0)
    roi = payout / cost if payout is not None and cost > 0 else None
    hit_cov = float(hit.notna().mean()) if len(df) > 0 else None
    pay_cov = float(pay.notna().mean()) if len(df) > 0 else None
    return {
        "candidate_count": int(len(df)),
        "hit_count": h,
        "hit_rate": (h / len(df)) if h is not None and len(df) > 0 else None,
        "total_payout": payout,
        "cost": cost,
        "roi_proxy": roi,
        "hit_label_coverage_rate": hit_cov,
        "payout_coverage_rate": pay_cov,
        "avg_payout_per_hit": (payout / h) if payout is not None and h not in (None, 0) else None,
        "avg_pair_model_score": float(_to_num(df.get("pair_model_score", pd.Series([None] * len(df)))).mean()),
        "avg_pair_value_score": float(_to_num(df.get("pair_value_score", pd.Series([None] * len(df)))).mean()),
        "avg_pair_market_implied_prob": float(_to_num(df.get("pair_market_implied_prob", pd.Series([None] * len(df)))).mean()),
        "avg_pair_edge": float(_to_num(df.get("pair_edge", pd.Series([None] * len(df)))).mean()),
        "avg_pair_edge_ratio": float(_to_num(df.get("pair_edge_ratio", pd.Series([None] * len(df)))).mean()),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare rule-selected vs non-rule candidate groups.")
    ap.add_argument("--input-csv", type=Path, required=True, help="expanded pair comparison csv")
    ap.add_argument("--out-csv", type=Path, default=Path("racing_ai/reports/rule_vs_non_rule_candidate_evaluation.csv"))
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/rule_vs_non_rule_candidate_evaluation.md"))
    ap.add_argument("--quality-ok-only", action="store_true", help="use only result_quality_status=ok rows")
    args = ap.parse_args()

    df = pd.read_csv(args.input_csv)
    for c in ["pair_selected_flag", "pair_model_score", "pair_value_score", "pair_market_implied_prob", "actual_wide_hit", "wide_payout", "model_dynamic_selected_flag", "pair_model_rank_in_race"]:
        if c not in df.columns:
            df[c] = pd.NA
    if "result_quality_status" not in df.columns:
        df["result_quality_status"] = pd.NA

    raw_cov = float(_to_num(df.get("raw_actual_wide_hit", pd.Series([None] * len(df)))).notna().mean()) if len(df) else None
    qual_cov = float(_to_num(df.get("actual_wide_hit", pd.Series([None] * len(df)))).notna().mean()) if len(df) else None
    qok_races = int(df.loc[df["result_quality_status"].astype(str) == "ok", "race_id"].nunique()) if "race_id" in df.columns else 0
    qng_races = int(df.loc[df["result_quality_status"].astype(str) != "ok", "race_id"].nunique()) if "race_id" in df.columns else 0
    qok_cands = int((df["result_quality_status"].astype(str) == "ok").sum())
    qng_cands = int((df["result_quality_status"].astype(str) != "ok").sum())
    if args.quality_ok_only:
        df = df[df["result_quality_status"].astype(str) == "ok"].copy()

    rule_mask = _to_bool(df["pair_selected_flag"])
    dyn_mask = _to_bool(df.get("model_dynamic_selected_flag", pd.Series([0] * len(df))))
    rank = _to_num(df["pair_model_rank_in_race"])
    non_rule = ~rule_mask

    groups = {
        "rule_selected": df[rule_mask],
        "non_rule_model_top1": df[non_rule & rank.le(1).fillna(False)],
        "non_rule_model_top3": df[non_rule & rank.le(3).fillna(False)],
        "model_dynamic_non_overlap": df[dyn_mask & non_rule],
    }

    rows = []
    for name, g in groups.items():
        m = _metrics(g)
        m["group"] = name
        rows.append(m)
    out = pd.DataFrame(rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    try:
        tbl = out.to_markdown(index=False)
    except Exception:
        tbl = out.to_string(index=False)
    md = [
        "# rule_vs_non_rule_candidate_evaluation",
        "",
        f"- quality_ok_only: {args.quality_ok_only}",
        f"- raw_actual_wide_hit_coverage: {raw_cov}",
        f"- quality_filtered_actual_wide_hit_coverage: {qual_cov}",
        f"- quality_ok_race_count: {qok_races}",
        f"- quality_ng_race_count: {qng_races}",
        f"- quality_ok_candidate_count: {qok_cands}",
        f"- quality_ng_candidate_count: {qng_cands}",
        "",
        tbl,
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
