from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

BASE = Path(r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai")
REP = BASE / "reports" / "2024_eval_full_v5"
PAIR = REP / "pair_shadow_pair_comparison_expanded_real_odds_2025_2026_with_results_external_priority.csv"
SPLITS = REP / "walk_forward_splits_real_odds_2025_2026.csv"


def to_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int).astype(bool)


def prep() -> pd.DataFrame:
    df = pd.read_csv(PAIR)
    df = df[to_bool(df["pair_selected_flag"])].copy()
    df = df[df["result_quality_status"].astype(str) == "ok"].copy()
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df["pair_value_score"] = pd.to_numeric(df["pair_value_score"], errors="coerce")
    df["pair_edge"] = pd.to_numeric(df["pair_edge"], errors="coerce")
    df["pair_market_implied_prob"] = pd.to_numeric(df["pair_market_implied_prob"], errors="coerce")
    # popularity bucket computed per race to avoid temporal leakage
    pct = df.groupby("race_id")["pair_market_implied_prob"].rank(method="first", pct=True)
    bins = pd.cut(pct, bins=[0, 0.2, 0.4, 0.6, 0.8, 1.0], labels=["longshot", "semi_long", "mid", "semi_pop", "popular"], include_lowest=True)
    df["popularity_bucket"] = bins.astype(str)
    df["date"] = df["race_date"].dt.strftime("%Y-%m-%d")
    df["month"] = df["race_date"].dt.to_period("M").astype(str)
    return df


def metrics(g: pd.DataFrame) -> dict:
    if len(g) == 0:
        return dict(candidate_count=0, hit_rate=np.nan, roi=np.nan, profit=0.0)
    hit = pd.to_numeric(g["actual_wide_hit"], errors="coerce").fillna(0)
    payout = pd.to_numeric(g["wide_payout"], errors="coerce").fillna(0)
    wins = (hit > 0).astype(float)
    total = float((payout * wins).sum())
    cost = float(len(g) * 100)
    return dict(candidate_count=int(len(g)), hit_rate=float(wins.mean()), roi=(total / cost if cost > 0 else np.nan), profit=total - cost)


def stability(g: pd.DataFrame) -> dict:
    if len(g) == 0:
        return dict(monthly_positive_count=0, top_day_dependency_ratio=np.nan)
    hit = pd.to_numeric(g["actual_wide_hit"], errors="coerce").fillna(0)
    payout = pd.to_numeric(g["wide_payout"], errors="coerce").fillna(0)
    wins = (hit > 0).astype(float)
    x = g.copy()
    x["profit"] = payout * wins - 100.0
    m = x.groupby("month", as_index=False)["profit"].sum()
    monthly_positive_count = int((m["profit"] > 0).sum())
    d = x.groupby("date", as_index=False)["profit"].sum()
    total = float(d["profit"].sum())
    mx = float(d["profit"].max()) if len(d) else 0.0
    pos_total = float(d.loc[d["profit"] > 0, "profit"].sum())
    dep = (mx / pos_total) if pos_total > 0 else np.nan
    return dict(monthly_positive_count=monthly_positive_count, top_day_dependency_ratio=dep)


def apply_condition(df: pd.DataFrame, name: str) -> pd.DataFrame:
    if name == "A_fixed_shadow":
        return df[(df["pair_value_score"] >= 0.0591) & (df["pair_edge"] >= 0.02)].copy()
    if name == "B_risk_adjusted":
        return df[
            (df["pair_value_score"] >= 0.04893)
            & (df["pair_edge"] >= 0.02)
            & (df["popularity_bucket"].isin(["longshot", "semi_long", "mid"]))
        ].copy()
    raise ValueError(name)


def main() -> None:
    df = prep()
    splits = pd.read_csv(SPLITS)
    rows = []
    for _, sp in splits.iterrows():
        te_s = pd.Timestamp(sp["test_start"])
        te_e = pd.Timestamp(sp["test_end"])
        test = df[(df["race_date"] >= te_s) & (df["race_date"] <= te_e)].copy()
        base = metrics(test)
        for cname in ["A_fixed_shadow", "B_risk_adjusted"]:
            t = apply_condition(test, cname)
            m = metrics(t)
            s = stability(t)
            rows.append(
                {
                    "fold_id": sp["fold_id"],
                    "train_start": sp["train_start"],
                    "train_end": sp["train_end"],
                    "test_start": sp["test_start"],
                    "test_end": sp["test_end"],
                    "condition_name": cname,
                    "test_candidate_count": m["candidate_count"],
                    "test_hit_rate": m["hit_rate"],
                    "test_roi": m["roi"],
                    "test_profit": m["profit"],
                    "test_monthly_positive_count": s["monthly_positive_count"],
                    "test_top_day_dependency_ratio": s["top_day_dependency_ratio"],
                    "baseline_rule_roi": base["roi"],
                    "baseline_rule_profit": base["profit"],
                    "condition_minus_baseline_roi": (m["roi"] - base["roi"]) if pd.notna(m["roi"]) and pd.notna(base["roi"]) else np.nan,
                    "condition_minus_baseline_profit": m["profit"] - base["profit"],
                }
            )

    ab = pd.DataFrame(rows)
    ab.to_csv(REP / "rule_filter_shadow_ab_walk_forward_real_odds.csv", index=False, encoding="utf-8-sig")
    (REP / "rule_filter_shadow_ab_walk_forward_real_odds.md").write_text(
        "# rule_filter_shadow_ab_walk_forward_real_odds\n\n" + ab.to_string(index=False), encoding="utf-8"
    )

    sum_rows = []
    for cname, g in ab.groupby("condition_name"):
        roi = pd.to_numeric(g["test_roi"], errors="coerce")
        prof = pd.to_numeric(g["test_profit"], errors="coerce")
        dep = pd.to_numeric(g["test_top_day_dependency_ratio"], errors="coerce")
        cnt = pd.to_numeric(g["test_candidate_count"], errors="coerce")
        diff_roi = pd.to_numeric(g["condition_minus_baseline_roi"], errors="coerce")
        sum_rows.append(
            {
                "condition_name": cname,
                "fold_count": int(len(g)),
                "positive_fold_count": int((roi > 1.0).sum()),
                "negative_fold_count": int((roi < 1.0).sum()),
                "roi_mean": float(roi.mean()),
                "roi_median": float(roi.median()),
                "profit_sum": float(prof.sum()),
                "median_candidate_count": float(cnt.median()),
                "avg_dependency": float(dep.mean()),
                "baseline_outperform_fold_count": int((diff_roi > 0).sum()),
                "worst_fold_roi": float(roi.min()),
                "worst_fold_profit": float(prof.min()),
            }
        )
    summary = pd.DataFrame(sum_rows).sort_values("roi_mean", ascending=False)
    summary.to_csv(REP / "rule_filter_shadow_ab_walk_forward_summary.csv", index=False, encoding="utf-8-sig")
    (REP / "rule_filter_shadow_ab_walk_forward_summary.md").write_text(
        "# rule_filter_shadow_ab_walk_forward_summary\n\n" + summary.to_string(index=False), encoding="utf-8"
    )

    sA = summary[summary["condition_name"] == "A_fixed_shadow"].iloc[0]
    sB = summary[summary["condition_name"] == "B_risk_adjusted"].iloc[0]

    b_stable = (
        (sB["roi_mean"] > 1.2)
        and (sB["roi_median"] > 1.0)
        and (sB["profit_sum"] > 0)
        and (sB["baseline_outperform_fold_count"] >= sA["baseline_outperform_fold_count"])
        and (sB["avg_dependency"] <= sA["avg_dependency"])
        and (sB["median_candidate_count"] >= 40)
        and (sB["worst_fold_profit"] > -5000)
    )

    lines = [
        "# rule_filter_shadow_ab_walk_forward_report",
        "",
        "## A/B Stability",
        f"- A roi_mean/median: {sA['roi_mean']} / {sA['roi_median']}",
        f"- B roi_mean/median: {sB['roi_mean']} / {sB['roi_median']}",
        f"- A profit_sum: {sA['profit_sum']}",
        f"- B profit_sum: {sB['profit_sum']}",
        f"- A avg_dependency: {sA['avg_dependency']}",
        f"- B avg_dependency: {sB['avg_dependency']}",
        f"- A median_candidate_count: {sA['median_candidate_count']}",
        f"- B median_candidate_count: {sB['median_candidate_count']}",
        "",
        f"- fixed shadow と risk-adjusted のどちらが安定か: {'B_risk_adjusted' if b_stable else 'A_fixed_shadow'}",
        f"- ROI重視なら: {'B_risk_adjusted' if sB['roi_mean'] > sA['roi_mean'] else 'A_fixed_shadow'}",
        f"- 利益額重視なら: {'B_risk_adjusted' if sB['profit_sum'] > sA['profit_sum'] else 'A_fixed_shadow'}",
        f"- 大当たり依存は改善したか: {'Yes' if sB['avg_dependency'] < sA['avg_dependency'] else 'No'}",
        f"- risk-adjustedは過学習っぽいか: {'Low-to-moderate' if b_stable else 'Possible'}",
        "- 本番採用ではなくshadow継続か: Yes",
        f"- 次の30日監視の採用候補: {'B_risk_adjusted' if sB['avg_dependency'] < sA['avg_dependency'] else 'A_fixed_shadow'}",
    ]
    (REP / "rule_filter_shadow_ab_walk_forward_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("done")


if __name__ == "__main__":
    main()
