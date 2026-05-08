from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import duckdb

BASE = Path(r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai")
REPORT = BASE / "reports" / "2024_eval_full_v5"
PAIR = REPORT / "pair_shadow_pair_comparison_expanded_real_odds_2025_2026_with_results_external_priority.csv"
DB = BASE / "data" / "warehouse" / "aikeiba.duckdb"

VALUE_THR = 0.0591
EDGE_THR = 0.02
START = "2025-04-20"
END = "2026-04-26"


def to_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int).astype(bool)


def calc_metrics(g: pd.DataFrame) -> dict:
    if len(g) == 0:
        return dict(candidate_count=0, hit_count=0, hit_rate=np.nan, total_payout=0.0, cost=0.0, roi=np.nan, profit=0.0)
    hit = pd.to_numeric(g["actual_wide_hit"], errors="coerce").fillna(0)
    payout = pd.to_numeric(g["wide_payout"], errors="coerce").fillna(0)
    wins = (hit > 0).astype(float)
    total_payout = float((payout * wins).sum())
    cost = float(len(g) * 100)
    roi = total_payout / cost if cost > 0 else np.nan
    return dict(
        candidate_count=int(len(g)),
        hit_count=int(hit.sum()),
        hit_rate=float(wins.mean()),
        total_payout=total_payout,
        cost=cost,
        roi=float(roi),
        profit=float(total_payout - cost),
    )


def summarize_by(df: pd.DataFrame, key: str) -> pd.DataFrame:
    rows = []
    for k, g in df.groupby(key, dropna=False):
        base = calc_metrics(g)
        fil = calc_metrics(g[g["rule_filter_shadow_flag"]])
        rows.append(
            {
                key: k,
                "original_candidate_count": base["candidate_count"],
                "filtered_candidate_count": fil["candidate_count"],
                "buy_reduction_rate": 1 - (fil["candidate_count"] / base["candidate_count"]) if base["candidate_count"] else np.nan,
                "original_roi": base["roi"],
                "filtered_roi": fil["roi"],
                "original_profit": base["profit"],
                "filtered_profit": fil["profit"],
                "roi_diff_filtered_minus_original": fil["roi"] - base["roi"] if pd.notna(fil["roi"]) and pd.notna(base["roi"]) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    REPORT.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(PAIR)
    df = df[to_bool(df["pair_selected_flag"])].copy()
    df = df[df["result_quality_status"].astype(str) == "ok"].copy()
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df[(df["race_date"] >= pd.Timestamp(START)) & (df["race_date"] <= pd.Timestamp(END))].copy()

    con = duckdb.connect(str(DB), read_only=True)
    meta = con.execute(
        """
        select race_id::varchar as race_id, venue, surface, distance
        from races
        where race_date between cast(? as date) and cast(? as date)
        """,
        [START, END],
    ).fetchdf()
    con.close()
    df = df.merge(meta, on="race_id", how="left")

    df["pair_value_score"] = pd.to_numeric(df["pair_value_score"], errors="coerce")
    df["pair_edge"] = pd.to_numeric(df["pair_edge"], errors="coerce")
    df["rule_filter_shadow_flag"] = (df["pair_value_score"] >= VALUE_THR) & (df["pair_edge"] >= EDGE_THR)

    # distance bucket
    df["distance_bucket"] = pd.cut(
        pd.to_numeric(df["distance"], errors="coerce"),
        bins=[0, 1400, 1800, 2200, 4000],
        labels=["sprint", "mile", "middle", "long"],
        include_lowest=True,
    ).astype(str)
    df["month"] = df["race_date"].dt.to_period("M").astype(str)

    # 1) detail daily
    detail_rows = []
    for d, g in df.groupby(df["race_date"].dt.strftime("%Y-%m-%d")):
        base = calc_metrics(g)
        fil = calc_metrics(g[g["rule_filter_shadow_flag"]])
        detail_rows.append(
            {
                "date": d,
                "original_rule_candidate_count": base["candidate_count"],
                "filtered_rule_candidate_count": fil["candidate_count"],
                "removed_candidate_count": base["candidate_count"] - fil["candidate_count"],
                "buy_reduction_rate": 1 - (fil["candidate_count"] / base["candidate_count"]) if base["candidate_count"] else np.nan,
                "original_hit_rate": base["hit_rate"],
                "filtered_hit_rate": fil["hit_rate"],
                "original_roi": base["roi"],
                "filtered_roi": fil["roi"],
                "original_profit": base["profit"],
                "filtered_profit": fil["profit"],
                "race_count_original": int(g["race_id"].nunique()),
                "race_count_with_filtered_candidates": int(g.loc[g["rule_filter_shadow_flag"], "race_id"].nunique()),
                "race_count_zero_after_filter": int(g["race_id"].nunique() - g.loc[g["rule_filter_shadow_flag"], "race_id"].nunique()),
                "few_filtered_candidates_flag": int(fil["candidate_count"] < 5),
            }
        )
    detail = pd.DataFrame(detail_rows).sort_values("date")
    detail.to_csv(REPORT / "rule_filter_shadow_backtest_detail_real_odds.csv", index=False, encoding="utf-8-sig")
    (REPORT / "rule_filter_shadow_backtest_detail_real_odds.md").write_text(
        "# rule_filter_shadow_backtest_detail_real_odds\n\n" + detail.to_string(index=False), encoding="utf-8"
    )

    # 2) monthly
    monthly = summarize_by(df, "month").sort_values("month")
    monthly.to_csv(REPORT / "rule_filter_shadow_monthly_summary_real_odds.csv", index=False, encoding="utf-8-sig")

    # 3) venue
    venue = summarize_by(df, "venue").sort_values("filtered_profit", ascending=False)
    venue.to_csv(REPORT / "rule_filter_shadow_venue_summary_real_odds.csv", index=False, encoding="utf-8-sig")

    # 4) surface
    surface = summarize_by(df, "surface").sort_values("filtered_profit", ascending=False)
    surface.to_csv(REPORT / "rule_filter_shadow_surface_summary_real_odds.csv", index=False, encoding="utf-8-sig")

    # 5) distance
    dist = summarize_by(df, "distance_bucket").sort_values("filtered_profit", ascending=False)
    dist.to_csv(REPORT / "rule_filter_shadow_distance_summary_real_odds.csv", index=False, encoding="utf-8-sig")

    # 6) big-hit dependency
    # day-level contribution in filtered stream
    filtered_daily_profit = detail[["date", "filtered_profit"]].copy()
    filtered_daily_profit["date"] = pd.to_datetime(filtered_daily_profit["date"])
    filtered_daily_profit["month"] = filtered_daily_profit["date"].dt.to_period("M").astype(str)
    dep_rows = []
    for m, g in filtered_daily_profit.groupby("month"):
        total = float(g["filtered_profit"].sum())
        top_day = g.sort_values("filtered_profit", ascending=False).head(1)
        top_profit = float(top_day["filtered_profit"].iloc[0]) if len(top_day) else 0.0
        top_date = top_day["date"].dt.strftime("%Y-%m-%d").iloc[0] if len(top_day) else ""
        ratio = (top_profit / total) if total > 0 else np.nan
        dep_rows.append(
            {
                "month": m,
                "filtered_profit_total": total,
                "top_day": top_date,
                "top_day_profit": top_profit,
                "top_day_dependency_ratio": ratio,
                "one_day_hit_dependency_flag": int(pd.notna(ratio) and ratio >= 0.5),
            }
        )
    dep = pd.DataFrame(dep_rows).sort_values("month")
    dep.to_csv(REPORT / "rule_filter_shadow_big_hit_dependency_real_odds.csv", index=False, encoding="utf-8-sig")

    # 8) comparison + final report
    all_orig = calc_metrics(df)
    all_fil = calc_metrics(df[df["rule_filter_shadow_flag"]])
    monthly_stable = int((pd.to_numeric(monthly["filtered_roi"], errors="coerce") > 1.0).sum())
    monthly_total = int(len(monthly))

    venue_bias = venue[["venue", "filtered_roi", "filtered_profit"]].head(5).to_string(index=False) if len(venue) else "(none)"
    surface_bias = surface[["surface", "filtered_roi", "filtered_profit"]].to_string(index=False) if len(surface) else "(none)"

    dep_flag_months = int(dep["one_day_hit_dependency_flag"].sum()) if len(dep) else 0

    lines = [
        "# rule_filter_shadow_final_backtest_report",
        "",
        f"- period: {START} to {END}",
        f"- fixed_condition: pair_value_score >= {VALUE_THR} and pair_edge >= {EDGE_THR}",
        "",
        "## Overall",
        f"- original_roi: {all_orig['roi']}",
        f"- filtered_roi: {all_fil['roi']}",
        f"- original_profit: {all_orig['profit']}",
        f"- filtered_profit: {all_fil['profit']}",
        f"- buy_reduction_rate: {1 - (all_fil['candidate_count']/all_orig['candidate_count']) if all_orig['candidate_count'] else np.nan}",
        "",
        "## Monthly Stability",
        f"- months_with_filtered_roi_gt_1: {monthly_stable}/{monthly_total}",
        monthly[["month", "filtered_roi", "filtered_profit", "buy_reduction_rate"]].to_string(index=False) if len(monthly) else "(none)",
        "",
        "## Venue Bias (Top filtered profit)",
        venue_bias,
        "",
        "## Surface Bias",
        surface_bias,
        "",
        "## Big Hit Dependency",
        f"- one_day_hit_dependency_month_count: {dep_flag_months}",
        dep.to_string(index=False) if len(dep) else "(none)",
        "",
        "## Stability vs Original Rule",
        f"- ROI stability: {'improved' if all_fil['roi'] > all_orig['roi'] else 'not improved'}",
        f"- Profit stability: {'reduced' if all_fil['profit'] < all_orig['profit'] else 'not reduced'}",
        "- decision: shadow continue (do not switch production yet)",
    ]
    (REPORT / "rule_filter_shadow_final_backtest_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("done")


if __name__ == "__main__":
    main()
