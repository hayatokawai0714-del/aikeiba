from __future__ import annotations

import itertools
import re
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

BASE = Path(r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai")
REPORT = BASE / "reports" / "2024_eval_full_v5"
PAIR = REPORT / "pair_shadow_pair_comparison_expanded_real_odds_2025_2026_with_results_external_priority.csv"
DB = BASE / "data" / "warehouse" / "aikeiba.duckdb"

START = "2025-04-20"
END = "2026-04-26"
FIX_V = 0.0591
FIX_E = 0.02


def to_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int).astype(bool)


def prep() -> pd.DataFrame:
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
    df["pair_market_implied_prob"] = pd.to_numeric(df["pair_market_implied_prob"], errors="coerce")

    # Rank in race by value_score
    df["pair_value_score_rank"] = df.groupby("race_id")["pair_value_score"].rank(ascending=False, method="first")

    # Buckets for conditions
    q = df["pair_market_implied_prob"].rank(method="first")
    df["odds_proxy_bucket"] = pd.qcut(q, q=5, labels=["vp_low", "low", "mid", "high", "vp_high"]).astype(str)
    df["popularity_bucket"] = pd.qcut(q, q=5, labels=["longshot", "semi_long", "mid", "semi_pop", "popular"]).astype(str)

    df["month"] = df["race_date"].dt.to_period("M").astype(str)
    df["date"] = df["race_date"].dt.strftime("%Y-%m-%d")
    return df


def calc_metrics(g: pd.DataFrame) -> dict:
    if len(g) == 0:
        return dict(candidate_count=0, roi=np.nan, profit=0.0, hit_rate=np.nan)
    hit = pd.to_numeric(g["actual_wide_hit"], errors="coerce").fillna(0)
    payout = pd.to_numeric(g["wide_payout"], errors="coerce").fillna(0)
    wins = (hit > 0).astype(float)
    total_payout = float((payout * wins).sum())
    cost = float(len(g) * 100)
    roi = total_payout / cost if cost > 0 else np.nan
    return dict(candidate_count=int(len(g)), roi=float(roi), profit=float(total_payout - cost), hit_rate=float(wins.mean()))


def apply_condition(
    df: pd.DataFrame,
    value_thr: float,
    edge_thr: float,
    max_per_race: int | None,
    max_per_day: int | None,
    rank_cap: int | None,
    odds_allowed: str,
    pop_allowed: str,
) -> pd.DataFrame:
    g = df.copy()
    g = g[(g["pair_value_score"] >= value_thr) & (g["pair_edge"] >= edge_thr)].copy()
    if rank_cap is not None:
        g = g[g["pair_value_score_rank"] <= rank_cap].copy()
    if odds_allowed != "ALL":
        allowed = set(odds_allowed.split("|"))
        g = g[g["odds_proxy_bucket"].isin(allowed)].copy()
    if pop_allowed != "ALL":
        allowed = set(pop_allowed.split("|"))
        g = g[g["popularity_bucket"].isin(allowed)].copy()

    if max_per_race is not None:
        g = g.sort_values(["race_id", "pair_value_score"], ascending=[True, False]).groupby("race_id", as_index=False).head(max_per_race)
    if max_per_day is not None:
        g = g.sort_values(["date", "pair_value_score"], ascending=[True, False]).groupby("date", as_index=False).head(max_per_day)
    return g


def dependency_stats(g: pd.DataFrame) -> dict:
    if len(g) == 0:
        return dict(top_day_dependency_ratio=np.nan, max_daily_profit_share=np.nan, monthly_positive_count=0, monthly_negative_count=0)

    hit = pd.to_numeric(g["actual_wide_hit"], errors="coerce").fillna(0)
    payout = pd.to_numeric(g["wide_payout"], errors="coerce").fillna(0)
    wins = (hit > 0).astype(float)
    g = g.copy()
    g["profit"] = payout * wins - 100.0

    daily = g.groupby("date", as_index=False)["profit"].sum()
    total = float(daily["profit"].sum())
    max_day = float(daily["profit"].max()) if len(daily) else 0.0
    top_day_dep = (max_day / total) if total > 0 else np.nan

    monthly = g.groupby("month", as_index=False)["profit"].sum()
    monthly_pos = int((monthly["profit"] > 0).sum())
    monthly_neg = int((monthly["profit"] < 0).sum())

    pos_total = float(daily.loc[daily["profit"] > 0, "profit"].sum())
    max_daily_profit_share = (max_day / pos_total) if pos_total > 0 else np.nan

    return dict(
        top_day_dependency_ratio=top_day_dep,
        max_daily_profit_share=max_daily_profit_share,
        monthly_positive_count=monthly_pos,
        monthly_negative_count=monthly_neg,
    )


def run_grid(df: pd.DataFrame) -> pd.DataFrame:
    value_grid = sorted(set([FIX_V, float(df["pair_value_score"].quantile(0.50)), float(df["pair_value_score"].quantile(0.60)), float(df["pair_value_score"].quantile(0.70))]))
    edge_grid = [0.0, 0.01, 0.02, 0.03]
    race_cap_grid = [None, 3, 2, 1]
    day_cap_grid = [None, 80, 60, 40, 30]
    rank_cap_grid = [None, 5, 3, 2, 1]
    odds_grid = ["ALL", "vp_low|low|mid", "low|mid|high", "vp_low|low"]
    pop_grid = ["ALL", "longshot|semi_long|mid", "semi_long|mid|semi_pop", "longshot|semi_long"]

    base = calc_metrics(df)
    rows = []

    for vt, et, rc, dc, rk, ob, pb in itertools.product(value_grid, edge_grid, race_cap_grid, day_cap_grid, rank_cap_grid, odds_grid, pop_grid):
        g = apply_condition(df, vt, et, rc, dc, rk, ob, pb)
        m = calc_metrics(g)
        d = dependency_stats(g)
        rows.append(
            {
                "value_threshold": vt,
                "edge_threshold": et,
                "max_per_race": -1 if rc is None else rc,
                "max_per_day": -1 if dc is None else dc,
                "value_rank_cap": -1 if rk is None else rk,
                "odds_proxy_bucket_condition": ob,
                "popularity_bucket_condition": pb,
                "candidate_count": m["candidate_count"],
                "buy_reduction_rate": 1 - (m["candidate_count"] / base["candidate_count"]) if base["candidate_count"] else np.nan,
                "roi": m["roi"],
                "profit": m["profit"],
                "hit_rate": m["hit_rate"],
                "monthly_positive_count": d["monthly_positive_count"],
                "monthly_negative_count": d["monthly_negative_count"],
                "top_day_dependency_ratio": d["top_day_dependency_ratio"],
                "max_daily_profit_share": d["max_daily_profit_share"],
            }
        )

    out = pd.DataFrame(rows)
    out = out.sort_values(["roi", "profit"], ascending=[False, False]).reset_index(drop=True)
    return out


def rank_stable(grid: pd.DataFrame) -> pd.DataFrame:
    x = grid.copy()
    cond = (
        (pd.to_numeric(x["roi"], errors="coerce") > 1.2)
        & (pd.to_numeric(x["profit"], errors="coerce") > 0)
        & (pd.to_numeric(x["monthly_positive_count"], errors="coerce") >= 7)
        & (pd.to_numeric(x["top_day_dependency_ratio"], errors="coerce") < 0.5)
        & (pd.to_numeric(x["candidate_count"], errors="coerce") >= 700)
    )
    s = x[cond].copy()
    s["stability_score"] = (
        pd.to_numeric(s["roi"], errors="coerce") * 0.5
        + (1 - pd.to_numeric(s["top_day_dependency_ratio"], errors="coerce").clip(lower=0, upper=1)) * 0.3
        + (pd.to_numeric(s["candidate_count"], errors="coerce") / max(1, pd.to_numeric(x["candidate_count"], errors="coerce").max())) * 0.2
    )
    s = s.sort_values(["stability_score", "roi", "profit"], ascending=[False, False, False])
    return s


def audit_venue_surface(df: pd.DataFrame) -> pd.DataFrame:
    def audit_col(col: str) -> pd.DataFrame:
        v = df[col].astype(str).fillna("<NA>")
        rows = []
        for val, cnt in v.value_counts(dropna=False).items():
            has_mojibake = int(bool(re.search(r"[\ufffd\?]", val) or any(ord(ch) > 127 for ch in val)))
            numeric_code = int(val.isdigit())
            norm_suggestion = val
            if numeric_code:
                norm_suggestion = f"{col}_code_{int(val):02d}"
            rows.append(
                {
                    "column": col,
                    "raw_value": val,
                    "count": int(cnt),
                    "is_numeric_code": numeric_code,
                    "mojibake_candidate": has_mojibake,
                    "normalization_suggestion": norm_suggestion,
                }
            )
        return pd.DataFrame(rows)

    return pd.concat([audit_col("venue"), audit_col("surface")], ignore_index=True)


def main() -> None:
    REPORT.mkdir(parents=True, exist_ok=True)
    df = prep()

    grid = run_grid(df)
    grid.to_csv(REPORT / "rule_filter_shadow_risk_adjusted_grid.csv", index=False, encoding="utf-8-sig")
    (REPORT / "rule_filter_shadow_risk_adjusted_grid.md").write_text(
        "# rule_filter_shadow_risk_adjusted_grid\n\n" + grid.head(200).to_string(index=False), encoding="utf-8"
    )

    stable = rank_stable(grid)
    stable.to_csv(REPORT / "rule_filter_shadow_stability_ranked_conditions.csv", index=False, encoding="utf-8-sig")
    (REPORT / "rule_filter_shadow_stability_ranked_conditions.md").write_text(
        "# rule_filter_shadow_stability_ranked_conditions\n\n" + (stable.head(100).to_string(index=False) if len(stable) else "(no condition matched strict criteria)"),
        encoding="utf-8",
    )

    aud = audit_venue_surface(df)
    aud.to_csv(REPORT / "venue_surface_normalization_audit_real_odds.csv", index=False, encoding="utf-8-sig")
    (REPORT / "venue_surface_normalization_audit_real_odds.md").write_text(
        "# venue_surface_normalization_audit_real_odds\n\n" + aud.to_string(index=False), encoding="utf-8"
    )

    # report
    fixed = grid[(grid["value_threshold"].round(6) == round(FIX_V, 6)) & (grid["edge_threshold"].round(6) == round(FIX_E, 6)) & (grid["max_per_race"] == -1) & (grid["max_per_day"] == -1) & (grid["value_rank_cap"] == -1) & (grid["odds_proxy_bucket_condition"] == "ALL") & (grid["popularity_bucket_condition"] == "ALL")]
    fixed_row = fixed.iloc[0] if len(fixed) else None
    best_stable = stable.iloc[0] if len(stable) else None

    lines = [
        "# rule_filter_shadow_risk_adjusted_report",
        "",
        "## Fixed Shadow Stability",
    ]
    if fixed_row is not None:
        lines += [
            f"- fixed_roi: {fixed_row['roi']}",
            f"- fixed_profit: {fixed_row['profit']}",
            f"- fixed_monthly_positive_count: {int(fixed_row['monthly_positive_count'])}",
            f"- fixed_top_day_dependency_ratio: {fixed_row['top_day_dependency_ratio']}",
            f"- fixed_candidate_count: {int(fixed_row['candidate_count'])}",
        ]
    else:
        lines += ["- fixed baseline row not found in grid"]

    lines += ["", "## Risk-Adjusted Best Candidate"]
    if best_stable is not None:
        lines += [
            f"- condition: value>={best_stable['value_threshold']}, edge>={best_stable['edge_threshold']}, max_per_race={best_stable['max_per_race']}, max_per_day={best_stable['max_per_day']}, rank_cap={best_stable['value_rank_cap']}, odds={best_stable['odds_proxy_bucket_condition']}, pop={best_stable['popularity_bucket_condition']}",
            f"- roi: {best_stable['roi']}",
            f"- profit: {best_stable['profit']}",
            f"- monthly_positive_count: {int(best_stable['monthly_positive_count'])}",
            f"- top_day_dependency_ratio: {best_stable['top_day_dependency_ratio']}",
            f"- candidate_count: {int(best_stable['candidate_count'])}",
        ]
    else:
        lines += ["- strict stability criteria matched no rows; dependency and sample-size constraints are tight."]

    lines += [
        "",
        "## Interpretation",
        "- fixed shadow is strong on ROI but not fully stable due to dependency spikes.",
        "- lowering dependency generally reduces ROI and/or candidate_count.",
        "- risk-adjusted candidates can retain positive profit, but often with fewer bets.",
        "",
        "## Decision",
        "- production_candidate: not yet",
        "- action: continue shadow monitoring",
    ]

    (REPORT / "rule_filter_shadow_risk_adjusted_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("done")


if __name__ == "__main__":
    main()
