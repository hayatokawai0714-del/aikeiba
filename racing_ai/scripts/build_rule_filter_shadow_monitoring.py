from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai")
REPORT = BASE / "reports" / "2024_eval_full_v5"
PAIR_REAL = REPORT / "pair_shadow_pair_comparison_expanded_real_odds_2025_2026_with_results_external_priority.csv"
BACKTEST = REPORT / "rule_filter_shadow_backtest_real_odds_2025_2026.csv"
TODAY_JSON = REPORT / "rule_filter_shadow_summary_today.json"


def to_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int).astype(bool)


def build_base_daily() -> pd.DataFrame:
    d = pd.read_csv(BACKTEST)
    d = d.rename(columns={"race_date": "date"})
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d["removed_candidate_count"] = d["original_rule_candidate_count"] - d["filtered_rule_candidate_count"]
    d["original_roi"] = d["original_rule_roi"]
    d["filtered_roi"] = d["filtered_rule_roi"]
    d["original_profit"] = d["original_rule_profit"]
    d["filtered_profit"] = d["filtered_rule_profit"]
    return d


def add_race_counts(daily: pd.DataFrame) -> pd.DataFrame:
    x = pd.read_csv(PAIR_REAL)
    x = x[x["result_quality_status"].astype(str) == "ok"].copy()
    x["date"] = pd.to_datetime(x["race_date"], errors="coerce")
    rule = x[to_bool(x["pair_selected_flag"])].copy()
    rule["rule_filter_shadow_flag"] = (pd.to_numeric(rule["pair_value_score"], errors="coerce") >= 0.0591) & (
        pd.to_numeric(rule["pair_edge"], errors="coerce") >= 0.02
    )
    agg = []
    for dt, g in rule.groupby("date"):
        orig_races = int(g["race_id"].nunique())
        fil_races = int(g.loc[g["rule_filter_shadow_flag"], "race_id"].nunique())
        agg.append(
            {
                "date": dt,
                "race_count_with_filtered_candidates": fil_races,
                "race_count_zero_after_filter": max(0, orig_races - fil_races),
            }
        )
    a = pd.DataFrame(agg)
    return daily.merge(a, on="date", how="left")


def add_today_shadow_row(daily: pd.DataFrame) -> pd.DataFrame:
    if not TODAY_JSON.exists():
        return daily
    j = json.loads(TODAY_JSON.read_text(encoding="utf-8"))
    m = re.search(r"_(\d{8})\.csv$", str(j.get("input_file", "")))
    if not m:
        return daily
    d = pd.to_datetime(m.group(1), format="%Y%m%d", errors="coerce")
    if pd.isna(d):
        return daily
    row = {
        "date": d,
        "original_rule_candidate_count": j.get("original_rule_candidate_count"),
        "filtered_rule_candidate_count": j.get("filtered_rule_candidate_count"),
        "removed_candidate_count": j.get("removed_candidate_count"),
        "buy_reduction_rate": j.get("buy_reduction_rate"),
        "original_roi": np.nan,
        "filtered_roi": np.nan,
        "original_profit": np.nan,
        "filtered_profit": np.nan,
        "race_count_with_filtered_candidates": j.get("race_count_with_filtered_candidates"),
        "race_count_zero_after_filter": j.get("race_count_zero_after_filter"),
        "source": "today_shadow",
    }
    if "source" not in daily.columns:
        daily["source"] = "backtest"
    daily = daily[daily["date"] != d].copy()
    daily = pd.concat([daily, pd.DataFrame([row])], ignore_index=True)
    return daily


def warning_flags(d: pd.DataFrame) -> pd.DataFrame:
    d = d.sort_values("date").copy()
    flags = []
    for i in range(len(d)):
        row = d.iloc[i]
        f = []
        orig_cnt = row.get("original_rule_candidate_count")
        fil_cnt = row.get("filtered_rule_candidate_count")
        zero_cnt = row.get("race_count_zero_after_filter")
        fil_roi = row.get("filtered_roi")
        fil_profit = row.get("filtered_profit")

        if pd.notna(orig_cnt) and pd.notna(zero_cnt) and orig_cnt > 0:
            if zero_cnt >= 4:
                f.append("ZERO_AFTER_FILTER_HIGH")
        if pd.notna(fil_cnt) and fil_cnt < 5:
            f.append("FILTERED_CANDIDATE_TOO_FEW")
        if pd.notna(fil_roi) and fil_roi < 1.0:
            f.append("ROI_BELOW_1")
        if pd.notna(fil_profit) and fil_profit < 0:
            f.append("PROFIT_NEGATIVE")

        # rolling 30-day one-day dependency
        date_now = row["date"]
        win = d[(d["date"] <= date_now) & (d["date"] > (date_now - pd.Timedelta(days=30)))].copy()
        rp = pd.to_numeric(win["filtered_profit"], errors="coerce").fillna(0)
        total = float(rp.sum())
        mx = float(rp.max()) if len(rp) else 0.0
        if total > 0 and mx / total >= 0.5:
            f.append("ONE_DAY_HIT_DEPENDENCY")

        flags.append("|".join(sorted(set(f))) if f else "")

    d["warning_flags"] = flags
    return d


def build_monitoring_summary(d: pd.DataFrame) -> str:
    d = d.sort_values("date").copy()
    end = d["date"].max()
    start30 = end - pd.Timedelta(days=30) if pd.notna(end) else None
    w = d[d["date"] > start30].copy() if start30 is not None else d.copy()

    def smean(col):
        return float(pd.to_numeric(w[col], errors="coerce").mean()) if len(w) else np.nan

    lines = [
        "# rule_filter_shadow_monitoring_summary",
        "",
        f"- latest_date: {end.date().isoformat() if pd.notna(end) else 'N/A'}",
        f"- window_days: 30",
        f"- window_rows: {len(w)}",
        f"- avg_buy_reduction_rate_30d: {smean('buy_reduction_rate')}",
        f"- avg_original_roi_30d: {smean('original_roi')}",
        f"- avg_filtered_roi_30d: {smean('filtered_roi')}",
        f"- total_original_profit_30d: {float(pd.to_numeric(w['original_profit'], errors='coerce').fillna(0).sum()) if len(w) else 0.0}",
        f"- total_filtered_profit_30d: {float(pd.to_numeric(w['filtered_profit'], errors='coerce').fillna(0).sum()) if len(w) else 0.0}",
        f"- avg_race_count_zero_after_filter_30d: {smean('race_count_zero_after_filter')}",
        f"- warning_days_30d: {int((w['warning_flags'].astype(str) != '').sum()) if len(w) else 0}",
        "",
        "## Latest Rows",
        w.tail(10).to_string(index=False) if len(w) else "(no data)",
    ]
    return "\n".join(lines) + "\n"


def build_adoption_checklist(d: pd.DataFrame) -> str:
    d = d.sort_values("date").copy()
    end = d["date"].max()
    start30 = end - pd.Timedelta(days=30) if pd.notna(end) else None
    w = d[d["date"] > start30].copy() if start30 is not None else d.copy()

    avg_filtered_roi = float(pd.to_numeric(w["filtered_roi"], errors="coerce").mean()) if len(w) else np.nan
    avg_original_roi = float(pd.to_numeric(w["original_roi"], errors="coerce").mean()) if len(w) else np.nan
    filtered_profit_total = float(pd.to_numeric(w["filtered_profit"], errors="coerce").fillna(0).sum()) if len(w) else 0.0
    original_profit_total = float(pd.to_numeric(w["original_profit"], errors="coerce").fillna(0).sum()) if len(w) else 0.0
    zero_high_days = int(w["warning_flags"].astype(str).str.contains("ZERO_AFTER_FILTER_HIGH").sum()) if len(w) else 0
    one_day_dep_days = int(w["warning_flags"].astype(str).str.contains("ONE_DAY_HIT_DEPENDENCY").sum()) if len(w) else 0

    checks = [
        ("ROI > 1.2 over additional 30+ days", avg_filtered_roi > 1.2),
        ("Filtered ROI > Original ROI", avg_filtered_roi > avg_original_roi),
        ("Filtered profit not excessively lower", filtered_profit_total >= 0.6 * original_profit_total if original_profit_total > 0 else True),
        ("race_count_zero_after_filter not too high", zero_high_days <= max(2, int(len(w) * 0.2)) if len(w) else False),
        ("No strong one-day hit dependency", one_day_dep_days <= max(2, int(len(w) * 0.2)) if len(w) else False),
    ]

    lines = [
        "# rule_filter_shadow_adoption_checklist",
        "",
        f"- evaluation_window_rows: {len(w)}",
        f"- avg_original_roi_30d: {avg_original_roi}",
        f"- avg_filtered_roi_30d: {avg_filtered_roi}",
        f"- original_profit_total_30d: {original_profit_total}",
        f"- filtered_profit_total_30d: {filtered_profit_total}",
        f"- zero_after_filter_high_days_30d: {zero_high_days}",
        f"- one_day_hit_dependency_days_30d: {one_day_dep_days}",
        "",
        "## Checks",
    ]
    for txt, ok in checks:
        lines.append(f"- {'[PASS]' if ok else '[HOLD]'} {txt}")

    decision = "SHADOW_CONTINUE"
    if all(ok for _, ok in checks):
        decision = "CONSIDER_PROD_TRIAL"
    lines += ["", f"- decision: {decision}"]
    return "\n".join(lines) + "\n"


def main() -> None:
    REPORT.mkdir(parents=True, exist_ok=True)
    d = build_base_daily()
    d = add_race_counts(d)
    d = add_today_shadow_row(d)
    d = warning_flags(d)

    cols = [
        "date",
        "original_rule_candidate_count",
        "filtered_rule_candidate_count",
        "removed_candidate_count",
        "buy_reduction_rate",
        "original_roi",
        "filtered_roi",
        "original_profit",
        "filtered_profit",
        "race_count_with_filtered_candidates",
        "race_count_zero_after_filter",
        "warning_flags",
    ]
    for c in cols:
        if c not in d.columns:
            d[c] = np.nan
    out = d[cols].sort_values("date").copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out.to_csv(REPORT / "rule_filter_shadow_daily_log.csv", index=False, encoding="utf-8-sig")

    (REPORT / "rule_filter_shadow_monitoring_summary.md").write_text(build_monitoring_summary(d), encoding="utf-8")
    (REPORT / "rule_filter_shadow_adoption_checklist.md").write_text(build_adoption_checklist(d), encoding="utf-8")

    print("done")


if __name__ == "__main__":
    main()
