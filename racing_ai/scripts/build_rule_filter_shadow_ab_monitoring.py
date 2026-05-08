from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai")
REPORT = BASE / "reports" / "2024_eval_full_v5"
PAIR_REAL = REPORT / "pair_shadow_pair_comparison_expanded_real_odds_2025_2026_with_results_external_priority.csv"

A_NAME = "fixed_A"
B_NAME = "risk_adjusted_B"
PRIMARY = B_NAME


def to_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int).astype(bool)


def add_popularity_bucket(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["pair_market_implied_prob"] = pd.to_numeric(x.get("pair_market_implied_prob"), errors="coerce")
    pct = x.groupby("race_id")["pair_market_implied_prob"].rank(method="first", pct=True)
    x["popularity_bucket"] = pd.cut(
        pct,
        bins=[0, 0.2, 0.4, 0.6, 0.8, 1.0],
        labels=["longshot", "semi_long", "mid", "semi_pop", "popular"],
        include_lowest=True,
    ).astype(str)
    return x


def add_ab_flags(df: pd.DataFrame) -> pd.DataFrame:
    x = add_popularity_bucket(df)
    x["pair_value_score"] = pd.to_numeric(x.get("pair_value_score"), errors="coerce")
    x["pair_edge"] = pd.to_numeric(x.get("pair_edge"), errors="coerce")
    rule = to_bool(x.get("pair_selected_flag", pd.Series([0] * len(x))))

    pass_a = rule & (x["pair_value_score"] >= 0.0591) & (x["pair_edge"] >= 0.02)
    pass_b = rule & (x["pair_value_score"] >= 0.04893) & (x["pair_edge"] >= 0.02) & x["popularity_bucket"].isin(["longshot", "semi_long", "mid"])

    def reason(rule_ok: bool, v: float, e: float, pop: str, mode: str) -> str:
        if not rule_ok:
            return "not_rule_selected"
        if pd.isna(v) or pd.isna(e):
            return "missing_score_or_edge"
        if mode == "A":
            if v < 0.0591 and e < 0.02:
                return "below_value_and_edge"
            if v < 0.0591:
                return "below_value_threshold"
            if e < 0.02:
                return "below_edge_threshold"
            return "pass"
        if v < 0.04893 and e < 0.02:
            return "below_value_and_edge"
        if v < 0.04893:
            return "below_value_threshold"
        if e < 0.02:
            return "below_edge_threshold"
        if pop not in {"longshot", "semi_long", "mid"}:
            return "outside_popularity_bucket"
        return "pass"

    a_reason = []
    b_reason = []
    for _, r in x.iterrows():
        a_reason.append(reason(bool(r.get("pair_selected_flag")), r["pair_value_score"], r["pair_edge"], str(r.get("popularity_bucket")), "A"))
        b_reason.append(reason(bool(r.get("pair_selected_flag")), r["pair_value_score"], r["pair_edge"], str(r.get("popularity_bucket")), "B"))

    x["shadow_A_flag"] = pass_a
    x["shadow_A_reason"] = a_reason
    x["shadow_B_flag"] = pass_b
    x["shadow_B_reason"] = b_reason
    x["shadow_primary_condition"] = PRIMARY
    x["shadow_primary_flag"] = x["shadow_B_flag"]
    return x


def choose_today_input() -> Path:
    files = sorted((BASE / "reports").glob("pair_shadow_pair_comparison_expanded_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    day_files = [p for p in files if re.search(r"_(\d{8})\.csv$", p.name)]
    for p in day_files:
        try:
            d = pd.read_csv(p, usecols=["pair_selected_flag"])
            if to_bool(d["pair_selected_flag"]).sum() > 0:
                return p
        except Exception:
            continue
    if not day_files:
        raise FileNotFoundError("no day-level pair csv")
    return day_files[0]


def metrics(df: pd.DataFrame) -> dict:
    if len(df) == 0:
        return dict(candidate_count=0, roi=np.nan, profit=0.0, hit_rate=np.nan)
    hit = pd.to_numeric(df.get("actual_wide_hit"), errors="coerce").fillna(0)
    payout = pd.to_numeric(df.get("wide_payout"), errors="coerce").fillna(0)
    wins = (hit > 0).astype(float)
    total = float((payout * wins).sum())
    cost = float(len(df) * 100)
    return dict(candidate_count=int(len(df)), roi=(total / cost if cost > 0 else np.nan), profit=total - cost, hit_rate=float(wins.mean()))


def build_today_outputs() -> dict:
    inp = choose_today_input()
    df = pd.read_csv(inp)
    x = add_ab_flags(df)
    rule = x[to_bool(x["pair_selected_flag"])].copy()

    out_cols = [
        "race_id", "pair_norm", "pair_selected_flag", "pair_value_score", "pair_edge", "popularity_bucket",
        "shadow_A_flag", "shadow_A_reason", "shadow_B_flag", "shadow_B_reason", "shadow_primary_condition", "shadow_primary_flag"
    ]
    for c in out_cols:
        if c not in rule.columns:
            rule[c] = pd.NA
    rule[out_cols].to_csv(REPORT / "rule_filter_shadow_ab_candidates_today.csv", index=False, encoding="utf-8-sig")

    removed = rule[~to_bool(rule["shadow_primary_flag"])].copy()
    removed_cols = ["race_id", "pair_norm", "pair_value_score", "pair_edge", "popularity_bucket", "shadow_A_reason", "shadow_B_reason"]
    removed[removed_cols].to_csv(REPORT / "rule_filter_shadow_ab_removed_candidates_today.csv", index=False, encoding="utf-8-sig")

    a = rule[to_bool(rule["shadow_A_flag"])].copy()
    b = rule[to_bool(rule["shadow_B_flag"])].copy()
    summary = {
        "input_file": str(inp),
        "original_rule_candidate_count": int(len(rule)),
        "A_filtered_candidate_count": int(len(a)),
        "B_filtered_candidate_count": int(len(b)),
        "A_removed_candidate_count": int(len(rule) - len(a)),
        "B_removed_candidate_count": int(len(rule) - len(b)),
        "A_buy_reduction_rate": float((len(rule) - len(a)) / len(rule)) if len(rule) else None,
        "B_buy_reduction_rate": float((len(rule) - len(b)) / len(rule)) if len(rule) else None,
        "shadow_primary_condition": PRIMARY,
    }
    (REPORT / "rule_filter_shadow_ab_summary_today.md").write_text(
        "# rule_filter_shadow_ab_summary_today\n\n```json\n" + json.dumps(summary, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )
    return summary


def build_ab_daily_log(today_summary: dict) -> pd.DataFrame:
    df = pd.read_csv(PAIR_REAL)
    df = df[to_bool(df["pair_selected_flag"])].copy()
    df = df[df["result_quality_status"].astype(str) == "ok"].copy()
    df = add_ab_flags(df)
    df["date"] = pd.to_datetime(df["race_date"], errors="coerce")

    rows = []
    for d, g in df.groupby("date"):
        base = metrics(g)
        orig_races = int(g["race_id"].nunique())
        for cond, flag_col in [(A_NAME, "shadow_A_flag"), (B_NAME, "shadow_B_flag")]:
            h = g[to_bool(g[flag_col])].copy()
            m = metrics(h)
            race_with = int(h["race_id"].nunique()) if len(h) else 0
            rows.append(
                {
                    "date": d.strftime("%Y-%m-%d"),
                    "condition_name": cond,
                    "original_rule_candidate_count": base["candidate_count"],
                    "filtered_candidate_count": m["candidate_count"],
                    "removed_candidate_count": base["candidate_count"] - m["candidate_count"],
                    "buy_reduction_rate": 1 - (m["candidate_count"] / base["candidate_count"]) if base["candidate_count"] else np.nan,
                    "filtered_roi": m["roi"],
                    "filtered_profit": m["profit"],
                    "race_count_with_filtered_candidates": race_with,
                    "race_count_zero_after_filter": max(0, orig_races - race_with),
                    "baseline_rule_roi": base["roi"],
                    "baseline_rule_profit": base["profit"],
                }
            )

    log = pd.DataFrame(rows)

    # add today row override with NaN ROI/profit if unresolved
    m = re.search(r"_(\d{8})\.csv$", str(today_summary.get("input_file", "")))
    if m:
        d = pd.to_datetime(m.group(1), format="%Y%m%d", errors="coerce")
        if pd.notna(d):
            date_s = d.strftime("%Y-%m-%d")
            log = log[log["date"] != date_s].copy()
            for cond in [A_NAME, B_NAME]:
                if cond == A_NAME:
                    fc = today_summary.get("A_filtered_candidate_count", 0)
                else:
                    fc = today_summary.get("B_filtered_candidate_count", 0)
                oc = today_summary.get("original_rule_candidate_count", 0)
                log = pd.concat(
                    [
                        log,
                        pd.DataFrame(
                            [
                                {
                                    "date": date_s,
                                    "condition_name": cond,
                                    "original_rule_candidate_count": oc,
                                    "filtered_candidate_count": fc,
                                    "removed_candidate_count": oc - fc,
                                    "buy_reduction_rate": (oc - fc) / oc if oc else np.nan,
                                    "filtered_roi": np.nan,
                                    "filtered_profit": np.nan,
                                    "race_count_with_filtered_candidates": np.nan,
                                    "race_count_zero_after_filter": np.nan,
                                    "baseline_rule_roi": np.nan,
                                    "baseline_rule_profit": np.nan,
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )

    log = log.sort_values(["date", "condition_name"]).reset_index(drop=True)
    return log


def add_warning_flags(log: pd.DataFrame) -> pd.DataFrame:
    x = log.copy()
    x["date_dt"] = pd.to_datetime(x["date"], errors="coerce")
    out_flags = []
    for idx, r in x.iterrows():
        f = []
        if pd.notna(r["race_count_zero_after_filter"]) and r["race_count_zero_after_filter"] >= 4:
            f.append("ZERO_AFTER_FILTER_HIGH")
        if pd.notna(r["filtered_candidate_count"]) and r["filtered_candidate_count"] < 5:
            f.append("FILTERED_CANDIDATE_TOO_FEW")
        if pd.notna(r["filtered_roi"]) and r["filtered_roi"] < 1.0:
            f.append("ROI_BELOW_1")
        if pd.notna(r["filtered_profit"]) and r["filtered_profit"] < 0:
            f.append("PROFIT_NEGATIVE")

        w = x[(x["condition_name"] == r["condition_name"]) & (x["date_dt"] <= r["date_dt"]) & (x["date_dt"] > r["date_dt"] - pd.Timedelta(days=30))].copy()
        pos_total = float(pd.to_numeric(w["filtered_profit"], errors="coerce").fillna(0).clip(lower=0).sum())
        max_day = float(pd.to_numeric(w["filtered_profit"], errors="coerce").fillna(0).max()) if len(w) else 0.0
        if pos_total > 0 and (max_day / pos_total) >= 0.5:
            f.append("ONE_DAY_HIT_DEPENDENCY")

        out_flags.append("|".join(sorted(set(f))) if f else "")

    x["warning_flags"] = out_flags
    return x.drop(columns=["date_dt"])


def build_monitoring_summary(log: pd.DataFrame) -> str:
    x = log.copy()
    x["date_dt"] = pd.to_datetime(x["date"], errors="coerce")
    end = x["date_dt"].max()
    start30 = end - pd.Timedelta(days=30)
    w = x[x["date_dt"] > start30].copy()

    lines = ["# rule_filter_shadow_ab_monitoring_summary", "", f"- latest_date: {end.date().isoformat()}", "- window_days: 30", ""]
    for cond in [A_NAME, B_NAME]:
        g = w[w["condition_name"] == cond].copy()
        lines += [
            f"## {cond}",
            f"- rows_30d: {len(g)}",
            f"- avg_filtered_roi_30d: {float(pd.to_numeric(g['filtered_roi'], errors='coerce').mean()) if len(g) else np.nan}",
            f"- total_filtered_profit_30d: {float(pd.to_numeric(g['filtered_profit'], errors='coerce').fillna(0).sum()) if len(g) else 0.0}",
            f"- avg_buy_reduction_rate_30d: {float(pd.to_numeric(g['buy_reduction_rate'], errors='coerce').mean()) if len(g) else np.nan}",
            f"- warning_days_30d: {int((g['warning_flags'].astype(str)!='').sum()) if len(g) else 0}",
            "",
        ]
    lines += ["## Latest Rows", w.sort_values(["date", "condition_name"]).tail(20).to_string(index=False) if len(w) else "(no data)"]
    return "\n".join(lines) + "\n"


def build_adoption_checklist(log: pd.DataFrame) -> str:
    x = log.copy()
    x["date_dt"] = pd.to_datetime(x["date"], errors="coerce")
    end = x["date_dt"].max()
    start30 = end - pd.Timedelta(days=30)
    w = x[x["date_dt"] > start30].copy()

    gA = w[w["condition_name"] == A_NAME].copy()
    gB = w[w["condition_name"] == B_NAME].copy()

    b_roi = float(pd.to_numeric(gB["filtered_roi"], errors="coerce").mean()) if len(gB) else np.nan
    b_profit = float(pd.to_numeric(gB["filtered_profit"], errors="coerce").fillna(0).sum()) if len(gB) else 0.0
    a_roi = float(pd.to_numeric(gA["filtered_roi"], errors="coerce").mean()) if len(gA) else np.nan
    base_roi = float(pd.to_numeric(gB["baseline_rule_roi"], errors="coerce").mean()) if len(gB) else np.nan
    zero_high = int(gB["warning_flags"].astype(str).str.contains("ZERO_AFTER_FILTER_HIGH").sum()) if len(gB) else 0
    dep_high = int(gB["warning_flags"].astype(str).str.contains("ONE_DAY_HIT_DEPENDENCY").sum()) if len(gB) else 0
    med_count = float(pd.to_numeric(gB["filtered_candidate_count"], errors="coerce").median()) if len(gB) else np.nan

    checks = [
        ("30日以上の観測", len(gB) >= 30),
        ("B filtered_roi > 1.2", b_roi > 1.2),
        ("B filtered_profit > 0", b_profit > 0),
        ("B ROI > A ROI", b_roi > a_roi),
        ("B ROI > baseline ROI", b_roi > base_roi),
        ("ZERO_AFTER_FILTER_HIGHが頻発しない", zero_high <= max(3, int(len(gB) * 0.3)) if len(gB) else False),
        ("ONE_DAY_HIT_DEPENDENCYが極端でない", dep_high <= max(3, int(len(gB) * 0.3)) if len(gB) else False),
        ("買い目数が少なすぎない", med_count >= 20 if pd.notna(med_count) else False),
    ]

    lines = [
        "# rule_filter_shadow_ab_adoption_checklist",
        "",
        f"- window_rows_B: {len(gB)}",
        f"- B_avg_roi_30d: {b_roi}",
        f"- B_profit_sum_30d: {b_profit}",
        f"- A_avg_roi_30d: {a_roi}",
        f"- baseline_avg_roi_30d: {base_roi}",
        f"- B_median_candidate_count_30d: {med_count}",
        f"- B_ZERO_AFTER_FILTER_HIGH_days_30d: {zero_high}",
        f"- B_ONE_DAY_HIT_DEPENDENCY_days_30d: {dep_high}",
        "",
        "## Checks",
    ]
    for txt, ok in checks:
        lines.append(f"- {'[PASS]' if ok else '[HOLD]'} {txt}")
    lines.append("")
    lines.append(f"- decision: {'CONSIDER_EXTENDED_SHADOW' if sum(int(ok) for _, ok in checks) >= 6 else 'SHADOW_CONTINUE'}")
    return "\n".join(lines) + "\n"


def build_operation_plan() -> str:
    return "\n".join(
        [
            "# rule_filter_shadow_ab_operation_plan",
            "",
            "- Bを主候補にする理由: AよりWF指標（ROI/利益/baseline超過fold）で優位だったため。",
            "- Aを対照群として残す理由: 市況変化時の劣化検知ベースラインとして有効だから。",
            "- 本番ruleをまだ変えない理由: 直近30日監視で再現性と依存度低下を確認しきっていないため。",
            "",
            "## 30日監視で見る指標",
            "- filtered_roi / filtered_profit（A/B）",
            "- A対BのROI差",
            "- baseline対BのROI差",
            "- buy_reduction_rate",
            "- race_count_zero_after_filter",
            "- ZERO_AFTER_FILTER_HIGH / ONE_DAY_HIT_DEPENDENCY の発生日数",
            "- filtered_candidate_count の中央値",
            "",
            "## 本番採用条件",
            "- 追加30日以上",
            "- B filtered_roi > 1.2",
            "- B filtered_profit > 0",
            "- B ROI > A ROI",
            "- B ROI > baseline ROI",
            "- ZERO_AFTER_FILTER_HIGHが頻発しない",
            "- ONE_DAY_HIT_DEPENDENCYが極端でない",
            "- 買い目数が少なすぎない",
            "",
            "- decision_now: shadow継続（A/B併走、B主候補）",
        ]
    ) + "\n"


def main() -> None:
    REPORT.mkdir(parents=True, exist_ok=True)
    today = build_today_outputs()
    log = build_ab_daily_log(today)
    log = add_warning_flags(log)

    out_cols = [
        "date",
        "condition_name",
        "original_rule_candidate_count",
        "filtered_candidate_count",
        "removed_candidate_count",
        "buy_reduction_rate",
        "filtered_roi",
        "filtered_profit",
        "race_count_with_filtered_candidates",
        "race_count_zero_after_filter",
        "warning_flags",
    ]
    for c in out_cols:
        if c not in log.columns:
            log[c] = np.nan
    log[out_cols].sort_values(["date", "condition_name"]).to_csv(REPORT / "rule_filter_shadow_ab_daily_log.csv", index=False, encoding="utf-8-sig")

    (REPORT / "rule_filter_shadow_ab_monitoring_summary.md").write_text(build_monitoring_summary(log), encoding="utf-8")
    (REPORT / "rule_filter_shadow_ab_adoption_checklist.md").write_text(build_adoption_checklist(log), encoding="utf-8")
    (REPORT / "rule_filter_shadow_ab_operation_plan.md").write_text(build_operation_plan(), encoding="utf-8")
    print("done")


if __name__ == "__main__":
    main()
