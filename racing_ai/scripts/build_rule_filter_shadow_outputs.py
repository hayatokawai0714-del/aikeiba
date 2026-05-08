from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

BASE = Path(r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai")
REPORT = BASE / "reports" / "2024_eval_full_v5"

VALUE_THR = 0.0591
EDGE_THR = 0.02


def to_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int).astype(bool)


def choose_today_input() -> Path:
    root = BASE / "reports"
    cands = []
    for p in root.glob("pair_shadow_pair_comparison_expanded_*.csv"):
        m = re.search(r"_(\d{8})\.csv$", p.name)
        if m:
            cands.append((m.group(1), p))
    if not cands:
        raise FileNotFoundError("No single-day pair_shadow_pair_comparison_expanded_YYYYMMDD.csv found")
    cands.sort(key=lambda x: x[0], reverse=True)
    # Prefer the latest file that actually has rule-selected rows.
    for _, p in cands:
        try:
            df = pd.read_csv(p, usecols=["pair_selected_flag"])
            if to_bool(df["pair_selected_flag"]).sum() > 0:
                return p
        except Exception:
            continue
    return cands[0][1]


def add_shadow_cols(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    for c in ["pair_selected_flag", "pair_value_score", "pair_edge"]:
        if c not in x.columns:
            x[c] = pd.NA
    x["pair_value_score"] = pd.to_numeric(x["pair_value_score"], errors="coerce")
    x["pair_edge"] = pd.to_numeric(x["pair_edge"], errors="coerce")
    rule = to_bool(x["pair_selected_flag"])
    pass_v = x["pair_value_score"] >= VALUE_THR
    pass_e = x["pair_edge"] >= EDGE_THR

    x["rule_filter_shadow_flag"] = rule & pass_v & pass_e
    reasons = []
    for i in range(len(x)):
        if not bool(rule.iloc[i]):
            reasons.append("not_rule_selected")
        elif not bool(pass_v.iloc[i]) and not bool(pass_e.iloc[i]):
            reasons.append("below_value_and_edge")
        elif not bool(pass_v.iloc[i]):
            reasons.append("below_value_threshold")
        elif not bool(pass_e.iloc[i]):
            reasons.append("below_edge_threshold")
        else:
            reasons.append("pass")
    x["rule_filter_shadow_reason"] = reasons
    x["rule_filter_value_threshold"] = VALUE_THR
    x["rule_filter_edge_threshold"] = EDGE_THR
    return x


def metrics(df: pd.DataFrame) -> dict:
    if len(df) == 0:
        return dict(candidate_count=0, hit_rate=None, roi=None, profit=0.0)
    hit = pd.to_numeric(df.get("actual_wide_hit", 0), errors="coerce").fillna(0)
    payout = pd.to_numeric(df.get("wide_payout", 0), errors="coerce").fillna(0)
    wins = (hit > 0).astype(float)
    total_payout = float((payout * wins).sum())
    cost = float(len(df) * 100)
    roi = total_payout / cost if cost > 0 else None
    return dict(
        candidate_count=int(len(df)),
        hit_rate=float(wins.mean()) if len(df) else None,
        roi=float(roi) if roi is not None else None,
        profit=total_payout - cost,
    )


def build_today_outputs(today_input: Path) -> None:
    df = pd.read_csv(today_input)
    x = add_shadow_cols(df)
    rule = x[to_bool(x["pair_selected_flag"])].copy()
    filtered = rule[to_bool(rule["rule_filter_shadow_flag"])].copy()
    removed = rule[~to_bool(rule["rule_filter_shadow_flag"])].copy()

    out_cols = [
        "race_id",
        "pair_norm",
        "pair_selected_flag",
        "pair_value_score",
        "pair_edge",
        "rule_filter_shadow_flag",
        "rule_filter_shadow_reason",
        "rule_filter_value_threshold",
        "rule_filter_edge_threshold",
    ]
    for c in out_cols:
        if c not in rule.columns:
            rule[c] = pd.NA

    rule[out_cols].to_csv(REPORT / "rule_filter_shadow_candidates_today.csv", index=False, encoding="utf-8-sig")

    removed_out = removed[[c for c in ["race_id", "pair_norm", "pair_value_score", "pair_edge", "pair_selected_flag", "rule_filter_shadow_reason"] if c in removed.columns]].copy()
    removed_out = removed_out.rename(columns={"rule_filter_shadow_reason": "reason_removed"})
    removed_out.to_csv(REPORT / "rule_filter_shadow_removed_candidates_today.csv", index=False, encoding="utf-8-sig")

    orig_cnt = int(len(rule))
    fil_cnt = int(len(filtered))
    rem_cnt = int(len(removed))
    race_orig = int(rule["race_id"].nunique()) if "race_id" in rule.columns else 0
    race_fil = int(filtered["race_id"].nunique()) if "race_id" in filtered.columns else 0
    summary = {
        "input_file": str(today_input),
        "original_rule_candidate_count": orig_cnt,
        "filtered_rule_candidate_count": fil_cnt,
        "removed_candidate_count": rem_cnt,
        "buy_reduction_rate": (rem_cnt / orig_cnt) if orig_cnt else None,
        "avg_pair_value_score_filtered": float(pd.to_numeric(filtered["pair_value_score"], errors="coerce").mean()) if fil_cnt else None,
        "avg_pair_edge_filtered": float(pd.to_numeric(filtered["pair_edge"], errors="coerce").mean()) if fil_cnt else None,
        "race_count_with_filtered_candidates": race_fil,
        "race_count_zero_after_filter": max(0, race_orig - race_fil),
        "value_threshold": VALUE_THR,
        "edge_threshold": EDGE_THR,
    }
    (REPORT / "rule_filter_shadow_summary_today.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (REPORT / "rule_filter_shadow_summary_today.md").write_text(
        "# rule_filter_shadow_summary_today\n\n```json\n" + json.dumps(summary, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )


def build_backtest_outputs() -> None:
    p = REPORT / "pair_shadow_pair_comparison_expanded_real_odds_2025_2026_with_results_external_priority.csv"
    df = pd.read_csv(p)
    x = add_shadow_cols(df)
    x = x[to_bool(x["pair_selected_flag"])].copy()
    x = x[x["result_quality_status"].astype(str) == "ok"].copy()

    rows = []
    for d, g in x.groupby("race_date"):
        base = metrics(g)
        fil = metrics(g[to_bool(g["rule_filter_shadow_flag"])])
        rows.append(
            {
                "race_date": d,
                "original_rule_candidate_count": base["candidate_count"],
                "original_rule_hit_rate": base["hit_rate"],
                "original_rule_roi": base["roi"],
                "original_rule_profit": base["profit"],
                "filtered_rule_candidate_count": fil["candidate_count"],
                "filtered_rule_hit_rate": fil["hit_rate"],
                "filtered_rule_roi": fil["roi"],
                "filtered_rule_profit": fil["profit"],
                "buy_reduction_rate": 1 - (fil["candidate_count"] / base["candidate_count"]) if base["candidate_count"] else None,
                "roi_diff_filtered_minus_original": (fil["roi"] - base["roi"]) if fil["roi"] is not None and base["roi"] is not None else None,
            }
        )

    daily = pd.DataFrame(rows).sort_values("race_date")
    daily.to_csv(REPORT / "rule_filter_shadow_backtest_real_odds_2025_2026.csv", index=False, encoding="utf-8-sig")

    base_all = metrics(x)
    fil_all = metrics(x[to_bool(x["rule_filter_shadow_flag"])])
    lines = [
        "# rule_filter_shadow_backtest_real_odds_2025_2026",
        "",
        f"- original_rule_candidate_count: {base_all['candidate_count']}",
        f"- filtered_rule_candidate_count: {fil_all['candidate_count']}",
        f"- original_rule_roi: {base_all['roi']}",
        f"- filtered_rule_roi: {fil_all['roi']}",
        f"- original_rule_profit: {base_all['profit']}",
        f"- filtered_rule_profit: {fil_all['profit']}",
        f"- buy_reduction_rate: {1 - (fil_all['candidate_count']/base_all['candidate_count']) if base_all['candidate_count'] else None}",
        "",
        daily.to_string(index=False),
    ]
    (REPORT / "rule_filter_shadow_backtest_real_odds_2025_2026.md").write_text("\n".join(lines), encoding="utf-8")


def build_operation_plan() -> None:
    wf = REPORT / "rule_filter_walk_forward_summary_real_odds_2025_2026.md"
    wf_text = wf.read_text(encoding="utf-8") if wf.exists() else "(walk-forward summary not found)"
    lines = [
        "# rule_filter_shadow_operation_plan",
        "",
        "## 固定条件の根拠",
        f"- pair_value_score >= {VALUE_THR}",
        f"- pair_edge >= {EDGE_THR}",
        "- 実odds期間でROI改善傾向を確認済み。",
        "",
        "## ウォークフォワード結果（要約）",
        "- 固定条件は複数foldでROI>1を維持。",
        "- ただし買い目減少により利益額が細るfoldがある。",
        "",
        "## なぜ本番変更ではなくshadow運用か",
        "- ROIは改善余地がある一方、総利益と買い目母数のトレードオフが大きい。",
        "- まず運用データを追加取得して安定性を確認する必要がある。",
        "",
        "## 方針",
        "- ROI重視なら有望。",
        "- 利益額重視なら慎重運用。",
        "",
        "## 監視すべき指標",
        "- original_rule_candidate_count / filtered_rule_candidate_count",
        "- buy_reduction_rate",
        "- filtered ROI, profit, hit_rate",
        "- race_count_zero_after_filter",
        "- baseline 대비 ROI差・profit差",
        "",
        "## 本番採用条件",
        "- 追加30日以上でROI > 1.2",
        "- 現行ruleよりROIが高い",
        "- 利益額が極端に落ちない",
        "- race_count_zero_after_filterが多すぎない",
        "- 特定1日の大当たり依存でない",
    ]
    (REPORT / "rule_filter_shadow_operation_plan.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    REPORT.mkdir(parents=True, exist_ok=True)
    today_input = choose_today_input()
    build_today_outputs(today_input)
    build_backtest_outputs()
    build_operation_plan()
    print("done", today_input)


if __name__ == "__main__":
    main()
