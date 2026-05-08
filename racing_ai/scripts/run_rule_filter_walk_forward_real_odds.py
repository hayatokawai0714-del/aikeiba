from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import statistics

import numpy as np
import pandas as pd

BASE = Path(r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai")
REPORT = BASE / "reports" / "2024_eval_full_v5"
PAIR_CSV = REPORT / "pair_shadow_pair_comparison_expanded_real_odds_2025_2026_with_results_external_priority.csv"
CAND_CSV = REPORT / "real_odds_evaluation_candidate_dates.csv"

FIX_VALUE = 0.0591
FIX_EDGE = 0.02


def to_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int).astype(bool)


def metrics(df: pd.DataFrame) -> dict:
    if len(df) == 0:
        return dict(candidate_count=0, hit_count=0, hit_rate=np.nan, total_payout=0.0, cost=0.0, roi=np.nan, profit=0.0)
    hit = pd.to_numeric(df["actual_wide_hit"], errors="coerce").fillna(0)
    payout = pd.to_numeric(df["wide_payout"], errors="coerce").fillna(0)
    wins = (hit > 0).astype(float)
    total_payout = float((payout * wins).sum())
    cost = float(len(df) * 100)
    profit = total_payout - cost
    roi = total_payout / cost if cost > 0 else np.nan
    return dict(
        candidate_count=int(len(df)),
        hit_count=int(hit.sum()),
        hit_rate=float(wins.mean()),
        total_payout=total_payout,
        cost=cost,
        roi=float(roi),
        profit=profit,
    )


def prepare_rule_df() -> pd.DataFrame:
    df = pd.read_csv(PAIR_CSV)
    df = df[to_bool(df["pair_selected_flag"])].copy()
    df = df[df["result_quality_status"].astype(str) == "ok"].copy()
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df[df["race_date"].notna()].copy()
    df["pair_value_score"] = pd.to_numeric(df["pair_value_score"], errors="coerce")
    df["pair_edge"] = pd.to_numeric(df["pair_edge"], errors="coerce")
    df["pair_value_score_rank"] = df.groupby("race_id")["pair_value_score"].rank(ascending=False, method="first")
    return df


def monthly_splits(candidate_dates: list[pd.Timestamp], train_months: int = 3, test_months: int = 1) -> list[dict]:
    if not candidate_dates:
        return []
    s = pd.Series(candidate_dates)
    months = sorted(s.dt.to_period("M").unique())
    out = []
    fid = 1
    i = 0
    while i + train_months + test_months <= len(months):
        tr = months[i : i + train_months]
        te = months[i + train_months : i + train_months + test_months]
        tr_dates = sorted([d for d in candidate_dates if d.to_period("M") in tr])
        te_dates = sorted([d for d in candidate_dates if d.to_period("M") in te])
        if tr_dates and te_dates:
            out.append(
                {
                    "split_type": "monthly_walk_forward",
                    "fold_id": f"WF_{fid:02d}",
                    "train_start": tr_dates[0].date().isoformat(),
                    "train_end": tr_dates[-1].date().isoformat(),
                    "test_start": te_dates[0].date().isoformat(),
                    "test_end": te_dates[-1].date().isoformat(),
                }
            )
            fid += 1
        i += test_months
    return out


def apply_filters(
    df: pd.DataFrame,
    value_thr: float,
    edge_thr: float,
    max_per_race: int | None,
    rank_cap: int | None,
) -> pd.DataFrame:
    g = df.copy()
    g = g[(g["pair_value_score"] >= value_thr) & (g["pair_edge"] >= edge_thr)].copy()
    if rank_cap is not None:
        g = g[g["pair_value_score_rank"] <= rank_cap].copy()
    if max_per_race is not None:
        g = g.sort_values(["race_id", "pair_value_score"], ascending=[True, False]).groupby("race_id", as_index=False).head(max_per_race)
    return g


def main() -> None:
    REPORT.mkdir(parents=True, exist_ok=True)

    cand = pd.read_csv(CAND_CSV)
    cand = cand[cand["is_candidate"] == 1].copy()
    cand["race_date"] = pd.to_datetime(cand["race_date"], errors="coerce")
    cand_dates = sorted(cand["race_date"].dropna().unique().tolist())

    splits = [
        {
            "split_type": "half_split",
            "fold_id": "A_half",
            "train_start": "2025-04-20",
            "train_end": "2025-12-31",
            "test_start": "2026-01-01",
            "test_end": "2026-04-26",
        }
    ]
    splits += monthly_splits([pd.Timestamp(x) for x in cand_dates], 3, 1)
    sp = pd.DataFrame(splits)
    sp.to_csv(REPORT / "walk_forward_splits_real_odds_2025_2026.csv", index=False, encoding="utf-8-sig")
    (REPORT / "walk_forward_splits_real_odds_2025_2026.md").write_text(
        "# walk_forward_splits_real_odds_2025_2026\n\n" + sp.to_string(index=False), encoding="utf-8"
    )

    df = prepare_rule_df()

    # Fixed threshold WF
    fixed_rows = []
    for r in splits:
        tr_s, tr_e = pd.Timestamp(r["train_start"]), pd.Timestamp(r["train_end"])
        te_s, te_e = pd.Timestamp(r["test_start"]), pd.Timestamp(r["test_end"])

        train = df[(df["race_date"] >= tr_s) & (df["race_date"] <= tr_e)].copy()
        test = df[(df["race_date"] >= te_s) & (df["race_date"] <= te_e)].copy()

        train_base = metrics(train)
        test_base = metrics(test)

        train_f = apply_filters(train, FIX_VALUE, FIX_EDGE, None, None)
        test_f = apply_filters(test, FIX_VALUE, FIX_EDGE, None, None)

        train_m = metrics(train_f)
        test_m = metrics(test_f)

        fixed_rows.append(
            {
                **r,
                "condition": f"value>={FIX_VALUE} & edge>={FIX_EDGE}",
                "train_candidate_count": train_m["candidate_count"],
                "train_roi": train_m["roi"],
                "train_profit": train_m["profit"],
                "test_candidate_count": test_m["candidate_count"],
                "test_roi": test_m["roi"],
                "test_profit": test_m["profit"],
                "test_hit_rate": test_m["hit_rate"],
                "buy_reduction_rate": (1 - test_m["candidate_count"] / test_base["candidate_count"]) if test_base["candidate_count"] else np.nan,
                "test_baseline_roi": test_base["roi"],
                "test_baseline_profit": test_base["profit"],
                "test_roi_diff_vs_baseline": test_m["roi"] - test_base["roi"] if pd.notna(test_m["roi"]) and pd.notna(test_base["roi"]) else np.nan,
                "test_profit_diff_vs_baseline": test_m["profit"] - test_base["profit"],
            }
        )

    fixed_df = pd.DataFrame(fixed_rows)
    fixed_df.to_csv(REPORT / "rule_filter_walk_forward_fixed_real_odds_2025_2026.csv", index=False, encoding="utf-8-sig")
    (REPORT / "rule_filter_walk_forward_fixed_real_odds_2025_2026.md").write_text(
        "# rule_filter_walk_forward_fixed_real_odds_2025_2026\n\n" + fixed_df.to_string(index=False), encoding="utf-8"
    )

    # Optimized on train only
    grid_value = sorted(set([FIX_VALUE, float(df["pair_value_score"].quantile(0.5)), float(df["pair_value_score"].quantile(0.6)), float(df["pair_value_score"].quantile(0.7))]))
    grid_edge = sorted(set([0.0, 0.01, 0.02, 0.03]))
    grid_max_per_race = [None, 3, 2, 1]
    grid_rank_cap = [None, 5, 3, 2, 1]

    opt_rows = []
    for r in splits:
        tr_s, tr_e = pd.Timestamp(r["train_start"]), pd.Timestamp(r["train_end"])
        te_s, te_e = pd.Timestamp(r["test_start"]), pd.Timestamp(r["test_end"])

        train = df[(df["race_date"] >= tr_s) & (df["race_date"] <= tr_e)].copy()
        test = df[(df["race_date"] >= te_s) & (df["race_date"] <= te_e)].copy()
        test_base = metrics(test)

        best = None
        best_key = None
        for vt in grid_value:
            for et in grid_edge:
                for mpr in grid_max_per_race:
                    for rc in grid_rank_cap:
                        tr_f = apply_filters(train, vt, et, mpr, rc)
                        tr_m = metrics(tr_f)
                        # optimize train by ROI then profit, with minimum sample guard
                        if tr_m["candidate_count"] < 80:
                            continue
                        key = (tr_m["roi"], tr_m["profit"], tr_m["candidate_count"])
                        if (best is None) or (key > best_key):
                            best = (vt, et, mpr, rc, tr_m)
                            best_key = key

        if best is None:
            vt, et, mpr, rc = FIX_VALUE, FIX_EDGE, None, None
            tr_f = apply_filters(train, vt, et, mpr, rc)
            tr_m = metrics(tr_f)
        else:
            vt, et, mpr, rc, tr_m = best

        te_f = apply_filters(test, vt, et, mpr, rc)
        te_m = metrics(te_f)

        opt_rows.append(
            {
                **r,
                "selected_value_threshold": vt,
                "selected_edge_threshold": et,
                "selected_max_per_race": -1 if mpr is None else mpr,
                "selected_value_rank_cap": -1 if rc is None else rc,
                "train_candidate_count": tr_m["candidate_count"],
                "train_roi": tr_m["roi"],
                "train_profit": tr_m["profit"],
                "test_candidate_count": te_m["candidate_count"],
                "test_roi": te_m["roi"],
                "test_profit": te_m["profit"],
                "test_hit_rate": te_m["hit_rate"],
                "buy_reduction_rate": (1 - te_m["candidate_count"] / test_base["candidate_count"]) if test_base["candidate_count"] else np.nan,
                "test_baseline_roi": test_base["roi"],
                "test_baseline_profit": test_base["profit"],
                "test_roi_diff_vs_baseline": te_m["roi"] - test_base["roi"] if pd.notna(te_m["roi"]) and pd.notna(test_base["roi"]) else np.nan,
                "test_profit_diff_vs_baseline": te_m["profit"] - test_base["profit"],
            }
        )

    opt_df = pd.DataFrame(opt_rows)
    opt_df.to_csv(REPORT / "rule_filter_walk_forward_optimized_real_odds_2025_2026.csv", index=False, encoding="utf-8-sig")
    (REPORT / "rule_filter_walk_forward_optimized_real_odds_2025_2026.md").write_text(
        "# rule_filter_walk_forward_optimized_real_odds_2025_2026\n\n" + opt_df.to_string(index=False), encoding="utf-8"
    )

    def summarize(x: pd.DataFrame, label: str) -> list[str]:
        rois = pd.to_numeric(x["test_roi"], errors="coerce").dropna().tolist()
        profits = pd.to_numeric(x["test_profit"], errors="coerce").dropna().tolist()
        diff = pd.to_numeric(x["test_roi_diff_vs_baseline"], errors="coerce")
        cands = pd.to_numeric(x["test_candidate_count"], errors="coerce")
        return [
            f"### {label}",
            f"- test_positive_fold_count: {int((pd.Series(rois) > 1.0).sum()) if rois else 0}",
            f"- test_negative_fold_count: {int((pd.Series(rois) < 1.0).sum()) if rois else 0}",
            f"- test_roi_mean: {float(np.nanmean(rois)) if rois else np.nan}",
            f"- test_roi_median: {float(np.nanmedian(rois)) if rois else np.nan}",
            f"- test_profit_total: {float(np.nansum(profits)) if profits else 0.0}",
            f"- baseline_outperform_fold_count: {int((diff > 0).sum())}",
            f"- test_candidate_count_mean: {float(cands.mean()) if len(cands) else np.nan}",
            f"- test_candidate_count_std: {float(cands.std()) if len(cands) else np.nan}",
            "",
        ]

    lines = [
        "# rule_filter_walk_forward_summary_real_odds_2025_2026",
        "",
        "## Fixed Condition",
        f"- pair_value_score >= {FIX_VALUE}",
        f"- pair_edge >= {FIX_EDGE}",
        "",
    ]
    lines += summarize(fixed_df, "Fixed Walk-Forward")
    lines += summarize(opt_df, "Optimized Walk-Forward (train-only)")

    # judgment notes
    fixed_roi_mean = float(pd.to_numeric(fixed_df["test_roi"], errors="coerce").mean())
    fixed_profit_total = float(pd.to_numeric(fixed_df["test_profit"], errors="coerce").sum())
    opt_roi_mean = float(pd.to_numeric(opt_df["test_roi"], errors="coerce").mean())
    opt_profit_total = float(pd.to_numeric(opt_df["test_profit"], errors="coerce").sum())

    lines += [
        "## Judgment",
        f"- 固定条件は未来期間でも有効か: {'Yes' if fixed_roi_mean > 1.0 else 'No'} (test ROI mean={fixed_roi_mean:.4f})",
        f"- 最適化条件は過剰最適化していないか: {'Likely acceptable' if opt_roi_mean > 1.0 else 'Risky'} (test ROI mean={opt_roi_mean:.4f})",
        f"- ROI重視なら採用候補か: {'Yes' if max(fixed_roi_mean,opt_roi_mean) > 1.1 else 'No'}",
        f"- 利益額重視なら採用候補か: {'Conditional' if max(fixed_profit_total,opt_profit_total) > 0 else 'No'}",
        "- 本番ruleを変えるべきか: まだ変更せず（shadow継続推奨）",
        "- まずshadow運用すべきか: Yes",
    ]

    (REPORT / "rule_filter_walk_forward_summary_real_odds_2025_2026.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("done")


if __name__ == "__main__":
    main()
