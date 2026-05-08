from __future__ import annotations

import argparse
import itertools
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


EPS = 1e-9
REQUIRED_BASE = ["race_id", "pair_norm", "pair_model_score", "pair_selected_flag"]
OPTIONAL_LABEL = ["actual_wide_hit", "wide_payout"]


def _parse_list(s: str, cast):
    return [cast(x.strip()) for x in s.split(",") if x.strip() != ""]


def _load(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _to_bool_series(s: pd.Series) -> pd.Series:
    if str(s.dtype) == "bool":
        return s.fillna(False)
    n = pd.to_numeric(s, errors="coerce")
    return n.fillna(0).astype(int).astype(bool)


def _ensure_base(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in REQUIRED_BASE:
        if c not in out.columns:
            raise ValueError(f"missing required column: {c}")
    if "pair_model_score_gap_to_next" not in out.columns:
        s = out.sort_values(["race_id", "pair_model_score"], ascending=[True, False]).copy()
        s["pair_model_score_gap_to_next"] = pd.to_numeric(s["pair_model_score"], errors="coerce") - pd.to_numeric(
            s.groupby("race_id")["pair_model_score"].shift(-1), errors="coerce"
        )
        out = out.merge(
            s[["race_id", "pair_norm", "pair_model_score_gap_to_next"]],
            on=["race_id", "pair_norm"],
            how="left",
            suffixes=("", "_bf"),
        )
        if "pair_model_score_gap_to_next_bf" in out.columns:
            out["pair_model_score_gap_to_next"] = out["pair_model_score_gap_to_next"].fillna(out["pair_model_score_gap_to_next_bf"])
            out = out.drop(columns=["pair_model_score_gap_to_next_bf"])
    if "pair_market_implied_prob" not in out.columns:
        if "pair_fused_prob_sum" in out.columns:
            out["pair_market_implied_prob"] = pd.to_numeric(out["pair_fused_prob_sum"], errors="coerce")
        elif "pair_edge" in out.columns:
            out["pair_market_implied_prob"] = pd.to_numeric(out["pair_model_score"], errors="coerce") - pd.to_numeric(
                out["pair_edge"], errors="coerce"
            )
        else:
            out["pair_market_implied_prob"] = pd.NA
    if "pair_edge" not in out.columns:
        out["pair_edge"] = pd.to_numeric(out["pair_model_score"], errors="coerce") - pd.to_numeric(
            out["pair_market_implied_prob"], errors="coerce"
        )
    out["pair_model_score"] = pd.to_numeric(out["pair_model_score"], errors="coerce")
    out["pair_market_implied_prob"] = pd.to_numeric(out["pair_market_implied_prob"], errors="coerce")
    out["pair_edge"] = pd.to_numeric(out["pair_edge"], errors="coerce")

    if "pair_edge_ratio" not in out.columns:
        out["pair_edge_ratio"] = out["pair_model_score"] / (out["pair_market_implied_prob"] + EPS)
    if "pair_edge_log_ratio" not in out.columns:
        out["pair_edge_log_ratio"] = np.log((out["pair_model_score"] + EPS) / (out["pair_market_implied_prob"] + EPS))

    s2 = out.sort_values(["race_id", "pair_model_score"], ascending=[True, False]).copy()
    s2["model_rank_in_race"] = s2.groupby("race_id")["pair_model_score"].rank(method="min", ascending=False)
    s2["model_rank_pct_in_race"] = s2.groupby("race_id")["pair_model_score"].rank(method="average", ascending=False, pct=True)
    s2["market_rank_in_race"] = s2.groupby("race_id")["pair_market_implied_prob"].rank(method="min", ascending=False)
    s2["market_rank_pct_in_race"] = s2.groupby("race_id")["pair_market_implied_prob"].rank(method="average", ascending=False, pct=True)
    s2["pair_edge_rank_gap"] = s2["market_rank_in_race"] - s2["model_rank_in_race"]
    s2["pair_edge_pct_gap"] = s2["market_rank_pct_in_race"] - s2["model_rank_pct_in_race"]
    out = out.drop(
        columns=[
            c
            for c in [
                "model_rank_in_race",
                "model_rank_pct_in_race",
                "market_rank_in_race",
                "market_rank_pct_in_race",
                "pair_edge_rank_gap",
                "pair_edge_pct_gap",
            ]
            if c in out.columns
        ],
        errors="ignore",
    )
    out = out.merge(
        s2[
            [
                "race_id",
                "pair_norm",
                "model_rank_in_race",
                "model_rank_pct_in_race",
                "market_rank_in_race",
                "market_rank_pct_in_race",
                "pair_edge_rank_gap",
                "pair_edge_pct_gap",
            ]
        ],
        on=["race_id", "pair_norm"],
        how="left",
    )
    return out


def _variant_score(df: pd.DataFrame, variant: str) -> pd.Series:
    if variant == "diff":
        return pd.to_numeric(df["pair_edge"], errors="coerce")
    if variant == "ratio":
        return pd.to_numeric(df["pair_edge_ratio"], errors="coerce")
    if variant == "log_ratio":
        return pd.to_numeric(df["pair_edge_log_ratio"], errors="coerce")
    if variant == "rank_gap":
        return pd.to_numeric(df["pair_edge_rank_gap"], errors="coerce")
    if variant == "pct_gap":
        return pd.to_numeric(df["pair_edge_pct_gap"], errors="coerce")
    raise ValueError(f"unknown variant: {variant}")


def _final_score(df: pd.DataFrame, variant: str, alpha: float) -> pd.Series:
    m = pd.to_numeric(df["pair_model_score"], errors="coerce")
    if variant == "diff":
        return m + pd.to_numeric(df["pair_edge"], errors="coerce")
    if variant == "ratio":
        return m * pd.to_numeric(df["pair_edge_ratio"], errors="coerce")
    if variant == "log_ratio":
        return m * np.exp(pd.to_numeric(df["pair_edge_log_ratio"], errors="coerce"))
    if variant == "rank_gap":
        return m + alpha * pd.to_numeric(df["pair_edge_rank_gap"], errors="coerce")
    if variant == "pct_gap":
        return m + alpha * pd.to_numeric(df["pair_edge_pct_gap"], errors="coerce")
    return m


def _roi_proxy(df: pd.DataFrame, flag_col: str) -> tuple[Any, Any, Any, Any]:
    sel = df[_to_bool_series(df[flag_col])].copy()
    bet = int(len(sel))
    if bet == 0:
        return 0, 0.0, 0.0, None
    hit = pd.to_numeric(sel.get("actual_wide_hit"), errors="coerce")
    pay = pd.to_numeric(sel.get("wide_payout"), errors="coerce")
    if hit.notna().any() and pay.notna().any():
        h = int(hit.fillna(0).sum())
        payout = float((pay.fillna(0) * (hit.fillna(0) > 0).astype(float)).sum())
        roi = payout / (bet * 100.0) if bet > 0 else None
        return h, float(h / bet), payout, roi
    return None, None, None, None


def _evaluate_combo(
    base_df: pd.DataFrame,
    *,
    variant: str,
    threshold: float,
    min_score: float,
    min_gap: float,
    default_k: int,
    max_k: int,
    alpha: float,
) -> dict[str, Any]:
    work = base_df.copy()
    work["variant_score"] = _variant_score(work, variant)
    work["model_variant_final_score"] = _final_score(work, variant, alpha)
    work["pair_model_score_gap_to_next"] = pd.to_numeric(work["pair_model_score_gap_to_next"], errors="coerce")
    work["pair_model_score"] = pd.to_numeric(work["pair_model_score"], errors="coerce")
    work["pass_min_score"] = work["pair_model_score"].ge(min_score) & work["pair_model_score"].notna()
    work["pass_variant"] = work["variant_score"].ge(threshold) & work["variant_score"].notna()
    work["pass_min_gap"] = work["pair_model_score_gap_to_next"].ge(min_gap) & work["pair_model_score_gap_to_next"].notna()
    work["pass_all"] = work["pass_min_score"] & work["pass_variant"] & work["pass_min_gap"]

    selected_idx: list[int] = []
    selected_race_count = 0
    k_base = max(1, int(min(default_k, max_k)))
    for _, g in work.groupby("race_id", sort=False):
        cand = g[g["pass_all"]].sort_values("model_variant_final_score", ascending=False)
        if len(cand) == 0:
            continue
        selected_race_count += 1
        k = min(k_base, len(cand))
        selected_idx.extend(cand.head(k).index.tolist())

    work["model_variant_selected_flag"] = False
    if selected_idx:
        work.loc[selected_idx, "model_variant_selected_flag"] = True

    sel = work[_to_bool_series(work["model_variant_selected_flag"])].copy()
    selected_pair_count = int(len(sel))
    selected_race_count = int(selected_race_count)
    avg_selected_pairs_per_race = (selected_pair_count / selected_race_count) if selected_race_count > 0 else None

    rule = work[_to_bool_series(work["pair_selected_flag"])].copy()
    rule_pairs = set(zip(rule["race_id"].astype(str), rule["pair_norm"].astype(str)))
    sel_pairs = set(zip(sel["race_id"].astype(str), sel["pair_norm"].astype(str)))
    overlap = len(rule_pairs.intersection(sel_pairs))
    overlap_rate = (overlap / len(sel_pairs)) if len(sel_pairs) > 0 else None

    non_overlap = work[_to_bool_series(work["model_variant_selected_flag"]) & ~_to_bool_series(work["pair_selected_flag"])].copy()

    dyn_hit_count, dyn_hit_rate, dyn_total_payout, dyn_roi = _roi_proxy(work, "model_variant_selected_flag")
    rule_hit_count, rule_hit_rate, rule_total_payout, rule_roi = _roi_proxy(work, "pair_selected_flag")
    no_hit_count, no_hit_rate, no_total_payout, no_roi = _roi_proxy(non_overlap.assign(model_variant_selected_flag=True), "model_variant_selected_flag")

    out = {
        "edge_variant": variant,
        "variant_threshold": threshold,
        "min_score": min_score,
        "min_gap": min_gap,
        "default_k": int(default_k),
        "max_k": int(max_k),
        "selected_pair_count": selected_pair_count,
        "selected_race_count": selected_race_count,
        "avg_selected_pairs_per_race": avg_selected_pairs_per_race,
        "rule_overlap_count": overlap,
        "rule_dynamic_overlap_rate": overlap_rate,
        "rule_non_overlap_dynamic_pair_count": int(len(non_overlap)),
        "rule_non_overlap_dynamic_hit_count": no_hit_count,
        "rule_non_overlap_dynamic_hit_rate": no_hit_rate,
        "rule_non_overlap_dynamic_total_payout": no_total_payout,
        "rule_non_overlap_dynamic_roi_proxy": no_roi,
        "dynamic_hit_count": dyn_hit_count,
        "dynamic_bet_count": selected_pair_count,
        "dynamic_hit_rate": dyn_hit_rate,
        "dynamic_total_payout": dyn_total_payout,
        "dynamic_roi_proxy": dyn_roi,
        "rule_hit_count": rule_hit_count,
        "rule_bet_count": int(len(rule)),
        "rule_hit_rate": rule_hit_rate,
        "rule_total_payout": rule_total_payout,
        "rule_roi_proxy": rule_roi,
        "dynamic_minus_rule_roi": (dyn_roi - rule_roi) if dyn_roi is not None and rule_roi is not None else None,
        "dynamic_minus_rule_hit_rate": (dyn_hit_rate - rule_hit_rate) if dyn_hit_rate is not None and rule_hit_rate is not None else None,
        "avg_variant_score_selected": float(pd.to_numeric(sel["variant_score"], errors="coerce").mean()) if len(sel) > 0 else None,
        "avg_pair_model_score_selected": float(pd.to_numeric(sel["pair_model_score"], errors="coerce").mean()) if len(sel) > 0 else None,
        "avg_pair_market_implied_prob_selected": float(pd.to_numeric(sel["pair_market_implied_prob"], errors="coerce").mean()) if len(sel) > 0 else None,
    }
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Grid search dynamic selection by edge variants (shadow-only, post-calc).")
    ap.add_argument("--input", type=Path, required=True, help="pair_shadow_pair_comparison.csv/parquet or diagnostics pair csv")
    ap.add_argument("--out-csv", type=Path, default=Path("racing_ai/reports/model_dynamic_edge_variant_grid_summary.csv"))
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/model_dynamic_edge_variant_grid_summary.md"))
    ap.add_argument("--variants", default="diff,ratio,log_ratio,rank_gap,pct_gap")
    ap.add_argument("--diff-thresholds", default="-0.50,-0.40,-0.30,-0.20,-0.10,-0.05,0.00")
    ap.add_argument("--ratio-thresholds", default="0.25,0.35,0.45,0.55,0.70,0.85,1.00")
    ap.add_argument("--log-ratio-thresholds", default="-1.50,-1.20,-1.00,-0.80,-0.60,-0.40,-0.20,0.00")
    ap.add_argument("--rank-gap-thresholds", default="-5,-3,-1,0,1,2,3,5")
    ap.add_argument("--pct-gap-thresholds", default="-0.50,-0.30,-0.20,-0.10,0.00,0.10,0.20")
    ap.add_argument("--min-score-values", default="0.04,0.06,0.08,0.10")
    ap.add_argument("--min-gap-values", default="0.000,0.005,0.010,0.020")
    ap.add_argument("--default-k-values", default="3,5")
    ap.add_argument("--max-k-values", default="3,5")
    ap.add_argument("--rank-alpha", type=float, default=0.01)
    args = ap.parse_args()

    base = _ensure_base(_load(args.input))
    for c in OPTIONAL_LABEL:
        if c not in base.columns:
            base[c] = pd.NA

    variants = [x.strip() for x in args.variants.split(",") if x.strip()]
    thresholds = {
        "diff": _parse_list(args.diff_thresholds, float),
        "ratio": _parse_list(args.ratio_thresholds, float),
        "log_ratio": _parse_list(args.log_ratio_thresholds, float),
        "rank_gap": _parse_list(args.rank_gap_thresholds, float),
        "pct_gap": _parse_list(args.pct_gap_thresholds, float),
    }
    min_scores = _parse_list(args.min_score_values, float)
    min_gaps = _parse_list(args.min_gap_values, float)
    default_ks = _parse_list(args.default_k_values, int)
    max_ks = _parse_list(args.max_k_values, int)

    rows: list[dict[str, Any]] = []
    for variant in variants:
        ths = thresholds[variant]
        for th, ms, mg, dk, mk in itertools.product(ths, min_scores, min_gaps, default_ks, max_ks):
            rows.append(
                _evaluate_combo(
                    base,
                    variant=variant,
                    threshold=th,
                    min_score=ms,
                    min_gap=mg,
                    default_k=dk,
                    max_k=mk,
                    alpha=args.rank_alpha,
                )
            )

    out = pd.DataFrame(rows)
    out = out.sort_values(
        ["rule_non_overlap_dynamic_pair_count", "rule_dynamic_overlap_rate", "dynamic_minus_rule_roi", "selected_pair_count"],
        ascending=[False, True, False, False],
        na_position="last",
    )
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    by_variant = (
        out.groupby("edge_variant", dropna=False)
        .agg(
            condition_count=("edge_variant", "size"),
            selected_nonzero_conditions=("selected_pair_count", lambda x: int((pd.to_numeric(x, errors="coerce") > 0).sum())),
            non_overlap_conditions=("rule_non_overlap_dynamic_pair_count", lambda x: int((pd.to_numeric(x, errors="coerce") > 0).sum())),
            min_overlap_rate=("rule_dynamic_overlap_rate", "min"),
            avg_model_score_selected=("avg_pair_model_score_selected", "mean"),
            avg_market_prob_selected=("avg_pair_market_implied_prob_selected", "mean"),
        )
        .reset_index()
    )
    top = out.head(20)
    try:
        by_variant_md = by_variant.to_markdown(index=False)
    except Exception:
        by_variant_md = by_variant.to_string(index=False)
    try:
        top_md = top.to_markdown(index=False)
    except Exception:
        top_md = top.to_string(index=False)

    mp = pd.to_numeric(base["pair_market_implied_prob"], errors="coerce")
    ms = pd.to_numeric(base["pair_model_score"], errors="coerce")
    ratio = mp / (ms + EPS)
    md_lines = [
        "# model_dynamic_edge_variant_grid_summary",
        "",
        f"- input: {args.input}",
        f"- rows: {len(base)}",
        f"- output_csv: {args.out_csv}",
        "",
        "## Variant Summary",
        "",
        by_variant_md,
        "",
        "## Top Candidates",
        "",
        top_md,
        "",
        "## Market Proxy Audit",
        "",
        f"- pair_market_implied_prob p50/p90/p95/max: {mp.quantile(0.5)} / {mp.quantile(0.9)} / {mp.quantile(0.95)} / {mp.max()}",
        f"- pair_model_score p50/p90/p95/max: {ms.quantile(0.5)} / {ms.quantile(0.9)} / {ms.quantile(0.95)} / {ms.max()}",
        f"- market/model ratio p50/p90/p95: {ratio.quantile(0.5)} / {ratio.quantile(0.9)} / {ratio.quantile(0.95)}",
        f"- pair_market_implied_prob >0.3 / >0.5 / >0.7: {int((mp>0.3).sum())} / {int((mp>0.5).sum())} / {int((mp>0.7).sum())}",
        f"- pair_model_score >0.08 / >0.10: {int((ms>0.08).sum())} / {int((ms>0.10).sum())}",
        "",
        "## Comments",
        "",
        "- diff が全負寄りの場合、ratio/log_ratio/rank_gap/pct_gap の比較を優先してください。",
        "- market proxy が常に model score を上回る場合、ペア確率尺度が揃っていない可能性があります。",
        "- rule非重複ペアが0なら、現行候補集合の時点でモデル差別化余地が小さい可能性があります。",
    ]
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(md_lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
