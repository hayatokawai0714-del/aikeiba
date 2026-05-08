from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from itertools import product
from pathlib import Path

import pandas as pd
import numpy as np


def _to_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    x = s.astype(str).str.lower()
    return x.isin(["true", "1", "t", "yes", "y"])


def _quant(x) -> float | None:
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return None


def _edge_variant(df: pd.DataFrame, variant: str, eps: float = 1e-9) -> pd.Series:
    if variant == "diff":
        return pd.to_numeric(df.get("pair_edge"), errors="coerce")
    if variant == "ratio":
        return pd.to_numeric(df.get("pair_edge_ratio"), errors="coerce")
    if variant == "log_ratio":
        return pd.to_numeric(df.get("pair_edge_log_ratio"), errors="coerce")
    if variant == "rank_gap":
        return pd.to_numeric(df.get("pair_edge_rank_gap"), errors="coerce")
    if variant == "pct_gap":
        return pd.to_numeric(df.get("pair_edge_pct_gap"), errors="coerce")
    raise ValueError(f"unknown edge_variant: {variant}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluation-only grid search for model_dynamic thresholds on 2025 (quality_ok races only).")
    ap.add_argument("--pairs-csv", type=Path, required=True, help="Joined pairs CSV with actual_wide_hit/wide_payout and result_quality_status.")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    ap.add_argument("--min-score-values", default="0.04,0.06,0.08,0.10")
    ap.add_argument("--min-edge-values", default="-0.10,-0.05,-0.02,0.00,0.02")
    ap.add_argument("--min-gap-values", default="0.000,0.005,0.010,0.020")
    ap.add_argument("--default-k-values", default="3,5")
    ap.add_argument("--max-k-values", default="3,5")
    ap.add_argument("--edge-variants", default="diff,ratio,log_ratio,rank_gap,pct_gap")
    args = ap.parse_args()

    df = pd.read_csv(args.pairs_csv)
    df["race_date"] = df.get("race_date", "").astype(str)

    # Quality OK filter
    if "result_quality_status" in df.columns:
        df = df[df["result_quality_status"] == "ok"].copy()
        df = df.reset_index(drop=True)

    # Required columns for simulation
    required = ["race_id", "race_date", "pair_norm", "pair_model_score", "pair_selected_flag"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError("missing required columns: " + ",".join(missing))

    df["pair_model_score"] = pd.to_numeric(df["pair_model_score"], errors="coerce")
    # Some historical joined CSVs don't include `pair_model_score_gap_to_next`.
    # Compute it deterministically from pair_model_score within each race_id if missing.
    if "pair_model_score_gap_to_next" in df.columns:
        df["pair_model_score_gap_to_next"] = pd.to_numeric(df["pair_model_score_gap_to_next"], errors="coerce").fillna(0.0)
    else:
        df["pair_model_score_gap_to_next"] = 0.0
        for rid, idx in df.groupby("race_id").groups.items():
            i = list(idx)
            scores = df.loc[i, "pair_model_score"].astype(float)
            order = scores.sort_values(ascending=False).index.tolist()
            if not order:
                continue
            next_scores = df.loc[order, "pair_model_score"].shift(-1)
            gaps = (df.loc[order, "pair_model_score"] - next_scores).fillna(0.0)
            df.loc[order, "pair_model_score_gap_to_next"] = gaps.clip(lower=0.0).astype(float)
    df["_rule"] = _to_bool(df["pair_selected_flag"])

    # Precompute group indices for speed (grid can be a few thousand cells).
    race_groups = [idx for _, idx in df.groupby("race_id").groups.items()]
    date_groups = {str(d): idx for d, idx in df.groupby("race_date").groups.items()}

    arr_model = df["pair_model_score"].to_numpy(dtype=float)
    arr_gap = df["pair_model_score_gap_to_next"].to_numpy(dtype=float)
    arr_rule = df["_rule"].to_numpy(dtype=bool)
    arr_hit = pd.to_numeric(df.get("actual_wide_hit"), errors="coerce").to_numpy() if "actual_wide_hit" in df.columns else None
    arr_payout = pd.to_numeric(df.get("wide_payout"), errors="coerce").to_numpy() if "wide_payout" in df.columns else None

    # Precompute rule metrics per day
    def _roi_from_mask(frame: pd.DataFrame, mask: pd.Series) -> tuple[float | None, int, int, float]:
        sel = frame[mask].copy()
        n = int(len(sel))
        if n == 0:
            return None, 0, 0, 0.0
        if "actual_wide_hit" not in sel.columns or "wide_payout" not in sel.columns:
            return None, n, 0, 0.0
        ok = sel["actual_wide_hit"].notna() & sel["wide_payout"].notna()
        sel2 = sel[ok]
        if len(sel2) == 0:
            return None, n, 0, 0.0
        payout = float((pd.to_numeric(sel2["wide_payout"], errors="coerce").fillna(0.0) * pd.to_numeric(sel2["actual_wide_hit"], errors="coerce").fillna(0.0)).sum())
        cost = float(n * 100)
        roi = float(payout / cost) if cost > 0 else None
        hit = int(pd.to_numeric(sel2["actual_wide_hit"], errors="coerce").fillna(0.0).sum())
        return roi, n, hit, payout

    # Rule metrics per day (for daily positive/negative days)
    rule_daily = {}
    for d, idx in date_groups.items():
        idx = np.asarray(idx)
        n = int(arr_rule[idx].sum())
        if n == 0 or arr_hit is None or arr_payout is None:
            rule_daily[str(d)] = {"roi": None, "n": n, "hit": 0, "payout": 0.0}
            continue
        ok = ~np.isnan(arr_hit[idx]) & ~np.isnan(arr_payout[idx])
        hit = int(np.nansum(arr_hit[idx][ok]))
        payout = float(np.nansum(arr_payout[idx][ok] * arr_hit[idx][ok]))
        cost = float(n * 100)
        roi = float(payout / cost) if cost > 0 else None
        rule_daily[str(d)] = {"roi": roi, "n": n, "hit": hit, "payout": payout}

    min_scores = [float(x) for x in args.min_score_values.split(",") if x.strip() != ""]
    min_edges = [float(x) for x in args.min_edge_values.split(",") if x.strip() != ""]
    min_gaps = [float(x) for x in args.min_gap_values.split(",") if x.strip() != ""]
    default_ks = [int(x) for x in args.default_k_values.split(",") if x.strip() != ""]
    max_ks = [int(x) for x in args.max_k_values.split(",") if x.strip() != ""]
    variants = [v.strip() for v in args.edge_variants.split(",") if v.strip()]

    rows = []
    for variant, min_score, thr, min_gap, default_k, max_k in product(variants, min_scores, min_edges, min_gaps, default_ks, max_ks):
        # For each race, if top pair passes all gates, select up to K pairs by final_score:
        # final_score = pair_model_score (keep simple; edge gates handled by threshold)
        sel = np.zeros(len(df), dtype=bool)
        non_overlap = np.zeros(len(df), dtype=bool)

        var_score = _edge_variant(df, variant).to_numpy(dtype=float)
        pass_score = arr_model >= float(min_score)
        pass_edge = var_score >= float(thr)
        pass_gap = arr_gap >= float(min_gap)
        pass_all = pass_score & pass_edge & pass_gap & ~np.isnan(arr_model)

        k = max(1, int(default_k))
        k = min(int(max_k), k)
        for idx in race_groups:
            idx = np.asarray(idx)
            m = pass_all[idx]
            if not m.any():
                continue
            cand = idx[m]
            order = cand[np.argsort(arr_model[cand])[::-1]]
            pick = order[:k]
            sel[pick] = True
            non_overlap[pick] = ~arr_rule[pick]

        # Daily metrics
        dyn_pos = 0
        dyn_neg = 0
        for d, didx in date_groups.items():
            didx = np.asarray(didx)
            # ROI for selected pairs in this date
            mask = sel[didx]
            dyn_n = int(mask.sum())
            dyn_roi = None
            if dyn_n > 0 and arr_hit is not None and arr_payout is not None:
                sidx = didx[mask]
                ok = ~np.isnan(arr_hit[sidx]) & ~np.isnan(arr_payout[sidx])
                payout = float(np.nansum(arr_payout[sidx][ok] * arr_hit[sidx][ok]))
                dyn_roi = float(payout / (dyn_n * 100)) if dyn_n > 0 else None
            rule_roi = rule_daily.get(str(d), {}).get("roi")
            if dyn_roi is None or rule_roi is None:
                continue
            diff = float(dyn_roi - rule_roi)
            if diff > 0:
                dyn_pos += 1
            elif diff < 0:
                dyn_neg += 1

        # Overall ROI
        dyn_n = int(sel.sum())
        dyn_hit = 0
        dyn_payout = 0.0
        dyn_roi = None
        if dyn_n > 0 and arr_hit is not None and arr_payout is not None:
            sidx = np.where(sel)[0]
            ok = ~np.isnan(arr_hit[sidx]) & ~np.isnan(arr_payout[sidx])
            dyn_hit = int(np.nansum(arr_hit[sidx][ok]))
            dyn_payout = float(np.nansum(arr_payout[sidx][ok] * arr_hit[sidx][ok]))
            dyn_roi = float(dyn_payout / (dyn_n * 100)) if dyn_n > 0 else None

        rule_n_overall = int(arr_rule.sum())
        rule_hit_overall = 0
        rule_payout_overall = 0.0
        rule_roi_overall = None
        if rule_n_overall > 0 and arr_hit is not None and arr_payout is not None:
            ridx = np.where(arr_rule)[0]
            ok = ~np.isnan(arr_hit[ridx]) & ~np.isnan(arr_payout[ridx])
            rule_hit_overall = int(np.nansum(arr_hit[ridx][ok]))
            rule_payout_overall = float(np.nansum(arr_payout[ridx][ok] * arr_hit[ridx][ok]))
            rule_roi_overall = float(rule_payout_overall / (rule_n_overall * 100)) if rule_n_overall > 0 else None

        non_overlap_n = int(non_overlap.sum())
        overlap_n = int((sel & arr_rule).sum())
        selected_n = int(sel.sum())

        rows.append(
            {
                "edge_variant": variant,
                "min_score": min_score,
                "variant_threshold": thr,
                "min_gap": min_gap,
                "default_k": default_k,
                "max_k": max_k,
                "selected_pair_count": selected_n,
                "non_overlap_count": non_overlap_n,
                "overlap_count": overlap_n,
                "non_overlap_rate": (float(non_overlap_n / selected_n) if selected_n > 0 else None),
                "dynamic_roi": dyn_roi,
                "rule_roi": rule_roi_overall,
                "dynamic_minus_rule_roi": (float(dyn_roi - rule_roi_overall) if (dyn_roi is not None and rule_roi_overall is not None) else None),
                "dynamic_hit_rate": (float(dyn_hit / dyn_n) if dyn_n > 0 else None),
                "rule_hit_rate": (float(rule_hit_overall / rule_n_overall) if rule_n_overall > 0 else None),
                "dynamic_positive_days": dyn_pos,
                "dynamic_negative_days": dyn_neg,
                "quality_ok_pair_rows": int(len(df)),
                "quality_ok_race_count": int(df["race_id"].nunique()),
                "quality_ok_date_count": int(df["race_date"].nunique()),
            }
        )

    out_df = pd.DataFrame(rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.out_csv, index=False, encoding="utf-8")

    # Rank: prefer non_overlap_count>0 and ROI improvement
    ranked = out_df.sort_values(
        ["non_overlap_count", "dynamic_minus_rule_roi", "selected_pair_count"],
        ascending=[False, False, True],
        na_position="last",
    ).head(20)

    md = [
        "# Model Dynamic Threshold Grid 2025 (quality_ok only)",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- input: {args.pairs_csv}",
        f"- rows(quality_ok pairs): {len(df)}",
        f"- races(quality_ok): {df['race_id'].nunique()}",
        f"- dates(quality_ok): {df['race_date'].nunique()}",
        "",
        "## Output",
        "",
        f"- csv: {args.out_csv}",
        "",
        "## Top 20 (ranked by non_overlap_count then ROI delta)",
        "",
    ]
    if len(ranked) > 0:
        md += [
            "| edge_variant | thr | min_score | min_gap | k | max_k | selected | non_overlap | non_overlap_rate | dyn_roi | rule_roi | delta_roi | pos_days | neg_days |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for _, r in ranked.iterrows():
            md.append(
                f"| {r['edge_variant']} | {r['variant_threshold']} | {r['min_score']} | {r['min_gap']} | {int(r['default_k'])} | {int(r['max_k'])} | {int(r['selected_pair_count'])} | {int(r['non_overlap_count'])} | {'' if pd.isna(r['non_overlap_rate']) else round(float(r['non_overlap_rate']),4)} | {'' if pd.isna(r['dynamic_roi']) else round(float(r['dynamic_roi']),6)} | {'' if pd.isna(r['rule_roi']) else round(float(r['rule_roi']),6)} | {'' if pd.isna(r['dynamic_minus_rule_roi']) else round(float(r['dynamic_minus_rule_roi']),6)} | {int(r['dynamic_positive_days'])} | {int(r['dynamic_negative_days'])} |"
            )
        md.append("")

    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
