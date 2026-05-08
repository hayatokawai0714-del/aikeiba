from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path
from typing import Any


REQUIRED_COLUMNS = [
    "race_id",
    "pair_norm",
    "pair_model_score",
    "pair_edge",
    "pair_model_score_gap_to_next",
    "pair_model_score_rank_in_race",
    "pair_value_score",
    "pair_selected_flag",
    "model_top5_flag",
]

OPTIONAL_LABEL_COLUMNS = ["actual_wide_hit", "wide_payout"]


def _parse_list(s: str, cast):
    return [cast(x.strip()) for x in s.split(",") if x.strip() != ""]


def _load_pairs(path: Path) -> pd.DataFrame:
    import pandas as pd
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_parquet(path)


def _ensure_required(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            "missing required columns for grid search: "
            + ", ".join(missing)
            + "\nrequired: "
            + ", ".join(cols)
        )


def _backfill_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    import pandas as pd
    out = df.copy()
    if "model_top5_flag" not in out.columns and "pair_model_score" in out.columns and "race_id" in out.columns:
        out["pair_model_score_rank_in_race"] = (
            out.groupby("race_id")["pair_model_score"].rank(method="min", ascending=False)
        )
        out["model_top5_flag"] = out["pair_model_score_rank_in_race"] <= 5
    if "pair_model_score_rank_in_race" not in out.columns and "pair_model_score" in out.columns and "race_id" in out.columns:
        out["pair_model_score_rank_in_race"] = (
            out.groupby("race_id")["pair_model_score"].rank(method="min", ascending=False)
        )
    if "pair_model_score_gap_to_next" not in out.columns and "pair_model_score" in out.columns and "race_id" in out.columns:
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
        if "pair_model_score_gap_to_next_bf" in out.columns and "pair_model_score_gap_to_next" in out.columns:
            out["pair_model_score_gap_to_next"] = out["pair_model_score_gap_to_next"].fillna(out["pair_model_score_gap_to_next_bf"])
            out = out.drop(columns=["pair_model_score_gap_to_next_bf"])
    return out


def _to_bool_series(s: pd.Series) -> pd.Series:
    import pandas as pd
    if str(s.dtype) == "bool":
        return s.fillna(False)
    num = pd.to_numeric(s, errors="coerce")
    return num.fillna(0).astype(int).astype(bool)


def _roi_proxy_metrics(df: pd.DataFrame, selected_flag_col: str) -> dict[str, Any]:
    import pandas as pd
    has_hit = "actual_wide_hit" in df.columns and pd.to_numeric(df["actual_wide_hit"], errors="coerce").notna().any()
    has_pay = "wide_payout" in df.columns and pd.to_numeric(df["wide_payout"], errors="coerce").notna().any()
    out: dict[str, Any] = {}
    sel = df[_to_bool_series(df[selected_flag_col])].copy()
    out["bet_count"] = int(len(sel))
    if has_hit:
        hit = pd.to_numeric(sel["actual_wide_hit"], errors="coerce").fillna(0)
        out["hit_count"] = int(hit.sum())
        out["hit_rate"] = float(hit.mean()) if len(sel) > 0 else None
    else:
        out["hit_count"] = None
        out["hit_rate"] = None
    if has_hit and has_pay:
        hit = pd.to_numeric(sel["actual_wide_hit"], errors="coerce").fillna(0)
        pay = pd.to_numeric(sel["wide_payout"], errors="coerce")
        valid = pay.notna() & hit.notna()
        if int(valid.sum()) == 0:
            out["total_payout"] = None
            out["roi_proxy"] = None
        else:
            payout_sum = float((pay.fillna(0) * (hit > 0).astype(float)).sum())
            cost = float(len(sel) * 100.0)
            out["total_payout"] = payout_sum
            out["roi_proxy"] = (payout_sum / cost) if cost > 0 else None
    else:
        out["total_payout"] = None
        out["roi_proxy"] = None
    return out


def _apply_dynamic_selection(
    df: pd.DataFrame,
    *,
    min_score: float,
    min_edge: float,
    min_gap: float,
    default_k: int,
    max_k: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    import pandas as pd
    work = df.copy()
    work["pair_model_score"] = pd.to_numeric(work["pair_model_score"], errors="coerce")
    work["pair_edge"] = pd.to_numeric(work["pair_edge"], errors="coerce")
    work["pair_model_score_gap_to_next"] = pd.to_numeric(work["pair_model_score_gap_to_next"], errors="coerce")
    rows: list[pd.DataFrame] = []
    reason_counts: dict[str, int] = {
        "DYNAMIC_BUY_OK": 0,
        "DYNAMIC_SKIP_MODEL_SCORE_WEAK": 0,
        "DYNAMIC_SKIP_EDGE_WEAK": 0,
        "DYNAMIC_SKIP_GAP_SMALL": 0,
        "DYNAMIC_SKIP_OTHER": 0,
    }
    buy = 0
    skip = 0
    k_base = max(1, int(min(default_k, max_k)))
    for _, g in work.groupby("race_id", sort=False):
        gg = g.sort_values("pair_model_score", ascending=False).copy()
        gg["pass_min_score"] = gg["pair_model_score"].ge(float(min_score)) & gg["pair_model_score"].notna()
        gg["pass_min_edge"] = gg["pair_edge"].ge(float(min_edge)) & gg["pair_edge"].notna()
        gg["pass_min_gap"] = gg["pair_model_score_gap_to_next"].ge(float(min_gap)) & gg["pair_model_score_gap_to_next"].notna()
        gg["pass_all_thresholds"] = gg["pass_min_score"] & gg["pass_min_edge"] & gg["pass_min_gap"]
        gg["dynamic_threshold_profile"] = (
            f"score>={min_score:.4f}|edge>={min_edge:.4f}|gap>={min_gap:.4f}|k={int(default_k)}-{int(max_k)}"
        )
        gg["dynamic_candidate_reason"] = "PASS_ALL"
        gg.loc[~gg["pass_min_score"], "dynamic_candidate_reason"] = "FAIL_MIN_SCORE"
        gg.loc[gg["pass_min_score"] & ~gg["pass_min_edge"], "dynamic_candidate_reason"] = "FAIL_MIN_EDGE"
        gg.loc[gg["pass_min_score"] & gg["pass_min_edge"] & ~gg["pass_min_gap"], "dynamic_candidate_reason"] = "FAIL_MIN_GAP"
        top1 = gg["pair_model_score"].iloc[0] if len(gg) > 0 else None
        top_edge = gg["pair_edge"].iloc[0] if len(gg) > 0 else None
        top_gap = gg["pair_model_score_gap_to_next"].iloc[0] if len(gg) > 0 else None
        reason = "DYNAMIC_BUY_OK"
        if pd.isna(top1) or float(top1) < float(min_score):
            reason = "DYNAMIC_SKIP_MODEL_SCORE_WEAK"
        elif pd.isna(top_edge) or float(top_edge) < float(min_edge):
            reason = "DYNAMIC_SKIP_EDGE_WEAK"
        elif pd.isna(top_gap) or float(top_gap) < float(min_gap):
            reason = "DYNAMIC_SKIP_GAP_SMALL"
        if reason.startswith("DYNAMIC_SKIP"):
            skip += 1
            k = 0
        else:
            buy += 1
            if pd.notna(top_edge) and float(top_edge) >= 0.08:
                k = min(3, k_base)
            elif pd.notna(top_edge) and float(top_edge) >= 0.03:
                k = min(4, k_base)
            else:
                k = k_base
        if reason not in reason_counts:
            reason_counts["DYNAMIC_SKIP_OTHER"] += 1
        else:
            reason_counts[reason] += 1
        gg["model_dynamic_rank"] = range(1, len(gg) + 1)
        gg["model_dynamic_skip_reason"] = reason
        gg["model_dynamic_k"] = int(k)
        gg["model_dynamic_selected_flag"] = gg["model_dynamic_rank"] <= int(k)
        gg["model_dynamic_final_score"] = gg["pair_model_score"] * (1.0 + gg["pair_edge"].fillna(0).clip(lower=0))
        rows.append(gg)
    out = pd.concat(rows, ignore_index=True) if rows else work
    metrics = {
        "buy_race_count": int(buy),
        "skip_race_count": int(skip),
        "selected_pair_count": int(_to_bool_series(out["model_dynamic_selected_flag"]).sum()) if "model_dynamic_selected_flag" in out.columns else 0,
        "skip_reason_counts": reason_counts,
    }
    return out, metrics


def _overlap_metrics(df: pd.DataFrame) -> dict[str, Any]:
    import pandas as pd
    rows = []
    for _, g in df.groupby("race_id", sort=False):
        rule = set(g.loc[_to_bool_series(g["pair_selected_flag"]), "pair_norm"].astype(str).tolist())
        dyn = set(g.loc[_to_bool_series(g["model_dynamic_selected_flag"]), "pair_norm"].astype(str).tolist())
        overlap = len(rule.intersection(dyn))
        rows.append(
            {
                "rule_count": len(rule),
                "dyn_count": len(dyn),
                "overlap_count": overlap,
                "overlap_rate_dyn_base": (overlap / len(dyn)) if len(dyn) > 0 else None,
            }
        )
    o = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["overlap_count", "overlap_rate_dyn_base"])
    dynamic_non_overlap = df[_to_bool_series(df["model_dynamic_selected_flag"]) & ~_to_bool_series(df["pair_selected_flag"])].copy()
    out = {
        "overlap_count_total": int(o["overlap_count"].sum()) if len(o) > 0 else 0,
        "overlap_rate_avg": float(o["overlap_rate_dyn_base"].dropna().mean()) if len(o) > 0 and o["overlap_rate_dyn_base"].notna().any() else None,
        "dynamic_non_overlap_count": int(len(dynamic_non_overlap)),
        "dynamic_non_overlap_hit_rate": None,
        "dynamic_non_overlap_roi_proxy": None,
    }
    if "actual_wide_hit" in dynamic_non_overlap.columns:
        hit = pd.to_numeric(dynamic_non_overlap["actual_wide_hit"], errors="coerce")
        if hit.notna().any() and len(dynamic_non_overlap) > 0:
            out["dynamic_non_overlap_hit_rate"] = float(hit.fillna(0).mean())
    if "actual_wide_hit" in dynamic_non_overlap.columns and "wide_payout" in dynamic_non_overlap.columns:
        hit = pd.to_numeric(dynamic_non_overlap["actual_wide_hit"], errors="coerce").fillna(0)
        pay = pd.to_numeric(dynamic_non_overlap["wide_payout"], errors="coerce")
        if pay.notna().any() and len(dynamic_non_overlap) > 0:
            payout_sum = float((pay.fillna(0) * (hit > 0).astype(float)).sum())
            cost = float(len(dynamic_non_overlap) * 100.0)
            out["dynamic_non_overlap_roi_proxy"] = (payout_sum / cost) if cost > 0 else None
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Grid search model_dynamic thresholds from pair comparison output.")
    ap.add_argument("--input", type=Path, required=True, help="pair_shadow_pair_comparison.csv/parquet")
    ap.add_argument("--out-csv", type=Path, default=Path("racing_ai/reports/model_dynamic_threshold_grid_summary.csv"))
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/model_dynamic_threshold_grid_summary.md"))
    ap.add_argument("--min-score-values", default="0.06,0.08,0.10")
    ap.add_argument("--min-edge-values", default="-0.05,-0.03,-0.02,-0.01,0.00,0.01")
    ap.add_argument("--min-gap-values", default="0.005,0.01,0.02")
    ap.add_argument("--default-k-values", default="3,5")
    ap.add_argument("--max-k-values", default="3,5")
    args = ap.parse_args()
    import pandas as pd

    df = _load_pairs(args.input)
    df = _backfill_required_columns(df)
    _ensure_required(df, REQUIRED_COLUMNS)

    for c in OPTIONAL_LABEL_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA

    min_scores = _parse_list(args.min_score_values, float)
    min_edges = _parse_list(args.min_edge_values, float)
    min_gaps = _parse_list(args.min_gap_values, float)
    default_ks = _parse_list(args.default_k_values, int)
    max_ks = _parse_list(args.max_k_values, int)

    rows: list[dict[str, Any]] = []
    for min_score, min_edge, min_gap, default_k, max_k in itertools.product(
        min_scores, min_edges, min_gaps, default_ks, max_ks
    ):
        selected_df, dyn = _apply_dynamic_selection(
            df,
            min_score=min_score,
            min_edge=min_edge,
            min_gap=min_gap,
            default_k=default_k,
            max_k=max_k,
        )
        dyn_eval = _roi_proxy_metrics(selected_df, "model_dynamic_selected_flag")
        rule_eval = _roi_proxy_metrics(selected_df, "pair_selected_flag")
        top5_eval = _roi_proxy_metrics(selected_df, "model_top5_flag")
        ov = _overlap_metrics(selected_df)
        buy = int(dyn["buy_race_count"])
        selected_pairs = int(dyn["selected_pair_count"])
        pass_score_pair_count = int(pd.to_numeric(selected_df["pass_min_score"], errors="coerce").fillna(0).astype(int).sum())
        pass_edge_pair_count = int(pd.to_numeric(selected_df["pass_min_edge"], errors="coerce").fillna(0).astype(int).sum())
        pass_gap_pair_count = int(pd.to_numeric(selected_df["pass_min_gap"], errors="coerce").fillna(0).astype(int).sum())
        pass_all_pair_count = int(pd.to_numeric(selected_df["pass_all_thresholds"], errors="coerce").fillna(0).astype(int).sum())
        race_with_any_pass_count = int(
            (selected_df.groupby("race_id")["pass_all_thresholds"].apply(lambda x: bool(x.fillna(False).any()))).sum()
        )
        race_with_selected_count = int(
            (selected_df.groupby("race_id")["model_dynamic_selected_flag"].apply(lambda x: bool(_to_bool_series(x).any()))).sum()
        )
        rows.append(
            {
                "min_score": min_score,
                "min_edge": min_edge,
                "min_gap": min_gap,
                "default_k": int(default_k),
                "max_k": int(max_k),
                "buy_race_count": buy,
                "skip_race_count": int(dyn["skip_race_count"]),
                "selected_pair_count": selected_pairs,
                "avg_selected_pairs_per_buy_race": (selected_pairs / buy) if buy > 0 else None,
                "avg_pair_edge_selected": (
                    float(pd.to_numeric(selected_df.loc[_to_bool_series(selected_df["model_dynamic_selected_flag"]), "pair_edge"], errors="coerce").dropna().mean())
                    if selected_pairs > 0
                    else None
                ),
                "rule_model_overlap_avg": ov["overlap_rate_avg"],
                "dynamic_hit_count": dyn_eval["hit_count"],
                "dynamic_bet_count": dyn_eval["bet_count"],
                "dynamic_hit_rate": dyn_eval["hit_rate"],
                "dynamic_total_payout": dyn_eval["total_payout"],
                "dynamic_roi_proxy": dyn_eval["roi_proxy"],
                "rule_hit_count": rule_eval["hit_count"],
                "rule_bet_count": rule_eval["bet_count"],
                "rule_hit_rate": rule_eval["hit_rate"],
                "rule_total_payout": rule_eval["total_payout"],
                "rule_roi_proxy": rule_eval["roi_proxy"],
                "model_top5_hit_count": top5_eval["hit_count"],
                "model_top5_bet_count": top5_eval["bet_count"],
                "model_top5_hit_rate": top5_eval["hit_rate"],
                "model_top5_total_payout": top5_eval["total_payout"],
                "model_top5_roi_proxy": top5_eval["roi_proxy"],
                "overlap_count_total": ov["overlap_count_total"],
                "overlap_rate": ov["overlap_rate_avg"],
                "dynamic_non_overlap_count": ov["dynamic_non_overlap_count"],
                "dynamic_non_overlap_hit_rate": ov["dynamic_non_overlap_hit_rate"],
                "dynamic_non_overlap_roi_proxy": ov["dynamic_non_overlap_roi_proxy"],
                "pass_score_pair_count": pass_score_pair_count,
                "pass_edge_pair_count": pass_edge_pair_count,
                "pass_gap_pair_count": pass_gap_pair_count,
                "pass_all_pair_count": pass_all_pair_count,
                "pass_score_rate": (pass_score_pair_count / len(selected_df)) if len(selected_df) > 0 else None,
                "pass_edge_rate": (pass_edge_pair_count / len(selected_df)) if len(selected_df) > 0 else None,
                "pass_gap_rate": (pass_gap_pair_count / len(selected_df)) if len(selected_df) > 0 else None,
                "pass_all_rate": (pass_all_pair_count / len(selected_df)) if len(selected_df) > 0 else None,
                "race_with_any_pass_count": race_with_any_pass_count,
                "race_with_selected_count": race_with_selected_count,
                "skip_reason_counts": json.dumps(dyn["skip_reason_counts"], ensure_ascii=False),
            }
        )

    out_df = pd.DataFrame(rows).sort_values(
        ["dynamic_roi_proxy", "dynamic_hit_rate", "avg_pair_edge_selected"],
        ascending=[False, False, False],
        na_position="last",
    )
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.out_csv, index=False, encoding="utf-8")

    top = out_df.head(10)
    try:
        top_table = top.to_markdown(index=False)
    except Exception:
        top_table = top.to_string(index=False)
    md_lines = [
        "# model_dynamic_threshold_grid_summary",
        "",
        f"- input: {args.input}",
        f"- rows: {len(out_df)}",
        f"- output_csv: {args.out_csv}",
        "",
        "## Top 10",
        "",
        top_table,
    ]
    if len(out_df) > 0 and int((pd.to_numeric(out_df["selected_pair_count"], errors="coerce").fillna(0) > 0).sum()) == 0:
        edge = pd.to_numeric(df.get("pair_edge"), errors="coerce")
        score = pd.to_numeric(df.get("pair_model_score"), errors="coerce")
        gap = pd.to_numeric(df.get("pair_model_score_gap_to_next"), errors="coerce")
        md_lines.extend(
            [
                "",
                "## All-Zero Selection Diagnostics",
                "",
                f"- pair_edge max/p90/p95: {edge.max():.6f} / {edge.quantile(0.90):.6f} / {edge.quantile(0.95):.6f}",
                f"- pair_model_score max/p90/p95: {score.max():.6f} / {score.quantile(0.90):.6f} / {score.quantile(0.95):.6f}",
                f"- pair_model_score_gap_to_next max/p90/p95: {gap.max():.6f} / {gap.quantile(0.90):.6f} / {gap.quantile(0.95):.6f}",
            ]
        )
        edge_bind = edge.max() < min(min_edges) if len(min_edges) > 0 and edge.notna().any() else False
        score_bind = score.max() < min(min_scores) if len(min_scores) > 0 and score.notna().any() else False
        gap_bind = gap.quantile(0.95) < min(min_gaps) if len(min_gaps) > 0 and gap.notna().any() else False
        if edge_bind:
            md_lines.append("- likely bottleneck: edge threshold (try smaller/negative min_edge).")
        if score_bind:
            md_lines.append("- likely bottleneck: model score threshold (lower min_score).")
        if gap_bind:
            md_lines.append("- likely bottleneck: gap threshold (lower min_gap).")
        if not any([edge_bind, score_bind, gap_bind]):
            md_lines.append("- likely bottleneck: compound conditions; inspect pass_score_rate/pass_edge_rate/pass_gap_rate.")
        md_lines.extend(
            [
                "- suggested next range: min_edge in [-0.08, -0.01], min_score in [0.03, 0.08], min_gap in [0.000, 0.010].",
            ]
        )
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(md_lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
