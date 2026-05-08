from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _load(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"unsupported input: {path}")


def _quant(s: pd.Series, q: float) -> float | None:
    t = pd.to_numeric(s, errors="coerce")
    if t.notna().sum() == 0:
        return None
    return float(t.quantile(q))


def main() -> None:
    ap = argparse.ArgumentParser(description="Diagnose model_dynamic edge distribution.")
    ap.add_argument("--input", type=Path, required=True, help="candidate_pairs parquet/csv or pair comparison csv")
    ap.add_argument(
        "--out-csv",
        type=Path,
        default=Path("racing_ai/reports/model_dynamic_edge_diagnostics.csv"),
    )
    ap.add_argument(
        "--out-md",
        type=Path,
        default=Path("racing_ai/reports/model_dynamic_edge_diagnostics.md"),
    )
    ap.add_argument(
        "--out-pair-csv",
        type=Path,
        default=Path("racing_ai/reports/model_dynamic_edge_pair_diagnostics.csv"),
    )
    ap.add_argument("--min-score", type=float, default=0.08)
    ap.add_argument("--min-edge", type=float, default=0.00)
    ap.add_argument("--min-gap", type=float, default=0.01)
    args = ap.parse_args()

    df = _load(args.input)
    required = ["race_id", "pair_edge", "pair_model_score", "model_dynamic_final_score"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"missing required columns: {', '.join(missing)}")

    edge = pd.to_numeric(df["pair_edge"], errors="coerce")
    score = pd.to_numeric(df["pair_model_score"], errors="coerce")
    final_score = pd.to_numeric(df["model_dynamic_final_score"], errors="coerce")
    market_implied = pd.to_numeric(df["pair_market_implied_prob"], errors="coerce") if "pair_market_implied_prob" in df.columns else pd.Series([pd.NA] * len(df))
    selected = (
        pd.to_numeric(df["model_dynamic_selected_flag"], errors="coerce").fillna(0).astype(int)
        if "model_dynamic_selected_flag" in df.columns
        else pd.Series([0] * len(df))
    )

    overall = {
        "row_count": int(len(df)),
        "race_count": int(df["race_id"].nunique(dropna=True)),
        "pair_edge_non_null_count": int(edge.notna().sum()),
        "pair_edge_null_count": int(edge.isna().sum()),
        "pair_edge_min": float(edge.min()) if edge.notna().any() else None,
        "pair_edge_p01": _quant(edge, 0.01),
        "pair_edge_p05": _quant(edge, 0.05),
        "pair_edge_p10": _quant(edge, 0.10),
        "pair_edge_p25": _quant(edge, 0.25),
        "pair_edge_p50": _quant(edge, 0.50),
        "pair_edge_p75": _quant(edge, 0.75),
        "pair_edge_p90": _quant(edge, 0.90),
        "pair_edge_p95": _quant(edge, 0.95),
        "pair_edge_p99": _quant(edge, 0.99),
        "pair_edge_max": float(edge.max()) if edge.notna().any() else None,
        "pair_edge_mean": float(edge.mean()) if edge.notna().any() else None,
    }

    work = df.copy()
    work["_pair_edge"] = edge
    work["_pair_model_score"] = score
    work["_final"] = final_score
    work["_selected"] = selected
    work["_pair_market_implied_prob"] = market_implied
    work["pair_market_implied_prob"] = work["_pair_market_implied_prob"]
    if "model_dynamic_skip_reason" not in work.columns:
        work["model_dynamic_skip_reason"] = pd.NA
    if "pair_model_score_gap_to_next" not in work.columns:
        work["pair_model_score_gap_to_next"] = pd.NA

    work["pass_min_score"] = work["_pair_model_score"].ge(float(args.min_score)) & work["_pair_model_score"].notna()
    work["pass_min_edge"] = work["_pair_edge"].ge(float(args.min_edge)) & work["_pair_edge"].notna()
    work["pass_min_gap"] = (
        pd.to_numeric(work["pair_model_score_gap_to_next"], errors="coerce").ge(float(args.min_gap))
        & pd.to_numeric(work["pair_model_score_gap_to_next"], errors="coerce").notna()
    )
    work["dynamic_candidate_reason"] = "PASS_ALL"
    work.loc[~work["pass_min_score"], "dynamic_candidate_reason"] = "FAIL_MIN_SCORE"
    work.loc[work["pass_min_score"] & ~work["pass_min_edge"], "dynamic_candidate_reason"] = "FAIL_MIN_EDGE"
    work.loc[work["pass_min_score"] & work["pass_min_edge"] & ~work["pass_min_gap"], "dynamic_candidate_reason"] = "FAIL_MIN_GAP"
    work["dynamic_threshold_profile"] = (
        f"score>={args.min_score:.4f}|edge>={args.min_edge:.4f}|gap>={args.min_gap:.4f}"
    )
    eps = 1e-9
    work["pair_edge_ratio"] = (
        pd.to_numeric(work["_pair_model_score"], errors="coerce")
        / (pd.to_numeric(work["_pair_market_implied_prob"], errors="coerce") + eps)
    )
    work["pair_edge_log_ratio"] = (
        (pd.to_numeric(work["_pair_model_score"], errors="coerce") + eps)
        / (pd.to_numeric(work["_pair_market_implied_prob"], errors="coerce") + eps)
    ).map(lambda x: pd.NA if pd.isna(x) or x <= 0 else float(__import__("math").log(x)))
    work["model_rank_in_race"] = (
        work.groupby("race_id")["_pair_model_score"].rank(method="min", ascending=False)
    )
    work["market_rank_in_race"] = (
        work.groupby("race_id")["_pair_market_implied_prob"].rank(method="min", ascending=False)
    )
    work["model_rank_pct_in_race"] = (
        work.groupby("race_id")["_pair_model_score"].rank(method="average", ascending=False, pct=True)
    )
    work["market_rank_pct_in_race"] = (
        work.groupby("race_id")["_pair_market_implied_prob"].rank(method="average", ascending=False, pct=True)
    )
    work["pair_edge_rank_gap"] = work["market_rank_in_race"] - work["model_rank_in_race"]
    work["pair_edge_pct_gap"] = work["market_rank_pct_in_race"] - work["model_rank_pct_in_race"]

    race_diag = (
        work.groupby("race_id", dropna=False)
        .agg(
            pair_count=("race_id", "size"),
            edge_min=("_pair_edge", "min"),
            edge_max=("_pair_edge", "max"),
            edge_mean=("_pair_edge", "mean"),
            edge_p50=("_pair_edge", lambda x: x.quantile(0.5) if x.notna().any() else pd.NA),
            edge_p75=("_pair_edge", lambda x: x.quantile(0.75) if x.notna().any() else pd.NA),
            edge_p90=("_pair_edge", lambda x: x.quantile(0.9) if x.notna().any() else pd.NA),
            positive_edge_pair_count=("_pair_edge", lambda x: int((x > 0).sum())),
            max_pair_model_score=("_pair_model_score", "max"),
            max_model_dynamic_final_score=("_final", "max"),
            current_selected_count=("_selected", "sum"),
            current_skip_reason=("model_dynamic_skip_reason", lambda x: x.dropna().iloc[0] if x.dropna().size > 0 else pd.NA),
        )
        .reset_index()
        .sort_values(["edge_max", "edge_mean"], ascending=[True, True], na_position="last")
    )

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    race_diag.to_csv(args.out_csv, index=False, encoding="utf-8")
    pair_cols = [
        "race_id",
        "pair_norm",
        "pair_edge",
        "pair_model_score",
        "pair_market_implied_prob",
        "pair_model_score_gap_to_next",
        "pair_edge_ratio",
        "pair_edge_log_ratio",
        "pair_edge_rank_gap",
        "pair_edge_pct_gap",
        "model_rank_in_race",
        "market_rank_in_race",
        "model_rank_pct_in_race",
        "market_rank_pct_in_race",
        "pass_min_score",
        "pass_min_edge",
        "pass_min_gap",
        "dynamic_candidate_reason",
        "dynamic_threshold_profile",
        "model_dynamic_skip_reason",
        "model_dynamic_selected_flag",
    ]
    available_pair_cols = [c for c in pair_cols if c in work.columns]
    args.out_pair_csv.parent.mkdir(parents=True, exist_ok=True)
    work[available_pair_cols].to_csv(args.out_pair_csv, index=False, encoding="utf-8")

    md_lines = [
        "# model_dynamic Edge Diagnostics",
        "",
        f"- input: {args.input}",
        f"- row_count: {overall['row_count']}",
        f"- race_count: {overall['race_count']}",
        f"- pair_edge non-null/null: {overall['pair_edge_non_null_count']} / {overall['pair_edge_null_count']}",
        f"- pair_edge min/mean/max: {overall['pair_edge_min']} / {overall['pair_edge_mean']} / {overall['pair_edge_max']}",
        f"- pair_edge p01/p05/p10: {overall['pair_edge_p01']} / {overall['pair_edge_p05']} / {overall['pair_edge_p10']}",
        f"- pair_edge p25/p50/p75: {overall['pair_edge_p25']} / {overall['pair_edge_p50']} / {overall['pair_edge_p75']}",
        f"- pair_edge p90/p95/p99: {overall['pair_edge_p90']} / {overall['pair_edge_p95']} / {overall['pair_edge_p99']}",
        f"- pair_model_score min/p50/p90/p95/max: {score.min()} / {score.quantile(0.5)} / {score.quantile(0.9)} / {score.quantile(0.95)} / {score.max()}",
        f"- pair_market_implied_prob min/p50/p90/p95/max: {market_implied.min()} / {market_implied.quantile(0.5)} / {market_implied.quantile(0.9)} / {market_implied.quantile(0.95)} / {market_implied.max()}",
        f"- (market-model) min/p50/p90/p95/max: {(market_implied-score).min()} / {(market_implied-score).quantile(0.5)} / {(market_implied-score).quantile(0.9)} / {(market_implied-score).quantile(0.95)} / {(market_implied-score).max()}",
        f"- pair_market_implied_prob >0.3 / >0.5 / >0.7: {int((market_implied>0.3).sum())} / {int((market_implied>0.5).sum())} / {int((market_implied>0.7).sum())}",
        f"- pair_model_score >0.05 / >0.08 / >0.10: {int((score>0.05).sum())} / {int((score>0.08).sum())} / {int((score>0.10).sum())}",
        f"- threshold_profile: score>={args.min_score}, edge>={args.min_edge}, gap>={args.min_gap}",
        f"- out_pair_csv: {args.out_pair_csv}",
        "",
        "## Race-Level (Top 30 edge_max ascending)",
        "",
    ]
    try:
        md_lines.append(race_diag.head(30).to_markdown(index=False))
    except Exception:
        md_lines.append(race_diag.head(30).to_string(index=False))
    md_lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- `edge_max < 0` が多い場合は `min_edge=0.00` が厳しすぎる可能性が高いです。",
            "- `edge_p90` が0未満なら負値edge探索を優先してください。",
            "- `positive_edge_pair_count=0` のレースは edge基準だけでは選定不能です。",
        ]
    )
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(md_lines), encoding="utf-8")

    print(str(args.out_csv))
    print(str(args.out_md))
    print(str(args.out_pair_csv))


if __name__ == "__main__":
    main()
