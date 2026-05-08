from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _load(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _to_bool(s: pd.Series) -> pd.Series:
    if str(s.dtype) == "bool":
        return s.fillna(False)
    n = pd.to_numeric(s, errors="coerce")
    return n.fillna(0).astype(int).astype(bool)


def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "pair_model_score" not in out.columns:
        out["pair_model_score"] = pd.NA
    out["pair_model_score"] = pd.to_numeric(out["pair_model_score"], errors="coerce")
    if "pair_selected_flag" not in out.columns:
        out["pair_selected_flag"] = False
    out["pair_selected_flag"] = _to_bool(out["pair_selected_flag"])
    if "model_top5_flag" not in out.columns:
        out["model_top5_flag"] = (
            out.groupby("race_id")["pair_model_score"].rank(method="min", ascending=False) <= 5
        )
    else:
        out["model_top5_flag"] = _to_bool(out["model_top5_flag"])
    for c in ["actual_wide_hit", "wide_payout", "pair_value_score", "pair_market_implied_prob", "pair_edge", "pair_edge_ratio", "pair_edge_rank_gap"]:
        if c not in out.columns:
            out[c] = pd.NA
    if "pair_market_implied_prob" in out.columns and out["pair_market_implied_prob"].isna().all() and "pair_edge" in out.columns:
        out["pair_market_implied_prob"] = pd.to_numeric(out["pair_model_score"], errors="coerce") - pd.to_numeric(out["pair_edge"], errors="coerce")
    return out


def _roi_proxy(df: pd.DataFrame) -> tuple[int | None, float | None, float | None]:
    if len(df) == 0:
        return 0, None, None
    hit = pd.to_numeric(df["actual_wide_hit"], errors="coerce")
    pay = pd.to_numeric(df["wide_payout"], errors="coerce")
    if hit.notna().any() and pay.notna().any():
        h = int(hit.fillna(0).sum())
        payout = float((pay.fillna(0) * (hit.fillna(0) > 0).astype(float)).sum())
        roi = payout / (len(df) * 100.0)
        return h, float(h / len(df)), roi
    return None, None, None


def main() -> None:
    ap = argparse.ArgumentParser(description="Diagnose candidate pool rule dominance and non-rule model picks.")
    ap.add_argument("--inputs", required=True, help="Comma-separated pair_shadow_pair_comparison.csv paths")
    ap.add_argument("--out-csv", type=Path, default=Path("racing_ai/reports/candidate_pool_rule_dominance.csv"))
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/candidate_pool_rule_dominance.md"))
    ap.add_argument("--out-non-rule-csv", type=Path, default=Path("racing_ai/reports/non_rule_model_candidates.csv"))
    ap.add_argument("--out-non-rule-md", type=Path, default=Path("racing_ai/reports/non_rule_model_candidates.md"))
    ap.add_argument("--non-rule-top-n", type=int, default=3)
    args = ap.parse_args()

    rows_race: list[dict] = []
    rows_non_rule: list[dict] = []
    global_stats: list[dict] = []

    for p in [Path(x.strip()) for x in args.inputs.split(",") if x.strip()]:
        df = _ensure_cols(_load(p))
        day = "UNKNOWN"
        for part in p.parts:
            if len(part) == 10 and part[4] == "-" and part[7] == "-":
                day = part
                break
        df["race_date"] = day
        total = len(df)
        rule_count = int(df["pair_selected_flag"].sum())
        non_rule_count = int((~df["pair_selected_flag"]).sum())
        model_top5_count = int(df["model_top5_flag"].sum())
        model_top5_non_rule_count = int((df["model_top5_flag"] & ~df["pair_selected_flag"]).sum())
        model_topk_non_rule_count = int(
            (
                (df.groupby("race_id")["pair_model_score"].rank(method="min", ascending=False) <= max(5, args.non_rule_top_n))
                & (~df["pair_selected_flag"])
            ).sum()
        )
        global_stats.append(
            {
                "race_date": day,
                "total_pair_count": total,
                "race_count": int(df["race_id"].nunique()),
                "rule_selected_pair_count": rule_count,
                "non_rule_pair_count": non_rule_count,
                "model_top5_pair_count": model_top5_count,
                "model_top5_non_rule_pair_count": model_top5_non_rule_count,
                "model_score_topK_non_rule_count": model_topk_non_rule_count,
                "model_score_topK_non_rule_rate": (model_topk_non_rule_count / total) if total > 0 else None,
            }
        )

        for rid, g in df.groupby("race_id", sort=False):
            gg = g.sort_values("pair_model_score", ascending=False).copy()
            gg["model_rank"] = range(1, len(gg) + 1)
            top1 = gg.iloc[0] if len(gg) > 0 else None
            non_rule = gg[~gg["pair_selected_flag"]].copy()
            top3_non_rule = int((non_rule["model_rank"] <= 3).sum()) if len(non_rule) > 0 else 0
            top5_non_rule = int((non_rule["model_rank"] <= 5).sum()) if len(non_rule) > 0 else 0
            top10_non_rule = int((non_rule["model_rank"] <= 10).sum()) if len(non_rule) > 0 else 0
            rule_max = pd.to_numeric(gg.loc[gg["pair_selected_flag"], "pair_model_score"], errors="coerce").max()
            non_rule_max = pd.to_numeric(non_rule["pair_model_score"], errors="coerce").max() if len(non_rule) > 0 else None
            best_non_rule = non_rule.head(1)
            rows_race.append(
                {
                    "race_date": day,
                    "race_id": rid,
                    "pair_count": int(len(gg)),
                    "rule_selected_count": int(gg["pair_selected_flag"].sum()),
                    "non_rule_count": int((~gg["pair_selected_flag"]).sum()),
                    "model_top1_pair_norm": (None if top1 is None else top1.get("pair_norm")),
                    "model_top1_is_rule": (None if top1 is None else bool(top1.get("pair_selected_flag"))),
                    "model_top3_non_rule_count": top3_non_rule,
                    "model_top5_non_rule_count": top5_non_rule,
                    "model_top10_non_rule_count": top10_non_rule,
                    "max_model_score_rule": (None if pd.isna(rule_max) else float(rule_max)),
                    "max_model_score_non_rule": (None if non_rule_max is None or pd.isna(non_rule_max) else float(non_rule_max)),
                    "model_score_gap_non_rule_minus_rule": (
                        None
                        if non_rule_max is None or pd.isna(non_rule_max) or pd.isna(rule_max)
                        else float(non_rule_max - rule_max)
                    ),
                    "best_non_rule_pair_norm": (None if len(best_non_rule) == 0 else best_non_rule.iloc[0].get("pair_norm")),
                    "best_non_rule_model_score": (None if len(best_non_rule) == 0 else best_non_rule.iloc[0].get("pair_model_score")),
                    "best_non_rule_pair_value_score": (None if len(best_non_rule) == 0 else best_non_rule.iloc[0].get("pair_value_score")),
                    "best_non_rule_market_proxy": (None if len(best_non_rule) == 0 else best_non_rule.iloc[0].get("pair_market_implied_prob")),
                    "best_non_rule_actual_wide_hit": (None if len(best_non_rule) == 0 else best_non_rule.iloc[0].get("actual_wide_hit")),
                    "best_non_rule_wide_payout": (None if len(best_non_rule) == 0 else best_non_rule.iloc[0].get("wide_payout")),
                }
            )

            # forced non-rule top N extraction
            non_rule_top = non_rule.head(max(1, args.non_rule_top_n)).copy()
            non_rule_top["non_rule_model_rank"] = range(1, len(non_rule_top) + 1)
            for _, r in non_rule_top.iterrows():
                rows_non_rule.append(
                    {
                        "race_date": day,
                        "race_id": rid,
                        "pair_norm": r.get("pair_norm"),
                        "pair_model_score": r.get("pair_model_score"),
                        "pair_value_score": r.get("pair_value_score"),
                        "pair_market_implied_prob": r.get("pair_market_implied_prob"),
                        "pair_edge": r.get("pair_edge"),
                        "pair_edge_ratio": r.get("pair_edge_ratio"),
                        "pair_edge_rank_gap": r.get("pair_edge_rank_gap"),
                        "non_rule_model_rank": r.get("non_rule_model_rank"),
                        "actual_wide_hit": r.get("actual_wide_hit"),
                        "wide_payout": r.get("wide_payout"),
                    }
                )

    race_df = pd.DataFrame(rows_race)
    non_rule_df = pd.DataFrame(rows_non_rule)
    non_rule_cols = [
        "race_date",
        "race_id",
        "pair_norm",
        "pair_model_score",
        "pair_value_score",
        "pair_market_implied_prob",
        "pair_edge",
        "pair_edge_ratio",
        "pair_edge_rank_gap",
        "non_rule_model_rank",
        "actual_wide_hit",
        "wide_payout",
    ]
    for c in non_rule_cols:
        if c not in non_rule_df.columns:
            non_rule_df[c] = pd.NA
    non_rule_df = non_rule_df[non_rule_cols]
    global_df = pd.DataFrame(global_stats)

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    race_df.to_csv(args.out_csv, index=False, encoding="utf-8")
    args.out_non_rule_csv.parent.mkdir(parents=True, exist_ok=True)
    non_rule_df.to_csv(args.out_non_rule_csv, index=False, encoding="utf-8")

    nr_hit_count, nr_hit_rate, nr_roi = _roi_proxy(non_rule_df)
    agg = {
        "non_rule_candidate_count": int(len(non_rule_df)),
        "non_rule_hit_count": nr_hit_count,
        "non_rule_hit_rate": nr_hit_rate,
        "non_rule_roi_proxy": nr_roi,
        "avg_pair_model_score": float(pd.to_numeric(non_rule_df["pair_model_score"], errors="coerce").mean()) if len(non_rule_df) > 0 else None,
        "avg_pair_value_score": float(pd.to_numeric(non_rule_df["pair_value_score"], errors="coerce").mean()) if len(non_rule_df) > 0 else None,
        "avg_pair_market_implied_prob": float(pd.to_numeric(non_rule_df["pair_market_implied_prob"], errors="coerce").mean()) if len(non_rule_df) > 0 else None,
    }

    try:
        g_tbl = global_df.to_markdown(index=False)
        r_tbl = race_df.head(40).to_markdown(index=False)
        n_tbl = non_rule_df.head(60).to_markdown(index=False)
    except Exception:
        g_tbl = global_df.to_string(index=False)
        r_tbl = race_df.head(40).to_string(index=False)
        n_tbl = non_rule_df.head(60).to_string(index=False)

    md_lines = [
        "# candidate_pool_rule_dominance",
        "",
        "## Global",
        "",
        g_tbl,
        "",
        f"- non_rule_candidate_count: {agg['non_rule_candidate_count']}",
        f"- non_rule_hit_count: {agg['non_rule_hit_count']}",
        f"- non_rule_hit_rate: {agg['non_rule_hit_rate']}",
        f"- non_rule_roi_proxy: {agg['non_rule_roi_proxy']}",
        f"- avg_pair_model_score: {agg['avg_pair_model_score']}",
        f"- avg_pair_value_score: {agg['avg_pair_value_score']}",
        f"- avg_pair_market_implied_prob: {agg['avg_pair_market_implied_prob']}",
        "",
        "## Race-level (head)",
        "",
        r_tbl,
    ]
    args.out_md.write_text("\n".join(md_lines), encoding="utf-8")

    non_md_lines = [
        "# non_rule_model_candidates",
        "",
        f"- top_n_per_race: {args.non_rule_top_n}",
        f"- rows: {len(non_rule_df)}",
        "",
        n_tbl,
    ]
    args.out_non_rule_md.write_text("\n".join(non_md_lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))
    print(str(args.out_non_rule_csv))
    print(str(args.out_non_rule_md))


if __name__ == "__main__":
    main()
