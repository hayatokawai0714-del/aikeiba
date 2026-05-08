from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pandas as pd

def _norm_pair(a: object, b: object) -> str:
    try:
        x = int(a)
        y = int(b)
    except Exception:
        return ""
    i, j = sorted((x, y))
    return f"{i:02d}-{j:02d}"


def _ensure_columns(df, columns: list[str]):
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = pd.NA
    return out


def _bool_series(df: pd.DataFrame, col: str, default: bool = False) -> pd.Series:
    if col in df.columns:
        s = df[col]
        if str(s.dtype) == "bool":
            return s.fillna(default)
        n = pd.to_numeric(s, errors="coerce")
        return n.fillna(1 if default else 0).astype(int).astype(bool)
    return pd.Series([default] * len(df), index=df.index, dtype=bool)


def _load_hits_and_payouts(db_path: Path, race_date: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    import pandas as pd
    try:
        import duckdb  # lazy import so --help works without dependency
    except Exception as exc:
        raise RuntimeError(
            "duckdb is required for DB join mode. "
            "Use --csv-only to skip labels/payout DB join."
        ) from exc

    con = duckdb.connect(str(db_path))
    try:
        res = con.execute(
            """
            SELECT r.race_id, rs.horse_no, rs.finish_position
            FROM results rs
            JOIN races r ON r.race_id = rs.race_id
            WHERE r.race_date = cast(? as DATE)
            """,
            [race_date],
        ).fetchdf()
        pay = con.execute(
            """
            SELECT p.race_id, lower(p.bet_type) AS bet_type, p.bet_key, p.payout
            FROM payouts p
            JOIN races r ON r.race_id = p.race_id
            WHERE r.race_date = cast(? as DATE)
              AND lower(p.bet_type) = 'wide'
            """,
            [race_date],
        ).fetchdf()
    finally:
        con.close()

    if len(res) == 0:
        hit_df = pd.DataFrame(columns=["race_id", "pair_norm", "actual_wide_hit"])
    else:
        top3 = res[pd.to_numeric(res["finish_position"], errors="coerce") <= 3].copy()
        rows: list[dict] = []
        for rid, g in top3.groupby("race_id"):
            nos = sorted([int(x) for x in g["horse_no"].dropna().tolist()])
            for i in range(len(nos)):
                for j in range(i + 1, len(nos)):
                    rows.append({"race_id": rid, "pair_norm": _norm_pair(nos[i], nos[j]), "actual_wide_hit": 1})
        hit_df = pd.DataFrame(rows).drop_duplicates(subset=["race_id", "pair_norm"]) if len(rows) > 0 else pd.DataFrame(columns=["race_id", "pair_norm", "actual_wide_hit"])

    if len(pay) > 0:
        pay = pay.copy()
        pay["pair_norm"] = (
            pay["bet_key"]
            .astype(str)
            .str.replace(" ", "", regex=False)
            .str.replace("－", "-", regex=False)
        )
        pay["wide_payout"] = pd.to_numeric(pay["payout"], errors="coerce")
        pay_df = pay[["race_id", "pair_norm", "wide_payout"]].dropna(subset=["pair_norm"]).copy()
    else:
        pay_df = pd.DataFrame(columns=["race_id", "pair_norm", "wide_payout"])
    return hit_df, pay_df


def main() -> None:
    ap = argparse.ArgumentParser(description="Build pair shadow comparison reports from candidate_pairs output.")
    ap.add_argument("--candidate-pairs", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, default=None)
    ap.add_argument("--race-date", required=True)
    ap.add_argument("--csv-only", action="store_true", help="Skip DB join and output comparison with NA labels/payouts.")
    ap.add_argument("--out-dir", type=Path, default=Path("racing_ai/reports"))
    ap.add_argument("--out-race-csv", type=Path, default=None)
    ap.add_argument("--out-pair-csv", type=Path, default=None)
    ap.add_argument("--out-md", type=Path, default=None)
    ap.add_argument("--include-all-candidates", action="store_true", help="Also emit all-candidate comparison CSVs.")
    ap.add_argument("--out-all-race-csv", type=Path, default=None)
    ap.add_argument("--out-all-pair-csv", type=Path, default=None)
    ap.add_argument("--out-expanded-race-csv", type=Path, default=None)
    ap.add_argument("--out-expanded-pair-csv", type=Path, default=None)
    args = ap.parse_args()

    if not args.candidate_pairs.exists():
        raise SystemExit(f"missing --candidate-pairs file: {args.candidate_pairs}")

    if args.candidate_pairs.suffix.lower() == ".csv":
        df = pd.read_csv(args.candidate_pairs)
    else:
        df = pd.read_parquet(args.candidate_pairs)
    if "pair_norm" not in df.columns:
        df["pair_norm"] = df.apply(lambda r: _norm_pair(r.get("horse1_umaban"), r.get("horse2_umaban")), axis=1)
    if "model_top5_flag" not in df.columns:
        df["model_top5_flag"] = df.get("pair_model_rank_in_race", pd.Series([None] * len(df))).apply(
            lambda x: bool(pd.notna(x) and float(x) <= 5)
        )
    df["model_dynamic_selected_flag"] = _bool_series(df, "model_dynamic_selected_flag", default=False)
    df["pair_selected_flag"] = _bool_series(df, "pair_selected_flag", default=False)

    if "pair_market_implied_prob" not in df.columns:
        if "pair_fused_prob_sum" in df.columns:
            df["pair_market_implied_prob"] = pd.to_numeric(df["pair_fused_prob_sum"], errors="coerce")
        else:
            df["pair_market_implied_prob"] = pd.NA
    if "pair_edge" not in df.columns:
        df["pair_edge"] = (
            pd.to_numeric(df.get("pair_model_score"), errors="coerce")
            - pd.to_numeric(df.get("pair_market_implied_prob"), errors="coerce")
        )

    join_mode = "csv_only"
    if args.csv_only:
        hit_df = pd.DataFrame(columns=["race_id", "pair_norm", "actual_wide_hit"])
        pay_df = pd.DataFrame(columns=["race_id", "pair_norm", "wide_payout"])
    else:
        if args.db_path is None:
            raise SystemExit("DB mode requires --db-path. Use --csv-only to skip DB join.")
        try:
            hit_df, pay_df = _load_hits_and_payouts(args.db_path, args.race_date)
            join_mode = "db_join"
        except Exception as exc:
            print(f"WARN: DB join unavailable, fallback to NA labels/payouts: {exc}")
            hit_df = pd.DataFrame(columns=["race_id", "pair_norm", "actual_wide_hit"])
            pay_df = pd.DataFrame(columns=["race_id", "pair_norm", "wide_payout"])
            join_mode = "db_join_failed_fallback_na"

    pair_df = df.merge(hit_df, on=["race_id", "pair_norm"], how="left").merge(pay_df, on=["race_id", "pair_norm"], how="left")
    pair_df = _ensure_columns(
        pair_df,
        [
            "race_id",
            "pair_norm",
            "pair_value_score",
            "pair_model_score",
            "pair_market_implied_prob",
            "pair_edge",
            "model_dynamic_final_score",
            "pair_selected_flag",
            "model_top5_flag",
            "model_dynamic_selected_flag",
            "model_dynamic_rank",
            "model_dynamic_skip_reason",
            "actual_wide_hit",
            "wide_payout",
        ],
    )
    pair_df["actual_wide_hit"] = pd.to_numeric(pair_df["actual_wide_hit"], errors="coerce").fillna(0).astype(int)
    pair_df["pair_edge_ratio"] = pd.to_numeric(pair_df["pair_model_score"], errors="coerce") / (pd.to_numeric(pair_df["pair_market_implied_prob"], errors="coerce") + 1e-9)
    pair_df["pair_edge_log_ratio"] = (
        (pd.to_numeric(pair_df["pair_model_score"], errors="coerce") + 1e-9)
        / (pd.to_numeric(pair_df["pair_market_implied_prob"], errors="coerce") + 1e-9)
    ).map(lambda x: pd.NA if pd.isna(x) or x <= 0 else float(__import__("math").log(x)))
    pair_df["model_rank_in_race"] = pair_df.groupby("race_id")["pair_model_score"].rank(method="min", ascending=False)
    pair_df["market_rank_in_race"] = pair_df.groupby("race_id")["pair_market_implied_prob"].rank(method="min", ascending=False)
    pair_df["model_rank_pct_in_race"] = pair_df.groupby("race_id")["pair_model_score"].rank(method="average", pct=True, ascending=False)
    pair_df["market_rank_pct_in_race"] = pair_df.groupby("race_id")["pair_market_implied_prob"].rank(method="average", pct=True, ascending=False)
    pair_df["pair_edge_rank_gap"] = pair_df["market_rank_in_race"] - pair_df["model_rank_in_race"]
    pair_df["pair_edge_pct_gap"] = pair_df["market_rank_pct_in_race"] - pair_df["model_rank_pct_in_race"]

    race_rows: list[dict] = []
    for rid, g in pair_df.groupby("race_id", sort=False):
        rule_set = set(g.loc[g["pair_selected_flag"], "pair_norm"].astype(str).tolist())
        model5_set = set(g.loc[g["model_top5_flag"], "pair_norm"].astype(str).tolist())
        model_dyn_set = set(g.loc[g["model_dynamic_selected_flag"], "pair_norm"].astype(str).tolist())
        top_rule = pd.to_numeric(g["pair_value_score"], errors="coerce").max()
        top_model = pd.to_numeric(g["pair_model_score"], errors="coerce").max()
        top_dynamic = pd.to_numeric(g["model_dynamic_final_score"], errors="coerce").max()
        top_edge = pd.to_numeric(g.get("pair_edge"), errors="coerce").max()
        selected_edges = pd.to_numeric(g.loc[g["model_dynamic_selected_flag"], "pair_edge"], errors="coerce").dropna()
        skip_reason = g.get("model_dynamic_skip_reason", pd.Series([None])).dropna().astype(str).head(1)
        race_rows.append(
            {
                "race_id": rid,
                "rule_selected_count": int(len(rule_set)),
                "model_top5_selected_count": int(len(model5_set)),
                "model_dynamic_selected_count": int(len(model_dyn_set)),
                "model_dynamic_skip_reason": (skip_reason.iloc[0] if len(skip_reason) > 0 else None),
                "top_rule_score": (None if pd.isna(top_rule) else float(top_rule)),
                "top_model_score": (None if pd.isna(top_model) else float(top_model)),
                "top_dynamic_score": (None if pd.isna(top_dynamic) else float(top_dynamic)),
                "top_edge": (None if pd.isna(top_edge) else float(top_edge)),
                "overlap_rule_model_top5": int(len(rule_set.intersection(model5_set))),
                "overlap_rule_model_dynamic": int(len(rule_set.intersection(model_dyn_set))),
                "avg_pair_edge_selected": (None if len(selected_edges) == 0 else float(selected_edges.mean())),
                "max_pair_edge_selected": (None if len(selected_edges) == 0 else float(selected_edges.max())),
            }
        )

    race_df = pd.DataFrame(race_rows)
    race_df = _ensure_columns(
        race_df,
        [
            "race_id",
            "rule_selected_count",
            "model_top5_selected_count",
            "model_dynamic_selected_count",
            "model_dynamic_skip_reason",
            "top_rule_score",
            "top_model_score",
            "top_dynamic_score",
            "top_edge",
            "overlap_rule_model_top5",
            "overlap_rule_model_dynamic",
            "avg_pair_edge_selected",
            "max_pair_edge_selected",
        ],
    )

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    race_csv = args.out_race_csv or (out_dir / "pair_shadow_race_comparison.csv")
    pair_csv = args.out_pair_csv or (out_dir / "pair_shadow_pair_comparison.csv")
    all_race_csv = args.out_all_race_csv or (out_dir / "pair_shadow_race_comparison_all_candidates.csv")
    all_pair_csv = args.out_all_pair_csv or (out_dir / "pair_shadow_pair_comparison_all_candidates.csv")
    expanded_race_csv = args.out_expanded_race_csv or (out_dir / "pair_shadow_race_comparison_expanded.csv")
    expanded_pair_csv = args.out_expanded_pair_csv or (out_dir / "pair_shadow_pair_comparison_expanded.csv")
    md_path = args.out_md or (out_dir / "pair_shadow_comparison_report.md")
    race_df.to_csv(race_csv, index=False, encoding="utf-8")
    pair_df[
        [
            "race_id",
            "pair_norm",
            "pair_value_score",
            "pair_model_score",
            "pair_market_implied_prob",
            "pair_edge",
            "model_dynamic_final_score",
            "pair_selected_flag",
            "model_top5_flag",
            "model_dynamic_selected_flag",
            "model_dynamic_rank",
            "model_dynamic_skip_reason",
            "actual_wide_hit",
            "wide_payout",
        ]
    ].to_csv(pair_csv, index=False, encoding="utf-8")

    if args.include_all_candidates:
        all_pair_cols = [
            "race_id",
            "pair_norm",
            "horse1_umaban",
            "horse2_umaban",
            "pair_selected_flag",
            "model_top5_flag",
            "model_dynamic_selected_flag",
            "pair_value_score",
            "pair_model_score",
            "pair_market_implied_prob",
            "pair_edge",
            "pair_edge_ratio",
            "pair_edge_log_ratio",
            "pair_edge_rank_gap",
            "pair_edge_pct_gap",
            "model_dynamic_final_score",
            "model_dynamic_rank",
            "model_dynamic_skip_reason",
            "actual_wide_hit",
            "wide_payout",
            "p_top3_fused_hmean",
            "p_top3_fused_min",
            "p_top3_fused_max",
            "p_top3_fused_abs_diff",
            "ai_market_gap_min",
            "ai_market_gap_max",
            "both_positive_gap_flag",
            "one_side_positive_gap_flag",
            "pair_model_score_rank_in_race",
            "pair_model_score_gap_to_next",
            "pair_value_score_rank_in_race",
            "pair_value_score_gap_to_next",
        ]
        pair_df_all = _ensure_columns(pair_df, all_pair_cols)
        pair_df_all[all_pair_cols].to_csv(all_pair_csv, index=False, encoding="utf-8")
        pair_df_all[all_pair_cols].to_csv(expanded_pair_csv, index=False, encoding="utf-8")

        all_race_rows: list[dict] = []
        for rid, g in pair_df_all.groupby("race_id", sort=False):
            rule_mask = _bool_series(g, "pair_selected_flag", default=False)
            top5_mask = _bool_series(g, "model_top5_flag", default=False)
            dyn_mask = _bool_series(g, "model_dynamic_selected_flag", default=False)
            non_rule_mask = ~rule_mask
            top_non_rule = g.loc[non_rule_mask].sort_values("pair_model_score", ascending=False).head(1)
            all_race_rows.append(
                {
                    "race_id": rid,
                    "candidate_pair_count": int(len(g)),
                    "rule_selected_count": int(rule_mask.sum()),
                    "non_rule_candidate_count": int(non_rule_mask.sum()),
                    "model_top5_selected_count": int(top5_mask.sum()),
                    "model_dynamic_selected_count": int(dyn_mask.sum()),
                    "model_top5_non_rule_count": int((top5_mask & non_rule_mask).sum()),
                    "model_dynamic_non_rule_count": int((dyn_mask & non_rule_mask).sum()),
                    "top_rule_score": float(pd.to_numeric(g.loc[rule_mask, "pair_value_score"], errors="coerce").max()) if rule_mask.any() else None,
                    "top_model_score": float(pd.to_numeric(g["pair_model_score"], errors="coerce").max()) if len(g) > 0 else None,
                    "top_non_rule_model_score": float(pd.to_numeric(g.loc[non_rule_mask, "pair_model_score"], errors="coerce").max()) if non_rule_mask.any() else None,
                    "best_non_rule_pair_norm": (top_non_rule["pair_norm"].iloc[0] if len(top_non_rule) > 0 else None),
                    "best_non_rule_pair_value_score": (top_non_rule["pair_value_score"].iloc[0] if len(top_non_rule) > 0 else None),
                    "best_non_rule_market_proxy": (top_non_rule["pair_market_implied_prob"].iloc[0] if len(top_non_rule) > 0 else None),
                    "best_non_rule_actual_wide_hit": (top_non_rule["actual_wide_hit"].iloc[0] if len(top_non_rule) > 0 else None),
                    "best_non_rule_wide_payout": (top_non_rule["wide_payout"].iloc[0] if len(top_non_rule) > 0 else None),
                    "overlap_rule_model_top5": int(len(set(g.loc[rule_mask, "pair_norm"].astype(str)).intersection(set(g.loc[top5_mask, "pair_norm"].astype(str))))),
                    "overlap_rule_model_dynamic": int(len(set(g.loc[rule_mask, "pair_norm"].astype(str)).intersection(set(g.loc[dyn_mask, "pair_norm"].astype(str))))),
                    "model_top5_non_rule_hit_count": int(pd.to_numeric(g.loc[top5_mask & non_rule_mask, "actual_wide_hit"], errors="coerce").fillna(0).sum()),
                    "model_dynamic_non_rule_hit_count": int(pd.to_numeric(g.loc[dyn_mask & non_rule_mask, "actual_wide_hit"], errors="coerce").fillna(0).sum()),
                }
            )
        pd.DataFrame(all_race_rows).to_csv(all_race_csv, index=False, encoding="utf-8")
        pd.DataFrame(all_race_rows).to_csv(expanded_race_csv, index=False, encoding="utf-8")

    lines = [
        "# pair_shadow_comparison_report",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- race_date: {args.race_date}",
        f"- candidate_pairs: {args.candidate_pairs}",
        f"- join_mode: {join_mode}",
        f"- race_report_csv: {race_csv}",
        f"- pair_report_csv: {pair_csv}",
        f"- include_all_candidates: {args.include_all_candidates}",
        f"- all_race_report_csv: {all_race_csv if args.include_all_candidates else None}",
        f"- all_pair_report_csv: {all_pair_csv if args.include_all_candidates else None}",
        f"- expanded_race_report_csv: {expanded_race_csv if args.include_all_candidates else None}",
        f"- expanded_pair_report_csv: {expanded_pair_csv if args.include_all_candidates else None}",
        "",
        f"- race_count: {int(race_df['race_id'].nunique()) if len(race_df)>0 else 0}",
        f"- pair_rows: {int(len(pair_df))}",
    ]
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(str(md_path))


if __name__ == "__main__":
    main()
