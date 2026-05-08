from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
from pathlib import Path
import sys
import time

import pandas as pd


def _run(cmd: list[str], timeout_sec: int = 1800) -> tuple[int, str]:
    # DuckDB can briefly lock on Windows when a previous writer connection closes.
    # Use small retry with backoff for known IOException patterns.
    last_out = ""
    for attempt in range(1, 6):
        p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout_sec)
        out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
        last_out = out.strip()
        if p.returncode == 0:
            return 0, last_out
        if "duckdb.duckdb.IOException" in last_out or "duckdb.IOException" in last_out:
            time.sleep(0.5 * attempt)
            continue
        break
    return int(p.returncode), last_out


def _to_bool_series(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    x = s.astype(str).str.lower()
    return x.isin(["true", "1", "t", "yes", "y"])


def _postcompute_model_dynamic_if_needed(
    joined_pairs_csv: Path,
    out_audit_csv: Path,
    out_audit_md: Path,
    *,
    min_score: float = 0.08,
    min_edge: float = 0.0,
    min_gap: float = 0.01,
    default_k: int = 5,
    min_k: int = 1,
    max_k: int = 5,
) -> None:
    """
    Evaluation-only fallback for years/dates where joined pairs CSV lacks model_dynamic columns.

    This MUST NOT change any production logic. It only fills missing/empty model_dynamic columns
    for downstream evaluation scripts that expect them.
    """
    if not joined_pairs_csv.exists():
        return

    df = pd.read_csv(joined_pairs_csv)
    total_rows = int(len(df))
    race_count = int(df["race_id"].nunique()) if "race_id" in df.columns else 0

    # Decide whether we need to compute.
    need_cols = ["model_dynamic_selected_flag", "model_dynamic_rank", "model_dynamic_final_score", "model_dynamic_skip_reason", "model_dynamic_k"]
    missing_cols = [c for c in need_cols if c not in df.columns]

    selected_before = 0
    if "model_dynamic_selected_flag" in df.columns:
        selected_before = int(_to_bool_series(df["model_dynamic_selected_flag"]).sum())

    if (not missing_cols) and selected_before > 0:
        # Already computed and non-empty; do not overwrite.
        out_audit_csv.parent.mkdir(parents=True, exist_ok=True)
        out_audit_md.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "status": "skipped_existing",
                    "joined_pairs_csv": str(joined_pairs_csv),
                    "total_pair_rows": total_rows,
                    "race_count": race_count,
                    "before_model_dynamic_selected_count": selected_before,
                    "after_model_dynamic_selected_count": selected_before,
                }
            ]
        ).to_csv(out_audit_csv, index=False, encoding="utf-8")
        out_audit_md.write_text(
            "\n".join(
                [
                    "# model_dynamic postcompute audit",
                    "",
                    "- status: skipped_existing",
                    f"- input: {joined_pairs_csv}",
                    f"- total_pair_rows: {total_rows}",
                    f"- race_count: {race_count}",
                    f"- selected_before: {selected_before}",
                ]
            ),
            encoding="utf-8",
        )
        return

    # Heuristic guard: if model score scale is clearly below the default threshold (common in older years),
    # auto-relax min_score/min_gap for evaluation-only postcompute to avoid "all races skip".
    # This DOES NOT affect production; it's only for evaluation comparability.
    score_max = float(pd.to_numeric(df["pair_model_score"], errors="coerce").max()) if "pair_model_score" in df.columns else float("nan")
    if score_max == score_max and score_max < min_score:
        min_score = max(0.0, float(score_max) * 0.9)
        # When score scale is this low, the "gap-to-next" gate is almost always 0.
        # Relax min_gap to 0.0 for evaluation-only postcompute so we can still form a shadow set.
        min_gap = 0.0
        # And relax edge slightly to allow some picks even if proxy is shifted.
        if "pair_edge" in df.columns:
            e = pd.to_numeric(df["pair_edge"], errors="coerce")
            if e.notna().any():
                # Use median as a gentle floor when default is 0.0 and most edges are negative.
                min_edge = min(float(min_edge), float(e.quantile(0.5)))
    gap_max = float(pd.to_numeric(df.get("pair_model_score_gap_to_next"), errors="coerce").max()) if "pair_model_score_gap_to_next" in df.columns else float("nan")
    if gap_max == gap_max and gap_max < min_gap:
        min_gap = max(0.0, float(gap_max) * 0.9)

    # Prepare columns (do not assume any exist).
    for c in need_cols:
        if c not in df.columns:
            df[c] = pd.NA

    # Required inputs
    for col in ["pair_model_score", "pair_edge", "pair_selected_flag"]:
        if col not in df.columns:
            df[col] = pd.NA
    if "pair_model_score_gap_to_next" not in df.columns:
        df["pair_model_score_gap_to_next"] = pd.NA
    if "pair_model_score_rank_in_race" not in df.columns:
        df["pair_model_score_rank_in_race"] = pd.NA

    df["pair_model_score"] = pd.to_numeric(df["pair_model_score"], errors="coerce")
    df["pair_edge"] = pd.to_numeric(df["pair_edge"], errors="coerce")
    df["pair_model_score_gap_to_next"] = pd.to_numeric(df["pair_model_score_gap_to_next"], errors="coerce")
    df["_rule"] = _to_bool_series(df["pair_selected_flag"])

    # Compute rank_in_race and gap_to_next if missing/empty.
    if df["pair_model_score_rank_in_race"].isna().all():
        df["pair_model_score_rank_in_race"] = df.groupby("race_id")["pair_model_score"].rank(ascending=False, method="first")
    if df["pair_model_score_gap_to_next"].isna().all():
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

    # Decide per-race selection.
    df["model_dynamic_final_score"] = df["pair_model_score"]  # evaluation-only: keep simple and consistent
    df["model_dynamic_selected_flag"] = False
    df["model_dynamic_rank"] = pd.NA
    df["model_dynamic_k"] = pd.NA
    df["model_dynamic_skip_reason"] = pd.NA

    skip_reason_counts: dict[str, int] = {}
    selected_after = 0
    selected_race_count = 0

    k_use = int(max(min_k, min(max_k, default_k)))

    for rid, g in df.groupby("race_id", sort=False):
        # Gates
        pass_score = g["pair_model_score"] >= float(min_score)
        pass_edge = g["pair_edge"].notna() & (g["pair_edge"] >= float(min_edge))
        pass_gap = g["pair_model_score_gap_to_next"].notna() & (g["pair_model_score_gap_to_next"] >= float(min_gap))
        pass_all = pass_score & pass_edge & pass_gap

        if not bool(pass_score.any()):
            reason = "DYNAMIC_SKIP_MODEL_SCORE_WEAK"
        elif not bool((pass_score & pass_edge).any()):
            reason = "DYNAMIC_SKIP_EDGE_WEAK"
        elif not bool(pass_all.any()):
            reason = "DYNAMIC_SKIP_GAP_SMALL"
        else:
            reason = "DYNAMIC_BUY_OK"

        skip_reason_counts[reason] = int(skip_reason_counts.get(reason, 0) + 1)
        df.loc[g.index, "model_dynamic_skip_reason"] = reason

        if reason != "DYNAMIC_BUY_OK":
            continue

        gg = g[pass_all].sort_values("model_dynamic_final_score", ascending=False)
        pick = gg.head(k_use).index
        if len(pick) == 0:
            continue
        selected_race_count += 1
        selected_after += int(len(pick))
        df.loc[pick, "model_dynamic_selected_flag"] = True
        df.loc[pick, "model_dynamic_k"] = int(k_use)
        # rank among picked by final_score
        ranks = df.loc[pick, "model_dynamic_final_score"].rank(ascending=False, method="first").astype(int)
        df.loc[pick, "model_dynamic_rank"] = ranks

    # Non-overlap count for audit
    non_overlap_after = int((df["model_dynamic_selected_flag"].astype(bool) & (~df["_rule"])).sum())

    # Coverage for audit
    pair_model_score_non_null = int(df["pair_model_score"].notna().sum())
    pair_edge_non_null = int(df["pair_edge"].notna().sum())
    gap_non_null = int(df["pair_model_score_gap_to_next"].notna().sum())
    zero_selected_race_count = int(race_count - selected_race_count)

    out_audit_csv.parent.mkdir(parents=True, exist_ok=True)
    out_audit_md.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "joined_pairs_csv": str(joined_pairs_csv),
                "total_pair_rows": total_rows,
                "race_count": race_count,
                "before_model_dynamic_selected_count": selected_before,
                "after_model_dynamic_selected_count": selected_after,
                "after_model_dynamic_non_overlap_count": non_overlap_after,
                "selected_race_count": selected_race_count,
                "zero_selected_race_count": zero_selected_race_count,
                "skip_reason_counts_json": json.dumps(skip_reason_counts, ensure_ascii=False),
                "pair_model_score_non_null_count": pair_model_score_non_null,
                "pair_edge_non_null_count": pair_edge_non_null,
                "gap_non_null_count": gap_non_null,
                "threshold_min_score": min_score,
                "threshold_min_edge": min_edge,
                "threshold_min_gap": min_gap,
                "threshold_k": k_use,
            }
        ]
    ).to_csv(out_audit_csv, index=False, encoding="utf-8")
    out_audit_md.write_text(
        "\n".join(
            [
                "# model_dynamic postcompute audit",
                "",
                f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
                f"- input: {joined_pairs_csv}",
                "",
                "## Summary",
                "",
                f"- total_pair_rows: {total_rows}",
                f"- race_count: {race_count}",
                f"- selected_before: {selected_before}",
                f"- selected_after: {selected_after}",
                f"- non_overlap_after: {non_overlap_after}",
                f"- selected_race_count: {selected_race_count}",
                f"- zero_selected_race_count: {zero_selected_race_count}",
                "",
                "## Thresholds (evaluation-only fallback)",
                "",
                f"- min_score: {min_score}",
                f"- min_edge: {min_edge}",
                f"- min_gap: {min_gap}",
                f"- k: {k_use}",
                "",
                "## Skip Reason Counts (race-level)",
                "",
                "```json",
                json.dumps(skip_reason_counts, ensure_ascii=False, indent=2),
                "```",
                "",
            ]
        ),
        encoding="utf-8",
    )

    # Overwrite joined CSV with filled columns for downstream evaluation scripts.
    df.drop(columns=["_rule"], errors="ignore").to_csv(joined_pairs_csv, index=False, encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Year-level shadow evaluation prep (predictions -> rule -> expanded -> join -> eval).")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--models-root", type=Path, required=True, help="models root for infer-top3 (expects .../top3/<model_version>)")
    ap.add_argument("--model-version", default="top3_stability_plus_pace_v3")
    ap.add_argument("--feature-snapshot-version", default="fs_v1")
    ap.add_argument("--odds-snapshot-version", default="odds_v1")
    ap.add_argument("--pair-model-root", type=Path, required=True)
    ap.add_argument("--pair-model-version", default="pair_reranker_ts_v4")
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--external-results-csv", type=Path, required=True)
    ap.add_argument("--external-wide-payouts-csv", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True, help="reports out dir")
    ap.add_argument("--race-day-root", type=Path, default=Path("racing_ai/data/race_day"))
    ap.add_argument("--max-dates", type=int, default=0, help="0=all; otherwise limit for a smoke run")
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    # Discover available dates from DB races (central only assumed in dataset).
    import duckdb

    con = duckdb.connect(str(args.db_path), read_only=True)
    dates = con.execute(
        "select distinct cast(race_date as varchar) as d from races where race_date between cast(? as date) and cast(? as date) order by d",
        [args.start_date, args.end_date],
    ).fetchdf()["d"].astype(str).tolist()
    con.close()

    if args.max_dates and args.max_dates > 0:
        dates = dates[: int(args.max_dates)]

    audit_rows = []
    expanded_pair_csvs: list[Path] = []

    for d in dates:
        # 1) build-features
        cmd = [
            sys.executable,
            "-m",
            "racing_ai.cli",
            "build-features",
            "--db-path",
            str(args.db_path),
            "--race-date",
            d,
            "--feature-snapshot-version",
            str(args.feature_snapshot_version),
        ]
        rc1, out1 = _run(cmd)

        # 2) infer-top3
        cmd2 = [
            sys.executable,
            "-m",
            "racing_ai.cli",
            "infer-top3",
            "--db-path",
            str(args.db_path),
            "--models-root",
            str(args.models_root),
            "--race-date",
            d,
            "--feature-snapshot-version",
            str(args.feature_snapshot_version),
            "--model-version",
            str(args.model_version),
            "--odds-snapshot-version",
            str(args.odds_snapshot_version),
        ]
        rc2, out2 = _run(cmd2)

        # 3) rebuild rule candidates (evaluation-only)
        base_dir = args.race_day_root / d / args.model_version
        base_dir.mkdir(parents=True, exist_ok=True)
        rule_parquet = base_dir / "candidate_pairs.parquet"
        rule_audit = args.out_dir / d / f"rebuild_rule_candidate_pairs_audit_{d.replace('-','')}.md"
        rule_audit.parent.mkdir(parents=True, exist_ok=True)
        cmd3 = [
            sys.executable,
            "racing_ai/scripts/rebuild_rule_candidate_pairs_from_db.py",
            "--db-path",
            str(args.db_path),
            "--race-date",
            d,
            "--model-version",
            str(args.model_version),
            "--out-parquet",
            str(rule_parquet),
            "--out-audit-md",
            str(rule_audit),
        ]
        rc3, out3 = _run(cmd3)

        # 4) build expanded + pair csv
        run_summary = base_dir / "run_summary.json"
        if not run_summary.exists():
            run_summary.write_text(
                json.dumps({"status": "evaluation_only", "race_date": d, "model_version": args.model_version}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        out_pair_csv = args.out_dir / d / f"pair_shadow_pair_comparison_expanded_{d.replace('-','')}.csv"
        out_race_csv = args.out_dir / d / f"pair_shadow_race_comparison_expanded_{d.replace('-','')}.csv"
        out_audit_md = args.out_dir / d / f"expanded_candidate_pool_audit_{d.replace('-','')}.md"
        cmd4 = [
            sys.executable,
            "racing_ai/scripts/build_expanded_candidates_from_artifacts.py",
            "--race-date",
            d,
            "--db-path",
            str(args.db_path),
            "--base-dir",
            str(base_dir),
            "--candidate-pairs",
            str(rule_parquet),
            "--run-summary",
            str(run_summary),
            "--model-version",
            str(args.model_version),
            "--pair-model-root",
            str(args.pair_model_root),
            "--pair-model-version",
            str(args.pair_model_version),
            "--out-expanded",
            str(base_dir / "candidate_pairs_expanded.parquet"),
            "--out-pair-csv",
            str(out_pair_csv),
            "--out-race-csv",
            str(out_race_csv),
            "--out-audit-md",
            str(out_audit_md),
        ]
        rc4, out4 = _run(cmd4)

        ok = (rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0 and out_pair_csv.exists())
        pair_rows = 0
        race_cnt = 0
        if out_pair_csv.exists():
            try:
                tmp = pd.read_csv(out_pair_csv)
                pair_rows = int(len(tmp))
                race_cnt = int(tmp["race_id"].nunique()) if "race_id" in tmp.columns else 0
                if "race_date" not in tmp.columns:
                    # ensure it is set (for downstream join)
                    tmp["race_date"] = d
                    tmp.to_csv(out_pair_csv, index=False, encoding="utf-8")
            except Exception:
                pass

        if ok:
            expanded_pair_csvs.append(out_pair_csv)

        audit_rows.append(
            {
                "race_date": d,
                "status": "ok" if ok else "error",
                "pair_rows": pair_rows,
                "race_count": race_cnt,
                "rc_build_features": rc1,
                "rc_infer_top3": rc2,
                "rc_rebuild_rule": rc3,
                "rc_build_expanded": rc4,
                "err_build_features": (out1[-300:] if rc1 != 0 else ""),
                "err_infer_top3": (out2[-300:] if rc2 != 0 else ""),
                "err_rebuild_rule": (out3[-300:] if rc3 != 0 else ""),
                "err_build_expanded": (out4[-300:] if rc4 != 0 else ""),
            }
        )

    audit_df = pd.DataFrame(audit_rows)
    label = f"{args.start_date.replace('-', '')}_{args.end_date.replace('-', '')}"
    audit_csv = args.out_dir / f"expanded_candidate_input_audit_{label}.csv"
    audit_md = args.out_dir / f"expanded_candidate_input_audit_{label}.md"
    audit_df.to_csv(audit_csv, index=False, encoding="utf-8")
    audit_md.write_text(
        "\n".join(
            [
                "# Expanded Candidate Input Audit (Year)",
                "",
                f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
                f"- date_count: {len(dates)}",
                f"- ok_count: {int((audit_df['status']=='ok').sum())}",
                f"- error_count: {int((audit_df['status']=='error').sum())}",
                "",
                f"- csv: {audit_csv}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    # Union candidate pairs csv
    union_candidates = args.out_dir / f"pair_shadow_pair_comparison_expanded_{label}_candidates.csv"
    if expanded_pair_csvs:
        frames = [pd.read_csv(p) for p in expanded_pair_csvs]
        pd.concat(frames, ignore_index=True).to_csv(union_candidates, index=False, encoding="utf-8")

    # Join labels using external-first results
    joined_pairs = args.out_dir / f"pair_shadow_pair_comparison_expanded_{label}_with_results_external_priority.csv"
    join_md = args.out_dir / f"join_wide_results_audit_{label}_external_priority.md"
    diff_csv = args.out_dir / f"external_vs_db_results_diff_{label}.csv"
    diff_md = args.out_dir / f"external_vs_db_results_diff_{label}.md"
    if union_candidates.exists():
        cmdj = [
            "py",
            "-3.11",
            "racing_ai/scripts/join_wide_results_to_candidate_pairs.py",
            "--input-csv",
            str(union_candidates),
            "--db-path",
            str(args.db_path),
            "--external-results-csv",
            str(args.external_results_csv),
            "--results-source-priority",
            "external,db",
            "--out-csv",
            str(joined_pairs),
            "--out-md",
            str(join_md),
            "--out-diff-csv",
            str(diff_csv),
            "--out-diff-md",
            str(diff_md),
        ]
        _run(cmdj)

        # Evaluation-only fallback: some years built via helper path don't have model_dynamic columns computed.
        post_audit_csv = args.out_dir / f"model_dynamic_postcompute_audit_{label}.csv"
        post_audit_md = args.out_dir / f"model_dynamic_postcompute_audit_{label}.md"
        _postcompute_model_dynamic_if_needed(joined_pairs, post_audit_csv, post_audit_md)

        # Evaluate
        eval_csv = args.out_dir / f"rule_vs_non_rule_candidate_evaluation_{label}_external_priority_quality_ok.csv"
        eval_md = args.out_dir / f"rule_vs_non_rule_candidate_evaluation_{label}_external_priority_quality_ok.md"
        cmd_eval = [
            "py",
            "-3.11",
            "racing_ai/scripts/evaluate_rule_vs_non_rule_candidates.py",
            "--input-csv",
            str(joined_pairs),
            "--out-csv",
            str(eval_csv),
            "--out-md",
            str(eval_md),
            "--quality-ok-only",
        ]
        _run(cmd_eval)

        cond_csv = args.out_dir / f"expanded_dynamic_candidate_conditions_with_results_{label}_external_priority_quality_ok.csv"
        cond_md = args.out_dir / f"expanded_dynamic_candidate_conditions_with_results_{label}_external_priority_quality_ok.md"
        cmd_cond = [
            "py",
            "-3.11",
            "racing_ai/scripts/evaluate_expanded_dynamic_conditions_with_results.py",
            "--input-csv",
            "racing_ai/reports/expanded_dynamic_candidate_conditions.csv",
            "--pairs-csv",
            str(joined_pairs),
            "--out-csv",
            str(cond_csv),
            "--out-md",
            str(cond_md),
            "--quality-ok-only",
        ]
        _run(cmd_cond)

        daily_csv = args.out_dir / f"dynamic_vs_rule_daily_stability_{label}.csv"
        daily_md = args.out_dir / f"dynamic_vs_rule_daily_stability_{label}.md"
        cmd_daily = [
            "py",
            "-3.11",
            "racing_ai/scripts/build_dynamic_vs_rule_daily_stability.py",
            "--input-csv",
            str(joined_pairs),
            "--start-date",
            args.start_date,
            "--end-date",
            args.end_date,
            "--out-csv",
            str(daily_csv),
            "--out-md",
            str(daily_md),
            "--quality-ok-only",
        ]
        _run(cmd_daily)

    print(str(audit_csv))
    print(str(audit_md))
    if union_candidates.exists():
        print(str(union_candidates))
    if joined_pairs.exists():
        print(str(joined_pairs))


if __name__ == "__main__":
    main()
