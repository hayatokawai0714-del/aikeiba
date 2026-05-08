from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd

from aikeiba.db.duckdb import DuckDb


def _norm_pair(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if "-" not in s:
        return None
    a, b = s.split("-", 1)
    try:
        x, y = sorted((int(a), int(b)))
        return f"{x:02d}-{y:02d}"
    except Exception:
        return None


def _wide_hit_pairs_for_dates(db: DuckDb, dates: list[str]) -> dict[tuple[str, str], int]:
    if len(dates) == 0:
        return {}
    ph = ",".join(["?"] * len(dates))
    rows = db.query_df(
        f"""
        SELECT cast(r.race_date as VARCHAR) AS race_date, p.race_id, p.bet_key
        FROM payouts p
        JOIN races r ON r.race_id=p.race_id
        WHERE lower(p.bet_type)='wide'
          AND cast(r.race_date as VARCHAR) IN ({ph})
        """,
        tuple(dates),
    )
    out: dict[tuple[str, str], int] = {}
    for r in rows.to_dict("records"):
        k = _norm_pair(r.get("bet_key"))
        if k is None:
            continue
        out[(str(r.get("race_id")), k)] = 1
    return out


def summarize(*, db_path: Path, race_day_root: Path, model_version: str, dates: list[str], out_md: Path) -> dict:
    db = DuckDb.connect(db_path)
    hit_map = _wide_hit_pairs_for_dates(db, dates)
    recs = []

    for d in dates:
        base = race_day_root / d / model_version
        run_summary_path = base / "run_summary.json"
        cand_path = base / "candidate_pairs.parquet"
        race_flags_path = base / "race_flags.parquet"
        skip_log_path = base / "skip_log.parquet"
        shadow_report_path = base / "pair_shadow_compare_report.md"

        if not run_summary_path.exists():
            recs.append(
                {
                    "race_date": d,
                    "status": "missing_run_summary",
                    "stop_reason": "missing_run_summary",
                    "candidate_pairs_count": 0,
                    "selected_pairs_count": 0,
                    "pair_model_available_count": 0,
                    "rule_top5_hit_rate": None,
                    "model_top5_hit_rate": None,
                    "rank_diff_mean": None,
                    "probability_gate_warn_count": None,
                    "race_meta_invalid_count": None,
                }
            )
            continue

        summary = json.loads(run_summary_path.read_text(encoding="utf-8"))
        if cand_path.exists():
            c = pd.read_parquet(cand_path)
        else:
            c = pd.DataFrame()

        if len(c) > 0:
            c = c.copy()
            c["pair_norm"] = c["pair"].apply(_norm_pair) if "pair" in c.columns else None
            c["hit"] = c.apply(lambda r: hit_map.get((str(r.get("race_id")), str(r.get("pair_norm"))), 0), axis=1)

            cand_cnt = int(len(c))
            sel_cnt = int(c["pair_selected_flag"].fillna(False).astype(bool).sum()) if "pair_selected_flag" in c.columns else 0
            avail_cnt = int(c["pair_model_available"].fillna(False).astype(bool).sum()) if "pair_model_available" in c.columns else 0

            rule_top5 = c[c["pair_rank_in_race"] <= 5] if "pair_rank_in_race" in c.columns else c.iloc[0:0]
            model_top5 = c[c["pair_model_rank_in_race"] <= 5] if "pair_model_rank_in_race" in c.columns else c.iloc[0:0]
            rule_hit = float(rule_top5["hit"].mean()) if len(rule_top5) > 0 else None
            model_hit = float(model_top5["hit"].mean()) if len(model_top5) > 0 else None
            rank_diff_mean = (
                float((c["pair_model_rank_in_race"] - c["pair_rank_in_race"]).abs().mean())
                if "pair_model_rank_in_race" in c.columns and "pair_rank_in_race" in c.columns
                else None
            )
        else:
            cand_cnt = 0
            sel_cnt = 0
            avail_cnt = 0
            rule_hit = None
            model_hit = None
            rank_diff_mean = None

        recs.append(
            {
                "race_date": d,
                "status": str(summary.get("status")),
                "stop_reason": summary.get("stop_reason"),
                "candidate_pairs_count": cand_cnt,
                "selected_pairs_count": sel_cnt,
                "pair_model_available_count": avail_cnt,
                "rule_top5_hit_rate": rule_hit,
                "model_top5_hit_rate": model_hit,
                "rank_diff_mean": rank_diff_mean,
                "probability_gate_warn_count": summary.get("probability_gate_warn_count"),
                "race_meta_invalid_count": summary.get("race_meta_invalid_count"),
                "run_summary_path": str(run_summary_path),
                "candidate_pairs_path": str(cand_path),
                "race_flags_path": str(race_flags_path),
                "skip_log_path": str(skip_log_path),
                "pair_shadow_compare_report_path": str(shadow_report_path),
            }
        )

    df = pd.DataFrame(recs).sort_values("race_date")
    df["excluded"] = df["status"].isin(["stop", "not_ready"])
    df["exclude_reason"] = None
    for i, row in df.iterrows():
        if not bool(row.get("excluded")):
            continue
        stop_reason = str(row.get("stop_reason") or "")
        if "raw_files_empty" in stop_reason:
            reason = "raw_files_empty"
        elif "missing_required_raw_files" in stop_reason:
            reason = "missing_required_raw_files"
        elif "zero_rows:races" in stop_reason:
            reason = "zero_rows:races"
        elif "zero_rows:entries" in stop_reason:
            reason = "zero_rows:entries"
        elif stop_reason.strip():
            reason = "other"
        else:
            reason = "other"
        df.at[i, "exclude_reason"] = reason
    comparable_df = df[~df["excluded"]].copy()

    comparable_df["top5_hit_rate_diff_model_minus_rule"] = (
        pd.to_numeric(comparable_df.get("model_top5_hit_rate"), errors="coerce")
        - pd.to_numeric(comparable_df.get("rule_top5_hit_rate"), errors="coerce")
    )
    cmp = comparable_df["top5_hit_rate_diff_model_minus_rule"]
    model_win_days = int((cmp > 0).sum())
    rule_win_days = int((cmp < 0).sum())
    tie_days = int((cmp == 0).sum())
    total_days = int(len(df))
    comparable_days = int(len(comparable_df))
    excluded_days = int(total_days - comparable_days)
    lines = [
        "# shadow_operation_summary",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- model_version: {model_version}",
        f"- dates: {', '.join(dates)}",
        f"- total_days: {total_days}",
        f"- comparable_days: {comparable_days}",
        f"- excluded_days: {excluded_days}",
        f"- model_win_days: {model_win_days}",
        f"- rule_win_days: {rule_win_days}",
        f"- tie_days: {tie_days}",
        "",
        "## Excluded Days",
    ]
    excluded_df = df[df["excluded"]].copy()
    if len(excluded_df) == 0:
        lines.append("- (none)")
    else:
        lines += [
            "| race_date | status | stop_reason | exclude_reason |",
            "|---|---|---|---|",
        ]
        for r in excluded_df.itertuples(index=False):
            lines.append(
                f"| {r.race_date} | {r.status} | {r.stop_reason if pd.notna(r.stop_reason) else 'NA'} | {r.exclude_reason if pd.notna(r.exclude_reason) else 'other'} |"
            )
    lines += [
        "",
        "## Daily Summary",
        "| race_date | status | candidate_pairs | selected_pairs | model_available | rule_top5_hit | model_top5_hit | model-rule_diff | rank_diff_abs_mean | gate_warn | race_meta_invalid |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in comparable_df.itertuples(index=False):
        lines.append(
            f"| {r.race_date} | {r.status} | {int(r.candidate_pairs_count)} | {int(r.selected_pairs_count)} | {int(r.pair_model_available_count)} | {r.rule_top5_hit_rate if pd.notna(r.rule_top5_hit_rate) else 'NA'} | {r.model_top5_hit_rate if pd.notna(r.model_top5_hit_rate) else 'NA'} | {r.top5_hit_rate_diff_model_minus_rule if pd.notna(r.top5_hit_rate_diff_model_minus_rule) else 'NA'} | {r.rank_diff_mean if pd.notna(r.rank_diff_mean) else 'NA'} | {r.probability_gate_warn_count if pd.notna(r.probability_gate_warn_count) else 'NA'} | {r.race_meta_invalid_count if pd.notna(r.race_meta_invalid_count) else 'NA'} |"
        )
    lines += ["", "## Race Meta Invalid (Daily)"]
    for r in comparable_df.itertuples(index=False):
        lines.append(f"- {r.race_date}: {r.race_meta_invalid_count if pd.notna(r.race_meta_invalid_count) else 'NA'}")
    lines += ["", "## Probability Gate Warning (Daily)"]
    for r in comparable_df.itertuples(index=False):
        lines.append(
            f"- {r.race_date}: {r.probability_gate_warn_count if pd.notna(r.probability_gate_warn_count) else 'NA'}"
        )
    lines += ["", "## Output Paths"]
    for r in df.itertuples(index=False):
        lines.append(f"- {r.race_date}: {r.run_summary_path}")
        lines.append(f"- {r.race_date}: {r.candidate_pairs_path}")
        lines.append(f"- {r.race_date}: {r.race_flags_path}")
        lines.append(f"- {r.race_date}: {r.skip_log_path}")
        lines.append(f"- {r.race_date}: {r.pair_shadow_compare_report_path}")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")
    return {
        "rows": int(len(df)),
        "total_days": total_days,
        "comparable_days": comparable_days,
        "excluded_days": excluded_days,
        "model_win_days": model_win_days,
        "rule_win_days": rule_win_days,
        "tie_days": tie_days,
        "report_path": str(out_md),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="racing_ai/data/warehouse/aikeiba.duckdb")
    ap.add_argument("--race-day-root", default="data/race_day")
    ap.add_argument("--model-version", default="top3_stability_plus_pace_v3")
    ap.add_argument("--dates", required=True, help="comma separated YYYY-MM-DD")
    ap.add_argument("--out-md", default="racing_ai/reports/shadow_operation_summary.md")
    args = ap.parse_args()
    dates = [x.strip() for x in str(args.dates).split(",") if x.strip()]
    res = summarize(
        db_path=Path(args.db_path),
        race_day_root=Path(args.race_day_root),
        model_version=args.model_version,
        dates=dates,
        out_md=Path(args.out_md),
    )
    print(res)


if __name__ == "__main__":
    main()
