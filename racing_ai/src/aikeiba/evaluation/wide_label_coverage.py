from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

from aikeiba.db.duckdb import DuckDb


def _resolve_pair_files(pairs_glob: str) -> list[Path]:
    base = Path(".")
    direct = sorted(base.glob(pairs_glob))
    if len(direct) > 0:
        return direct
    return sorted(base.rglob(Path(pairs_glob).name))


def build_wide_label_coverage_report(*, db_path: Path, pairs_glob: str, pair_base_path: Path, out_md: Path) -> dict:
    db = DuckDb.connect(db_path)

    races = db.query_df("SELECT cast(race_date as varchar) race_date, count(*) races_count FROM races GROUP BY 1")
    results = db.query_df(
        """
        SELECT cast(r.race_date as varchar) race_date,
               count(*) results_rows,
               sum(CASE WHEN res.finish_position IS NOT NULL THEN 1 ELSE 0 END) results_finish_nonnull
        FROM results res
        JOIN races r ON r.race_id=res.race_id
        GROUP BY 1
        """
    )
    payouts = db.query_df(
        """
        SELECT cast(r.race_date as varchar) race_date,
               count(*) payouts_rows,
               sum(CASE WHEN lower(p.bet_type)='wide' THEN 1 ELSE 0 END) wide_payouts_rows
        FROM payouts p
        JOIN races r ON r.race_id=p.race_id
        GROUP BY 1
        """
    )

    pair_rows = []
    for p in _resolve_pair_files(pairs_glob):
        try:
            df = pd.read_parquet(p)
            race_date = str(df["race_date"].iloc[0]) if len(df) > 0 and "race_date" in df.columns else p.name.split("_")[3]
            pair_rows.append({"race_date": race_date, "pair_candidates_rows": int(len(df)), "pair_file": str(p)})
        except Exception:
            continue
    pair_df = pd.DataFrame(pair_rows)
    if len(pair_df) > 0:
        pair_agg = pair_df.groupby("race_date", as_index=False).agg(pair_candidates_rows=("pair_candidates_rows", "sum"))
    else:
        pair_agg = pd.DataFrame(columns=["race_date", "pair_candidates_rows"])

    if pair_base_path.exists():
        base = pd.read_parquet(pair_base_path)
        hit_agg = base.groupby("race_date", as_index=False).agg(
            actual_wide_hit_rows=("actual_wide_hit", "sum"),
            label_rows=("actual_wide_hit", "size"),
        )
        ls = (
            base.groupby(["race_date", "label_source"], as_index=False)
            .size()
            .rename(columns={"size": "count"})
            if "label_source" in base.columns
            else pd.DataFrame(columns=["race_date", "label_source", "count"])
        )
    else:
        hit_agg = pd.DataFrame(columns=["race_date", "actual_wide_hit_rows", "label_rows"])
        ls = pd.DataFrame(columns=["race_date", "label_source", "count"])

    merged = (
        races.merge(results, on="race_date", how="left")
        .merge(payouts, on="race_date", how="left")
        .merge(pair_agg, on="race_date", how="left")
        .merge(hit_agg, on="race_date", how="left")
    )
    for c in [
        "results_rows",
        "results_finish_nonnull",
        "payouts_rows",
        "wide_payouts_rows",
        "pair_candidates_rows",
        "actual_wide_hit_rows",
        "label_rows",
    ]:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0).astype(int)

    warnings = []
    for r in merged.itertuples(index=False):
        if int(r.results_rows) > 0 and int(r.results_finish_nonnull) == 0:
            warnings.append(f"{r.race_date}:finish_position_nonnull_zero")
        if int(r.results_rows) > int(r.results_finish_nonnull):
            warnings.append(f"{r.race_date}:finish_position_partial_missing")
        if int(r.payouts_rows) == 0 or int(r.wide_payouts_rows) == 0:
            warnings.append(f"{r.race_date}:wide_payouts_missing")
        if int(r.pair_candidates_rows) > 0 and int(r.actual_wide_hit_rows) == 0:
            warnings.append(f"{r.race_date}:no_actual_wide_hit_rows")

    lines = [
        "# wide_label_coverage_report",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- db_path: {db_path}",
        f"- pairs_glob: {pairs_glob}",
        f"- pair_learning_base: {pair_base_path}",
        "",
        "## race_date coverage",
        "| race_date | races | results | finish_nonnull | payouts | wide_payouts | pair_candidates | actual_wide_hit |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in merged.sort_values("race_date").itertuples(index=False):
        lines.append(
            f"| {r.race_date} | {int(r.races_count)} | {int(r.results_rows)} | {int(r.results_finish_nonnull)} | {int(r.payouts_rows)} | {int(r.wide_payouts_rows)} | {int(r.pair_candidates_rows)} | {int(r.actual_wide_hit_rows)} |"
        )

    lines += ["", "## label_source counts by race_date"]
    if len(ls) == 0:
        lines.append("- none")
    else:
        lines.append("| race_date | label_source | count |")
        lines.append("|---|---|---:|")
        for r in ls.sort_values(["race_date", "label_source"]).itertuples(index=False):
            lines.append(f"| {r.race_date} | {r.label_source} | {int(r.count)} |")

    lines += ["", "## warnings"]
    if warnings:
        lines += [f"- {w}" for w in warnings]
    else:
        lines.append("- none")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")

    return {
        "rows": int(len(merged)),
        "warnings": warnings,
        "report_path": str(out_md),
        "label_source_rows": int(len(ls)),
    }
