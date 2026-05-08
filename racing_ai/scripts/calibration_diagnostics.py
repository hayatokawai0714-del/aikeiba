from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd

from aikeiba.db.duckdb import DuckDb
from aikeiba.decision.value_model import blend_ai_market_prob, compute_market_top3_prob_from_place_odds
from aikeiba.inference.top3 import _shrink_race_sum_top3
from aikeiba.modeling.registry import load_model_bundle


def _bucket_popularity(v: object) -> str:
    if v is None or pd.isna(v):
        return "UNKNOWN"
    x = float(v)
    if x <= 1:
        return "1人気"
    if x <= 3:
        return "2-3人気"
    if x <= 5:
        return "4-5人気"
    if x <= 9:
        return "6-9人気"
    return "10人気以下"


def _bucket_field_size(v: object) -> str:
    if v is None or pd.isna(v):
        return "UNKNOWN"
    n = int(v)
    if n <= 10:
        return "small"
    if n <= 14:
        return "medium"
    return "large"


def _bucket_distance(v: object) -> str:
    if v is None or pd.isna(v):
        return "UNKNOWN"
    d = int(v)
    if d < 1400:
        return "sprint"
    if d < 2000:
        return "mile"
    if d < 2600:
        return "middle"
    return "long"


def _market_proxy(db: DuckDb, start_date: str, end_date: str) -> pd.DataFrame:
    rows = db.query_df(
        """
        WITH ranked AS (
          SELECT
            r.race_date,
            o.race_id, o.horse_no, lower(o.odds_type) AS odds_type, o.odds_value, o.captured_at,
            row_number() OVER (
              PARTITION BY o.race_id, o.horse_no, lower(o.odds_type)
              ORDER BY o.captured_at DESC NULLS LAST, o.odds_snapshot_version DESC
            ) AS rn
          FROM odds o
          JOIN races r ON r.race_id=o.race_id
          WHERE r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
            AND lower(o.odds_type) IN ('place', 'place_max')
            AND o.horse_no > 0
        )
        SELECT race_date, race_id, horse_no,
               max(CASE WHEN odds_type='place' THEN odds_value END) AS odds_place,
               max(CASE WHEN odds_type='place_max' THEN odds_value END) AS odds_place_max
        FROM ranked
        WHERE rn=1
        GROUP BY race_date, race_id, horse_no
        """,
        (start_date, end_date),
    )
    if len(rows) == 0:
        return rows
    rows["market_top3_proxy"] = rows.apply(
        lambda r: compute_market_top3_prob_from_place_odds(r["odds_place"], r["odds_place_max"]),
        axis=1,
    )
    out = []
    for rid, g in rows.groupby("race_id"):
        gg = g.copy()
        s = gg["market_top3_proxy"].dropna().sum()
        if s and s > 0:
            gg["market_top3_proxy"] = gg["market_top3_proxy"].apply(
                lambda v: min(1.0, max(0.0, (v * 3.0 / s) if pd.notna(v) else v))
            )
        out.append(gg)
    return pd.concat(out, ignore_index=True) if out else rows


def _curve(df: pd.DataFrame, group_col: str, prob_col: str, out_col: str = "actual_top3", bins: int = 10) -> pd.DataFrame:
    work = df[[group_col, prob_col, out_col]].copy()
    work = work.dropna(subset=[prob_col])
    if len(work) == 0:
        return pd.DataFrame(columns=[group_col, "bin", "count", "prob_mean", "actual_rate"])
    work["bin"] = pd.qcut(work[prob_col].rank(method="first"), q=min(bins, max(2, int(len(work) / 50))), labels=False, duplicates="drop")
    agg = (
        work.groupby([group_col, "bin"], as_index=False)
        .agg(count=(prob_col, "size"), prob_mean=(prob_col, "mean"), actual_rate=(out_col, "mean"))
    )
    return agg


def run_diagnostics(
    *,
    db_path: Path,
    models_root: Path,
    model_version: str,
    feature_snapshot_version: str,
    start_date: str,
    end_date: str,
    ai_weight: float,
    out_parquet: Path,
    out_md: Path,
) -> dict:
    warnings: list[str] = []
    db = DuckDb.connect(db_path)
    feats = db.query_df(
        """
        SELECT
          fs.*,
          cast(r.race_date as VARCHAR) AS race_date,
          r.venue, r.surface, r.distance, r.field_size_expected
        FROM feature_store fs
        JOIN races r ON r.race_id=fs.race_id
        WHERE r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
          AND fs.feature_snapshot_version = ?
        """,
        (start_date, end_date, feature_snapshot_version),
    )
    if len(feats) == 0:
        out_parquet.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_parquet(out_parquet, index=False)
        out_md.parent.mkdir(parents=True, exist_ok=True)
        out_md.write_text("# top3_calibration_diagnostics\n\n- no feature rows\n", encoding="utf-8")
        return {"rows": 0, "warnings": ["no_feature_rows"], "out_parquet": str(out_parquet), "out_md": str(out_md)}

    model, calibrator, _ = load_model_bundle(root=models_root, task="top3", model_version=model_version)
    p_raw = model.predict(feats)
    p_cal = calibrator.predict(np.asarray(p_raw))

    base = feats[["race_id", "race_date", "horse_no", "venue", "surface", "distance", "field_size_expected"]].copy()
    base["race_date"] = base["race_date"].astype(str)
    base["p_top3_raw"] = p_raw
    base["p_top3_calibrated"] = p_cal

    shr = base[["race_id", "horse_no", "p_top3_calibrated"]].rename(columns={"p_top3_calibrated": "p_top3"})
    shr = _shrink_race_sum_top3(shr, race_col="race_id", prob_col="p_top3", target_sum=3.0).rename(columns={"p_top3": "p_top3_shrunk"})
    base = base.merge(shr, on=["race_id", "horse_no"], how="left")

    mkt = _market_proxy(db, start_date, end_date)
    base = base.merge(
        mkt[["race_id", "horse_no", "odds_place", "odds_place_max", "market_top3_proxy"]],
        on=["race_id", "horse_no"],
        how="left",
    )
    base["p_top3_fused"] = base.apply(
        lambda r: blend_ai_market_prob(
            p_ai=r["p_top3_shrunk"] if pd.notna(r["p_top3_shrunk"]) else None,
            p_market=r["market_top3_proxy"] if pd.notna(r["market_top3_proxy"]) else None,
            ai_weight=ai_weight,
        ),
        axis=1,
    )

    results = db.query_df(
        """
        SELECT
          cast(r.race_date as VARCHAR) AS race_date,
          res.race_id, res.horse_no,
          CASE WHEN res.finish_position BETWEEN 1 AND 3 THEN 1 ELSE 0 END AS actual_top3,
          res.pop_rank
        FROM results res
        JOIN races r ON r.race_id=res.race_id
        WHERE r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
        """,
        (start_date, end_date),
    )
    results["race_date"] = results["race_date"].astype(str)
    base = base.merge(results, on=["race_date", "race_id", "horse_no"], how="left")
    base["actual_top3"] = base["actual_top3"].fillna(0).astype(int)
    if base["pop_rank"].isna().all():
        warnings.append("pop_rank_all_missing")
    base["popularity_bucket"] = base["pop_rank"].apply(_bucket_popularity)
    base["field_size"] = base["field_size_expected"]
    base["field_size_bucket"] = base["field_size"].apply(_bucket_field_size)
    base["distance_bucket"] = base["distance"].apply(_bucket_distance)
    base["raw_minus_calibrated"] = base["p_top3_raw"] - base["p_top3_calibrated"]
    base["generated_at"] = dt.datetime.now().isoformat(timespec="seconds")

    race = (
        base.groupby(["race_date", "race_id"], as_index=False)
        .agg(
            sum_p_top3_raw=("p_top3_raw", "sum"),
            sum_p_top3_calibrated=("p_top3_calibrated", "sum"),
            sum_p_top3_shrunk=("p_top3_shrunk", "sum"),
            sum_p_top3_fused=("p_top3_fused", "sum"),
            actual_top3_count=("actual_top3", "sum"),
            field_size=("field_size", "max"),
            max_p_top3_fused=("p_top3_fused", "max"),
            density_top3=("p_top3_fused", lambda s: s.dropna().nlargest(3).sum()),
            venue=("venue", "first"),
            surface=("surface", "first"),
            distance=("distance", "first"),
        )
    )
    race["race_meta_invalid"] = (
        race["field_size"].isna()
        | (pd.to_numeric(race["field_size"], errors="coerce").fillna(0) <= 0)
        | race["distance"].isna()
        | race["surface"].isna()
        | race["venue"].isna()
    )
    race["doctor_status"] = race["sum_p_top3_shrunk"].apply(lambda v: "stop" if (pd.notna(v) and (v < 0.5 or v > 6.0)) else ("warn" if (pd.notna(v) and (v < 1.2 or v > 4.8)) else "pass"))
    race["doctor_reason"] = race["doctor_status"].map({"stop": "sum_top3_extreme", "warn": "sum_top3_unusual", "pass": "ok"})

    extreme = race[race["doctor_status"] == "stop"].copy()
    non_extreme = race[race["doctor_status"] != "stop"].copy()
    invalid_race = race[race["race_meta_invalid"]].copy()
    valid_race = race[~race["race_meta_invalid"]].copy()
    if len(extreme) == 0:
        warnings.append("no_sum_top3_extreme_races")

    curves = []
    for gcol in ["popularity_bucket", "field_size_bucket", "distance_bucket", "venue"]:
        c = _curve(base, gcol, "p_top3_calibrated")
        if len(c) > 0:
            c["group_type"] = gcol
            curves.append(c.rename(columns={gcol: "group_value"}))
    curve_df = pd.concat(curves, ignore_index=True) if len(curves) > 0 else pd.DataFrame(columns=["group_type", "group_value", "bin", "count", "prob_mean", "actual_rate"])

    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    base.to_parquet(out_parquet, index=False)

    lines = [
        "# top3_calibration_diagnostics",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- model_version: {model_version}",
        f"- feature_snapshot_version: {feature_snapshot_version}",
        f"- date_range: {start_date} .. {end_date}",
        f"- horse_rows: {len(base)}",
        f"- race_rows: {len(race)}",
        f"- sum_top3_extreme_races: {len(extreme)}",
        f"- race_meta_invalid_races: {len(invalid_race)}",
        "",
        "## Raw vs Calibrated",
        f"- mean(p_top3_raw): {float(base['p_top3_raw'].mean()):.6f}",
        f"- mean(p_top3_calibrated): {float(base['p_top3_calibrated'].mean()):.6f}",
        f"- mean(raw-calibrated): {float(base['raw_minus_calibrated'].mean()):.6f}",
        f"- mean(sum_raw by race): {float(race['sum_p_top3_raw'].mean()):.6f}",
        f"- mean(sum_calibrated by race): {float(race['sum_p_top3_calibrated'].mean()):.6f}",
        f"- mean(sum_shrunk by race): {float(race['sum_p_top3_shrunk'].mean()):.6f}",
        f"- mean(sum_fused by race): {float(race['sum_p_top3_fused'].mean()):.6f}",
        "",
        "## Extreme Race IDs",
    ]
    if len(extreme) == 0:
        lines.append("- none")
    else:
        for r in extreme.itertuples(index=False):
            lines.append(f"- {r.race_id} ({r.race_date}): sum_shrunk={r.sum_p_top3_shrunk:.6f}, field_size={int(r.field_size)}")

    lines += ["", "## Extreme vs Non-Extreme (Race-level means)"]
    cmp_cols = ["field_size", "distance", "sum_p_top3_raw", "sum_p_top3_calibrated", "sum_p_top3_shrunk", "sum_p_top3_fused", "density_top3", "max_p_top3_fused"]
    lines.append("| metric | extreme_mean | non_extreme_mean |")
    lines.append("|---|---:|---:|")
    for c in cmp_cols:
        ex_val = pd.to_numeric(extreme[c], errors="coerce").mean() if len(extreme) > 0 and c in extreme.columns else np.nan
        nx_val = pd.to_numeric(non_extreme[c], errors="coerce").mean() if len(non_extreme) > 0 and c in non_extreme.columns else np.nan
        ex = float(ex_val) if pd.notna(ex_val) else float("nan")
        nx = float(nx_val) if pd.notna(nx_val) else float("nan")
        lines.append(f"| {c} | {ex:.6f} | {nx:.6f} |")

    lines += ["", "## Race Meta Invalid vs Valid (Race-level means)"]
    lines.append("| metric | invalid_mean | valid_mean |")
    lines.append("|---|---:|---:|")
    for c in cmp_cols:
        iv_val = pd.to_numeric(invalid_race[c], errors="coerce").mean() if len(invalid_race) > 0 and c in invalid_race.columns else np.nan
        vv_val = pd.to_numeric(valid_race[c], errors="coerce").mean() if len(valid_race) > 0 and c in valid_race.columns else np.nan
        iv = float(iv_val) if pd.notna(iv_val) else float("nan")
        vv = float(vv_val) if pd.notna(vv_val) else float("nan")
        lines.append(f"| {c} | {iv:.6f} | {vv:.6f} |")

    lines += ["", "## Race Meta Invalid IDs"]
    if len(invalid_race) == 0:
        lines.append("- none")
    else:
        for r in invalid_race.itertuples(index=False):
            reasons = []
            if pd.isna(r.field_size) or float(r.field_size) <= 0:
                reasons.append("field_size_expected_le_0")
            if pd.isna(r.distance):
                reasons.append("distance_null")
            if pd.isna(r.surface):
                reasons.append("surface_null")
            if pd.isna(r.venue):
                reasons.append("venue_null")
            lines.append(f"- {r.race_id}: {','.join(reasons)}")

    lines += ["", "## Calibration Curves (sample rows)"]
    if len(curve_df) == 0:
        lines.append("- no calibration curve rows")
    else:
        lines.append("| group_type | group_value | bin | count | prob_mean | actual_rate |")
        lines.append("|---|---|---:|---:|---:|---:|")
        for r in curve_df.sort_values(["group_type", "group_value", "bin"]).head(80).itertuples(index=False):
            lines.append(f"| {r.group_type} | {r.group_value} | {int(r.bin)} | {int(r.count)} | {float(r.prob_mean):.6f} | {float(r.actual_rate):.6f} |")

    lines += ["", "## Warnings"]
    if warnings:
        lines.extend([f"- {w}" for w in warnings])
    else:
        lines.append("- none")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")
    return {
        "horse_rows": int(len(base)),
        "race_rows": int(len(race)),
        "extreme_races": int(len(extreme)),
        "race_meta_invalid_races": int(len(invalid_race)),
        "mean_raw_minus_calibrated": float(base["raw_minus_calibrated"].mean()),
        "warnings": warnings,
        "out_parquet": str(out_parquet),
        "out_md": str(out_md),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="racing_ai/data/warehouse/aikeiba.duckdb")
    ap.add_argument("--models-root", default="racing_ai/data/models_compare")
    ap.add_argument("--model-version", default="top3_stability_plus_pace_v3")
    ap.add_argument("--feature-snapshot-version", default="fs_v1")
    ap.add_argument("--start-date", default="2026-04-12")
    ap.add_argument("--end-date", default="2026-04-12")
    ap.add_argument("--ai-weight", type=float, default=0.65)
    ap.add_argument("--out-parquet", default="racing_ai/data/diagnostics/top3_calibration_diagnostics.parquet")
    ap.add_argument("--out-md", default="racing_ai/reports/top3_calibration_diagnostics.md")
    args = ap.parse_args()
    res = run_diagnostics(
        db_path=Path(args.db_path),
        models_root=Path(args.models_root),
        model_version=args.model_version,
        feature_snapshot_version=args.feature_snapshot_version,
        start_date=args.start_date,
        end_date=args.end_date,
        ai_weight=float(args.ai_weight),
        out_parquet=Path(args.out_parquet),
        out_md=Path(args.out_md),
    )
    print(res)


if __name__ == "__main__":
    main()
