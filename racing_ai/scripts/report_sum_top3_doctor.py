from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pandas as pd

from aikeiba.db.duckdb import DuckDb
from aikeiba.decision.value_model import blend_ai_market_prob, compute_market_top3_prob_from_place_odds
from aikeiba.inference.top3 import _shrink_race_sum_top3
from aikeiba.modeling.registry import load_model_bundle


def _market_proxy(db: DuckDb, race_date: str) -> pd.DataFrame:
    rows = db.query_df(
        """
        WITH ranked AS (
          SELECT o.race_id, o.horse_no, lower(o.odds_type) AS odds_type, o.odds_value, o.captured_at,
                 row_number() OVER (
                   PARTITION BY o.race_id, o.horse_no, lower(o.odds_type)
                   ORDER BY o.captured_at DESC NULLS LAST, o.odds_snapshot_version DESC
                 ) AS rn
          FROM odds o
          JOIN races r ON r.race_id=o.race_id
          WHERE r.race_date = cast(? as DATE)
            AND lower(o.odds_type) IN ('place', 'place_max')
            AND o.horse_no > 0
        )
        SELECT race_id, horse_no,
               max(CASE WHEN odds_type='place' THEN odds_value END) AS odds_place,
               max(CASE WHEN odds_type='place_max' THEN odds_value END) AS odds_place_max
        FROM ranked
        WHERE rn=1
        GROUP BY race_id, horse_no
        """,
        (race_date,),
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


def _doctor_status(sum_shrunk: float | None) -> tuple[str, str]:
    if sum_shrunk is None or pd.isna(sum_shrunk):
        return "stop", "sum_top3_null"
    v = float(sum_shrunk)
    if v < 0.5 or v > 6.0:
        return "stop", "sum_top3_extreme"
    if v < 1.2 or v > 4.8:
        return "warn", "sum_top3_unusual"
    return "pass", "ok"


def build_report(
    *,
    db_path: Path,
    models_root: Path,
    race_date: str,
    model_version: str,
    feature_snapshot_version: str,
    ai_weight: float,
    out_md: Path,
) -> dict:
    db = DuckDb.connect(db_path)
    feats = db.query_df(
        """
        SELECT fs.race_id, fs.horse_no, fs.*
        FROM feature_store fs
        JOIN races r ON r.race_id=fs.race_id
        WHERE r.race_date = cast(? as DATE)
          AND fs.feature_snapshot_version = ?
        """,
        (race_date, feature_snapshot_version),
    )
    if len(feats) == 0:
        out_md.parent.mkdir(parents=True, exist_ok=True)
        out_md.write_text("# sum_top3_doctor_report\n\n- no feature rows\n", encoding="utf-8")
        return {"rows": 0, "report_path": str(out_md)}

    model, calibrator, _ = load_model_bundle(root=models_root, task="top3", model_version=model_version)
    p_raw = model.predict(feats)
    p_cal = calibrator.predict(p_raw)
    pred = feats[["race_id", "horse_no"]].copy()
    pred["p_top3_raw"] = p_raw
    pred["p_top3_calibrated"] = p_cal

    shr = pred[["race_id", "horse_no", "p_top3_calibrated"]].rename(columns={"p_top3_calibrated": "p_top3"})
    shr = _shrink_race_sum_top3(shr, race_col="race_id", prob_col="p_top3", target_sum=3.0).rename(
        columns={"p_top3": "p_top3_shrunk"}
    )
    merged = pred.merge(shr, on=["race_id", "horse_no"], how="left")

    mkt = _market_proxy(db, race_date)
    merged = merged.merge(mkt[["race_id", "horse_no", "market_top3_proxy"]], on=["race_id", "horse_no"], how="left")
    merged["p_top3_fused"] = merged.apply(
        lambda r: blend_ai_market_prob(
            p_ai=r["p_top3_shrunk"] if pd.notna(r["p_top3_shrunk"]) else None,
            p_market=r["market_top3_proxy"] if pd.notna(r["market_top3_proxy"]) else None,
            ai_weight=ai_weight,
        ),
        axis=1,
    )

    field = db.query_df(
        """
        SELECT e.race_id, count(*) AS field_size
        FROM entries e JOIN races r ON r.race_id=e.race_id
        WHERE r.race_date=cast(? as DATE)
          AND (e.is_scratched IS NULL OR e.is_scratched=FALSE)
        GROUP BY e.race_id
        """,
        (race_date,),
    )

    by_race = merged.groupby("race_id", as_index=False).agg(
        sum_p_top3_raw=("p_top3_raw", "sum"),
        sum_p_top3_calibrated=("p_top3_calibrated", "sum"),
        sum_p_top3_shrunk=("p_top3_shrunk", "sum"),
        sum_p_top3_fused=("p_top3_fused", "sum"),
        max_p_top3_fused=("p_top3_fused", "max"),
    )
    by_race = by_race.merge(field, on="race_id", how="left")
    by_race["density_top3"] = merged.groupby("race_id")["p_top3_fused"].apply(lambda s: s.dropna().nlargest(3).sum()).values
    status = by_race["sum_p_top3_shrunk"].apply(_doctor_status)
    by_race["doctor_status"] = status.apply(lambda x: x[0])
    by_race["doctor_reason"] = status.apply(lambda x: x[1])
    by_race = by_race.sort_values("race_id").reset_index(drop=True)

    by_race["shrink_improve"] = by_race["sum_p_top3_shrunk"] - by_race["sum_p_top3_calibrated"]
    abnormal = by_race[by_race["doctor_status"] != "pass"].copy()

    lines = [
        "# sum_top3_doctor_report",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- race_date: {race_date}",
        f"- model_version: {model_version}",
        f"- feature_snapshot_version: {feature_snapshot_version}",
        f"- ai_weight: {ai_weight}",
        f"- race_count: {len(by_race)}",
        f"- stop_count: {int((by_race['doctor_status']=='stop').sum())}",
        f"- warn_count: {int((by_race['doctor_status']=='warn').sum())}",
        "",
        "## Abnormal Race IDs",
    ]
    if len(abnormal) == 0:
        lines.append("- none")
    else:
        for r in abnormal.itertuples(index=False):
            lines.append(f"- {r.race_id}: {r.doctor_status} ({r.doctor_reason}) sum_shrunk={r.sum_p_top3_shrunk:.6f}")

    lines += [
        "",
        "## Shrink Improvement Summary",
        f"- calibrated_sum_mean: {float(by_race['sum_p_top3_calibrated'].mean()):.6f}",
        f"- shrunk_sum_mean: {float(by_race['sum_p_top3_shrunk'].mean()):.6f}",
        f"- mean_delta(shrunk-calibrated): {float(by_race['shrink_improve'].mean()):.6f}",
        "",
        "## Race Table",
        "| race_id | sum_p_top3_raw | sum_p_top3_calibrated | sum_p_top3_shrunk | sum_p_top3_fused | field_size | max_p_top3_fused | density_top3 | doctor_status | doctor_reason |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for r in by_race.itertuples(index=False):
        lines.append(
            f"| {r.race_id} | {float(r.sum_p_top3_raw):.6f} | {float(r.sum_p_top3_calibrated):.6f} | {float(r.sum_p_top3_shrunk):.6f} | {float(r.sum_p_top3_fused):.6f} | {int(r.field_size) if pd.notna(r.field_size) else 0} | {float(r.max_p_top3_fused):.6f} | {float(r.density_top3):.6f} | {r.doctor_status} | {r.doctor_reason} |"
        )

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")
    return {
        "rows": int(len(by_race)),
        "abnormal_rows": int(len(abnormal)),
        "report_path": str(out_md),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="racing_ai/data/warehouse/aikeiba.duckdb")
    ap.add_argument("--models-root", default="racing_ai/data/models_compare")
    ap.add_argument("--race-date", default="2026-04-12")
    ap.add_argument("--model-version", default="top3_stability_plus_pace_v3")
    ap.add_argument("--feature-snapshot-version", default="fs_v1")
    ap.add_argument("--ai-weight", type=float, default=0.65)
    ap.add_argument("--out-md", default="racing_ai/reports/sum_top3_doctor_report.md")
    args = ap.parse_args()
    res = build_report(
        db_path=Path(args.db_path),
        models_root=Path(args.models_root),
        race_date=args.race_date,
        model_version=args.model_version,
        feature_snapshot_version=args.feature_snapshot_version,
        ai_weight=float(args.ai_weight),
        out_md=Path(args.out_md),
    )
    print(res)


if __name__ == "__main__":
    main()
