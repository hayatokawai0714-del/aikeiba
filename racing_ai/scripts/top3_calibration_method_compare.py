from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss

from aikeiba.db.duckdb import DuckDb
from aikeiba.inference.top3 import _shrink_race_sum_top3
from aikeiba.modeling.calibration import fit_isotonic, fit_none, fit_sigmoid
from aikeiba.modeling.datasets import load_top3_dataset
from aikeiba.modeling.lgbm import train_binary_lgbm


def _fit_calibrator(method: str, p_raw: np.ndarray, y: np.ndarray):
    m = method.lower()
    if m == "isotonic":
        return fit_isotonic(p_raw, y)
    if m in {"sigmoid", "platt"}:
        return fit_sigmoid(p_raw, y)
    if m == "none":
        return fit_none()
    raise ValueError(method)


def _brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((np.asarray(p, dtype=float) - np.asarray(y, dtype=float)) ** 2))


def _gate_counts(sum_series: pd.Series) -> tuple[int, int]:
    stop = int(((sum_series < 0.5) | (sum_series > 6.0)).sum())
    warn = int((((sum_series < 1.2) | (sum_series > 4.8)) & ~((sum_series < 0.5) | (sum_series > 6.0))).sum())
    return stop, warn


def _invalid_races(db: DuckDb, valid_start_date: str, valid_end_date: str) -> set[str]:
    rows = db.query_df(
        """
        SELECT race_id
        FROM races
        WHERE race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
          AND (
            field_size_expected IS NULL OR field_size_expected <= 0
            OR distance IS NULL OR surface IS NULL OR venue IS NULL
          )
        """,
        (valid_start_date, valid_end_date),
    ).to_dict("records")
    return set(str(r["race_id"]) for r in rows)


def run_compare(
    *,
    db_path: Path,
    feature_snapshot_version: str,
    train_end_date: str,
    valid_start_date: str,
    valid_end_date: str,
    calibration_start_date: str | None,
    calibration_end_date: str | None,
    calibration_ratio: float,
    feature_set: str,
    out_md: Path,
) -> dict:
    db = DuckDb.connect(db_path)
    split = load_top3_dataset(
        db=db,
        feature_snapshot_version=feature_snapshot_version,
        train_end_date=train_end_date,
        valid_start_date=valid_start_date,
        valid_end_date=valid_end_date,
        calibration_start_date=calibration_start_date,
        calibration_end_date=calibration_end_date,
        calibration_ratio=calibration_ratio,
        include_stability_features=(feature_set in {"stability", "stability_plus_pace"}),
        include_pace_features=(feature_set == "stability_plus_pace"),
    )

    model = train_binary_lgbm(
        X_train=split.X_train,
        y_train=split.y_train,
        X_valid=split.X_calibration,
        y_valid=split.y_calibration,
        seed=42,
    )
    p_raw_cal = np.asarray(model.predict(split.X_calibration), dtype=float)
    y_cal = np.asarray(split.y_calibration, dtype=int)
    p_raw_valid = np.asarray(model.predict(split.X_valid), dtype=float)
    y_valid = np.asarray(split.y_valid, dtype=int)
    race_id_valid = np.asarray(split.race_id_valid, dtype=str)

    invalid = _invalid_races(db, valid_start_date, valid_end_date)
    valid_mask = np.array([rid not in invalid for rid in race_id_valid], dtype=bool)

    methods = ["isotonic", "sigmoid", "none"]
    rows = []
    curves = []
    for m in methods:
        cal = _fit_calibrator(m, p_raw_cal, y_cal)
        p = np.asarray(cal.predict(p_raw_valid), dtype=float)
        df = pd.DataFrame({"race_id": race_id_valid, "p": p, "y": y_valid})
        df = df[valid_mask].copy()
        shr = _shrink_race_sum_top3(df[["race_id", "p"]].rename(columns={"p": "p_top3"}), race_col="race_id", prob_col="p_top3", target_sum=3.0)
        sum_by_race = shr.groupby("race_id", as_index=False)["p_top3"].sum().rename(columns={"p_top3": "sum_p_top3"})
        stop_count, warn_count = _gate_counts(sum_by_race["sum_p_top3"])
        rows.append(
            {
                "calibration_method": m,
                "logloss": float(log_loss(df["y"].values, np.clip(df["p"].values, 1e-6, 1 - 1e-6))),
                "brier_score": _brier(df["y"].values, df["p"].values),
                "mean_sum_p_top3": float(sum_by_race["sum_p_top3"].mean()) if len(sum_by_race) > 0 else float("nan"),
                "probability_gate_stop_count": stop_count,
                "probability_gate_warn_count": warn_count,
                "race_count_eval": int(df["race_id"].nunique()),
            }
        )
        # curve (10 bins)
        c = df.copy()
        c["bin"] = pd.qcut(c["p"].rank(method="first"), q=min(10, max(2, int(len(c) / 50))), labels=False, duplicates="drop")
        cagg = c.groupby("bin", as_index=False).agg(count=("p", "size"), prob_mean=("p", "mean"), actual_rate=("y", "mean"))
        cagg["calibration_method"] = m
        curves.append(cagg)

    res = pd.DataFrame(rows).sort_values("calibration_method")
    curve_df = pd.concat(curves, ignore_index=True) if len(curves) > 0 else pd.DataFrame()

    lines = [
        "# top3_calibration_method_compare",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- feature_snapshot_version: {feature_snapshot_version}",
        f"- train_end_date: {train_end_date}",
        f"- valid_start_date: {valid_start_date}",
        f"- valid_end_date: {valid_end_date}",
        f"- race_meta_policy: skip (invalid races excluded in evaluation)",
        f"- invalid_race_count: {len(invalid)}",
        "",
        "## Summary",
        "| method | logloss | brier_score | mean(sum_p_top3) | gate_stop | gate_warn | eval_races |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for r in res.itertuples(index=False):
        lines.append(
            f"| {r.calibration_method} | {r.logloss:.6f} | {r.brier_score:.6f} | {r.mean_sum_p_top3:.6f} | {int(r.probability_gate_stop_count)} | {int(r.probability_gate_warn_count)} | {int(r.race_count_eval)} |"
        )

    lines += ["", "## Calibration Curve (Validation)"]
    if len(curve_df) == 0:
        lines.append("- no curve rows")
    else:
        lines.append("| method | bin | count | prob_mean | actual_rate |")
        lines.append("|---|---:|---:|---:|---:|")
        for r in curve_df.sort_values(["calibration_method", "bin"]).itertuples(index=False):
            lines.append(f"| {r.calibration_method} | {int(r.bin)} | {int(r.count)} | {float(r.prob_mean):.6f} | {float(r.actual_rate):.6f} |")

    lines += ["", "## Invalid Race IDs"]
    if len(invalid) == 0:
        lines.append("- none")
    else:
        for rid in sorted(invalid):
            lines.append(f"- {rid}")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")

    # recommendation: best logloss among methods with lower gate warns and mean sum closer to 3
    tmp = res.copy()
    tmp["sum_distance_to_3"] = (tmp["mean_sum_p_top3"] - 3.0).abs()
    tmp = tmp.sort_values(["logloss", "brier_score", "sum_distance_to_3", "probability_gate_warn_count"])
    recommended = str(tmp.iloc[0]["calibration_method"]) if len(tmp) > 0 else "isotonic"
    return {
        "report_path": str(out_md),
        "methods": res.to_dict("records"),
        "invalid_race_count": int(len(invalid)),
        "recommended_calibration_method": recommended,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="racing_ai/data/warehouse/aikeiba.duckdb")
    ap.add_argument("--feature-snapshot-version", default="fs_v1")
    ap.add_argument("--train-end-date", default="2026-03-31")
    ap.add_argument("--valid-start-date", default="2026-04-05")
    ap.add_argument("--valid-end-date", default="2026-04-10")
    ap.add_argument("--calibration-start-date", default="2026-04-03")
    ap.add_argument("--calibration-end-date", default="2026-04-04")
    ap.add_argument("--calibration-ratio", type=float, default=0.5)
    ap.add_argument("--feature-set", default="stability_plus_pace")
    ap.add_argument("--out-md", default="racing_ai/reports/top3_calibration_method_compare.md")
    args = ap.parse_args()
    cal_s = args.calibration_start_date if str(args.calibration_start_date).strip() else None
    cal_e = args.calibration_end_date if str(args.calibration_end_date).strip() else None
    res = run_compare(
        db_path=Path(args.db_path),
        feature_snapshot_version=args.feature_snapshot_version,
        train_end_date=args.train_end_date,
        valid_start_date=args.valid_start_date,
        valid_end_date=args.valid_end_date,
        calibration_start_date=cal_s,
        calibration_end_date=cal_e,
        calibration_ratio=float(args.calibration_ratio),
        feature_set=args.feature_set,
        out_md=Path(args.out_md),
    )
    print(res)


if __name__ == "__main__":
    main()
