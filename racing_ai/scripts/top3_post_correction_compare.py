from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss

from aikeiba.db.duckdb import DuckDb
from aikeiba.modeling.calibration import fit_isotonic
from aikeiba.modeling.datasets import load_top3_dataset
from aikeiba.modeling.lgbm import train_binary_lgbm
from aikeiba.modeling.post_correction import apply_post_correction


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


def _field_sizes(db: DuckDb, valid_start_date: str, valid_end_date: str) -> dict[str, int]:
    rows = db.query_df(
        """
        SELECT e.race_id, count(*) AS field_size
        FROM entries e
        JOIN races r ON r.race_id=e.race_id
        WHERE r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
          AND (e.is_scratched IS NULL OR e.is_scratched=FALSE)
        GROUP BY e.race_id
        """,
        (valid_start_date, valid_end_date),
    ).to_dict("records")
    return {str(r["race_id"]): int(r["field_size"]) for r in rows}


def _decile_table(df: pd.DataFrame, prob_col: str) -> pd.DataFrame:
    work = df.copy()
    work["decile"] = pd.qcut(work[prob_col].rank(method="first"), q=min(10, max(2, int(len(work) / 50))), labels=False, duplicates="drop")
    return (
        work.groupby("decile", as_index=False)
        .agg(count=(prob_col, "size"), prob_mean=(prob_col, "mean"), hit_rate=("y", "mean"))
        .sort_values("decile")
    )


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
    clip_max: float,
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
    cal = fit_isotonic(p_raw_cal, y_cal)

    p_valid_cal = np.asarray(cal.predict(np.asarray(model.predict(split.X_valid), dtype=float)), dtype=float)
    y_valid = np.asarray(split.y_valid, dtype=int)
    race_id_valid = np.asarray(split.race_id_valid, dtype=str)

    invalid = _invalid_races(db, valid_start_date, valid_end_date)
    fs_map = _field_sizes(db, valid_start_date, valid_end_date)

    df = pd.DataFrame({"race_id": race_id_valid, "p_cal": p_valid_cal, "y": y_valid})
    df["field_size"] = df["race_id"].map(fs_map)
    df = df[~df["race_id"].isin(invalid)].copy()

    methods = ["none", "current_shrink_only", "scale_to_3", "scale_to_expected_top3_clip"]
    res_rows = []
    deciles = []
    for m in methods:
        corrected = apply_post_correction(
            df,
            race_col="race_id",
            prob_col="p_cal",
            method=m,
            field_size_col="field_size",
            clip_max=clip_max,
        ).rename(columns={"p_cal": "p"})
        sums = corrected.groupby("race_id", as_index=False)["p"].sum().rename(columns={"p": "sum_p_top3"})
        stop_count, warn_count = _gate_counts(sums["sum_p_top3"])
        res_rows.append(
            {
                "post_correction": m,
                "logloss": float(log_loss(corrected["y"].values, np.clip(corrected["p"].values, 1e-6, 1 - 1e-6))),
                "brier_score": _brier(corrected["y"].values, corrected["p"].values),
                "mean_sum_p_top3": float(sums["sum_p_top3"].mean()) if len(sums) > 0 else float("nan"),
                "probability_gate_stop_count": stop_count,
                "probability_gate_warn_count": warn_count,
                "eval_races": int(corrected["race_id"].nunique()),
            }
        )
        d = _decile_table(corrected, "p")
        d["post_correction"] = m
        deciles.append(d)

    res = pd.DataFrame(res_rows)
    decile_df = pd.concat(deciles, ignore_index=True) if len(deciles) > 0 else pd.DataFrame()

    lines = [
        "# top3_post_correction_compare",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- base_calibration_method: isotonic",
        f"- feature_snapshot_version: {feature_snapshot_version}",
        f"- valid_period: {valid_start_date}..{valid_end_date}",
        f"- race_meta_policy: skip",
        f"- invalid_race_count: {len(invalid)}",
        f"- clip_max: {clip_max}",
        "",
        "## Summary",
        "| method | logloss | brier_score | mean(sum_p_top3) | gate_stop | gate_warn | eval_races |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for r in res.sort_values("post_correction").itertuples(index=False):
        lines.append(
            f"| {r.post_correction} | {r.logloss:.6f} | {r.brier_score:.6f} | {r.mean_sum_p_top3:.6f} | {int(r.probability_gate_stop_count)} | {int(r.probability_gate_warn_count)} | {int(r.eval_races)} |"
        )

    lines += ["", "## Hit Rate by Decile"]
    if len(decile_df) == 0:
        lines.append("- no rows")
    else:
        lines.append("| method | decile | count | prob_mean | hit_rate |")
        lines.append("|---|---:|---:|---:|---:|")
        for r in decile_df.sort_values(["post_correction", "decile"]).itertuples(index=False):
            lines.append(f"| {r.post_correction} | {int(r.decile)} | {int(r.count)} | {float(r.prob_mean):.6f} | {float(r.hit_rate):.6f} |")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")

    # lightweight recommendation: prioritize logloss/brier then gate_warn then sum distance to 3
    tmp = res.copy()
    tmp["sum_dist_3"] = (tmp["mean_sum_p_top3"] - 3.0).abs()
    tmp = tmp.sort_values(["logloss", "brier_score", "probability_gate_warn_count", "sum_dist_3"])
    recommended = str(tmp.iloc[0]["post_correction"]) if len(tmp) > 0 else "current_shrink_only"
    return {
        "report_path": str(out_md),
        "results": res.sort_values("post_correction").to_dict("records"),
        "invalid_race_count": int(len(invalid)),
        "recommended_post_correction": recommended,
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
    ap.add_argument("--clip-max", type=float, default=0.95)
    ap.add_argument("--out-md", default="racing_ai/reports/top3_post_correction_compare.md")
    args = ap.parse_args()
    res = run_compare(
        db_path=Path(args.db_path),
        feature_snapshot_version=args.feature_snapshot_version,
        train_end_date=args.train_end_date,
        valid_start_date=args.valid_start_date,
        valid_end_date=args.valid_end_date,
        calibration_start_date=args.calibration_start_date if str(args.calibration_start_date).strip() else None,
        calibration_end_date=args.calibration_end_date if str(args.calibration_end_date).strip() else None,
        calibration_ratio=float(args.calibration_ratio),
        feature_set=args.feature_set,
        clip_max=float(args.clip_max),
        out_md=Path(args.out_md),
    )
    print(res)


if __name__ == "__main__":
    main()
