from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from aikeiba.db.duckdb import DuckDb
from aikeiba.modeling.registry import load_model_bundle


def _shrink_race_sum_top3(
    df: pd.DataFrame,
    *,
    race_col: str = "race_id",
    prob_col: str = "p_top3",
    target_sum: float = 3.0,
) -> pd.DataFrame:
    """
    Shrink-only race-wise normalization for top3 probabilities.
    If sum(p_top3) in a race is above target_sum, scale all horses down by
    factor=(target_sum/sum). Do not inflate when sum <= target_sum.
    """
    out = df.copy()
    sums = out.groupby(race_col)[prob_col].sum(min_count=1).rename("race_sum")
    out = out.merge(sums, left_on=race_col, right_index=True, how="left")
    race_sum = out["race_sum"].to_numpy(dtype=float)
    scale = np.ones(len(out), dtype=float)
    valid = np.isfinite(race_sum) & (race_sum > target_sum)
    scale[valid] = target_sum / race_sum[valid]
    out[prob_col] = np.clip(out[prob_col].to_numpy(dtype=float) * scale, 0.0, 1.0)
    return out.drop(columns=["race_sum"])


def infer_top3_for_date(
    *,
    db: DuckDb,
    models_root: Path,
    race_date: str,
    feature_snapshot_version: str,
    model_version: str,
    odds_snapshot_version: str,
    dataset_fingerprint: str,
    excluded_race_ids: set[str] | None = None,
) -> dict[str, Any]:
    df = db.query_df(
        """
        SELECT fs.*
        FROM feature_store fs
        JOIN races r ON r.race_id = fs.race_id
        WHERE r.race_date = cast(? as DATE)
          AND fs.feature_snapshot_version = ?
        """,
        (race_date, feature_snapshot_version),
    )
    if len(df) == 0:
        raise ValueError("no feature_store rows for date/snapshot")
    if excluded_race_ids:
        df = df[~df["race_id"].astype(str).isin(set(str(x) for x in excluded_race_ids))].copy()
    if len(df) == 0:
        raise ValueError("no feature_store rows after excluded_race_ids filter")

    model, calibrator, meta = load_model_bundle(root=models_root, task="top3", model_version=model_version)
    # Guard against object dtypes (e.g., bool + null mixed as object) that LightGBM cannot consume.
    for c in getattr(model, "feature_names", []):
        if c not in df.columns:
            continue
        if str(df[c].dtype) == "object":
            df[c] = pd.to_numeric(df[c], errors="coerce")
    p_raw = model.predict(df)
    p_cal = calibrator.predict(np.asarray(p_raw))

    # Rank within race.
    df2 = df[["race_id", "horse_no"]].copy()
    df2["p_top3"] = p_cal
    # Keep race-level sanity by shrinking only when sum(p_top3) is too large.
    df2 = _shrink_race_sum_top3(df2, target_sum=3.0)
    df2["ai_rank"] = df2.groupby("race_id")["p_top3"].rank(ascending=False, method="first").astype(int)

    ts = dt.datetime.now().isoformat(timespec="seconds")
    df2["model_version"] = model_version
    df2["inference_timestamp"] = ts
    df2["p_win"] = None
    df2["ability"] = None
    df2["stability"] = None
    df2["role"] = None
    df2["feature_snapshot_version"] = feature_snapshot_version
    df2["odds_snapshot_version"] = odds_snapshot_version
    df2["dataset_fingerprint"] = dataset_fingerprint

    db.con.register("tmp_pred_rows", df2)
    db.execute(
        """
        INSERT INTO horse_predictions(
          race_id, horse_no, model_version, inference_timestamp,
          p_top3, p_win, ability, stability,
          ai_rank, role,
          feature_snapshot_version, odds_snapshot_version, dataset_fingerprint
        )
        SELECT
          race_id,
          horse_no,
          model_version,
          cast(inference_timestamp as TIMESTAMP),
          p_top3, p_win, ability, stability,
          ai_rank, role,
          feature_snapshot_version, odds_snapshot_version, dataset_fingerprint
        FROM tmp_pred_rows
        """
    )
    db.con.unregister("tmp_pred_rows")

    return {
        "race_date": race_date,
        "rows": int(len(df2)),
        "model_meta": meta,
        "inference_timestamp": ts,
    }
