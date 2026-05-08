from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from aikeiba.db.duckdb import DuckDb


@dataclass(frozen=True)
class DatasetSplit:
    X_train: pd.DataFrame
    y_train: pd.Series
    X_calibration: pd.DataFrame
    y_calibration: pd.Series
    X_valid: pd.DataFrame
    y_valid: pd.Series
    race_id_train: pd.Series
    race_id_calibration: pd.Series
    race_id_valid: pd.Series
    race_date_train_min: str
    race_date_train_max: str
    race_date_calibration_min: str
    race_date_calibration_max: str
    race_date_valid_min: str
    race_date_valid_max: str
    feature_names: list[str]


def load_top3_dataset(
    *,
    db: DuckDb,
    feature_snapshot_version: str,
    train_end_date: str,
    valid_start_date: str,
    valid_end_date: str,
    calibration_start_date: str | None = None,
    calibration_end_date: str | None = None,
    calibration_ratio: float = 0.5,
    include_stability_features: bool = True,
    include_pace_features: bool = False,
) -> DatasetSplit:
    """
    Build dataset for is_top3 target using feature_store joined with results.
    Time-series strict by date ranges.
    """
    df = db.query_df(
        """
        SELECT
          fs.*,
          CASE WHEN res.finish_position BETWEEN 1 AND 3 THEN 1 ELSE 0 END AS is_top3
        FROM feature_store fs
        JOIN races r ON r.race_id = fs.race_id
        LEFT JOIN results res ON res.race_id = fs.race_id AND res.horse_no = fs.horse_no
        WHERE fs.feature_snapshot_version = ?
          AND r.race_date <= cast(? as DATE)
        """,
        # Need to load rows for both train and validation ranges.
        (feature_snapshot_version, valid_end_date),
    )

    if len(df) == 0:
        raise ValueError("no training rows (feature_store/results missing)")

    # Split
    df["race_date"] = pd.to_datetime(df["race_date"])
    # Keep training strictly <= train_end_date, and strictly before the validation window.
    # This prevents accidentally pulling very old rows (where results may be missing) into train.
    train_end = pd.to_datetime(train_end_date)
    valid_start = pd.to_datetime(valid_start_date)
    valid_end = pd.to_datetime(valid_end_date)

    train_mask = (df["race_date"] <= train_end) & (df["race_date"] < valid_start)
    holdout_mask = (df["race_date"] >= valid_start) & (df["race_date"] <= valid_end)

    if calibration_start_date and calibration_end_date:
        cal_start = pd.to_datetime(calibration_start_date)
        cal_end = pd.to_datetime(calibration_end_date)
        calib_mask = (df["race_date"] >= cal_start) & (df["race_date"] <= cal_end)
        valid_mask = holdout_mask & (df["race_date"] > cal_end)
    else:
        holdout_dates = sorted(df.loc[holdout_mask, "race_date"].dropna().dt.normalize().unique().tolist())
        if len(holdout_dates) <= 1:
            calib_mask = holdout_mask
            valid_mask = holdout_mask
        else:
            split_idx = int(len(holdout_dates) * calibration_ratio)
            split_idx = min(max(split_idx, 1), len(holdout_dates) - 1)
            cal_end_date = pd.to_datetime(holdout_dates[split_idx - 1])
            calib_mask = holdout_mask & (df["race_date"] <= cal_end_date)
            valid_mask = holdout_mask & (df["race_date"] > cal_end_date)

    base_feature_cols = [
        # baseline feature set
        "field_size",
        "prev_last3f_rank",
        "avg_last3f_rank_3",
        "best_last3f_count",
        "prev_margin",
        "avg_margin_3",
        "margin_std_3",
        "prev_corner4_pos",
        "avg_corner4_pos_3",
        "dist_change",
        "finish_pos_std_5",
        "big_loss_count_10",
        "itb_rate_10",
        "waku",
        "horse_no_rel",
        "jockey_top3_rate_1y",
    ]

    stability_feature_cols = [
        "finish_pos_std_last5",
        "finish_pos_std_last10",
        "margin_std_last5",
        "margin_std_last10",
        "top3_rate_last5",
        "top3_rate_last10",
        "board_rate_last5",
        "board_rate_last10",
        "big_loss_rate_last5",
        "big_loss_rate_last10",
        "worst_finish_last5",
        "worst_finish_last10",
        "top3_rate_same_distance_bucket",
        "top3_rate_same_course",
        "finish_pos_std_same_course",
        "margin_std_same_course",
        "consecutive_bad_runs",
        "consecutive_top3_runs",
        "avg_finish_pos_last5",
        "avg_margin_last5",
    ]
    pace_feature_cols = [
        "avg_corner4_pos_last5",
        "corner4_pos_std_last5",
        "front_runner_rate_last5",
        "closer_rate_last5",
        "avg_last3f_rank_last5",
        "pace_finish_delta_last5",
    ]
    feature_cols = list(base_feature_cols)
    if include_stability_features:
        feature_cols.extend(stability_feature_cols)
    if include_pace_features:
        feature_cols.extend(pace_feature_cols)

    # Booleans -> 0/1
    bool_cols = ["course_change", "surface_change"]
    if include_stability_features:
        bool_cols.append("min_history_flag")
    if include_pace_features:
        bool_cols.append("pace_min_history_flag")
    for b in bool_cols:
        if b in df.columns:
            df[b] = df[b].fillna(False).astype(int)
            feature_cols.append(b)

    X = df[feature_cols].copy()
    y = df["is_top3"].astype(int)

    X_train = X[train_mask].reset_index(drop=True)
    y_train = y[train_mask].reset_index(drop=True)
    X_calibration = X[calib_mask].reset_index(drop=True)
    y_calibration = y[calib_mask].reset_index(drop=True)
    X_valid = X[valid_mask].reset_index(drop=True)
    y_valid = y[valid_mask].reset_index(drop=True)
    race_id_train = df.loc[train_mask, "race_id"].reset_index(drop=True)
    race_id_calibration = df.loc[calib_mask, "race_id"].reset_index(drop=True)
    race_id_valid = df.loc[valid_mask, "race_id"].reset_index(drop=True)

    if len(X_calibration) == 0:
        raise ValueError("no calibration rows (check date ranges)")
    if len(X_valid) == 0:
        raise ValueError("no validation rows (check valid/calibration split)")
    if len(X_train) == 0:
        raise ValueError("no training rows after split (check date ranges)")

    train_dates = df.loc[train_mask, "race_date"]
    calibration_dates = df.loc[calib_mask, "race_date"]
    valid_dates = df.loc[valid_mask, "race_date"]

    # LightGBM handles NaN natively; keep them.
    return DatasetSplit(
        X_train=X_train,
        y_train=y_train,
        X_calibration=X_calibration,
        y_calibration=y_calibration,
        X_valid=X_valid,
        y_valid=y_valid,
        race_id_train=race_id_train,
        race_id_calibration=race_id_calibration,
        race_id_valid=race_id_valid,
        race_date_train_min=str(train_dates.min().date()),
        race_date_train_max=str(train_dates.max().date()),
        race_date_calibration_min=str(calibration_dates.min().date()),
        race_date_calibration_max=str(calibration_dates.max().date()),
        race_date_valid_min=str(valid_dates.min().date()),
        race_date_valid_max=str(valid_dates.max().date()),
        feature_names=feature_cols,
    )
