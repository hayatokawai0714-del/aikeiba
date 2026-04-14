from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from aikeiba.db.duckdb import DuckDb


@dataclass(frozen=True)
class DatasetSplit:
    X_train: pd.DataFrame
    y_train: pd.Series
    X_valid: pd.DataFrame
    y_valid: pd.Series
    feature_names: list[str]


def load_top3_dataset(
    *,
    db: DuckDb,
    feature_snapshot_version: str,
    train_end_date: str,
    valid_start_date: str,
    valid_end_date: str,
) -> DatasetSplit:
    """
    Build dataset for is_top3 target using feature_store joined with results.
    Time-series strict by date ranges.
    """
    df = db.query_df(
        """
        SELECT
          fs.*,
          CASE WHEN res.finish_position IS NOT NULL AND res.finish_position <= 3 THEN 1 ELSE 0 END AS is_top3
        FROM feature_store fs
        JOIN races r ON r.race_id = fs.race_id
        LEFT JOIN results res ON res.race_id = fs.race_id AND res.horse_no = fs.horse_no
        WHERE fs.feature_snapshot_version = ?
          AND r.race_date <= cast(? as DATE)
        """,
        (feature_snapshot_version, train_end_date),
    )

    if len(df) == 0:
        raise ValueError("no training rows (feature_store/results missing)")

    # Split
    df["race_date"] = pd.to_datetime(df["race_date"])
    train_mask = df["race_date"] < pd.to_datetime(valid_start_date)
    valid_mask = (df["race_date"] >= pd.to_datetime(valid_start_date)) & (df["race_date"] <= pd.to_datetime(valid_end_date))

    feature_cols = [
        # numeric/categorical-feeling features (keep minimal MVP)
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

    # Booleans -> 0/1
    for b in ["course_change", "surface_change"]:
        if b in df.columns:
            df[b] = df[b].fillna(False).astype(int)
            feature_cols.append(b)

    X = df[feature_cols].copy()
    y = df["is_top3"].astype(int)

    X_train = X[train_mask].reset_index(drop=True)
    y_train = y[train_mask].reset_index(drop=True)
    X_valid = X[valid_mask].reset_index(drop=True)
    y_valid = y[valid_mask].reset_index(drop=True)

    if len(X_valid) == 0:
        raise ValueError("no validation rows (check valid date range)")

    # LightGBM handles NaN natively; keep them.
    return DatasetSplit(
        X_train=X_train,
        y_train=y_train,
        X_valid=X_valid,
        y_valid=y_valid,
        feature_names=feature_cols,
    )
