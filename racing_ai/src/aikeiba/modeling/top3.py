from __future__ import annotations

import datetime as dt
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np

from aikeiba.common.hashing import stable_fingerprint
from aikeiba.db.duckdb import DuckDb
from aikeiba.modeling.calibration import fit_isotonic
from aikeiba.modeling.datasets import load_top3_dataset
from aikeiba.modeling.lgbm import train_binary_lgbm
from aikeiba.modeling.registry import ModelMeta, save_model_bundle


def train_top3_bundle(
    *,
    db: DuckDb,
    models_root: Path,
    model_version: str,
    feature_snapshot_version: str,
    train_end_date: str,
    valid_start_date: str,
    valid_end_date: str,
    seed: int = 42,
) -> dict[str, Any]:
    split = load_top3_dataset(
        db=db,
        feature_snapshot_version=feature_snapshot_version,
        train_end_date=train_end_date,
        valid_start_date=valid_start_date,
        valid_end_date=valid_end_date,
    )

    model = train_binary_lgbm(
        X_train=split.X_train,
        y_train=split.y_train,
        X_valid=split.X_valid,
        y_valid=split.y_valid,
        seed=seed,
    )

    p_raw = model.predict(split.X_valid)
    calibrator = fit_isotonic(np.asarray(p_raw), np.asarray(split.y_valid))

    dataset_fp = stable_fingerprint(
        {
            "feature_snapshot_version": feature_snapshot_version,
            "train_end_date": train_end_date,
            "valid_start_date": valid_start_date,
            "valid_end_date": valid_end_date,
            "n_train": int(len(split.X_train)),
            "n_valid": int(len(split.X_valid)),
        }
    )

    meta = ModelMeta(
        task="top3",
        model_version=model_version,
        feature_snapshot_version=feature_snapshot_version,
        train_end_date=train_end_date,
        valid_start_date=valid_start_date,
        valid_end_date=valid_end_date,
        dataset_fingerprint=dataset_fp,
        created_at=dt.datetime.now().isoformat(timespec="seconds"),
    )

    out_dir = save_model_bundle(
        root=models_root,
        task="top3",
        model_version=model_version,
        model=model,
        calibrator=calibrator,
        meta=meta,
    )

    return {"model_dir": str(out_dir), "meta": asdict(meta)}
