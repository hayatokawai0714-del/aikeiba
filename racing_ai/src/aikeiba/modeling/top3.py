from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np

from aikeiba.common.hashing import stable_fingerprint
from aikeiba.db.duckdb import DuckDb
from aikeiba.modeling.calibration import fit_isotonic, fit_none, fit_sigmoid
from aikeiba.modeling.calibration_report import build_and_save_calibration_reports
from aikeiba.modeling.datasets import load_top3_dataset
from aikeiba.modeling.feature_importance import build_and_save_feature_importance_reports
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
    calibration_start_date: str | None = None,
    calibration_end_date: str | None = None,
    calibration_ratio: float = 0.5,
    split_config_path: str | None = None,
    split_config_hash: str | None = None,
    test_period: str | None = None,
    feature_set: str = "stability",
    calibration_method: str = "isotonic",
    seed: int = 42,
) -> dict[str, Any]:
    if feature_set not in {"baseline", "stability", "stability_plus_pace"}:
        raise ValueError(f"feature_set must be baseline|stability|stability_plus_pace: {feature_set}")
    include_stability_features = feature_set in {"stability", "stability_plus_pace"}
    include_pace_features = feature_set == "stability_plus_pace"
    split = load_top3_dataset(
        db=db,
        feature_snapshot_version=feature_snapshot_version,
        train_end_date=train_end_date,
        valid_start_date=valid_start_date,
        valid_end_date=valid_end_date,
        calibration_start_date=calibration_start_date,
        calibration_end_date=calibration_end_date,
        calibration_ratio=calibration_ratio,
        include_stability_features=include_stability_features,
        include_pace_features=include_pace_features,
    )

    model = train_binary_lgbm(
        X_train=split.X_train,
        y_train=split.y_train,
        X_valid=split.X_calibration,
        y_valid=split.y_calibration,
        seed=seed,
    )

    p_raw_cal = np.asarray(model.predict(split.X_calibration), dtype=float)
    y_cal = np.asarray(split.y_calibration, dtype=int)
    if np.any(~np.isfinite(p_raw_cal)):
        raise ValueError("raw calibration prediction contains NaN/Inf")

    cal_method = str(calibration_method or "isotonic").strip().lower()
    if cal_method == "isotonic":
        calibrator = fit_isotonic(p_raw_cal, y_cal)
    elif cal_method in {"sigmoid", "platt"}:
        calibrator = fit_sigmoid(p_raw_cal, y_cal)
    elif cal_method == "none":
        calibrator = fit_none()
    else:
        raise ValueError(f"calibration_method must be isotonic|sigmoid|none: {calibration_method}")

    p_raw_valid = np.asarray(model.predict(split.X_valid), dtype=float)
    y_valid = np.asarray(split.y_valid, dtype=int)
    if np.any(~np.isfinite(p_raw_valid)):
        raise ValueError("raw validation prediction contains NaN/Inf")
    p_cal_valid = np.asarray(calibrator.predict(p_raw_valid), dtype=float)
    if np.any(~np.isfinite(p_cal_valid)):
        raise ValueError("calibrated validation prediction contains NaN/Inf")

    train_period = f"{split.race_date_train_min}..{split.race_date_train_max}"
    valid_period = f"{split.race_date_valid_min}..{split.race_date_valid_max}"
    test_period = test_period or valid_period
    uniq_race_ids = sorted(set(list(split.race_id_train.astype(str)) + list(split.race_id_valid.astype(str))))
    dataset_fp = stable_fingerprint(
        {
            "task_name": "top3",
            "feature_snapshot_version": feature_snapshot_version,
            "train_period": train_period,
            "valid_period": valid_period,
            "test_period": test_period,
            "filters": {},
            "excluded_rules": [],
            "race_ids": uniq_race_ids,
            "race_count": len(uniq_race_ids),
            "row_count": int(len(split.X_train) + len(split.X_valid)),
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
        notes=f"feature_set={feature_set}",
    )

    out_dir = save_model_bundle(
        root=models_root,
        task="top3",
        model_version=model_version,
        model=model,
        calibrator=calibrator,
        meta=meta,
    )

    report = build_and_save_calibration_reports(
        out_dir=out_dir,
        model_version=model_version,
        feature_snapshot_version=feature_snapshot_version,
        train_period=train_period,
        valid_period=valid_period,
        test_period=test_period,
        y_valid=y_valid,
        p_before=p_raw_valid,
        p_after=p_cal_valid,
        race_id_valid=np.asarray(split.race_id_valid, dtype=str),
        calibration_method=calibrator.method,
        sample_count_train=len(split.X_train),
        sample_count_valid=len(split.X_valid),
        positive_rate_train=float(np.mean(np.asarray(split.y_train, dtype=float))),
        positive_rate_valid=float(np.mean(y_valid.astype(float))),
        dataset_fingerprint=dataset_fp,
        feature_set=feature_set,
    )
    fi = build_and_save_feature_importance_reports(
        out_dir=out_dir,
        model=model,
        model_version=model_version,
        feature_snapshot_version=feature_snapshot_version,
    )
    meta_warnings: list[str] = []
    if split.race_date_train_min is None:
        meta_warnings.append("meta_missing_train_start_date")
    if split.race_date_train_max is None:
        meta_warnings.append("meta_missing_train_end_date")
    if split.race_date_calibration_min is None:
        meta_warnings.append("meta_missing_calibration_start_date")
    if split.race_date_calibration_max is None:
        meta_warnings.append("meta_missing_calibration_end_date")
    if split.race_date_valid_min is None:
        meta_warnings.append("meta_missing_validation_start_date")
    if split.race_date_valid_max is None:
        meta_warnings.append("meta_missing_validation_end_date")

    # Formal training meta schema for leakage/ops checks.
    formal_meta = {
        "model_version": model_version,
        "feature_set_version": feature_snapshot_version,
        "train_start_date": split.race_date_train_min if split.race_date_train_min else None,
        "train_end_date": split.race_date_train_max if split.race_date_train_max else None,
        "calibration_start_date": split.race_date_calibration_min if split.race_date_calibration_min else None,
        "calibration_end_date": split.race_date_calibration_max if split.race_date_calibration_max else None,
        "validation_start_date": split.race_date_valid_min if split.race_date_valid_min else None,
        "validation_end_date": split.race_date_valid_max if split.race_date_valid_max else None,
        "model_created_at": meta.created_at,
        "target": "is_top3",
        "objective": "binary",
        "calibration_method": calibrator.method,
        "source_table": "feature_store + results",
        "row_count_train": int(len(split.X_train)),
        "row_count_calibration": int(len(split.X_calibration)),
        "row_count_validation": int(len(split.X_valid)),
        "meta_warnings": meta_warnings,
        "feature_set": feature_set,
        "dataset_fingerprint": dataset_fp,
        "split_config_path": split_config_path,
        "split_config_hash": split_config_hash,
    }
    # Keep backward-compatible keys for loader users.
    formal_meta.update(asdict(meta))
    (out_dir / "meta.json").write_text(json.dumps(formal_meta, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "model_dir": str(out_dir),
        "meta": asdict(meta),
        "report_files": {
            "calibration_summary_json": str(report.calibration_summary_path),
            "calibration_bins_csv": str(report.calibration_bins_path),
            "model_metrics_json": str(report.model_metrics_path),
            "race_sum_top3_csv": str(report.race_sum_top3_path),
            "feature_importance_csv": fi["feature_importance_csv"],
            "feature_importance_summary_json": fi["feature_importance_summary_json"],
        },
        "calibration_summary": report.calibration_summary,
        "model_metrics": report.model_metrics,
        "feature_importance_summary": fi["feature_importance_summary"],
        "feature_set": feature_set,
        "meta_warnings": meta_warnings,
    }
