from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import joblib

from aikeiba.modeling.calibration import Calibrator
from aikeiba.modeling.lgbm import LgbmModel


@dataclass(frozen=True)
class ModelMeta:
    task: str
    model_version: str
    feature_snapshot_version: str
    train_end_date: str
    valid_start_date: str
    valid_end_date: str
    dataset_fingerprint: str
    created_at: str
    notes: str | None = None


def model_dir(root: Path, task: str, model_version: str) -> Path:
    return root / task / model_version


def save_model_bundle(
    *,
    root: Path,
    task: str,
    model_version: str,
    model: LgbmModel,
    calibrator: Calibrator,
    meta: ModelMeta,
) -> Path:
    out = model_dir(root, task, model_version)
    out.mkdir(parents=True, exist_ok=True)

    # Save booster
    model.booster.save_model(str(out / "model.txt"))
    # Save feature names
    (out / "features.json").write_text(json.dumps(model.feature_names, ensure_ascii=True, indent=2), encoding="utf-8")
    # Save calibrator
    joblib.dump(calibrator, out / "calibrator.joblib")
    # Save meta
    (out / "meta.json").write_text(json.dumps(asdict(meta), ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def load_model_bundle(*, root: Path, task: str, model_version: str) -> tuple[LgbmModel, Calibrator, dict[str, Any]]:
    out = model_dir(root, task, model_version)
    if not out.exists():
        raise FileNotFoundError(f"model bundle not found: {out}")

    import lightgbm as lgb

    booster = lgb.Booster(model_file=str(out / "model.txt"))
    feature_names = json.loads((out / "features.json").read_text(encoding="utf-8"))
    calibrator: Calibrator = joblib.load(out / "calibrator.joblib")
    meta = json.loads((out / "meta.json").read_text(encoding="utf-8"))
    return LgbmModel(booster=booster, feature_names=feature_names), calibrator, meta
