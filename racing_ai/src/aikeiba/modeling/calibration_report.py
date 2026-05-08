from __future__ import annotations

import csv
import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score


@dataclass(frozen=True)
class CalibrationArtifacts:
    calibration_summary_path: Path
    calibration_bins_path: Path
    model_metrics_path: Path
    race_sum_top3_path: Path
    calibration_summary: dict[str, Any]
    model_metrics: dict[str, Any]


def _safe_auc(y_true: np.ndarray, p: np.ndarray) -> float | None:
    classes = np.unique(y_true)
    if classes.size < 2:
        return None
    return float(roc_auc_score(y_true, p))


def _ece(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> float:
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    n = len(y_true)
    if n == 0:
        return 0.0
    ece = 0.0
    for i in range(n_bins):
        lo, hi = edges[i], edges[i + 1]
        if i == n_bins - 1:
            mask = (p_pred >= lo) & (p_pred <= hi)
        else:
            mask = (p_pred >= lo) & (p_pred < hi)
        cnt = int(np.sum(mask))
        if cnt == 0:
            continue
        pred_mean = float(np.mean(p_pred[mask]))
        obs_rate = float(np.mean(y_true[mask]))
        ece += (cnt / n) * abs(pred_mean - obs_rate)
    return float(ece)


def _build_bins(
    *,
    model_version: str,
    feature_snapshot_version: str,
    split_name: str,
    y_true: np.ndarray,
    p_before: np.ndarray,
    p_after: np.ndarray,
    created_at: str,
    n_bins: int = 10,
) -> list[dict[str, Any]]:
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    rows: list[dict[str, Any]] = []
    for i in range(n_bins):
        lo = float(edges[i])
        hi = float(edges[i + 1])
        if i == n_bins - 1:
            mask = (p_before >= lo) & (p_before <= hi)
        else:
            mask = (p_before >= lo) & (p_before < hi)
        cnt = int(np.sum(mask))
        if cnt == 0:
            pred_mean_before = None
            pred_mean_after = None
            obs_rate = None
            abs_gap_before = None
            abs_gap_after = None
        else:
            pred_mean_before = float(np.mean(p_before[mask]))
            pred_mean_after = float(np.mean(p_after[mask]))
            obs_rate = float(np.mean(y_true[mask]))
            abs_gap_before = abs(pred_mean_before - obs_rate)
            abs_gap_after = abs(pred_mean_after - obs_rate)
        rows.append(
            {
                "model_version": model_version,
                "feature_snapshot_version": feature_snapshot_version,
                "split_name": split_name,
                "bin_index": i,
                "bin_lower": lo,
                "bin_upper": hi,
                "count": cnt,
                "pred_mean_before": pred_mean_before,
                "pred_mean_after": pred_mean_after,
                "obs_rate": obs_rate,
                "abs_gap_before": abs_gap_before,
                "abs_gap_after": abs_gap_after,
                "created_at": created_at,
            }
        )
    return rows


def _race_sum_stats(race_ids: np.ndarray, p_before: np.ndarray, p_after: np.ndarray) -> tuple[dict[str, float | None], list[dict[str, Any]]]:
    uniq = np.unique(race_ids)
    per_race: list[dict[str, Any]] = []
    sums_before = []
    sums_after = []
    for rid in uniq:
        mask = race_ids == rid
        sb = float(np.sum(p_before[mask]))
        sa = float(np.sum(p_after[mask]))
        sums_before.append(sb)
        sums_after.append(sa)
        per_race.append({"race_id": str(rid), "sum_top3_before": sb, "sum_top3_after": sa})

    stats = {
        "race_sum_top3_mean_before": float(np.mean(sums_before)) if sums_before else None,
        "race_sum_top3_mean_after": float(np.mean(sums_after)) if sums_after else None,
        "race_sum_top3_std_before": float(np.std(sums_before, ddof=1)) if len(sums_before) > 1 else 0.0 if len(sums_before) == 1 else None,
        "race_sum_top3_std_after": float(np.std(sums_after, ddof=1)) if len(sums_after) > 1 else 0.0 if len(sums_after) == 1 else None,
    }
    return stats, per_race


def build_and_save_calibration_reports(
    *,
    out_dir: Path,
    model_version: str,
    feature_snapshot_version: str,
    train_period: str,
    valid_period: str,
    test_period: str | None,
    y_valid: np.ndarray,
    p_before: np.ndarray,
    p_after: np.ndarray,
    race_id_valid: np.ndarray,
    calibration_method: str,
    sample_count_train: int,
    sample_count_valid: int,
    positive_rate_train: float,
    positive_rate_valid: float,
    dataset_fingerprint: str | None = None,
    feature_set: str | None = None,
    n_bins: int = 10,
) -> CalibrationArtifacts:
    out_dir.mkdir(parents=True, exist_ok=True)
    created_at = dt.datetime.now().isoformat(timespec="seconds")

    # Safety checks
    if len(y_valid) == 0:
        raise ValueError("valid set is empty for calibration report")
    if np.any(~np.isfinite(p_before)) or np.any(~np.isfinite(p_after)):
        raise ValueError("prediction array has NaN/Inf for calibration report")

    p_before_clip = np.clip(p_before, 1e-8, 1.0 - 1e-8)
    p_after_clip = np.clip(p_after, 1e-8, 1.0 - 1e-8)

    brier_before = float(brier_score_loss(y_valid, p_before_clip))
    brier_after = float(brier_score_loss(y_valid, p_after_clip))
    logloss_before = float(log_loss(y_valid, p_before_clip, labels=[0, 1]))
    logloss_after = float(log_loss(y_valid, p_after_clip, labels=[0, 1]))
    ece_before = _ece(y_valid, p_before_clip, n_bins=n_bins)
    ece_after = _ece(y_valid, p_after_clip, n_bins=n_bins)
    auc_before = _safe_auc(y_valid, p_before_clip)
    auc_after = _safe_auc(y_valid, p_after_clip)

    race_stats, race_rows = _race_sum_stats(race_id_valid, p_before_clip, p_after_clip)
    bins_rows = _build_bins(
        model_version=model_version,
        feature_snapshot_version=feature_snapshot_version,
        split_name="valid",
        y_true=y_valid,
        p_before=p_before_clip,
        p_after=p_after_clip,
        created_at=created_at,
        n_bins=n_bins,
    )

    calibration_summary = {
        "model_version": model_version,
        "feature_snapshot_version": feature_snapshot_version,
        "train_period": train_period,
        "valid_period": valid_period,
        "test_period": test_period,
        "calibration_method": calibration_method,
        "brier_before": brier_before,
        "brier_after": brier_after,
        "ece_before": ece_before,
        "ece_after": ece_after,
        "logloss_before": logloss_before,
        "logloss_after": logloss_after,
        **race_stats,
        "dataset_fingerprint": dataset_fingerprint,
        "feature_set": feature_set,
        "created_at": created_at,
    }

    model_metrics = {
        "model_version": model_version,
        "feature_snapshot_version": feature_snapshot_version,
        "train_period": train_period,
        "valid_period": valid_period,
        "test_period": test_period,
        "sample_count_train": int(sample_count_train),
        "sample_count_valid": int(sample_count_valid),
        "positive_rate_train": float(positive_rate_train),
        "positive_rate_valid": float(positive_rate_valid),
        "dataset_fingerprint": dataset_fingerprint,
        "feature_set": feature_set,
        "auc_before": auc_before,
        "auc_after": auc_after,
        "logloss_before": logloss_before,
        "logloss_after": logloss_after,
        "brier_before": brier_before,
        "brier_after": brier_after,
        "calibration_method": calibration_method,
        "created_at": created_at,
    }

    calibration_summary_path = out_dir / "calibration_summary.json"
    calibration_bins_path = out_dir / "calibration_bins.csv"
    model_metrics_path = out_dir / "model_metrics.json"
    race_sum_top3_path = out_dir / "race_sum_top3.csv"

    calibration_summary_path.write_text(json.dumps(calibration_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    model_metrics_path.write_text(json.dumps(model_metrics, ensure_ascii=False, indent=2), encoding="utf-8")

    with calibration_bins_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "model_version",
                "feature_snapshot_version",
                "split_name",
                "bin_index",
                "bin_lower",
                "bin_upper",
                "count",
                "pred_mean_before",
                "pred_mean_after",
                "obs_rate",
                "abs_gap_before",
                "abs_gap_after",
                "created_at",
            ],
        )
        writer.writeheader()
        for row in bins_rows:
            writer.writerow(row)

    with race_sum_top3_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["race_id", "sum_top3_before", "sum_top3_after"])
        writer.writeheader()
        for row in race_rows:
            writer.writerow(row)

    return CalibrationArtifacts(
        calibration_summary_path=calibration_summary_path,
        calibration_bins_path=calibration_bins_path,
        model_metrics_path=model_metrics_path,
        race_sum_top3_path=race_sum_top3_path,
        calibration_summary=calibration_summary,
        model_metrics=model_metrics,
    )
