from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any

from aikeiba.common.run_summary import normalize_run_summary, validate_run_summary


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _atomic_write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    tmp.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def _safe_delta(after: Any, before: Any) -> float | None:
    if after is None or before is None:
        return None
    return float(after) - float(before)


def _collect_experiment_row(
    *,
    manifest: dict[str, Any],
    experiment_name: str,
    model_dir: Path,
    run_summary_path: Path | None,
) -> tuple[dict[str, Any], list[str], list[str]]:
    reasons: list[str] = []
    missing_inputs: list[str] = []
    created_at = dt.datetime.now().isoformat(timespec="seconds")

    metrics_path = model_dir / "model_metrics.json"
    calibration_path = model_dir / "calibration_summary.json"
    feature_importance_summary_path = model_dir / "feature_importance_summary.json"

    metrics: dict[str, Any] = {}
    if metrics_path.exists():
        metrics = _read_json(metrics_path)
    else:
        reasons.append("model_metrics_missing")
        missing_inputs.append("model_metrics")

    calibration: dict[str, Any] = {}
    if calibration_path.exists():
        calibration = _read_json(calibration_path)
    else:
        missing_inputs.append("calibration_summary")

    run_summary = None
    run_summary_exists = False
    if run_summary_path is not None and run_summary_path.exists():
        run_summary = _read_json(run_summary_path)
        run_summary_exists = True
    else:
        candidate = model_dir / "run_summary.json"
        if candidate.exists():
            run_summary = _read_json(candidate)
            run_summary_exists = True
    if not run_summary_exists:
        missing_inputs.append("run_summary")
    else:
        run_summary = normalize_run_summary(run_summary)
        run_summary_validation = validate_run_summary(run_summary, strict=True)
        if run_summary_validation["errors"]:
            missing_inputs.append("run_summary_invalid")
            for reason in run_summary_validation["errors"][:5]:
                missing_inputs.append(f"run_summary_invalid:{reason}")
            run_summary = None
        else:
            if run_summary_validation["warnings"]:
                missing_inputs.append("run_summary_schema_warn")
                for reason in run_summary_validation["warnings"][:3]:
                    missing_inputs.append(f"run_summary_schema_warn:{reason}")

    manifest_fp = manifest.get("dataset_fingerprint")
    exp_fp = metrics.get("dataset_fingerprint")
    if exp_fp != manifest_fp:
        reasons.append("dataset_fingerprint_mismatch")

    for key in ["train_period", "valid_period", "test_period", "feature_snapshot_version"]:
        manifest_val = manifest.get(key)
        exp_val = metrics.get(key)
        if exp_val is None:
            reasons.append(f"{key}_missing_in_metrics")
        elif manifest_val != exp_val:
            reasons.append(f"{key}_mismatch")

    for key in ["race_count", "row_count"]:
        if key in metrics and metrics.get(key) != manifest.get(key):
            reasons.append(f"{key}_mismatch")

    calibration_method = (
        calibration.get("calibration_method")
        or metrics.get("calibration_method")
    )

    if run_summary is not None:
        metrics_model_version = metrics.get("model_version")
        run_model_version = run_summary.get("model_version")
        if metrics_model_version and run_model_version and str(metrics_model_version) != str(run_model_version):
            missing_inputs.append("run_summary_model_version_mismatch")
            run_summary = None

    logloss_before = calibration.get("logloss_before", metrics.get("logloss_before"))
    logloss_after = calibration.get("logloss_after", metrics.get("logloss_after", metrics.get("logloss")))
    brier_before = calibration.get("brier_before", metrics.get("brier_before"))
    brier_after = calibration.get("brier_after", metrics.get("brier_after", metrics.get("brier")))
    ece_before = calibration.get("ece_before")
    ece_after = calibration.get("ece_after")

    roi_by_popularity_band = (run_summary or {}).get("roi_by_popularity_band")
    roi_by_odds_band = (run_summary or {}).get("roi_by_odds_band")
    missing_inputs = sorted(set(missing_inputs))

    row = {
        "experiment_name": experiment_name,
        "task_name": manifest.get("task_name"),
        "model_version": metrics.get("model_version"),
        "feature_set": metrics.get("feature_set"),
        "feature_snapshot_version": metrics.get("feature_snapshot_version"),
        "train_period": metrics.get("train_period"),
        "valid_period": metrics.get("valid_period"),
        "test_period": metrics.get("test_period", manifest.get("test_period")),
        "race_count": manifest.get("race_count"),
        "row_count": manifest.get("row_count"),
        "dataset_fingerprint": exp_fp,
        "logloss": logloss_after,
        "brier": brier_after,
        "calibration_method": calibration_method,
        "logloss_before": logloss_before,
        "logloss_after": logloss_after,
        "logloss_delta": _safe_delta(logloss_after, logloss_before),
        "brier_before": brier_before,
        "brier_after": brier_after,
        "brier_delta": _safe_delta(brier_after, brier_before),
        "ece_before": ece_before,
        "ece_after": ece_after,
        "ece_delta": _safe_delta(ece_after, ece_before),
        "race_sum_top3_mean_before": calibration.get("race_sum_top3_mean_before"),
        "race_sum_top3_mean_after": calibration.get("race_sum_top3_mean_after"),
        "race_sum_top3_std_before": calibration.get("race_sum_top3_std_before"),
        "race_sum_top3_std_after": calibration.get("race_sum_top3_std_after"),
        "hit_rate": (run_summary or {}).get("hit_rate"),
        "roi": (run_summary or {}).get("roi"),
        "buy_races": (run_summary or {}).get("buy_races"),
        "total_bets": (run_summary or {}).get("total_bets"),
        "hit_bets": (run_summary or {}).get("hit_bets"),
        "total_return_yen": (run_summary or {}).get("total_return_yen"),
        "total_bet_yen": (run_summary or {}).get("total_bet_yen"),
        "max_losing_streak": (run_summary or {}).get("max_losing_streak"),
        "roi_by_popularity_band": roi_by_popularity_band,
        "roi_by_odds_band": roi_by_odds_band,
        "calibration_summary_path": str(calibration_path),
        "feature_importance_summary_path": str(feature_importance_summary_path),
        "model_metrics_path": str(metrics_path),
        "missing_inputs": missing_inputs,
        "created_at": created_at,
    }
    return row, reasons, missing_inputs


def make_comparison_report(
    *,
    dataset_manifest_path: Path,
    report_dir: Path,
    experiment_names: list[str],
    experiment_model_dirs: list[Path],
    experiment_run_summary_paths: list[Path | None] | None = None,
    strict_mismatch: bool = False,
) -> dict[str, Any]:
    if len(experiment_names) != len(experiment_model_dirs):
        raise ValueError("experiment_names and experiment_model_dirs length mismatch")
    if experiment_run_summary_paths is None:
        experiment_run_summary_paths = [None] * len(experiment_names)
    if len(experiment_run_summary_paths) != len(experiment_names):
        raise ValueError("experiment_run_summary_paths length mismatch")

    manifest = _read_json(dataset_manifest_path)

    rows: list[dict[str, Any]] = []
    mismatch_reasons: dict[str, list[str]] = {}
    missing_inputs_by_experiment: dict[str, list[str]] = {}
    has_mismatch = False
    has_missing_calibration = False
    for name, model_dir, run_summary_path in zip(experiment_names, experiment_model_dirs, experiment_run_summary_paths):
        row, reasons, missing_inputs = _collect_experiment_row(
            manifest=manifest,
            experiment_name=name,
            model_dir=model_dir,
            run_summary_path=run_summary_path,
        )
        rows.append(row)
        if reasons:
            has_mismatch = True
            mismatch_reasons[name] = reasons
        if missing_inputs:
            missing_inputs_by_experiment[name] = missing_inputs
        if "calibration_summary" in missing_inputs:
            has_missing_calibration = True

    comparison_status = "mismatch" if has_mismatch else "ok"
    if not has_mismatch and has_missing_calibration:
        comparison_status = "ok_with_missing_calibration"
    if strict_mismatch and has_mismatch:
        # still write report for audit, but mark strict mismatch.
        comparison_status = "mismatch"

    report_dir.mkdir(parents=True, exist_ok=True)
    csv_path = report_dir / "comparison_report.csv"
    json_path = report_dir / "comparison_report.json"

    csv_rows: list[dict[str, Any]] = []
    for row in rows:
        csv_row = dict(row)
        csv_row["roi_by_popularity_band"] = json.dumps(row.get("roi_by_popularity_band"), ensure_ascii=False)
        csv_row["roi_by_odds_band"] = json.dumps(row.get("roi_by_odds_band"), ensure_ascii=False)
        csv_row["missing_inputs"] = json.dumps(row.get("missing_inputs"), ensure_ascii=False)
        csv_rows.append(csv_row)

    _atomic_write_csv(
        csv_path,
        csv_rows,
        fieldnames=[
            "experiment_name",
            "task_name",
            "model_version",
            "feature_set",
            "feature_snapshot_version",
            "train_period",
            "valid_period",
            "test_period",
            "race_count",
            "row_count",
            "dataset_fingerprint",
            "logloss",
            "brier",
            "calibration_method",
            "logloss_before",
            "logloss_after",
            "logloss_delta",
            "brier_before",
            "brier_after",
            "brier_delta",
            "ece_before",
            "ece_after",
            "ece_delta",
            "race_sum_top3_mean_before",
            "race_sum_top3_mean_after",
            "race_sum_top3_std_before",
            "race_sum_top3_std_after",
            "hit_rate",
            "roi",
            "buy_races",
            "total_bets",
            "hit_bets",
            "total_return_yen",
            "total_bet_yen",
            "max_losing_streak",
            "roi_by_popularity_band",
            "roi_by_odds_band",
            "calibration_summary_path",
            "feature_importance_summary_path",
            "model_metrics_path",
            "missing_inputs",
            "created_at",
        ],
    )

    comparison_created_at = dt.datetime.now().isoformat(timespec="seconds")
    json_rows: list[dict[str, Any]] = []
    for row in rows:
        json_row = dict(row)
        json_row["comparison_created_at"] = comparison_created_at
        json_rows.append(json_row)

    payload = {
        "dataset_manifest": manifest,
        "compared_experiments": json_rows,
        "comparison_created_at": comparison_created_at,
        "comparison_status": comparison_status,
        "mismatch_reasons": mismatch_reasons,
        "missing_inputs": missing_inputs_by_experiment,
        "comparison_report_csv_path": str(csv_path),
    }
    _atomic_write_text(json_path, json.dumps(payload, ensure_ascii=False, indent=2))

    return {
        "comparison_report_csv": str(csv_path),
        "comparison_report_json": str(json_path),
        "comparison_status": comparison_status,
        "mismatch_reasons": mismatch_reasons,
    }
