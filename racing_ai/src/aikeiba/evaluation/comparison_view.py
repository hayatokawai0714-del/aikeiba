from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path
from typing import Any


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _atomic_copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    tmp.write_bytes(src.read_bytes())
    tmp.replace(dst)


def _to_finite_or_none(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _sanitize(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return _to_finite_or_none(obj)


def _experiment_status(name: str, mismatch_reasons: dict[str, list[str]], missing_inputs: list[str]) -> str:
    if name in mismatch_reasons:
        return "mismatch"
    if "calibration_summary" in missing_inputs:
        return "ok_with_missing_calibration"
    return "ok"


def _metric_entry(row: dict[str, Any], key: str) -> dict[str, Any]:
    return {
        "experiment_name": row.get("experiment_name"),
        "model_version": row.get("model_version"),
        "value": row.get(key),
    }


def _build_rank(
    rows: list[dict[str, Any]],
    *,
    key: str,
    smaller_is_better: bool,
) -> list[dict[str, Any]]:
    candidates = []
    for row in rows:
        if row.get("comparison_status") == "mismatch":
            continue
        value = row.get(key)
        if value is None or (isinstance(value, float) and not math.isfinite(value)):
            continue
        candidates.append(_metric_entry(row, key))
    candidates.sort(key=lambda x: (x["value"], x["experiment_name"]) if smaller_is_better else (-x["value"], x["experiment_name"]))
    return candidates


def _pick_best(rows: list[dict[str, Any]], *, key: str, smaller_is_better: bool) -> dict[str, Any] | None:
    ranking = _build_rank(rows, key=key, smaller_is_better=smaller_is_better)
    return ranking[0] if ranking else None


def build_comparison_view(
    *,
    comparison_report_json_path: Path,
    dataset_manifest_path: Path | None = None,
    comparison_report_csv_path: Path | None = None,
    out_path: Path | None = None,
) -> dict[str, Any]:
    report = _read_json(comparison_report_json_path)
    manifest = report.get("dataset_manifest", {})
    compared = report.get("compared_experiments", [])
    mismatch_reasons = report.get("mismatch_reasons", {})
    missing_inputs_map = report.get("missing_inputs", {})
    comparison_created_at = report.get("comparison_created_at") or dt.datetime.now().isoformat(timespec="seconds")

    leaderboard: list[dict[str, Any]] = []
    mismatch_experiments: list[str] = []
    missing_calibration_experiments: list[str] = []
    missing_run_summary_experiments: list[str] = []
    skipped_from_best_selection: list[str] = []
    reason_counter: dict[str, int] = {}

    for row in compared:
        name = str(row.get("experiment_name"))
        missing_inputs = missing_inputs_map.get(name, row.get("missing_inputs", [])) or []
        status = _experiment_status(name, mismatch_reasons, missing_inputs)
        if status == "mismatch":
            mismatch_experiments.append(name)
            skipped_from_best_selection.append(name)
        if "calibration_summary" in missing_inputs:
            missing_calibration_experiments.append(name)
        if "run_summary" in missing_inputs:
            missing_run_summary_experiments.append(name)
        for reason in mismatch_reasons.get(name, []):
            reason_counter[reason] = reason_counter.get(reason, 0) + 1

        leaderboard.append(
            {
                "experiment_name": name,
                "model_version": row.get("model_version"),
                "feature_set": row.get("feature_set"),
                "feature_snapshot_version": row.get("feature_snapshot_version"),
                "dataset_fingerprint": row.get("dataset_fingerprint"),
                "comparison_status": status,
                "has_calibration": "calibration_summary" not in missing_inputs,
                "logloss_after": row.get("logloss_after", row.get("logloss")),
                "brier_after": row.get("brier_after", row.get("brier")),
                "ece_after": row.get("ece_after"),
                "roi": row.get("roi"),
                "hit_rate": row.get("hit_rate"),
                "buy_races": row.get("buy_races"),
                "total_bets": row.get("total_bets"),
                "hit_bets": row.get("hit_bets"),
                "total_return_yen": row.get("total_return_yen"),
                "total_bet_yen": row.get("total_bet_yen"),
                "max_losing_streak": row.get("max_losing_streak"),
                "logloss_delta": row.get("logloss_delta"),
                "brier_delta": row.get("brier_delta"),
                "ece_delta": row.get("ece_delta"),
                "calibration_method": row.get("calibration_method"),
                "missing_inputs": missing_inputs,
                "feature_importance_summary_path": row.get("feature_importance_summary_path"),
            }
        )

    experiment_count = len(leaderboard)
    valid_rows = [r for r in leaderboard if r.get("comparison_status") != "mismatch"]
    valid_experiment_count = len(valid_rows)
    mismatch_experiment_count = len(mismatch_experiments)
    missing_calibration_count = len(missing_calibration_experiments)
    valid_roi_experiment_count = len([r for r in valid_rows if r.get("roi") is not None])
    valid_hit_rate_experiment_count = len([r for r in valid_rows if r.get("hit_rate") is not None])
    valid_buy_races_experiment_count = len([r for r in valid_rows if r.get("buy_races") is not None])
    valid_total_return_experiment_count = len([r for r in valid_rows if r.get("total_return_yen") is not None])

    best_summary = {
        "best_logloss_after": _pick_best(leaderboard, key="logloss_after", smaller_is_better=True),
        "best_brier_after": _pick_best(leaderboard, key="brier_after", smaller_is_better=True),
        "best_ece_after": _pick_best(leaderboard, key="ece_after", smaller_is_better=True),
        "best_roi": _pick_best(leaderboard, key="roi", smaller_is_better=False),
        "best_hit_rate": _pick_best(leaderboard, key="hit_rate", smaller_is_better=False),
        "best_buy_races": _pick_best(leaderboard, key="buy_races", smaller_is_better=False),
    }

    ranking_views = {
        "by_logloss_after": _build_rank(leaderboard, key="logloss_after", smaller_is_better=True),
        "by_brier_after": _build_rank(leaderboard, key="brier_after", smaller_is_better=True),
        "by_ece_after": _build_rank(leaderboard, key="ece_after", smaller_is_better=True),
        "by_roi": _build_rank(leaderboard, key="roi", smaller_is_better=False),
        "by_hit_rate": _build_rank(leaderboard, key="hit_rate", smaller_is_better=False),
        "by_logloss_delta": _build_rank(leaderboard, key="logloss_delta", smaller_is_better=True),
        "by_brier_delta": _build_rank(leaderboard, key="brier_delta", smaller_is_better=True),
        "by_ece_delta": _build_rank(leaderboard, key="ece_delta", smaller_is_better=True),
    }

    view = {
        "dataset_name": manifest.get("dataset_name"),
        "task_name": manifest.get("task_name"),
        "dataset_fingerprint": manifest.get("dataset_fingerprint"),
        "comparison_status": report.get("comparison_status"),
        "comparison_created_at": comparison_created_at,
        "experiment_count": experiment_count,
        "valid_experiment_count": valid_experiment_count,
        "mismatch_experiment_count": mismatch_experiment_count,
        "missing_calibration_count": missing_calibration_count,
        "valid_roi_experiment_count": valid_roi_experiment_count,
        "valid_hit_rate_experiment_count": valid_hit_rate_experiment_count,
        "valid_buy_races_experiment_count": valid_buy_races_experiment_count,
        "valid_total_return_experiment_count": valid_total_return_experiment_count,
        "best_summary": best_summary,
        "leaderboard": leaderboard,
        "ranking_views": ranking_views,
        "issues_summary": {
            "mismatch_experiments": sorted(mismatch_experiments),
            "missing_calibration_experiments": sorted(missing_calibration_experiments),
            "missing_run_summary_experiments": sorted(missing_run_summary_experiments),
            "skipped_from_best_selection": sorted(set(skipped_from_best_selection)),
            "mismatch_reasons_summary": reason_counter,
        },
        "source_paths": {
            "comparison_report_json_path": str(comparison_report_json_path),
            "comparison_report_csv_path": str(comparison_report_csv_path or report.get("comparison_report_csv_path")),
            "dataset_manifest_path": str(dataset_manifest_path) if dataset_manifest_path else None,
            "feature_importance_summary_paths": sorted(
                {
                    str(row.get("feature_importance_summary_path"))
                    for row in leaderboard
                    if row.get("feature_importance_summary_path")
                }
            ),
        },
        "created_at": dt.datetime.now().isoformat(timespec="seconds"),
    }

    out_path = out_path or comparison_report_json_path.with_name("comparison_view.json")
    _atomic_write_text(out_path, json.dumps(_sanitize(view), ensure_ascii=False, indent=2))
    return {
        "comparison_view_json": str(out_path),
        "comparison_view": _sanitize(view),
    }


def publish_latest_comparison_files(
    *,
    comparison_report_json_path: Path,
    comparison_view_json_path: Path,
    latest_dir: Path,
) -> dict[str, str]:
    latest_dir.mkdir(parents=True, exist_ok=True)
    latest_report = latest_dir / "comparison_report.json"
    latest_view = latest_dir / "comparison_view.json"
    _atomic_copy_file(comparison_report_json_path, latest_report)
    _atomic_copy_file(comparison_view_json_path, latest_view)
    return {
        "latest_comparison_report_json": str(latest_report),
        "latest_comparison_view_json": str(latest_view),
    }
