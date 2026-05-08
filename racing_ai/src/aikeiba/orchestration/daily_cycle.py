from __future__ import annotations

import datetime as dt
import json
import uuid
from pathlib import Path
from typing import Any

from aikeiba.common.discovery import discover_experiments
from aikeiba.common.run_log import write_daily_cycle_run_log
from aikeiba.db.duckdb import DuckDb
from aikeiba.evaluation.comparison_report import make_comparison_report
from aikeiba.evaluation.comparison_view import build_comparison_view, publish_latest_comparison_files
from aikeiba.orchestration.race_day import run_race_day_pipeline


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_experiments(
    *,
    manual_experiment_names: list[str],
    manual_experiment_model_dirs: list[Path],
    manual_experiment_run_summary_paths: list[Path | None] | None,
    models_root: Path,
    task: str,
) -> dict[str, Any]:
    discovered = discover_experiments(models_root=models_root, task=task)
    discovered_names = [str(rec["experiment_name"]) for rec in discovered]

    if len(manual_experiment_names) > 0:
        if manual_experiment_run_summary_paths is None:
            manual_experiment_run_summary_paths = [None] * len(manual_experiment_names)
        return {
            "mode": "manual",
            "discovered_experiment_names": discovered_names,
            "selected_experiment_names": manual_experiment_names,
            "selected_experiment_model_dirs": manual_experiment_model_dirs,
            "selected_experiment_run_summary_paths": manual_experiment_run_summary_paths,
        }

    selected_names = [str(rec["experiment_name"]) for rec in discovered]
    selected_dirs = [Path(rec["model_dir"]) for rec in discovered]
    selected_run_paths = [None] * len(selected_names)
    return {
        "mode": "auto_discovered",
        "discovered_experiment_names": discovered_names,
        "selected_experiment_names": selected_names,
        "selected_experiment_model_dirs": selected_dirs,
        "selected_experiment_run_summary_paths": selected_run_paths,
    }


def run_daily_cycle(
    *,
    db: DuckDb,
    raw_dir: Path,
    normalized_root: Path,
    race_date: str,
    snapshot_version: str,
    feature_snapshot_version: str,
    model_version: str,
    odds_snapshot_version: str,
    models_root: Path,
    export_out_dir: Path,
    run_summary_path: Path,
    allow_no_wide_odds: bool,
    dataset_manifest_path: Path | None,
    experiment_names: list[str],
    experiment_model_dirs: list[Path],
    experiment_run_summary_paths: list[Path | None] | None,
    experiment_task: str,
    report_out_dir: Path,
    skip_compare: bool,
    publish_latest: bool,
    latest_out_dir: Path,
    strict_compare_mismatch: bool,
    daily_cycle_summary_path: Path,
) -> dict[str, Any]:
    run_id = str(uuid.uuid4())
    cycle_started_at = dt.datetime.now().isoformat(timespec="seconds")
    warnings: list[str] = []
    stop_reason: str | None = None
    race_day_status: str | None = None
    compare_status: str | None = None
    comparison_report_path: str | None = None
    comparison_view_path: str | None = None
    latest_comparison_report_path: str | None = None
    latest_comparison_view_path: str | None = None
    comparison_dataset_fingerprint: str | None = None
    cycle_status = "ok"

    selection = _resolve_experiments(
        manual_experiment_names=experiment_names,
        manual_experiment_model_dirs=experiment_model_dirs,
        manual_experiment_run_summary_paths=experiment_run_summary_paths,
        models_root=models_root,
        task=experiment_task,
    )
    selected_experiment_names = selection["selected_experiment_names"]
    selected_experiment_model_dirs = selection["selected_experiment_model_dirs"]
    selected_experiment_run_paths = selection["selected_experiment_run_summary_paths"]
    experiment_selection_mode = selection["mode"]
    discovered_experiment_names = selection["discovered_experiment_names"]

    model_dir = models_root / "top3" / model_version
    calibration_summary_path = model_dir / "calibration_summary.json"
    feature_importance_summary_path = model_dir / "feature_importance_summary.json"
    selected_primary_experiment = selected_experiment_names[0] if selected_experiment_names else None

    try:
        race_day_summary = run_race_day_pipeline(
            db=db,
            raw_dir=raw_dir,
            normalized_root=normalized_root,
            race_date=race_date,
            snapshot_version=snapshot_version,
            feature_snapshot_version=feature_snapshot_version,
            model_version=model_version,
            odds_snapshot_version=odds_snapshot_version,
            models_root=models_root,
            export_out_dir=export_out_dir,
            run_summary_path=run_summary_path,
            auto_run_summary_dir=None,
            allow_no_wide_odds=allow_no_wide_odds,
            experiment_name=selected_primary_experiment,
            calibration_summary_path=str(calibration_summary_path) if calibration_summary_path.exists() else None,
            feature_importance_summary_path=str(feature_importance_summary_path) if feature_importance_summary_path.exists() else None,
        )
        race_day_status = race_day_summary.get("status")
        warnings.extend(race_day_summary.get("warnings", []))

        if race_day_status == "stop":
            cycle_status = "race_day_failed"
            stop_reason = race_day_summary.get("stop_reason") or "run_race_day_stop"
        elif skip_compare:
            compare_status = "skipped_by_flag"
            cycle_status = "compare_skipped"
            warnings.append("compare_skipped_by_flag")
        elif dataset_manifest_path is None:
            compare_status = "skipped_manifest_missing"
            cycle_status = "compare_skipped"
            warnings.append("dataset_manifest_missing_compare_skipped")
        elif not dataset_manifest_path.exists():
            compare_status = "skipped_manifest_not_found"
            cycle_status = "compare_skipped"
            warnings.append("dataset_manifest_not_found_compare_skipped")
        elif len(selected_experiment_names) == 0:
            compare_status = "skipped_no_experiments"
            cycle_status = "compare_skipped"
            warnings.append("no_experiments_compare_skipped")
        else:
            compare = make_comparison_report(
                dataset_manifest_path=dataset_manifest_path,
                report_dir=report_out_dir,
                experiment_names=selected_experiment_names,
                experiment_model_dirs=selected_experiment_model_dirs,
                experiment_run_summary_paths=selected_experiment_run_paths,
                strict_mismatch=strict_compare_mismatch,
            )
            comparison_report_path = compare.get("comparison_report_json")
            compare_status = compare.get("comparison_status")

            view = build_comparison_view(
                comparison_report_json_path=Path(compare["comparison_report_json"]),
                dataset_manifest_path=dataset_manifest_path,
                comparison_report_csv_path=Path(compare["comparison_report_csv"]),
                out_path=report_out_dir / "comparison_view.json",
            )
            comparison_view_path = view.get("comparison_view_json")

            if publish_latest:
                latest = publish_latest_comparison_files(
                    comparison_report_json_path=Path(compare["comparison_report_json"]),
                    comparison_view_json_path=Path(view["comparison_view_json"]),
                    latest_dir=latest_out_dir,
                )
                latest_comparison_report_path = latest.get("latest_comparison_report_json")
                latest_comparison_view_path = latest.get("latest_comparison_view_json")

            comp_report_json = _read_json(Path(compare["comparison_report_json"]))
            comparison_dataset_fingerprint = (
                comp_report_json.get("dataset_manifest", {}).get("dataset_fingerprint")
            )

            if compare_status == "mismatch":
                cycle_status = "compare_failed"
                stop_reason = "compare_mismatch"
            elif race_day_status == "warn" or compare_status == "ok_with_missing_calibration":
                cycle_status = "ok_with_warnings"
            else:
                cycle_status = "ok"
    except Exception as exc:
        cycle_status = "stop"
        stop_reason = f"exception:{exc.__class__.__name__}:{exc}"

    cycle_finished_at = dt.datetime.now().isoformat(timespec="seconds")
    summary = {
        "run_id": run_id,
        "race_date": race_date,
        "cycle_started_at": cycle_started_at,
        "cycle_finished_at": cycle_finished_at,
        "cycle_status": cycle_status,
        "race_day_status": race_day_status,
        "compare_status": compare_status,
        "run_race_day_summary_path": str(run_summary_path),
        "comparison_report_path": comparison_report_path,
        "comparison_view_path": comparison_view_path,
        "latest_comparison_report_path": latest_comparison_report_path,
        "latest_comparison_view_path": latest_comparison_view_path,
        "dataset_manifest_path": str(dataset_manifest_path) if dataset_manifest_path else None,
        "comparison_dataset_fingerprint": comparison_dataset_fingerprint,
        "model_version": model_version,
        "feature_snapshot_version": feature_snapshot_version,
        "snapshot_version": snapshot_version,
        "experiment_names": selected_experiment_names,
        "selected_experiment_names": selected_experiment_names,
        "experiment_selection_mode": experiment_selection_mode,
        "discovered_experiment_names": discovered_experiment_names,
        "warnings": warnings,
        "stop_reason": stop_reason,
    }
    _atomic_write_json(daily_cycle_summary_path, summary)

    write_daily_cycle_run_log(
        db=db,
        run_id=run_id,
        race_date=race_date,
        cycle_started_at=cycle_started_at,
        cycle_finished_at=cycle_finished_at,
        cycle_status=cycle_status,
        race_day_status=race_day_status,
        compare_status=compare_status,
        stop_reason=stop_reason,
        warning_count=len(warnings),
        model_version=model_version,
        feature_snapshot_version=feature_snapshot_version,
        snapshot_version=snapshot_version,
        dataset_manifest_path=str(dataset_manifest_path) if dataset_manifest_path else None,
        comparison_report_path=comparison_report_path,
        comparison_view_path=comparison_view_path,
        selected_experiment_names=selected_experiment_names,
        discovered_experiment_names=discovered_experiment_names,
    )

    return summary
