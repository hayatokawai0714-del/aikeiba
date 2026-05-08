from __future__ import annotations

import uuid
import json

from aikeiba.db.duckdb import DuckDb


def write_race_day_run_log(
    *,
    db: DuckDb,
    run_id: str | None,
    race_date: str,
    snapshot_version: str,
    feature_snapshot_version: str,
    model_version: str,
    odds_snapshot_version: str,
    status: str,
    doctor_overall_status: str | None,
    stop_reason: str | None,
    warning_count: int,
    warnings: list[str] | None,
    run_summary_path: str | None,
) -> str:
    run_id = run_id or str(uuid.uuid4())
    db.execute(
        """
        INSERT INTO race_day_run_log(
          run_id, race_date, snapshot_version, feature_snapshot_version, model_version,
          odds_snapshot_version, status, doctor_overall_status, stop_reason, warning_count, run_summary_path, warnings_json
        ) VALUES (?, cast(? as DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            race_date,
            snapshot_version,
            feature_snapshot_version,
            model_version,
            odds_snapshot_version,
            status,
            doctor_overall_status,
            stop_reason,
            int(warning_count),
            run_summary_path,
            json.dumps({"warnings": warnings or []}, ensure_ascii=False),
        ),
    )
    return run_id


def write_daily_cycle_run_log(
    *,
    db: DuckDb,
    run_id: str | None,
    race_date: str,
    cycle_started_at: str | None,
    cycle_finished_at: str | None,
    cycle_status: str,
    race_day_status: str | None,
    compare_status: str | None,
    stop_reason: str | None,
    warning_count: int,
    model_version: str | None,
    feature_snapshot_version: str | None,
    snapshot_version: str | None,
    dataset_manifest_path: str | None,
    comparison_report_path: str | None,
    comparison_view_path: str | None,
    selected_experiment_names: list[str] | None,
    discovered_experiment_names: list[str] | None,
) -> str:
    run_id = run_id or str(uuid.uuid4())
    db.execute(
        """
        INSERT INTO daily_cycle_run_log(
          run_id, race_date, cycle_started_at, cycle_finished_at, cycle_status,
          race_day_status, compare_status, stop_reason, warning_count,
          model_version, feature_snapshot_version, snapshot_version,
          dataset_manifest_path, comparison_report_path, comparison_view_path,
          selected_experiment_names_json, discovered_experiment_names_json
        ) VALUES (
          ?, cast(? as DATE), cast(? as TIMESTAMP), cast(? as TIMESTAMP), ?,
          ?, ?, ?, ?,
          ?, ?, ?,
          ?, ?, ?,
          ?, ?
        )
        """,
        (
            run_id,
            race_date,
            cycle_started_at,
            cycle_finished_at,
            cycle_status,
            race_day_status,
            compare_status,
            stop_reason,
            int(warning_count),
            model_version,
            feature_snapshot_version,
            snapshot_version,
            dataset_manifest_path,
            comparison_report_path,
            comparison_view_path,
            json.dumps({"selected_experiment_names": selected_experiment_names or []}, ensure_ascii=False),
            json.dumps({"discovered_experiment_names": discovered_experiment_names or []}, ensure_ascii=False),
        ),
    )
    return run_id


def write_raw_precheck_to_daily_cycle_log(
    *,
    db: DuckDb,
    run_id: str | None,
    command_name: str,
    race_date: str,
    model_version: str | None,
    raw_dir: str | None,
    status: str,
    stop_reason: str | None,
    required_files: list[str] | None,
    missing_files: list[str] | None,
    empty_files: list[str] | None,
    row_counts: dict[str, int | None] | None,
    raw_precheck_log_path: str | None,
    run_summary_path: str | None,
    daily_cycle_summary_path: str | None,
    generated_at: str | None,
) -> str:
    run_id = run_id or str(uuid.uuid4())
    db.execute(
        """
        INSERT INTO daily_cycle_run_log(
          run_id, race_date, cycle_started_at, cycle_finished_at, cycle_status,
          race_day_status, compare_status, stop_reason, warning_count,
          model_version, feature_snapshot_version, snapshot_version,
          dataset_manifest_path, comparison_report_path, comparison_view_path,
          selected_experiment_names_json, discovered_experiment_names_json,
          command_name, raw_dir, status, required_files, missing_files,
          empty_files, row_counts,
          raw_precheck_log_path, run_summary_path, daily_cycle_summary_path, generated_at
        ) VALUES (
          ?, cast(? as DATE), cast(? as TIMESTAMP), cast(? as TIMESTAMP), ?,
          NULL, NULL, ?, 0,
          ?, NULL, NULL,
          NULL, NULL, NULL,
          ?, ?,
          ?, ?, ?, ?, ?,
          ?, ?,
          ?, ?, ?, cast(? as TIMESTAMP)
        )
        """,
        (
            run_id,
            race_date,
            generated_at,
            generated_at,
            status,
            stop_reason,
            model_version,
            json.dumps({"selected_experiment_names": []}, ensure_ascii=False),
            json.dumps({"discovered_experiment_names": []}, ensure_ascii=False),
            command_name,
            raw_dir,
            status,
            json.dumps(required_files or [], ensure_ascii=False),
            json.dumps(missing_files or [], ensure_ascii=False),
            json.dumps(empty_files or [], ensure_ascii=False),
            json.dumps(row_counts or {}, ensure_ascii=False),
            raw_precheck_log_path,
            run_summary_path,
            daily_cycle_summary_path,
            generated_at,
        ),
    )
    return run_id
