from __future__ import annotations

import uuid
import json

from aikeiba.db.duckdb import DuckDb


def write_race_day_run_log(
    *,
    db: DuckDb,
    race_date: str,
    snapshot_version: str,
    feature_snapshot_version: str,
    model_version: str,
    odds_snapshot_version: str,
    status: str,
    stop_reason: str | None,
    warning_count: int,
    warnings: list[str] | None,
    run_summary_path: str | None,
) -> str:
    run_id = str(uuid.uuid4())
    db.execute(
        """
        INSERT INTO race_day_run_log(
          run_id, race_date, snapshot_version, feature_snapshot_version, model_version,
          odds_snapshot_version, status, stop_reason, warning_count, run_summary_path, warnings_json
        ) VALUES (?, cast(? as DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            race_date,
            snapshot_version,
            feature_snapshot_version,
            model_version,
            odds_snapshot_version,
            status,
            stop_reason,
            int(warning_count),
            run_summary_path,
            json.dumps({"warnings": warnings or []}, ensure_ascii=False),
        ),
    )
    return run_id
