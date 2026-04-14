from __future__ import annotations

import datetime as dt
import json
import uuid
from typing import Any

from aikeiba.db.duckdb import DuckDb


def log_pipeline_event(
    *,
    db: DuckDb,
    stage: str,
    snapshot_version: str,
    target_race_date: str,
    status: str,
    source_file_name: str | None = None,
    source_file_path: str | None = None,
    row_count: int | None = None,
    message: str | None = None,
    metrics: dict[str, Any] | None = None,
) -> None:
    event_id = str(uuid.uuid4())
    event_time = dt.datetime.now().isoformat(timespec="seconds")
    metrics_json = json.dumps(metrics or {}, ensure_ascii=False)
    db.execute(
        """
        INSERT INTO pipeline_audit_log(
          event_id, stage, snapshot_version, target_race_date, event_time, status,
          source_file_name, source_file_path, row_count, message, metrics_json
        ) VALUES (?, ?, ?, cast(? as DATE), cast(? as TIMESTAMP), ?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            stage,
            snapshot_version,
            target_race_date,
            event_time,
            status,
            source_file_name,
            source_file_path,
            row_count,
            message,
            metrics_json,
        ),
    )
