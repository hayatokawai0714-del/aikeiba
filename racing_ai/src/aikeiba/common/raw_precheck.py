from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any


REQUIRED_RAW_FILES = ["races.csv", "entries.csv"]


def _count_data_rows(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
            line_count = sum(1 for _ in f)
        return max(0, line_count - 1)
    except Exception:
        return None


def run_raw_precheck(
    *,
    raw_dir: Path,
    race_date: str,
    model_version: str,
    logs_dir: Path = Path("data/logs"),
    required_files: list[str] | None = None,
) -> dict[str, Any]:
    req = required_files or REQUIRED_RAW_FILES
    missing = [f for f in req if not (raw_dir / f).exists()]
    row_counts: dict[str, int | None] = {}
    empty_files: list[str] = []
    for file_name in req:
        p = raw_dir / file_name
        cnt = _count_data_rows(p)
        row_counts[file_name] = cnt
        if p.exists() and cnt == 0:
            empty_files.append(file_name)
    if missing:
        stop_reason = "missing_required_raw_files"
    elif empty_files:
        stop_reason = "raw_files_empty"
    else:
        stop_reason = None
    payload = {
        "race_date": race_date,
        "model_version": model_version,
        "raw_dir": str(raw_dir),
        "required_files": req,
        "missing_files": missing,
        "empty_files": empty_files,
        "row_counts": row_counts,
        "status": "fail" if stop_reason else "pass",
        "stop_reason": stop_reason,
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
    }
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_path = logs_dir / f"raw_precheck_{race_date}_{model_version}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    payload["log_path"] = str(out_path)
    return payload
