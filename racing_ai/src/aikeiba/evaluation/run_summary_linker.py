from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from aikeiba.common.run_summary import normalize_run_summary, validate_run_summary


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def find_latest_run_summary_for_model_version(
    *,
    model_version: str,
    search_dirs: list[Path],
) -> Path | None:
    """
    Find the latest run_summary-like JSON containing matching model_version.
    Assumption: run summary files are small enough to scan recursively.
    """
    candidates: list[Path] = []
    for root in search_dirs:
        if not root.exists():
            continue
        for path in root.rglob("*.json"):
            name = path.name.lower()
            if "run_summary" not in name and "daily_cycle_summary" not in name:
                continue
            try:
                payload = _read_json(path)
            except Exception:
                continue
            normalized = normalize_run_summary(payload)
            validation = validate_run_summary(normalized, strict=True)
            if validation["errors"]:
                continue
            if str(normalized.get("model_version")) == model_version:
                candidates.append(path)
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]
