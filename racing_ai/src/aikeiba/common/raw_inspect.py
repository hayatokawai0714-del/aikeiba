from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REQUIRED_FILES = ("races.csv", "entries.csv")
ROI_FILES = ("results.csv", "payouts.csv")
OPTIONAL_FILES = ("odds.csv",)


def _count_rows(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            # header-only csv -> 0 rows
            line_count = sum(1 for _ in f)
        return max(0, line_count - 1)
    except Exception:
        return None


def inspect_raw_dir(raw_dir: Path) -> dict[str, Any]:
    raw_dir = raw_dir.resolve()
    checks: dict[str, dict[str, Any]] = {}
    for name in [*REQUIRED_FILES, *ROI_FILES, *OPTIONAL_FILES]:
        p = raw_dir / name
        checks[name] = {
            "path": str(p),
            "exists": p.exists(),
            "rows": _count_rows(p),
        }

    has_races = bool(checks["races.csv"]["exists"])
    has_entries = bool(checks["entries.csv"]["exists"])
    has_results = bool(checks["results.csv"]["exists"])
    has_payouts = bool(checks["payouts.csv"]["exists"])

    missing_required = [name for name in REQUIRED_FILES if not checks[name]["exists"]]
    missing_roi_files = [name for name in ROI_FILES if not checks[name]["exists"]]
    roi_metrics_possible = len(missing_roi_files) == 0

    return {
        "raw_dir": str(raw_dir),
        "has_races": has_races,
        "has_entries": has_entries,
        "has_results": has_results,
        "has_payouts": has_payouts,
        "roi_metrics_possible": roi_metrics_possible,
        "missing_required_files": missing_required,
        "missing_roi_files": missing_roi_files,
        "missing_files": sorted(set([*missing_required, *missing_roi_files])),
        "files": checks,
        "status": "ok" if len(missing_required) == 0 else "stop",
    }


def write_raw_inspect(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

