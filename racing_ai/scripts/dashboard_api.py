"""Local dashboard API for same-day race prediction updates.

Run (local only):
  cd C:\\Users\\HND2205\\Documents\\git\\aikeiba
  set AIIKEIBA_DASHBOARD_TOKEN=your_local_token
  python -m uvicorn racing_ai.scripts.dashboard_api:app --reload --host 127.0.0.1 --port 8000

Security policy:
  - Must run on 127.0.0.1 only.
  - Do not run with --host 0.0.0.0.
  - /run-today requires token auth via X-Dashboard-Token or Bearer token.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_SCRIPT = REPO_ROOT / "racing_ai" / "scripts" / "run_today_wide_phase2_pipeline.py"
FETCH_ODDS_SCRIPT = REPO_ROOT / "racing_ai" / "scripts" / "fetch_odds_latest.py"
TARGET_AHK_SCRIPT = REPO_ROOT / "racing_ai" / "scripts" / "export_target.ahk"
JV_DIRECT_PROJECT = (
    REPO_ROOT
    / "racing_ai"
    / "tools"
    / "jvlink_direct_exporter"
    / "Aikeiba.JVLinkDirectExporter.csproj"
)
PIPELINE_OUTPUT = REPO_ROOT / "data" / "today_wide_predictions.csv"
PIPELINE_LOG = REPO_ROOT / "data" / "today_wide_predictions_log.txt"
RACES_TODAY_JSON = REPO_ROOT / "data" / "races_today.json"
HORSE_NAME_OVERRIDE_PATH = REPO_ROOT / "data" / "horse_name_overrides.csv"
HISTORY_DATASET_PATH = Path(r"C:\TXT\dataset_top3_with_history_phase2.csv")
ENABLE_MASTER_NAME_FALLBACK = os.getenv("AIIKEIBA_ENABLE_MASTER_NAME_FALLBACK", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
NORMALIZED_ROOT = REPO_ROOT / "racing_ai" / "data" / "normalized"
RAW_ROOT = REPO_ROOT / "racing_ai" / "data" / "raw"
TARGET_EXPORT_ROOT = REPO_ROOT / "racing_ai" / "data" / "target_exports"
DB_PATH = REPO_ROOT / "racing_ai" / "data" / "warehouse" / "aikeiba.duckdb"
AIKEIBA_SRC = REPO_ROOT / "racing_ai" / "src"
ODDS_LATEST_PATH = NORMALIZED_ROOT / "odds_latest.csv"
ODDS_REPORT_PATH = NORMALIZED_ROOT / "odds_latest_report.json"
TOKEN_ENV_KEY = "AIIKEIBA_DASHBOARD_TOKEN"
TARGET_AHK_EXE = os.getenv("AIIKEIBA_AHK_EXE", "AutoHotkey.exe")
TARGET_AHK_TIMEOUT_SEC = int(os.getenv("AIIKEIBA_AHK_TIMEOUT_SEC", "240"))
TARGET_EXPORT_WAIT_SEC = int(os.getenv("AIIKEIBA_TARGET_EXPORT_WAIT_SEC", "60"))
JV_DIRECT_TIMEOUT_SEC = int(os.getenv("AIIKEIBA_JV_DIRECT_TIMEOUT_SEC", "600"))
JV_DATA_DIR = os.getenv("AIIKEIBA_JV_DATA_DIR", "").strip()
PREDICTION_REQUIRED_FILES = ("races.csv", "entries.csv")
PREDICTION_OPTIONAL_FILES = ("odds.csv", "results.csv", "payouts.csv")
BACKTEST_REQUIRED_FILES = ("races.csv", "entries.csv", "odds.csv", "results.csv", "payouts.csv")
ALLOW_PREDICTION_CACHE = os.getenv("AIIKEIBA_ALLOW_PREDICTION_CACHE", "").strip().lower() in {"1", "true", "yes"}
SKIP_TARGET_AHK = os.getenv("AIIKEIBA_SKIP_TARGET_AHK", "").strip().lower() in {"1", "true", "yes"}
ENABLE_TARGET_AHK_FALLBACK = os.getenv("AIIKEIBA_ENABLE_TARGET_AHK", "").strip().lower() in {"1", "true", "yes"}
TARGET_AHK_MANUAL_MENU = os.getenv("AIIKEIBA_TARGET_AHK_MANUAL_MENU", "1").strip().lower() in {"1", "true", "yes"}


def _allowed_origins() -> list[str]:
    ports = ["", ":3000", ":5173", ":5500", ":8080"]
    origins: list[str] = []
    for host in ["http://localhost", "http://127.0.0.1"]:
        for port in ports:
            origins.append(f"{host}{port}")
    return origins


app = FastAPI(title="aikeiba dashboard local api", version="1.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins(),
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type", "X-Dashboard-Token"],
)


class RunTodayRequest(BaseModel):
    today_date: str = Field(..., examples=["2026-04-24"])
    odds_cutoff: str = Field(..., examples=["2026-04-24 09:30:00"])
    mode: str = Field(default="prediction", examples=["prediction", "backtest"])
    target_ready_confirmed: bool = Field(
        default=False,
        description="User confirms TARGET/JRA-VAN latest data retrieval was completed.",
    )


def _folder_to_date(folder_name: str) -> str | None:
    m_iso = re.fullmatch(r"(20\d{2})-(\d{2})-(\d{2})", folder_name)
    if m_iso:
        return f"{m_iso.group(1)}-{m_iso.group(2)}-{m_iso.group(3)}"
    m_compact = re.match(r"(20\d{2})(\d{2})(\d{2})", folder_name)
    if m_compact:
        return f"{m_compact.group(1)}-{m_compact.group(2)}-{m_compact.group(3)}"
    return None


def _mode_file_rules(mode: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    normalized = mode.strip().lower()
    if normalized == "backtest":
        return BACKTEST_REQUIRED_FILES, ()
    return PREDICTION_REQUIRED_FILES, PREDICTION_OPTIONAL_FILES


def _normalize_mode(mode: str) -> str:
    normalized = mode.strip().lower()
    if normalized in {"prediction", "backtest"}:
        return normalized
    raise HTTPException(status_code=400, detail="mode must be 'prediction' or 'backtest'")


def _find_input_set_for_date(today_date: str, *, mode: str) -> dict[str, Any] | None:
    if not NORMALIZED_ROOT.exists():
        return None
    required_files, optional_files = _mode_file_rules(mode)
    candidates: list[dict[str, Any]] = []
    for folder in NORMALIZED_ROOT.rglob("*"):
        if not folder.is_dir():
            continue
        folder_date = _folder_to_date(folder.name)
        if folder_date != today_date:
            continue
        missing_required = [name for name in required_files if not (folder / name).exists()]
        if missing_required:
            continue
        paths = {name: (folder / name) for name in set(required_files + optional_files)}
        missing_optional = [name for name in optional_files if not paths[name].exists()]
        candidates.append(
            {
                "normalized_dir": folder,
                "paths": paths,
                "required_files": list(required_files),
                "optional_files": list(optional_files),
                "missing_required": missing_required,
                "missing_optional": missing_optional,
            }
        )
    if not candidates:
        return None
    candidates.sort(key=lambda item: str(item["normalized_dir"]), reverse=True)
    return candidates[0]


def _find_raw_dirs_for_date(today_date: str) -> list[Path]:
    if not RAW_ROOT.exists():
        return []
    compact = today_date.replace("-", "")
    dirs: list[Path] = []
    for folder in RAW_ROOT.iterdir():
        if not folder.is_dir():
            continue
        if not folder.name.startswith(compact):
            continue
        if not (folder / "races.csv").exists():
            continue
        if not (folder / "entries.csv").exists():
            continue
        dirs.append(folder)
    dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return dirs


def _available_dates(limit: int = 20) -> list[str]:
    if not NORMALIZED_ROOT.exists():
        return []
    date_set: set[str] = set()
    for folder in NORMALIZED_ROOT.rglob("*"):
        if not folder.is_dir():
            continue
        if not ((folder / "entries.csv").exists() and (folder / "races.csv").exists()):
            continue
        folder_date = _folder_to_date(folder.name)
        if not folder_date:
            continue
        date_set.add(folder_date)
    return sorted(date_set, reverse=True)[:limit]


def _available_raw_dates(limit: int = 20) -> list[str]:
    if not RAW_ROOT.exists():
        return []
    date_set: set[str] = set()
    for folder in RAW_ROOT.iterdir():
        if not folder.is_dir():
            continue
        folder_date = _folder_to_date(folder.name)
        if not folder_date:
            continue
        if not ((folder / "entries.csv").exists() and (folder / "races.csv").exists()):
            continue
        date_set.add(folder_date)
    return sorted(date_set, reverse=True)[:limit]


def _require_local_client(request: Request) -> None:
    host = request.client.host if request.client else ""
    if host not in {"127.0.0.1", "::1", "localhost"}:
        raise HTTPException(status_code=403, detail="Local requests only.")


def _extract_token(auth_header: str | None, x_token: str | None) -> str:
    if x_token:
        return x_token.strip()
    if auth_header and auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return ""


def _authorize(request: Request, authorization: str | None, x_dashboard_token: str | None) -> None:
    _require_local_client(request)
    expected = os.getenv(TOKEN_ENV_KEY, "").strip()
    if not expected:
        raise HTTPException(
            status_code=500,
            detail=f"{TOKEN_ENV_KEY} is not set. Set local token before using /run-today.",
        )
    actual = _extract_token(authorization, x_dashboard_token)
    if not actual or actual != expected:
        raise HTTPException(status_code=401, detail="Unauthorized token.")


def _run_aikeiba_cli(args: list[str]) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    py_path = str(AIKEIBA_SRC)
    if env.get("PYTHONPATH", "").strip():
        py_path = py_path + os.pathsep + env["PYTHONPATH"]
    env["PYTHONPATH"] = py_path
    cmd = [
        sys.executable,
        "-c",
        "from aikeiba.cli import app; app()",
    ] + args
    return subprocess.run(
        cmd,
        cwd=str(REPO_ROOT / "racing_ai"),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
        env=env,
    )


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _raw_manifest(raw_dir: Path) -> dict[str, Any]:
    return _read_json_file(raw_dir / "raw_manifest_check.json")


def _inspect_raw_dir(raw_dir: Path) -> tuple[dict[str, Any], subprocess.CompletedProcess[str]]:
    proc = _run_aikeiba_cli(["inspect-raw-dir", "--raw-dir", str(raw_dir)])
    report: dict[str, Any] = {}
    if proc.stdout.strip():
        try:
            report = json.loads(proc.stdout)
        except Exception:
            report = {}
    return report, proc


def _raw_row_counts(raw_dir: Path, inspect_report: dict[str, Any] | None = None) -> dict[str, int]:
    counts: dict[str, int] = {}
    manifest = _raw_manifest(raw_dir)
    for key, value in manifest.get("row_counts", {}).items():
        try:
            counts[str(key)] = int(value)
        except Exception:
            counts[str(key)] = 0

    if inspect_report:
        files = inspect_report.get("files", {})
        for file_name in ("races.csv", "entries.csv", "odds.csv", "results.csv", "payouts.csv"):
            stem = file_name.replace(".csv", "")
            value = files.get(file_name, {}).get("rows", files.get(stem, {}).get("rows"))
            if value is None:
                continue
            try:
                counts[stem] = int(value)
            except Exception:
                counts[stem] = counts.get(stem, 0)

    for file_name in ("races.csv", "entries.csv", "odds.csv", "results.csv", "payouts.csv"):
        stem = file_name.replace(".csv", "")
        if stem in counts:
            continue
        path = raw_dir / file_name
        if not path.exists():
            counts[stem] = 0
            continue
        try:
            counts[stem] = int(len(pd.read_csv(path, encoding="utf-8", low_memory=False)))
        except Exception:
            counts[stem] = 0
    return counts


def _has_prediction_minimum_rows(counts: dict[str, int]) -> bool:
    return int(counts.get("races", 0)) > 0 and int(counts.get("entries", 0)) > 0


def _validate_raw_prediction_quality(raw_dir: Path) -> dict[str, Any]:
    races_path = raw_dir / "races.csv"
    entries_path = raw_dir / "entries.csv"
    if not races_path.exists() or not entries_path.exists():
        return {"ok": False, "reason": "missing_raw_files"}

    try:
        races = pd.read_csv(races_path, encoding="utf-8", low_memory=False)
        entries = pd.read_csv(entries_path, encoding="utf-8", low_memory=False)
    except Exception as exc:
        return {"ok": False, "reason": "raw_csv_read_failed", "error": str(exc)}

    required_race_cols = {"race_id", "field_size", "distance"}
    if not required_race_cols.issubset(races.columns):
        return {
            "ok": False,
            "reason": "raw_races_missing_columns",
            "missing_columns": sorted(required_race_cols - set(races.columns)),
        }

    horse_no_col = "horse_no" if "horse_no" in entries.columns else ("umaban" if "umaban" in entries.columns else "")
    if "race_id" not in entries.columns or not horse_no_col:
        return {"ok": False, "reason": "raw_entries_missing_columns"}

    races = races.copy()
    entries = entries.copy()
    races["field_size"] = pd.to_numeric(races["field_size"], errors="coerce")
    races["distance"] = pd.to_numeric(races["distance"], errors="coerce")
    entry_counts = entries.groupby("race_id", dropna=False)[horse_no_col].count().rename("entry_count").reset_index()
    merged = races.merge(entry_counts, on="race_id", how="left")
    merged["entry_count"] = merged["entry_count"].fillna(0).astype(int)
    bad = merged[
        merged["field_size"].isna()
        | (merged["field_size"] <= 0)
        | (merged["field_size"] > 18)
        | merged["distance"].isna()
        | (merged["distance"] <= 0)
        | (merged["field_size"].astype("Int64") != merged["entry_count"].astype("Int64"))
    ].copy()
    if bad.empty:
        return {"ok": True, "bad_race_count": 0}
    cols = [c for c in ["race_id", "field_size", "entry_count", "distance", "surface"] if c in bad.columns]
    return {
        "ok": False,
        "reason": "raw_quality_failed",
        "bad_race_count": int(len(bad)),
        "bad_races": bad[cols].head(20).to_dict(orient="records"),
    }


def _jv_auto_raw_dir(today_date: str) -> Path:
    return RAW_ROOT / f"{today_date.replace('-', '')}_jv_auto"


def _run_jv_direct_exporter(today_date: str) -> dict[str, Any]:
    raw_dir = _jv_auto_raw_dir(today_date)
    raw_dir.mkdir(parents=True, exist_ok=True)
    if not JV_DIRECT_PROJECT.exists():
        return {
            "strategy": "jv_direct_exporter",
            "status": "skipped",
            "raw_dir": str(raw_dir),
            "message": f"JV-Link direct exporter project not found: {JV_DIRECT_PROJECT}",
            "row_counts": {},
        }

    cmd = [
        "dotnet",
        "run",
        "--project",
        str(JV_DIRECT_PROJECT),
        "-r",
        "win-x86",
        "--",
        "--race-date",
        today_date,
        "--output-dir",
        str(raw_dir),
        "--overwrite",
        "--verbose",
    ]
    if JV_DATA_DIR:
        cmd.extend(["--jv-data-dir", JV_DATA_DIR])
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT / "racing_ai"),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=JV_DIRECT_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        return {
            "strategy": "jv_direct_exporter",
            "status": "timeout",
            "raw_dir": str(raw_dir),
            "timeout_sec": JV_DIRECT_TIMEOUT_SEC,
            "row_counts": _raw_row_counts(raw_dir),
        }
    except FileNotFoundError:
        return {
            "strategy": "jv_direct_exporter",
            "status": "failed",
            "raw_dir": str(raw_dir),
            "message": "dotnet executable not found.",
            "row_counts": {},
        }

    report, inspect_proc = _inspect_raw_dir(raw_dir)
    counts = _raw_row_counts(raw_dir, report)
    manifest = _raw_manifest(raw_dir)
    manifest_warnings = [str(item) for item in manifest.get("warnings", [])]
    stdout_tail = proc.stdout[-2000:]
    stderr_tail = proc.stderr[-2000:]
    # JV-Link sometimes returns placeholder/partial data when the target date isn't fully downloaded.
    # Treat this as "not ready" so we don't produce mismatched race cards (e.g., wrong field size).
    not_ready_markers = [
        "JVRead/JVGets returned rc=-1 with empty buffer",
        "No readable data was returned",
        "target date not downloaded yet",
        "data state not ready",
    ]
    is_not_ready = any(marker in proc.stdout for marker in not_ready_markers) or any(
        marker in proc.stderr for marker in not_ready_markers
    )
    target_date_missing = any(w == "target_date_records_missing" for w in manifest_warnings)
    has_minimum_rows = _has_prediction_minimum_rows(counts)
    warnings = []
    if is_not_ready:
        warnings.append("jv_read_empty_buffer")
    warnings.extend(manifest_warnings)
    return {
        "strategy": "jv_direct_exporter",
        "status": "not_ready" if (is_not_ready or target_date_missing) and not has_minimum_rows else ("ok" if proc.returncode == 0 else "failed"),
        "raw_dir": str(raw_dir),
        "return_code": proc.returncode,
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
        "warnings": warnings,
        "inspect_return_code": inspect_proc.returncode,
        "inspect_stdout_tail": inspect_proc.stdout[-1000:],
        "row_counts": counts,
    }


def _convert_raw_to_inputs(
    today_date: str,
    *,
    mode: str,
    raw_dir: Path,
    data_source: str,
    attempts: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    report, inspect_proc = _inspect_raw_dir(raw_dir)
    counts = _raw_row_counts(raw_dir, report)
    attempts.append(
        {
            "strategy": data_source,
            "step": "inspect_raw",
            "raw_dir": str(raw_dir),
            "return_code": inspect_proc.returncode,
            "row_counts": counts,
        }
    )
    if inspect_proc.returncode != 0:
        attempts[-1]["stderr_tail"] = inspect_proc.stderr[-1000:]
        return None
    if not _has_prediction_minimum_rows(counts):
        attempts[-1]["status"] = "skipped_zero_required_rows"
        return None
    quality = _validate_raw_prediction_quality(raw_dir)
    attempts[-1]["quality"] = quality
    if not quality.get("ok"):
        attempts[-1]["status"] = "skipped_raw_quality_failed"
        return None

    snapshot_version = raw_dir.name
    convert_proc = _run_aikeiba_cli(
        [
            "jv-file-pipeline",
            "--db-path",
            str(DB_PATH),
            "--raw-dir",
            str(raw_dir),
            "--normalized-root",
            str(NORMALIZED_ROOT),
            "--race-date",
            today_date,
            "--snapshot-version",
            snapshot_version,
        ]
    )
    attempts.append(
        {
            "strategy": data_source,
            "step": "normalize",
            "raw_dir": str(raw_dir),
            "snapshot_version": snapshot_version,
            "return_code": convert_proc.returncode,
            "stdout_tail": convert_proc.stdout[-1000:],
            "stderr_tail": convert_proc.stderr[-1000:],
        }
    )
    if convert_proc.returncode != 0:
        return None

    generated = _find_input_set_for_date(today_date, mode=mode)
    if not generated:
        attempts.append(
            {
                "strategy": data_source,
                "step": "find_normalized_after_convert",
                "status": "missing",
                "raw_dir": str(raw_dir),
                "snapshot_version": snapshot_version,
            }
        )
        return None

    summary = _input_set_summary(generated)
    if summary["normalized_races_rows"] <= 0 or summary["normalized_entries_rows"] <= 0:
        attempts.append(
            {
                "strategy": data_source,
                "step": "validate_normalized",
                "status": "skipped_zero_required_rows",
                "normalized_dir": summary["normalized_dir"],
            }
        )
        return None

    summary.update(
        {
            "normalized_auto_built": True,
            "raw_dir_used": str(raw_dir),
            "snapshot_version": snapshot_version,
            "target_export_dir": None,
            "target_export_rows": {},
            "target_export_stdout_tail": "",
            "build_raw_stdout_tail": "",
            "parse_stdout_tail": inspect_proc.stdout[-1000:],
            "convert_stdout_tail": convert_proc.stdout[-1000:],
            "data_source": data_source,
            "strategy_selected": data_source,
            "strategy_attempts": attempts,
            "raw_row_counts": counts,
        }
    )
    return generated, summary


def _target_export_dir(today_date: str) -> Path:
    compact = today_date.replace("-", "")
    return TARGET_EXPORT_ROOT / compact


def _run_target_export_ahk(today_date: str, *, mode: str) -> dict[str, Any]:
    if not TARGET_AHK_SCRIPT.exists():
        raise HTTPException(status_code=500, detail=f"AHK script not found: {TARGET_AHK_SCRIPT}")

    export_dir = _target_export_dir(today_date)
    export_dir.mkdir(parents=True, exist_ok=True)
    target_exe = os.getenv("AIIKEIBA_TARGET_EXE_PATH", "").strip()
    cmd = [TARGET_AHK_EXE, str(TARGET_AHK_SCRIPT), today_date, str(export_dir), "--silent"]
    if TARGET_AHK_MANUAL_MENU:
        cmd.append("--manual-menu")
    if target_exe:
        cmd.append(target_exe)

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=TARGET_AHK_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "TARGET CSV export timeout.",
                "timeout_sec": TARGET_AHK_TIMEOUT_SEC,
                "script": str(TARGET_AHK_SCRIPT),
            },
        ) from None
    except FileNotFoundError:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "AutoHotkey executable not found.",
                "expected_exe": TARGET_AHK_EXE,
                "hint": "Set AIIKEIBA_AHK_EXE to full path (e.g. C:\\Program Files\\AutoHotkey\\AutoHotkey.exe).",
            },
        ) from None

    if proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "TARGET CSV export failed.",
                "return_code": proc.returncode,
                "stdout": proc.stdout[-4000:],
                "stderr": proc.stderr[-4000:],
                "script": str(TARGET_AHK_SCRIPT),
            },
        )

    required_files, optional_files = _mode_file_rules(mode)
    deadline = time.time() + TARGET_EXPORT_WAIT_SEC
    while time.time() <= deadline:
        missing = [name for name in required_files if not (export_dir / name).exists()]
        if not missing:
            break
        time.sleep(1)
    else:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "TARGET export files were not found before timeout.",
                "export_dir": str(export_dir),
                "missing_files": [name for name in required_files if not (export_dir / name).exists()],
                "wait_sec": TARGET_EXPORT_WAIT_SEC,
                "mode": mode,
            },
        )

    file_rows: dict[str, int] = {}
    all_files = tuple(dict.fromkeys(required_files + optional_files))
    for name in all_files:
        path = export_dir / name
        if not path.exists():
            file_rows[name] = -1
            continue
        try:
            file_rows[name] = int(len(pd.read_csv(path, encoding="utf-8", low_memory=False)))
        except Exception:
            file_rows[name] = -1

    return {
        "target_export_dir": str(export_dir),
        "target_export_files": list(all_files),
        "target_export_rows": file_rows,
        "target_export_stdout_tail": proc.stdout[-1000:],
        "required_files": list(required_files),
        "optional_files": list(optional_files),
        "missing_required": [name for name in required_files if not (export_dir / name).exists()],
        "missing_optional": [name for name in optional_files if not (export_dir / name).exists()],
    }


def _build_raw_from_target_export(today_date: str, target_export_dir: Path) -> dict[str, Any]:
    raw_dir = RAW_ROOT / f"{today_date.replace('-', '')}_real"
    cmd = [
        "build-real-raw-from-jv",
        "--source-dir",
        str(target_export_dir),
        "--target-date",
        today_date,
        "--out-raw-dir",
        str(raw_dir),
    ]
    proc = _run_aikeiba_cli(cmd)
    if proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "build-real-raw-from-jv failed.",
                "stdout": proc.stdout[-4000:],
                "stderr": proc.stderr[-4000:],
                "return_code": proc.returncode,
                "target_export_dir": str(target_export_dir),
            },
        )

    inspect = _run_aikeiba_cli(["inspect-raw-dir", "--raw-dir", str(raw_dir)])
    if inspect.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "inspect-raw-dir failed after build-real-raw-from-jv.",
                "raw_dir": str(raw_dir),
                "stdout": inspect.stdout[-4000:],
                "stderr": inspect.stderr[-4000:],
                "return_code": inspect.returncode,
            },
        )

    inspect_report: dict[str, Any] = {}
    try:
        inspect_report = json.loads(inspect.stdout)
    except Exception:
        pass

    races_rows = int(inspect_report.get("files", {}).get("races.csv", {}).get("rows", 0))
    entries_rows = int(inspect_report.get("files", {}).get("entries.csv", {}).get("rows", 0))
    if races_rows <= 0 or entries_rows <= 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Raw conversion completed but rows are zero.",
                "raw_dir": str(raw_dir),
                "races_rows": races_rows,
                "entries_rows": entries_rows,
                "inspect_report": inspect_report,
            },
        )

    return {
        "raw_dir": str(raw_dir),
        "raw_inspect_report": inspect_report,
        "build_raw_stdout_tail": proc.stdout[-1000:],
    }


def _ensure_prediction_stub_files(target_export_dir: Path) -> None:
    stubs: dict[str, list[str]] = {
        "results.csv": ["race_id", "horse_no", "finish_position"],
        "payouts.csv": ["race_id", "bet_type", "bet_key", "payout"],
    }
    for name, headers in stubs.items():
        path = target_export_dir / name
        if path.exists():
            continue
        pd.DataFrame(columns=headers).to_csv(path, index=False, encoding="utf-8")


def _ensure_inputs_from_existing_data(today_date: str, *, mode: str) -> tuple[dict[str, Any], dict[str, Any]]:
    existing = _find_input_set_for_date(today_date, mode=mode)
    if existing:
        summary = _input_set_summary(existing)
        summary.update(
            {
                "normalized_auto_built": False,
                "raw_dir_used": None,
                "snapshot_version": None,
                "target_export_dir": None,
                "target_export_rows": {},
                "target_export_stdout_tail": "",
                "build_raw_stdout_tail": "",
                "parse_stdout_tail": "",
                "convert_stdout_tail": "",
            }
        )
        return existing, summary

    raw_candidates = _find_raw_dirs_for_date(today_date)
    if not raw_candidates:
        raise HTTPException(
            status_code=400,
            detail={
                "message": f"No normalized input set for today_date={today_date}",
                "hint": "No raw TARGET/JRA-VAN files found. Export latest data first.",
                "available_normalized_dates": _available_dates(limit=20),
                "available_raw_dates": _available_raw_dates(limit=20),
            },
        )

    raw_dir = raw_candidates[0]
    snapshot_version = raw_dir.name
    parse_proc = _run_aikeiba_cli(["inspect-raw-dir", "--raw-dir", str(raw_dir)])
    if parse_proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "TARGET parse step failed (inspect-raw-dir).",
                "raw_dir": str(raw_dir),
                "stdout": parse_proc.stdout[-4000:],
                "stderr": parse_proc.stderr[-4000:],
                "return_code": parse_proc.returncode,
            },
        )

    convert_proc = _run_aikeiba_cli(
        [
            "jv-file-pipeline",
            "--db-path",
            str(DB_PATH),
            "--raw-dir",
            str(raw_dir),
            "--normalized-root",
            str(NORMALIZED_ROOT),
            "--race-date",
            today_date,
            "--snapshot-version",
            snapshot_version,
        ]
    )
    if convert_proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "normalize/ingest conversion failed from existing raw data.",
                "raw_dir": str(raw_dir),
                "snapshot_version": snapshot_version,
                "stdout": convert_proc.stdout[-4000:],
                "stderr": convert_proc.stderr[-4000:],
                "return_code": convert_proc.returncode,
            },
        )

    generated = _find_input_set_for_date(today_date, mode=mode)
    if not generated:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Conversion finished, but normalized files for today_date were not created.",
                "today_date": today_date,
                "raw_dir": str(raw_dir),
                "snapshot_version": snapshot_version,
                "available_normalized_dates": _available_dates(limit=20),
            },
        )

    summary = _input_set_summary(generated)
    summary.update(
        {
            "normalized_auto_built": True,
            "raw_dir_used": str(raw_dir),
            "snapshot_version": snapshot_version,
            "target_export_dir": None,
            "target_export_rows": {},
            "target_export_stdout_tail": "",
            "build_raw_stdout_tail": "",
            "parse_stdout_tail": parse_proc.stdout[-1000:],
            "convert_stdout_tail": convert_proc.stdout[-1000:],
        }
    )
    return generated, summary


def _input_set_summary(inputs: dict[str, Any]) -> dict[str, Any]:
    paths = inputs["paths"]
    entries = paths["entries.csv"]
    races = paths["races.csv"]
    odds = paths.get("odds.csv")
    entries_rows = int(len(pd.read_csv(entries, encoding="utf-8", low_memory=False)))
    races_rows = int(len(pd.read_csv(races, encoding="utf-8", low_memory=False)))
    odds_exists = bool(odds and odds.exists())
    odds_rows = int(len(pd.read_csv(odds, encoding="utf-8", low_memory=False))) if odds_exists else 0
    return {
        "normalized_dir": str(inputs["normalized_dir"]),
        "normalized_entries_rows": entries_rows,
        "normalized_races_rows": races_rows,
        "normalized_odds_exists": bool(odds_exists),
        "normalized_odds_rows": odds_rows,
        "required_files": list(inputs.get("required_files", [])),
        "optional_files": list(inputs.get("optional_files", [])),
        "missing_required": list(inputs.get("missing_required", [])),
        "missing_optional": list(inputs.get("missing_optional", [])),
    }


def _ensure_inputs_for_date(today_date: str, *, mode: str) -> tuple[dict[str, Any], dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    allow_cached_inputs = mode == "backtest" or ALLOW_PREDICTION_CACHE

    existing = _find_input_set_for_date(today_date, mode=mode)
    if existing and allow_cached_inputs:
        summary = _input_set_summary(existing)
        attempts.append(
            {
                "strategy": "existing_normalized",
                "normalized_dir": summary["normalized_dir"],
                "entries_rows": summary["normalized_entries_rows"],
                "races_rows": summary["normalized_races_rows"],
            }
        )
        if summary["normalized_races_rows"] > 0 and summary["normalized_entries_rows"] > 0:
            summary.update(
                {
                    "normalized_auto_built": False,
                    "raw_dir_used": None,
                    "snapshot_version": None,
                    "target_export_dir": None,
                    "target_export_rows": {},
                    "target_export_stdout_tail": "",
                    "build_raw_stdout_tail": "",
                    "parse_stdout_tail": "",
                    "convert_stdout_tail": "",
                    "data_source": "existing_normalized",
                    "strategy_selected": "existing_normalized",
                    "strategy_attempts": attempts,
                    "raw_row_counts": {},
                }
            )
            return existing, summary
        attempts[-1]["status"] = "skipped_zero_required_rows"
    elif existing:
        attempts.append(
            {
                "strategy": "existing_normalized",
                "normalized_dir": str(existing["normalized_dir"]),
                "status": "skipped_prediction_cache_disabled",
            }
        )

    if allow_cached_inputs:
        for raw_dir in _find_raw_dirs_for_date(today_date):
            converted = _convert_raw_to_inputs(
                today_date,
                mode=mode,
                raw_dir=raw_dir,
                data_source="existing_raw",
                attempts=attempts,
            )
            if converted:
                return converted
    else:
        raw_dirs = _find_raw_dirs_for_date(today_date)
        if raw_dirs:
            attempts.append(
                {
                    "strategy": "existing_raw",
                    "raw_dirs": [str(path) for path in raw_dirs[:5]],
                    "status": "skipped_prediction_cache_disabled",
                }
            )

    jv_export = _run_jv_direct_exporter(today_date)
    attempts.append(jv_export)
    jv_counts = dict(jv_export.get("row_counts", {}))
    if jv_export.get("status") == "ok" and _has_prediction_minimum_rows(jv_counts):
        converted = _convert_raw_to_inputs(
            today_date,
            mode=mode,
            raw_dir=Path(str(jv_export["raw_dir"])),
            data_source="jv_direct_exporter",
            attempts=attempts,
        )
        if converted:
            return converted
    elif jv_export.get("status") == "ok":
        attempts[-1]["status"] = "skipped_zero_required_rows"
    elif jv_export.get("status") == "not_ready":
        attempts[-1]["status"] = "not_ready"

    auto_fallback_needed = mode == "prediction" and jv_export.get("status") == "not_ready"
    try_target_fallback = (ENABLE_TARGET_AHK_FALLBACK or auto_fallback_needed) and (not SKIP_TARGET_AHK)
    if try_target_fallback:
        try:
            target_export = _run_target_export_ahk(today_date, mode=mode)
            target_export_dir = Path(str(target_export["target_export_dir"]))
            if mode == "prediction":
                _ensure_prediction_stub_files(target_export_dir)
            raw_report = _build_raw_from_target_export(today_date=today_date, target_export_dir=target_export_dir)
            converted = _convert_raw_to_inputs(
                today_date,
                mode=mode,
                raw_dir=Path(str(raw_report["raw_dir"])),
                data_source="target_ahk_fallback",
                attempts=attempts,
            )
            if converted:
                inputs, summary = converted
                summary.update(
                    {
                        "target_export_dir": str(target_export_dir),
                        "target_export_rows": target_export.get("target_export_rows", {}),
                        "target_export_stdout_tail": target_export.get("target_export_stdout_tail", ""),
                        "build_raw_stdout_tail": raw_report.get("build_raw_stdout_tail", ""),
                    }
                )
                return inputs, summary
        except HTTPException as fallback_error:
            attempts.append(
                {
                    "strategy": "target_ahk_fallback",
                    "status": "failed",
                    "detail": fallback_error.detail,
                    "auto_fallback_needed": auto_fallback_needed,
                }
            )

    fallback_failed_detail: dict[str, Any] | None = None
    for attempt in reversed(attempts):
        if attempt.get("strategy") == "target_ahk_fallback" and attempt.get("status") == "failed":
            raw_detail = attempt.get("detail")
            if isinstance(raw_detail, dict):
                fallback_failed_detail = raw_detail
            else:
                fallback_failed_detail = {"detail": raw_detail}
            break

    warning_set = {
        str(warning)
        for attempt in attempts
        for warning in attempt.get("warnings", [])
    }
    available_record_dates = sorted(
        warning.removeprefix("available_record_dates=")
        for warning in warning_set
        if warning.startswith("available_record_dates=")
    )
    if fallback_failed_detail is not None:
        message = "JV target-date mismatch and TARGET CSV fallback failed"
        hint = "Check TARGET export flow and retry. See fallback_error for the concrete failure reason."
    elif "target_date_records_missing" in warning_set:
        message = f"JV-Link returned records, but none matched today_date={today_date}"
        hint = (
            "The returned JV-Link stream appears to contain a different race date. "
            "Prediction was stopped to avoid wrong horse numbers. "
            "In TARGET/JRA-VAN, download the JV速報/出馬表 data for the exact target date, then run update again."
        )
    else:
        message = f"No usable input data for today_date={today_date}"
        hint = (
            "JV-Link data for the date may be not ready. In TARGET, run File > JV速報データの一括ダウンロード "
            "(Shift+F5), then press update again. External index output is not enough for JV-Link direct export."
        )

    raise HTTPException(
        status_code=400,
        detail={
            "message": message,
            "hint": hint,
            "mode": mode,
            "available_record_dates": available_record_dates,
            "fallback_error": fallback_failed_detail,
            "available_normalized_dates": _available_dates(limit=20),
            "available_raw_dates": _available_raw_dates(limit=20),
            "strategy_attempts": attempts,
        },
    )


def _parse_race_no(race_id: str) -> int | None:
    matched = re.search(r"(\d+)R$", race_id)
    if not matched:
        return None
    return int(matched.group(1))


def _is_garbled_name(name: str) -> bool:
    text = (name or "").strip()
    if not text:
        return True
    bad_chars = sum(1 for c in text if c in "?@�")
    return (bad_chars / max(len(text), 1)) >= 0.2


def _load_horse_name_map(entries_path: Path | None) -> dict[tuple[str, str], str]:
    if not entries_path or not entries_path.exists():
        return {}
    name_master = _load_horse_name_master() if ENABLE_MASTER_NAME_FALLBACK else {}
    for enc in ("cp932", "utf-8-sig", "utf-8"):
        try:
            entries = pd.read_csv(entries_path, encoding=enc, low_memory=False)
            break
        except Exception:
            entries = None
    if entries is None or entries.empty:
        return {}
    required = {"race_id", "horse_no", "horse_name"}
    if not required.issubset(entries.columns):
        return {}
    out: dict[tuple[str, str], str] = {}
    cols = ["race_id", "horse_no", "horse_name"]
    has_horse_id = "horse_id" in entries.columns
    if has_horse_id:
        cols.append("horse_id")
    for row in entries[cols].dropna(subset=["race_id", "horse_no"]).itertuples(index=False):
        key = (str(row.race_id), str(int(float(row.horse_no))))
        name = str(row.horse_name).strip() if pd.notna(row.horse_name) else ""
        if _is_garbled_name(name):
            if has_horse_id and ENABLE_MASTER_NAME_FALLBACK:
                horse_id = str(getattr(row, "horse_id", "")).strip().replace(".0", "")
                name = name_master.get(horse_id, "")
            else:
                name = ""
        out[key] = name
    return out


def _load_horse_name_master() -> dict[str, str]:
    if not HISTORY_DATASET_PATH.exists():
        return {}
    for enc in ("cp932", "utf-8-sig", "utf-8"):
        try:
            base = pd.read_csv(HISTORY_DATASET_PATH, encoding=enc, usecols=["horse_id", "horse_name"], low_memory=False)
            break
        except Exception:
            base = None
    if base is None or base.empty:
        return {}
    base = base.dropna(subset=["horse_id", "horse_name"]).copy()
    base["horse_id"] = base["horse_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    base["horse_name"] = base["horse_name"].astype(str).str.strip()
    base = base[~base["horse_name"].map(_is_garbled_name)]
    return base.drop_duplicates(subset=["horse_id"], keep="last").set_index("horse_id")["horse_name"].to_dict()


def _load_horse_name_overrides() -> dict[tuple[str, str, str], str]:
    if not HORSE_NAME_OVERRIDE_PATH.exists():
        return {}
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            df = pd.read_csv(HORSE_NAME_OVERRIDE_PATH, encoding=enc, low_memory=False)
            break
        except Exception:
            df = None
    if df is None or df.empty:
        return {}
    required = {"race_id", "horse_no", "horse_name"}
    if not required.issubset(df.columns):
        return {}
    out: dict[tuple[str, str, str], str] = {}
    for row in df[list(required)].dropna(subset=["race_id", "horse_no"]).itertuples(index=False):
        race_id = str(row.race_id).strip()
        horse_no = str(int(float(row.horse_no)))
        horse_name = str(row.horse_name).strip() if pd.notna(row.horse_name) else ""
        horse_id = ""
        if "horse_id" in df.columns:
            try:
                raw_horse_id = getattr(row, "horse_id")
                if pd.notna(raw_horse_id):
                    horse_id = str(int(float(raw_horse_id)))
            except Exception:
                horse_id = ""
        if horse_name:
            out[(race_id, horse_no, horse_id)] = horse_name
    return out


def _build_races_today_json(
    df: pd.DataFrame,
    horse_name_map: dict[tuple[str, str], str] | None = None,
    horse_id_map: dict[tuple[str, str], str] | None = None,
    verified_name_map: dict[tuple[str, str, str], str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    horse_name_map = horse_name_map or {}
    horse_id_map = horse_id_map or {}
    verified_name_map = verified_name_map or {}
    pair_df = (
        df.sort_values(["race_id", "value_score_rank", "horse_no"], ascending=[True, True, True], kind="mergesort")
        .groupby("race_id", dropna=False)
        .head(2)
    )
    pair_map: dict[str, str] = {}
    pair_name_map: dict[str, str] = {}
    pair_id_map: dict[str, str] = {}
    pair_name_status_map: dict[str, str] = {}
    for race_id, g in pair_df.groupby("race_id", dropna=False):
        race_id_s = str(race_id)
        horses = g["horse_no"].dropna().astype(int).astype(str).tolist()
        pair_map[str(race_id)] = "-".join(horses) if len(horses) >= 2 else (horses[0] if horses else "-")
        horse_ids = [horse_id_map.get((race_id_s, horse_no), "") for horse_no in horses]
        pair_id_map[race_id_s] = "-".join([horse_id for horse_id in horse_ids if horse_id]) if horse_ids else "-"
        verified_names: list[str] = []
        for horse_no, horse_id in zip(horses, horse_ids):
            verified_name = verified_name_map.get((race_id_s, horse_no, horse_id))
            if verified_name:
                verified_names.append(verified_name)
        if len(verified_names) >= 2:
            pair_name_map[race_id_s] = " - ".join(verified_names[:2])
            pair_name_status_map[race_id_s] = "verified"
        else:
            pair_name_map[race_id_s] = "-"
            pair_name_status_map[race_id_s] = "unverified"

    reason_map = (
        df.sort_values(["race_id", "value_score_rank"], ascending=[True, True], kind="mergesort")
        .drop_duplicates(subset=["race_id"], keep="first")
        .set_index("race_id")["selection_reason"]
        .astype(str)
        .to_dict()
    )
    cutoff_map = df.groupby("race_id", dropna=False)["odds_cutoff"].first().astype(str).to_dict()
    pair_score_map = df.groupby("race_id", dropna=False)["pair_score"].max().to_dict() if "pair_score" in df.columns else {}
    pair_value_map = (
        df.groupby("race_id", dropna=False)["pair_value_score"].max().to_dict() if "pair_value_score" in df.columns else {}
    )

    race_df = (
        df.groupby("race_id", dropna=False)
        .agg(
            buy_flag=("race_selected_top15", lambda s: bool(s.fillna(False).any())),
            field_size=("horse_no", "count"),
            recommendation_src=("value_score", "max"),
            candidate_source=("selection_reason", lambda s: int((s.str.contains("candidate", na=False)).sum())),
            expected_roi=("pred_top3", lambda s: float(s.nlargest(2).prod()) if len(s) >= 2 else float("nan")),
            ai_market_gap=("ability_gap", "mean"),
        )
        .reset_index()
        .sort_values("race_id", ascending=True, kind="mergesort")
    )
    rec_min = race_df["recommendation_src"].min()
    rec_max = race_df["recommendation_src"].max()
    for row in race_df.itertuples(index=False):
        race_id = str(row.race_id)
        if pd.notna(rec_min) and pd.notna(rec_max) and rec_max > rec_min:
            recommendation = int(
                round(100 * (float(row.recommendation_src) - float(rec_min)) / (float(rec_max) - float(rec_min)))
            )
        else:
            recommendation = 50
        rows.append(
            {
                "race_id": race_id,
                "venue": race_id.split("-")[1] if "-" in race_id and len(race_id.split("-")) >= 2 else "-",
                "venue_name": race_id.split("-")[1] if "-" in race_id and len(race_id.split("-")) >= 2 else "-",
                "race_no": _parse_race_no(race_id),
                "post_time": None,
                "condition": None,
                "field_size": int(row.field_size),
                "buy_flag": bool(row.buy_flag),
                "recommendation": recommendation,
                "candidate_pairs": int(row.candidate_source),
                "expected_roi": None if pd.isna(row.expected_roi) else float(row.expected_roi),
                "ai_market_gap": None if pd.isna(row.ai_market_gap) else float(row.ai_market_gap),
                "top_pair": pair_map.get(race_id, "-"),
                "top_pair_horse_ids": pair_id_map.get(race_id, "-"),
                "top_pair_horse_names": pair_name_map.get(race_id, "-"),
                "top_pair_horse_name_status": pair_name_status_map.get(race_id, "unverified"),
                "selection_reason": reason_map.get(race_id, "-"),
                "odds_cutoff": cutoff_map.get(race_id, "-"),
                "pair_score": None if pd.isna(pair_score_map.get(race_id)) else float(pair_score_map.get(race_id)),
                "pair_value_score": None
                if pd.isna(pair_value_map.get(race_id))
                else float(pair_value_map.get(race_id)),
                "density_top3": None,
                "gap12": None,
                "chaos_index": None,
                "track": None,
                "surface": None,
            }
        )
    return rows


def _run_pipeline(payload: RunTodayRequest, inputs: dict[str, Any] | None = None) -> subprocess.CompletedProcess[str]:
    if not PIPELINE_SCRIPT.exists():
        raise HTTPException(status_code=500, detail=f"Pipeline script not found: {PIPELINE_SCRIPT}")

    cmd = [
        sys.executable,
        str(PIPELINE_SCRIPT),
        "--today-date",
        payload.today_date,
        "--odds-cutoff",
        payload.odds_cutoff,
        "--out",
        str(PIPELINE_OUTPUT),
        "--log-out",
        str(PIPELINE_LOG),
        "--odds-latest",
        str(ODDS_LATEST_PATH),
    ]
    if inputs:
        paths = inputs["paths"]
        cmd.extend(["--entries", str(paths["entries.csv"]), "--races", str(paths["races.csv"])])
        odds_path = paths.get("odds.csv")
        if odds_path and Path(odds_path).exists():
            cmd.extend(["--odds", str(odds_path)])
    return subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )


def _fetch_latest_odds(today_date: str) -> subprocess.CompletedProcess[str]:
    if not FETCH_ODDS_SCRIPT.exists():
        raise HTTPException(status_code=500, detail=f"Odds fetch script not found: {FETCH_ODDS_SCRIPT}")
    cmd = [
        sys.executable,
        str(FETCH_ODDS_SCRIPT),
        "--normalized-root",
        str(NORMALIZED_ROOT),
        "--today-date",
        today_date,
        "--output",
        str(ODDS_LATEST_PATH),
        "--report-out",
        str(ODDS_REPORT_PATH),
    ]
    return subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )


def _parse_cutoff_utc(odds_cutoff: str) -> pd.Timestamp:
    ts = pd.to_datetime(odds_cutoff, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if pd.isna(ts):
        raise HTTPException(status_code=400, detail="odds_cutoff must be YYYY-MM-DD HH:MM:SS")
    return ts.tz_localize("Asia/Tokyo").tz_convert("UTC")


def _validate_latest_odds(odds_cutoff: str) -> dict[str, Any]:
    if not ODDS_LATEST_PATH.exists():
        raise HTTPException(status_code=500, detail=f"odds_latest.csv not found: {ODDS_LATEST_PATH}")
    df = pd.read_csv(ODDS_LATEST_PATH, encoding="utf-8", low_memory=False)
    if len(df) == 0:
        raise HTTPException(status_code=500, detail="odds_latest.csv is empty.")

    required_cols = {"captured_at", "odds_type", "horse_no", "odds_value"}
    if not required_cols.issubset(set(df.columns)):
        raise HTTPException(status_code=500, detail=f"odds_latest.csv missing columns: {sorted(required_cols - set(df.columns))}")

    df["captured_at"] = pd.to_datetime(df["captured_at"], errors="coerce", utc=True)
    df["odds_value"] = pd.to_numeric(df["odds_value"], errors="coerce")
    df["horse_no"] = pd.to_numeric(df["horse_no"], errors="coerce")
    df["odds_type"] = df["odds_type"].astype(str).str.lower()
    df = df[df["captured_at"].notna() & df["odds_value"].notna() & df["horse_no"].notna() & (df["odds_type"] == "win")].copy()
    if len(df) == 0:
        raise HTTPException(status_code=500, detail="No valid win odds in odds_latest.csv.")

    cutoff_utc = _parse_cutoff_utc(odds_cutoff)
    valid = df[df["captured_at"] <= cutoff_utc].copy()
    if len(valid) == 0:
        raise HTTPException(status_code=500, detail="No valid odds rows <= odds_cutoff in odds_latest.csv.")

    captured_at_max = df["captured_at"].max()
    mtime = datetime.fromtimestamp(ODDS_LATEST_PATH.stat().st_mtime).isoformat(timespec="seconds")
    warnings: list[str] = []
    if captured_at_max > cutoff_utc:
        warnings.append("odds_after_cutoff_excluded")
    return {
        "odds_latest_rows": int(len(df)),
        "odds_latest_valid_rows": int(len(valid)),
        "odds_latest_captured_at_max": captured_at_max.isoformat(),
        "odds_latest_file_mtime": mtime,
        "odds_warnings": warnings,
    }


def _append_odds_log(
    *,
    payload: RunTodayRequest,
    input_report: dict[str, Any],
    fetch_report: dict[str, Any],
    validate_report: dict[str, Any],
    fetch_stdout_tail: str,
    mode: str,
) -> None:
    lines = [
        f"dashboard_run_at={datetime.now().isoformat(timespec='seconds')}",
        f"mode={mode}",
        f"today_date={payload.today_date}",
        f"odds_cutoff={payload.odds_cutoff}",
        f"normalized_auto_built={input_report.get('normalized_auto_built')}",
        f"data_source={input_report.get('data_source')}",
        f"strategy_selected={input_report.get('strategy_selected')}",
        f"normalized_dir={input_report.get('normalized_dir')}",
        f"raw_dir_used={input_report.get('raw_dir_used')}",
        f"snapshot_version={input_report.get('snapshot_version')}",
        f"raw_row_counts={input_report.get('raw_row_counts')}",
        f"normalized_entries_rows={input_report.get('normalized_entries_rows')}",
        f"normalized_races_rows={input_report.get('normalized_races_rows')}",
        f"normalized_odds_exists={input_report.get('normalized_odds_exists')}",
        f"normalized_odds_rows={input_report.get('normalized_odds_rows')}",
        f"required_files={input_report.get('required_files')}",
        f"optional_files={input_report.get('optional_files')}",
        f"missing_required={input_report.get('missing_required')}",
        f"missing_optional={input_report.get('missing_optional')}",
        f"target_export_dir={input_report.get('target_export_dir')}",
        f"target_export_rows={input_report.get('target_export_rows')}",
        f"odds_latest_path={ODDS_LATEST_PATH}",
        f"odds_latest_rows={validate_report.get('odds_latest_rows')}",
        f"odds_latest_valid_rows={validate_report.get('odds_latest_valid_rows')}",
        f"odds_latest_captured_at_max={validate_report.get('odds_latest_captured_at_max')}",
        f"odds_latest_file_mtime={validate_report.get('odds_latest_file_mtime')}",
        f"normalized_odds_file_count={fetch_report.get('normalized_odds_file_count')}",
        f"normalized_odds_file_mtime_max={fetch_report.get('normalized_odds_file_mtime_max')}",
    ]
    warnings = list(fetch_report.get("warnings", [])) + list(validate_report.get("odds_warnings", []))
    if warnings:
        lines.append(f"odds_warnings={','.join(map(str, warnings))}")
    strategy_attempts = input_report.get("strategy_attempts")
    if strategy_attempts:
        lines.append("strategy_attempts=" + json.dumps(strategy_attempts, ensure_ascii=False)[:4000])
    parse_tail = str(input_report.get("parse_stdout_tail", ""))
    convert_tail = str(input_report.get("convert_stdout_tail", ""))
    target_export_tail = str(input_report.get("target_export_stdout_tail", ""))
    build_raw_tail = str(input_report.get("build_raw_stdout_tail", ""))
    if target_export_tail:
        lines.append("target_export_tail=" + target_export_tail.replace("\n", " | "))
    if build_raw_tail:
        lines.append("build_raw_tail=" + build_raw_tail.replace("\n", " | "))
    if parse_tail:
        lines.append("raw_parse_tail=" + parse_tail.replace("\n", " | "))
    if convert_tail:
        lines.append("normalized_build_tail=" + convert_tail.replace("\n", " | "))
    if fetch_stdout_tail:
        lines.append("odds_fetch_tail=" + fetch_stdout_tail.replace("\n", " | "))
    PIPELINE_LOG.parent.mkdir(parents=True, exist_ok=True)
    with PIPELINE_LOG.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def _refresh_races_json(inputs: dict[str, Any] | None = None) -> int:
    if not PIPELINE_OUTPUT.exists():
        raise HTTPException(status_code=500, detail=f"Pipeline output not found: {PIPELINE_OUTPUT}")
    pred = pd.read_csv(PIPELINE_OUTPUT, encoding="cp932", low_memory=False)
    entries_path: Path | None = None
    if inputs:
        maybe = inputs.get("paths", {}).get("entries.csv")
        if maybe:
            entries_path = Path(str(maybe))
    horse_name_map = _load_horse_name_map(entries_path)
    horse_id_map: dict[tuple[str, str], str] = {}
    if entries_path and entries_path.exists():
        for enc in ("cp932", "utf-8-sig", "utf-8"):
            try:
                entries = pd.read_csv(entries_path, encoding=enc, low_memory=False)
                break
            except Exception:
                entries = None
        if entries is not None and not entries.empty and {"race_id", "horse_no", "horse_id"}.issubset(entries.columns):
            for row in entries[["race_id", "horse_no", "horse_id"]].dropna(subset=["race_id", "horse_no"]).itertuples(index=False):
                try:
                    horse_id = str(int(float(row.horse_id))) if pd.notna(row.horse_id) else ""
                except Exception:
                    horse_id = ""
                horse_id_map[(str(row.race_id), str(int(float(row.horse_no))))] = horse_id
    verified_name_map = _load_horse_name_overrides()
    races = _build_races_today_json(
        pred,
        horse_name_map=horse_name_map,
        horse_id_map=horse_id_map,
        verified_name_map=verified_name_map,
    )
    RACES_TODAY_JSON.parent.mkdir(parents=True, exist_ok=True)
    RACES_TODAY_JSON.write_text(json.dumps(races, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(races)


@app.get("/health")
def health(request: Request) -> dict[str, Any]:
    _require_local_client(request)
    available = _available_dates(limit=5)
    return {
        "status": "ok",
        "local_only": True,
        "token_configured": bool(os.getenv(TOKEN_ENV_KEY, "").strip()),
        "allow_prediction_cache": ALLOW_PREDICTION_CACHE,
        "skip_target_ahk": SKIP_TARGET_AHK,
        "enable_target_ahk_fallback": ENABLE_TARGET_AHK_FALLBACK,
        "target_ahk_manual_menu": TARGET_AHK_MANUAL_MENU,
        "jv_direct_project_exists": JV_DIRECT_PROJECT.exists(),
        "jv_data_dir": JV_DATA_DIR or None,
        "latest_available_date": available[0] if available else None,
        "server_time": datetime.now().isoformat(timespec="seconds"),
    }


@app.get("/available-dates")
def available_dates(request: Request) -> dict[str, Any]:
    _require_local_client(request)
    return {"dates": _available_dates(limit=60)}


@app.post("/run-today")
def run_today(
    payload: RunTodayRequest,
    request: Request,
    authorization: str | None = Header(default=None),
    x_dashboard_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _authorize(request, authorization=authorization, x_dashboard_token=x_dashboard_token)
    mode = _normalize_mode(payload.mode)

    inputs, input_report = _ensure_inputs_for_date(payload.today_date, mode=mode)
    paths = inputs["paths"]
    missing_optional = list(input_report.get("missing_optional", []))
    odds_path = paths.get("odds.csv")
    odds_missing = ("odds.csv" in missing_optional) or (not odds_path) or (not Path(odds_path).exists())

    fetch_report: dict[str, Any] = {}
    validate_report: dict[str, Any] = {}
    odds_fetch_tail = ""

    if not odds_missing:
        prev_report = _read_json_file(ODDS_REPORT_PATH)
        odds_fetch = _fetch_latest_odds(payload.today_date)
        odds_fetch_tail = odds_fetch.stdout[-1000:]
        if odds_fetch.returncode != 0:
            if mode == "backtest":
                raise HTTPException(
                    status_code=500,
                    detail={
                        "message": "Latest odds fetch failed.",
                        "stdout": odds_fetch.stdout[-4000:],
                        "stderr": odds_fetch.stderr[-4000:],
                        "return_code": odds_fetch.returncode,
                        "mode": mode,
                    },
                )
            fetch_report = {"warnings": ["odds_fetch_failed_prediction_mode"]}
        else:
            fetch_report = _read_json_file(ODDS_REPORT_PATH)
            if prev_report and fetch_report:
                prev_mtime = str(prev_report.get("normalized_odds_file_mtime_max") or "")
                curr_mtime = str(fetch_report.get("normalized_odds_file_mtime_max") or "")
                if prev_mtime and curr_mtime and prev_mtime == curr_mtime:
                    warnings = list(fetch_report.get("warnings", []))
                    warnings.append("normalized_odds_csv_not_updated_since_previous_run")
                    fetch_report["warnings"] = warnings
            try:
                validate_report = _validate_latest_odds(payload.odds_cutoff)
            except HTTPException:
                if mode == "backtest":
                    raise
                warnings = list(fetch_report.get("warnings", []))
                warnings.append("odds_validation_failed_prediction_mode")
                fetch_report["warnings"] = warnings
                odds_missing = True
    else:
        fetch_report = {"warnings": ["odds_missing"]}

    if not validate_report:
        validate_report = {
            "odds_latest_rows": 0,
            "odds_latest_valid_rows": 0,
            "odds_latest_captured_at_max": None,
            "odds_latest_file_mtime": None,
            "odds_warnings": ["odds_missing"] if odds_missing else [],
        }

    run = _run_pipeline(payload, inputs=inputs)
    if run.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Pipeline failed.",
                "stdout": run.stdout[-4000:],
                "stderr": run.stderr[-4000:],
                "return_code": run.returncode,
                "mode": mode,
            },
        )

    _append_odds_log(
        payload=payload,
        input_report=input_report,
        fetch_report=fetch_report,
        validate_report=validate_report,
        fetch_stdout_tail=odds_fetch_tail,
        mode=mode,
    )

    race_count = _refresh_races_json(inputs=inputs)
    return {
        "status": "ok",
        "message": "Pipeline completed and races_today.json updated.",
        "mode": mode,
        "race_count": race_count,
        "normalized_auto_built": input_report.get("normalized_auto_built"),
        "data_source": input_report.get("data_source"),
        "strategy_selected": input_report.get("strategy_selected"),
        "strategy_attempts": input_report.get("strategy_attempts"),
        "normalized_dir": input_report.get("normalized_dir"),
        "raw_dir_used": input_report.get("raw_dir_used"),
        "snapshot_version": input_report.get("snapshot_version"),
        "raw_row_counts": input_report.get("raw_row_counts"),
        "normalized_entries_rows": input_report.get("normalized_entries_rows"),
        "normalized_races_rows": input_report.get("normalized_races_rows"),
        "normalized_odds_exists": input_report.get("normalized_odds_exists"),
        "normalized_odds_rows": input_report.get("normalized_odds_rows"),
        "required_files": input_report.get("required_files"),
        "optional_files": input_report.get("optional_files"),
        "missing_required": input_report.get("missing_required"),
        "missing_optional": input_report.get("missing_optional"),
        "odds_missing": odds_missing,
        "target_export_dir": input_report.get("target_export_dir"),
        "target_export_rows": input_report.get("target_export_rows"),
        "odds_latest_path": str(ODDS_LATEST_PATH),
        "odds_latest_rows": validate_report.get("odds_latest_rows"),
        "odds_latest_valid_rows": validate_report.get("odds_latest_valid_rows"),
        "odds_latest_captured_at_max": validate_report.get("odds_latest_captured_at_max"),
        "odds_latest_file_mtime": validate_report.get("odds_latest_file_mtime"),
        "normalized_odds_file_count": fetch_report.get("normalized_odds_file_count"),
        "odds_warnings": list(fetch_report.get("warnings", [])) + list(validate_report.get("odds_warnings", [])),
        "odds_fetch_tail": odds_fetch_tail,
        "stdout_tail": run.stdout[-2000:],
    }
