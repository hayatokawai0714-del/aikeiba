from __future__ import annotations

import datetime as dt
import json
import math
import uuid
from pathlib import Path
from typing import Any

RUN_SUMMARY_SCHEMA_VERSION = "run_summary.v1"
RUN_SUMMARY_SCHEMA_PATH = Path(__file__).with_name("run_summary_schema.json")
_SCHEMA_CACHE: dict[str, Any] | None = None

REQUIRED_STRING_KEYS: tuple[str, ...] = (
    "summary_schema_version",
    "created_at",
    "race_date",
    "run_id",
    "experiment_name",
    "model_version",
    "feature_snapshot_version",
    "dataset_fingerprint",
    "status",
)

NUMERIC_OR_NULL_KEYS: tuple[str, ...] = (
    "roi",
    "hit_rate",
    "buy_races",
    "total_bets",
    "hit_bets",
    "total_return_yen",
    "total_bet_yen",
    "max_losing_streak",
)

ALL_SCHEMA_KEYS: tuple[str, ...] = (
    "summary_schema_version",
    "created_at",
    "race_date",
    "run_id",
    "experiment_name",
    "model_version",
    "feature_snapshot_version",
    "dataset_fingerprint",
    "status",
    "stop_reason",
    "warnings",
    "roi",
    "hit_rate",
    "buy_races",
    "total_bets",
    "hit_bets",
    "total_return_yen",
    "total_bet_yen",
    "max_losing_streak",
    "calibration_summary_path",
    "feature_importance_summary_path",
)


def get_run_summary_schema() -> dict[str, Any]:
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is not None:
        return _SCHEMA_CACHE
    try:
        _SCHEMA_CACHE = json.loads(RUN_SUMMARY_SCHEMA_PATH.read_text(encoding="utf-8"))
    except Exception:
        _SCHEMA_CACHE = {}
    return _SCHEMA_CACHE


def _is_nullish(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


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


def sanitize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_json_value(v) for v in value]
    return _to_finite_or_none(value)


def normalize_run_summary(
    payload: dict[str, Any] | None,
    *,
    default_race_date: str | None = None,
    default_run_id: str | None = None,
    default_experiment_name: str | None = None,
    default_model_version: str | None = None,
    default_feature_snapshot_version: str | None = None,
    default_dataset_fingerprint: str | None = None,
    default_status: str | None = None,
    default_calibration_summary_path: str | None = None,
    default_feature_importance_summary_path: str | None = None,
) -> dict[str, Any]:
    src = sanitize_json_value(payload or {})
    now_iso = dt.datetime.now().isoformat(timespec="seconds")

    experiment_name = src.get("experiment_name")
    if _is_nullish(experiment_name):
        experiment_name = default_experiment_name or src.get("model_version") or default_model_version

    out: dict[str, Any] = {
        "summary_schema_version": str(src.get("summary_schema_version") or RUN_SUMMARY_SCHEMA_VERSION),
        "created_at": str(src.get("created_at") or now_iso),
        "race_date": str(src.get("race_date") or default_race_date or ""),
        "run_id": str(src.get("run_id") or default_run_id or str(uuid.uuid4())),
        "experiment_name": str(experiment_name or ""),
        "model_version": str(src.get("model_version") or default_model_version or ""),
        "feature_snapshot_version": str(src.get("feature_snapshot_version") or default_feature_snapshot_version or ""),
        "dataset_fingerprint": str(src.get("dataset_fingerprint") or default_dataset_fingerprint or ""),
        "status": str(src.get("status") or default_status or "warn"),
        "stop_reason": src.get("stop_reason"),
        "warnings": src.get("warnings") if isinstance(src.get("warnings"), list) else [],
        "roi": src.get("roi"),
        "hit_rate": src.get("hit_rate"),
        "buy_races": src.get("buy_races"),
        "total_bets": src.get("total_bets"),
        "hit_bets": src.get("hit_bets"),
        "total_return_yen": src.get("total_return_yen"),
        "total_bet_yen": src.get("total_bet_yen"),
        "max_losing_streak": src.get("max_losing_streak"),
        "calibration_summary_path": src.get("calibration_summary_path") or default_calibration_summary_path,
        "feature_importance_summary_path": src.get("feature_importance_summary_path") or default_feature_importance_summary_path,
    }
    for key, value in src.items():
        if key not in out:
            out[key] = value

    for key in NUMERIC_OR_NULL_KEYS:
        value = out.get(key)
        if value is None:
            continue
        try:
            out[key] = float(value)
        except Exception:
            out[key] = None

    if out["stop_reason"] is not None and not isinstance(out["stop_reason"], str):
        out["stop_reason"] = str(out["stop_reason"])
    if out["status"] != "stop" and _is_nullish(out["stop_reason"]):
        out["stop_reason"] = None
    if out["status"] == "stop" and _is_nullish(out["stop_reason"]):
        out["stop_reason"] = "unspecified_stop"

    out["warnings"] = [str(w) for w in out["warnings"] if not _is_nullish(w)]
    if out.get("calibration_summary_path") is not None:
        out["calibration_summary_path"] = str(out["calibration_summary_path"])
    if out.get("feature_importance_summary_path") is not None:
        out["feature_importance_summary_path"] = str(out["feature_importance_summary_path"])
    return sanitize_json_value(out)


def validate_run_summary(
    payload: dict[str, Any],
    *,
    strict: bool = True,
) -> dict[str, list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    schema = get_run_summary_schema()
    required_keys = schema.get("required") if isinstance(schema.get("required"), list) else list(ALL_SCHEMA_KEYS)

    for key in required_keys:
        if key not in payload:
            code = f"missing_required_key:{key}"
            if strict:
                errors.append(code)
            else:
                warnings.append(code)

    for key in REQUIRED_STRING_KEYS:
        value = payload.get(key)
        if not isinstance(value, str) or value.strip() == "":
            code = f"missing_or_empty:{key}"
            if strict:
                errors.append(code)
            else:
                warnings.append(code)

    if not isinstance(payload.get("warnings"), list):
        code = "warnings_not_array"
        if strict:
            errors.append(code)
        else:
            warnings.append(code)
    else:
        for index, item in enumerate(payload.get("warnings", [])):
            if not isinstance(item, str):
                code = f"warnings_item_not_string:{index}"
                if strict:
                    errors.append(code)
                else:
                    warnings.append(code)

    if payload.get("status") not in {"ok", "warn", "stop"}:
        code = f"invalid_status:{payload.get('status')}"
        if strict:
            errors.append(code)
        else:
            warnings.append(code)

    for key in NUMERIC_OR_NULL_KEYS:
        value = payload.get(key)
        if value is None:
            continue
        if not isinstance(value, (int, float)):
            code = f"invalid_numeric_type:{key}"
            if strict:
                errors.append(code)
            else:
                warnings.append(code)
            continue
        if isinstance(value, float) and not math.isfinite(value):
            code = f"non_finite_numeric:{key}"
            if strict:
                errors.append(code)
            else:
                warnings.append(code)

    for key in ("stop_reason", "calibration_summary_path", "feature_importance_summary_path"):
        value = payload.get(key)
        if value is None:
            continue
        if not isinstance(value, str):
            code = f"invalid_type:{key}"
            if strict:
                errors.append(code)
            else:
                warnings.append(code)

    if payload.get("summary_schema_version") != RUN_SUMMARY_SCHEMA_VERSION:
        code = f"schema_version_mismatch:{payload.get('summary_schema_version')}!= {RUN_SUMMARY_SCHEMA_VERSION}"
        if strict:
            errors.append(code)
        else:
            warnings.append(code)

    return {"errors": errors, "warnings": warnings}
