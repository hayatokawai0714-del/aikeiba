from __future__ import annotations

from copy import deepcopy

from aikeiba.common.run_summary import RUN_SUMMARY_SCHEMA_VERSION, validate_run_summary


def _valid_summary() -> dict:
    return {
        "summary_schema_version": RUN_SUMMARY_SCHEMA_VERSION,
        "created_at": "2026-04-15T12:00:00",
        "race_date": "2026-04-14",
        "run_id": "run-001",
        "experiment_name": "exp_top3_stability_v2",
        "model_version": "top3_stability_v2",
        "feature_snapshot_version": "fs_v1",
        "dataset_fingerprint": "fp-001",
        "status": "ok",
        "stop_reason": None,
        "warnings": [],
        "roi": 1.05,
        "hit_rate": 0.2,
        "buy_races": 10,
        "total_bets": 30,
        "hit_bets": 6,
        "total_return_yen": 31500,
        "total_bet_yen": 30000,
        "max_losing_streak": 4,
        "calibration_summary_path": None,
        "feature_importance_summary_path": None,
    }


def test_validate_run_summary_success_with_valid_v1() -> None:
    result = validate_run_summary(_valid_summary(), strict=True)
    assert result["errors"] == []


def test_validate_run_summary_success_with_numeric_nulls() -> None:
    payload = _valid_summary()
    for key in [
        "roi",
        "hit_rate",
        "buy_races",
        "total_bets",
        "hit_bets",
        "total_return_yen",
        "total_bet_yen",
        "max_losing_streak",
    ]:
        payload[key] = None
    result = validate_run_summary(payload, strict=True)
    assert result["errors"] == []


def test_validate_run_summary_success_with_empty_warnings() -> None:
    payload = _valid_summary()
    payload["warnings"] = []
    result = validate_run_summary(payload, strict=True)
    assert result["errors"] == []


def test_validate_run_summary_success_with_additional_properties() -> None:
    payload = _valid_summary()
    payload["extra_field"] = {"note": "allowed"}
    result = validate_run_summary(payload, strict=True)
    assert result["errors"] == []


def test_validate_run_summary_fail_on_schema_version_mismatch() -> None:
    payload = _valid_summary()
    payload["summary_schema_version"] = "run_summary.v0"
    result = validate_run_summary(payload, strict=True)
    assert any("schema_version_mismatch" in error for error in result["errors"])


def test_validate_run_summary_fail_on_missing_required_key() -> None:
    payload = _valid_summary()
    del payload["model_version"]
    result = validate_run_summary(payload, strict=True)
    assert "missing_required_key:model_version" in result["errors"]


def test_validate_run_summary_fail_on_empty_required_string() -> None:
    payload = _valid_summary()
    payload["experiment_name"] = ""
    result = validate_run_summary(payload, strict=True)
    assert "missing_or_empty:experiment_name" in result["errors"]


def test_validate_run_summary_fail_on_warnings_not_array() -> None:
    payload = _valid_summary()
    payload["warnings"] = "warn"
    result = validate_run_summary(payload, strict=True)
    assert "warnings_not_array" in result["errors"]


def test_validate_run_summary_fail_on_warnings_item_not_string() -> None:
    payload = _valid_summary()
    payload["warnings"] = ["ok", 123]
    result = validate_run_summary(payload, strict=True)
    assert "warnings_item_not_string:1" in result["errors"]


def test_validate_run_summary_fail_on_numeric_string_value() -> None:
    payload = _valid_summary()
    payload["total_bets"] = "30"
    result = validate_run_summary(payload, strict=True)
    assert "invalid_numeric_type:total_bets" in result["errors"]


def test_validate_run_summary_fail_on_invalid_status() -> None:
    payload = _valid_summary()
    payload["status"] = "running"
    result = validate_run_summary(payload, strict=True)
    assert "invalid_status:running" in result["errors"]


def test_validate_run_summary_fail_on_null_not_allowed_field() -> None:
    payload = _valid_summary()
    payload["run_id"] = None
    result = validate_run_summary(payload, strict=True)
    assert "missing_or_empty:run_id" in result["errors"]


def test_validate_run_summary_does_not_mutate_input() -> None:
    payload = _valid_summary()
    before = deepcopy(payload)
    _ = validate_run_summary(payload, strict=True)
    assert payload == before
