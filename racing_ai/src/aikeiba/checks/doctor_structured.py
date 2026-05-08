from __future__ import annotations

import csv
import datetime as dt
import json
import math
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from aikeiba.db.duckdb import DuckDb
from aikeiba.decision.overlap_guard import overlap_guard_pairs


@dataclass(frozen=True)
class DoctorCheckRecord:
    run_id: str
    check_code: str
    check_name: str
    severity: str
    status: str
    message: str
    metric_name: str | None
    metric_value: float | None
    threshold: str | None
    race_date: str
    snapshot_version: str
    feature_snapshot_version: str
    model_version: str
    created_at: str


def _to_float_or_none(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        if isinstance(v, float) and math.isnan(v):
            return None
        return float(v)
    try:
        f = float(v)
        if math.isnan(f):
            return None
        return f
    except Exception:
        return None


def _status_rank(status: str) -> int:
    return {"pass": 0, "warn": 1, "stop": 2}.get(status, 2)


def _overall_status(checks: list[DoctorCheckRecord]) -> str:
    if any(c.status == "stop" for c in checks):
        return "stop"
    if any(c.status == "warn" for c in checks):
        return "warn"
    return "pass"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: list[DoctorCheckRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "run_id",
                "check_code",
                "check_name",
                "severity",
                "status",
                "message",
                "metric_name",
                "metric_value",
                "threshold",
                "race_date",
                "snapshot_version",
                "feature_snapshot_version",
                "model_version",
                "created_at",
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))


def _save_doctor_rows_db(db: DuckDb, rows: list[DoctorCheckRecord]) -> None:
    for r in rows:
        db.execute(
            """
            INSERT INTO doctor_result_log(
              doctor_id, run_id, check_code, check_name, severity, status, message,
              metric_name, metric_value, threshold, race_date, snapshot_version,
              feature_snapshot_version, model_version, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, cast(? as DATE), ?, ?, ?, cast(? as TIMESTAMP))
            """,
            (
                str(uuid.uuid4()),
                r.run_id,
                r.check_code,
                r.check_name,
                r.severity,
                r.status,
                r.message,
                r.metric_name,
                r.metric_value,
                r.threshold,
                r.race_date,
                r.snapshot_version,
                r.feature_snapshot_version,
                r.model_version,
                r.created_at,
            ),
        )


def build_doctor_structured_result(
    *,
    db: DuckDb,
    run_id: str,
    race_date: str,
    snapshot_version: str,
    feature_snapshot_version: str,
    model_version: str,
    step_results: dict[str, Any],
    warnings: list[str],
    json_path: Path,
    csv_path: Path,
    force_overlap_guard_fail_for_test: bool = False,
) -> dict[str, Any]:
    created_at = dt.datetime.now().isoformat(timespec="seconds")
    checks: list[DoctorCheckRecord] = []

    normalize = step_results.get("normalize", {})
    ingest = step_results.get("ingest", {})
    doctor = step_results.get("doctor", {})
    post_infer_gate = step_results.get("post_infer_gate", {})
    decision = step_results.get("decision", {})

    def add(
        code: str,
        name: str,
        severity: str,
        status: str,
        message: str,
        metric_name: str | None = None,
        metric_value: float | None = None,
        threshold: str | None = None,
    ):
        checks.append(
            DoctorCheckRecord(
                run_id=run_id,
                check_code=code,
                check_name=name,
                severity=severity,
                status=status,
                message=message,
                metric_name=metric_name,
                metric_value=metric_value,
                threshold=threshold,
                race_date=race_date,
                snapshot_version=snapshot_version,
                feature_snapshot_version=feature_snapshot_version,
                model_version=model_version,
                created_at=created_at,
            )
        )

    # 1) required_raw_missing
    stop_reasons = normalize.get("stop_reasons", []) if isinstance(normalize, dict) else []
    missing_required = [r for r in stop_reasons if str(r).startswith("missing_raw_file:")]
    add(
        "required_raw_missing",
        "Required raw file missing",
        "critical",
        "stop" if len(missing_required) > 0 else "pass",
        ",".join(missing_required) if missing_required else "required raw files exist",
        "missing_required_count",
        float(len(missing_required)),
        "==0",
    )

    # 2) required_table_empty
    races_rows = int(ingest.get("races_rows", 0)) if isinstance(ingest, dict) else 0
    entries_rows = int(ingest.get("entries_rows", 0)) if isinstance(ingest, dict) else 0
    is_empty = races_rows == 0 or entries_rows == 0
    add(
        "required_table_empty",
        "Required table has zero rows",
        "critical",
        "stop" if is_empty else "pass",
        f"races_rows={races_rows}, entries_rows={entries_rows}",
        "min_required_rows",
        float(min(races_rows, entries_rows)),
        ">0",
    )

    # 3) race_id_missing_rate
    race_id_missing_high = any(str(x) == "race_id_missing_rate_entries_high" for x in stop_reasons)
    race_id_missing_rate = 1.0 if race_id_missing_high else 0.0
    add(
        "race_id_missing_rate",
        "Race ID missing rate",
        "high",
        "stop" if race_id_missing_high else "pass",
        "race_id missing rate exceeds stop threshold" if race_id_missing_high else "race_id missing rate within threshold",
        "race_id_missing_rate",
        race_id_missing_rate,
        "<=0.0",
    )

    # 4) horse_id_missing_rate
    horse_attach = None
    if isinstance(doctor, dict):
        horse_attach = doctor.get("stats", {}).get("horse_id_attach_rate")
    horse_missing = None
    if horse_attach is not None:
        horse_missing = 1.0 - float(horse_attach)
    horse_missing_high = any(str(x) == "horse_id_missing_rate_entries_high" for x in stop_reasons)
    if isinstance(doctor, dict) and any(str(x) == "horse_id_attach_rate_low" for x in doctor.get("stop_reasons", [])):
        horse_missing_high = True
    add(
        "horse_id_missing_rate",
        "Horse ID missing rate",
        "critical",
        "stop" if horse_missing_high else "pass",
        "horse_id missing/attach rate exceeds threshold" if horse_missing_high else "horse_id missing rate within threshold",
        "horse_id_missing_rate",
        _to_float_or_none(horse_missing),
        "<=0.02",
    )

    # 5) odds_attach_rate_low
    odds_rows = int(ingest.get("odds_rows", 0)) if isinstance(ingest, dict) else 0
    odds_attach_rate = float(odds_rows) / float(entries_rows) if entries_rows > 0 else None
    odds_warn = (odds_attach_rate is None) or (odds_attach_rate < 0.5)
    add(
        "odds_attach_rate_low",
        "Odds attach rate is low",
        "medium",
        "warn" if odds_warn else "pass",
        f"odds_rows={odds_rows}, entries_rows={entries_rows}",
        "odds_attach_rate",
        _to_float_or_none(odds_attach_rate),
        ">=0.5",
    )

    # 6) top3_all_null
    top3_all_null = False
    if isinstance(post_infer_gate, dict):
        top3_all_null = any(str(x) == "top3_all_null" for x in post_infer_gate.get("stop_reasons", []))
    add(
        "top3_all_null",
        "Top3 predictions are all null",
        "critical",
        "stop" if top3_all_null else "pass",
        "all p_top3 values are null/NaN" if top3_all_null else "p_top3 has valid values",
        "top3_all_null_flag",
        1.0 if top3_all_null else 0.0,
        "==0",
    )

    # 7) sum_top3_unusual
    sum_top3_stop = False
    sum_top3_warn = False
    race_sums = []
    if isinstance(post_infer_gate, dict):
        race_sums = post_infer_gate.get("race_sums", []) or []
        sum_top3_stop = any(str(x).startswith("sum_top3_") for x in post_infer_gate.get("stop_reasons", []))
        sum_top3_warn = any(str(x).startswith("sum_top3_unusual:") for x in post_infer_gate.get("warn_reasons", []))
    sum_top3_mean = None
    if len(race_sums) > 0:
        vals = [float(r["sum_top3"]) for r in race_sums if r.get("sum_top3") is not None and not (isinstance(r.get("sum_top3"), float) and math.isnan(r.get("sum_top3")))]
        if len(vals) > 0:
            sum_top3_mean = sum(vals) / len(vals)
    sum_status = "stop" if sum_top3_stop else ("warn" if sum_top3_warn else "pass")
    add(
        "sum_top3_unusual",
        "Race-level sum(p_top3) unusual/extreme",
        "high" if sum_status == "stop" else "medium",
        sum_status,
        "sum(p_top3) out of expected range" if sum_status != "pass" else "sum(p_top3) within expected range",
        "sum_top3_mean",
        _to_float_or_none(sum_top3_mean),
        "warn:[1.2,4.8], stop:[0.5,6.0]",
    )

    # 8) buy_races_zero
    buy_races = int(decision.get("buy_races", 0)) if isinstance(decision, dict) else 0
    add(
        "buy_races_zero",
        "Buy races count is zero",
        "medium",
        "warn" if buy_races == 0 else "pass",
        f"buy_races={buy_races}",
        "buy_races",
        float(buy_races),
        ">0",
    )

    # 9) overlap_guard_failed
    overlap_status = "pass"
    overlap_message = "overlap guard pass"
    overlap_val = 0.0
    if force_overlap_guard_fail_for_test:
        overlap_status = "stop"
        overlap_message = "forced overlap guard failure for test"
        overlap_val = 0.0
    else:
        candidate_pairs = set()
        for rec in decision.get("candidate_pairs", []) if isinstance(decision, dict) else []:
            rid = str(rec.get("race_id"))
            pair = str(rec.get("pair"))
            if rid and pair:
                candidate_pairs.add((rid, pair))

        export_static = step_results.get("export_static", {})
        bets_pairs = set()
        if isinstance(export_static, dict):
            out_dir = export_static.get("out_dir")
            if out_dir:
                bets_path = Path(out_dir) / "today_pipeline_bets.json"
                if bets_path.exists():
                    try:
                        bets_json = json.loads(bets_path.read_text(encoding="utf-8"))
                        for b in bets_json:
                            rid = str(b.get("race_id"))
                            pair = str(b.get("pair"))
                            if rid and pair:
                                bets_pairs.add((rid, pair))
                    except Exception:
                        pass
        overlap = overlap_guard_pairs(today_wide_candidates=candidate_pairs, today_pipeline_bets=bets_pairs)
        overlap_val = float(overlap.get("overlap_candidates_vs_bets", 0))
        if overlap.get("should_stop"):
            overlap_status = "stop"
            overlap_message = "overlap guard failed: candidates vs bets overlap is zero"
    add(
        "overlap_guard_failed",
        "Overlap guard for race_id+pair",
        "critical",
        overlap_status,
        overlap_message,
        "overlap_candidates_vs_bets",
        overlap_val,
        ">0 when both sides exist",
    )

    # 10) field_size_outlier
    field_df = db.query_df(
        """
        SELECT e.race_id, count(*) AS field_size
        FROM entries e
        JOIN races r ON r.race_id=e.race_id
        WHERE r.race_date = cast(? as DATE)
          AND (e.is_scratched IS NULL OR e.is_scratched=FALSE)
        GROUP BY e.race_id
        """,
        (race_date,),
    )
    f_stop = False
    f_warn = False
    min_field = None
    max_field = None
    if len(field_df) > 0:
        min_field = int(field_df["field_size"].min())
        max_field = int(field_df["field_size"].max())
        if min_field < 3 or max_field > 20:
            f_stop = True
        elif min_field < 6 or max_field > 18:
            f_warn = True
    fs_status = "stop" if f_stop else ("warn" if f_warn else "pass")
    add(
        "field_size_outlier",
        "Field size outlier check",
        "high" if fs_status == "stop" else "low",
        fs_status,
        f"min_field={min_field}, max_field={max_field}",
        "field_size_range",
        _to_float_or_none(min_field),
        "warn:[6,18], stop:[3,20]",
    )

    overall = _overall_status(checks)
    stop_codes = [c.check_code for c in checks if c.status == "stop"]
    warn_codes = [c.check_code for c in checks if c.status == "warn"]

    payload = {
        "run_id": run_id,
        "overall_status": overall,
        "race_date": race_date,
        "snapshot_version": snapshot_version,
        "feature_snapshot_version": feature_snapshot_version,
        "model_version": model_version,
        "created_at": created_at,
        "stop_check_codes": stop_codes,
        "warn_check_codes": warn_codes,
        "checks": [asdict(c) for c in checks],
    }
    _write_json(json_path, payload)
    _write_csv(csv_path, checks)
    _save_doctor_rows_db(db, checks)
    return payload
