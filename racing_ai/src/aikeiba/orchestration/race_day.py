from __future__ import annotations

import datetime as dt
import json
import math
import uuid
from pathlib import Path
from typing import Any

from aikeiba.checks.data_quality import run_doctor
from aikeiba.common.hashing import stable_fingerprint
from aikeiba.common.run_log import write_race_day_run_log
from aikeiba.datalab.ingest_csv import ingest_from_csv_dir
from aikeiba.datalab.raw_pipeline import normalize_raw_jv_to_normalized
from aikeiba.db.duckdb import DuckDb
from aikeiba.decision.skip_rules import decide_buy_or_skip
from aikeiba.decision.wide_rules import generate_wide_candidates_rule_based
from aikeiba.export.to_static import export_for_dashboard
from aikeiba.features.assemble import build_feature_store_snapshot
from aikeiba.inference.top3 import infer_top3_for_date


def _latest_top3_predictions_for_date(db: DuckDb, race_date: str, model_version: str) -> list[dict[str, Any]]:
    return db.query_df(
        """
        WITH latest AS (
          SELECT race_id, horse_no, model_version, max(inference_timestamp) AS ts
          FROM horse_predictions
          WHERE model_version = ?
          GROUP BY race_id, horse_no, model_version
        )
        SELECT hp.race_id, hp.horse_no, hp.p_top3
        FROM horse_predictions hp
        JOIN latest l
          ON l.race_id=hp.race_id
         AND l.horse_no=hp.horse_no
         AND l.model_version=hp.model_version
         AND l.ts=hp.inference_timestamp
        JOIN races r ON r.race_id = hp.race_id
        WHERE r.race_date = cast(? as DATE)
        ORDER BY hp.race_id, hp.horse_no
        """,
        (model_version, race_date),
    ).to_dict("records")


def _decision_preview(db: DuckDb, race_date: str, model_version: str) -> dict[str, Any]:
    races = db.query_df("SELECT race_id FROM races WHERE race_date = cast(? as DATE)", (race_date,)).to_dict("records")
    entries = db.query_df(
        """
        SELECT r.race_id, e.horse_no
        FROM entries e
        JOIN races r ON r.race_id=e.race_id
        WHERE r.race_date = cast(? as DATE)
          AND (e.is_scratched IS NULL OR e.is_scratched=FALSE)
        """,
        (race_date,),
    ).to_dict("records")
    preds = _latest_top3_predictions_for_date(db, race_date, model_version)
    pred_map = {(p["race_id"], int(p["horse_no"])): p.get("p_top3") for p in preds}

    by_race_horse: dict[str, list[int]] = {}
    for e in entries:
        by_race_horse.setdefault(e["race_id"], []).append(int(e["horse_no"]))

    buy_races = 0
    total_candidates = 0
    race_flags: list[dict[str, Any]] = []
    for r in races:
        rid = r["race_id"]
        horse_nos = by_race_horse.get(rid, [])
        p_top3_dict = {hn: float(pred_map[(rid, hn)]) for hn in horse_nos if pred_map.get((rid, hn)) is not None}
        decision = decide_buy_or_skip(p_top3=list(p_top3_dict.values()))
        if decision.buy_flag:
            buy_races += 1
            cands = generate_wide_candidates_rule_based(
                race_id=rid,
                horse_nos=horse_nos,
                p_top3=p_top3_dict,
                axis_k=1,
                partner_k=min(6, len(horse_nos)),
            )
            total_candidates += len(cands)
        race_flags.append(
            {
                "race_id": rid,
                "buy_flag": decision.buy_flag,
                "reason": decision.reason,
                "density_top3": decision.density_top3,
                "gap12": decision.gap12,
            }
        )
    return {"buy_races": buy_races, "total_candidates": total_candidates, "race_flags": race_flags}


def _post_infer_probability_gate(
    db: DuckDb,
    race_date: str,
    model_version: str,
) -> dict[str, Any]:
    rows = _latest_top3_predictions_for_date(db, race_date, model_version)
    stop_reasons: list[str] = []
    warn_reasons: list[str] = []

    if len(rows) == 0:
        stop_reasons.append("no_top3_predictions")
        return {"status": "stop", "stop_reasons": stop_reasons, "warn_reasons": warn_reasons, "race_sums": []}

    pvals = [r["p_top3"] for r in rows]

    def is_nullish(v: Any) -> bool:
        return v is None or (isinstance(v, float) and math.isnan(v))

    if all(is_nullish(v) for v in pvals):
        stop_reasons.append("top3_all_null")
        return {"status": "stop", "stop_reasons": stop_reasons, "warn_reasons": warn_reasons, "race_sums": []}

    sums = db.query_df(
        """
        WITH latest AS (
          SELECT race_id, horse_no, model_version, max(inference_timestamp) AS ts
          FROM horse_predictions
          WHERE model_version = ?
          GROUP BY race_id, horse_no, model_version
        )
        SELECT hp.race_id, sum(hp.p_top3) AS sum_top3, count(*) AS n
        FROM horse_predictions hp
        JOIN latest l
          ON l.race_id=hp.race_id
         AND l.horse_no=hp.horse_no
         AND l.model_version=hp.model_version
         AND l.ts=hp.inference_timestamp
        JOIN races r ON r.race_id=hp.race_id
        WHERE r.race_date = cast(? as DATE)
        GROUP BY hp.race_id
        ORDER BY hp.race_id
        """,
        (model_version, race_date),
    ).to_dict("records")

    for s in sums:
        race_id = s["race_id"]
        sum_top3 = s["sum_top3"]
        if sum_top3 is None or (isinstance(sum_top3, float) and math.isnan(sum_top3)):
            stop_reasons.append(f"sum_top3_null:{race_id}")
            continue
        v = float(sum_top3)
        # Soft/hard bounds as sanity gates (config化は次段で可能)
        if v < 0.5 or v > 6.0:
            stop_reasons.append(f"sum_top3_extreme:{race_id}:{v:.3f}")
        elif v < 1.2 or v > 4.8:
            warn_reasons.append(f"sum_top3_unusual:{race_id}:{v:.3f}")

    status = "stop" if len(stop_reasons) > 0 else ("warn" if len(warn_reasons) > 0 else "ok")
    return {"status": status, "stop_reasons": stop_reasons, "warn_reasons": warn_reasons, "race_sums": sums}


def _write_run_summary(run_summary_path: Path, payload: dict[str, Any]) -> None:
    run_summary_path.parent.mkdir(parents=True, exist_ok=True)
    run_summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_race_day_pipeline(
    *,
    db: DuckDb,
    raw_dir: Path,
    normalized_root: Path,
    race_date: str,
    snapshot_version: str,
    feature_snapshot_version: str,
    model_version: str,
    odds_snapshot_version: str,
    models_root: Path,
    export_out_dir: Path,
    run_summary_path: Path,
    allow_no_wide_odds: bool,
    force_null_top3_for_test: bool = False,
) -> dict[str, Any]:
    warnings: list[str] = []
    step_results: dict[str, Any] = {}
    status = "ok"
    stop_reason: str | None = None
    try:
        # 1) jv-file-pipeline (normalize + ingest)
        normalize = normalize_raw_jv_to_normalized(
            raw_dir=raw_dir,
            normalized_root=normalized_root,
            target_race_date=race_date,
            snapshot_version=snapshot_version,
            db=db,
        )
        step_results["normalize"] = normalize
        if normalize["status"] == "stop":
            status = "stop"
            stop_reason = ";".join(normalize.get("stop_reasons", [])) or "normalize_stop"
        else:
            warnings.extend(normalize.get("warn_reasons", []))
            normalized_dir = Path(normalize["normalized_dir"])
            ingest = ingest_from_csv_dir(db=db, in_dir=normalized_dir)
            step_results["ingest"] = ingest

        # 2) build-features
        if status != "stop":
            step_results["build_features"] = build_feature_store_snapshot(
                db=db,
                race_date=race_date,
                feature_snapshot_version=feature_snapshot_version,
            )

        # 3) doctor
        if status != "stop":
            doctor = run_doctor(db, race_date=race_date)
            step_results["doctor"] = doctor
            if doctor["should_stop"]:
                status = "stop"
                stop_reason = ";".join(doctor.get("stop_reasons", [])) or "doctor_stop"
            warnings.extend(doctor.get("warn_reasons", []))

        # 4) infer-top3
        if status != "stop":
            dataset_fingerprint = stable_fingerprint(
                {
                    "race_date": race_date,
                    "snapshot_version": snapshot_version,
                    "feature_snapshot_version": feature_snapshot_version,
                    "model_version": model_version,
                    "odds_snapshot_version": odds_snapshot_version,
                }
            )
            infer = infer_top3_for_date(
                db=db,
                models_root=models_root,
                race_date=race_date,
                feature_snapshot_version=feature_snapshot_version,
                model_version=model_version,
                odds_snapshot_version=odds_snapshot_version,
                dataset_fingerprint=dataset_fingerprint,
            )
            step_results["infer_top3"] = infer
            if force_null_top3_for_test:
                db.execute(
                    """
                    UPDATE horse_predictions
                    SET p_top3 = NULL
                    WHERE model_version = ?
                      AND race_id IN (SELECT race_id FROM races WHERE race_date = cast(? as DATE))
                    """,
                    (model_version, race_date),
                )
                step_results["infer_top3"]["forced_null_top3"] = True

        # post-infer gates
        if status != "stop":
            prob_gate = _post_infer_probability_gate(db=db, race_date=race_date, model_version=model_version)
            step_results["post_infer_gate"] = prob_gate
            if prob_gate["status"] == "stop":
                status = "stop"
                stop_reason = ";".join(prob_gate.get("stop_reasons", [])) or "post_infer_stop"
            warnings.extend(prob_gate.get("warn_reasons", []))

        # 5) decision
        if status != "stop":
            decision = _decision_preview(db=db, race_date=race_date, model_version=model_version)
            step_results["decision"] = decision
            if int(decision.get("buy_races", 0)) == 0:
                warnings.append("buy_races_zero")

        # 6) export-static
        if status != "stop":
            export = export_for_dashboard(
                db=db,
                race_date=race_date,
                out_dir=export_out_dir,
                feature_snapshot_version=feature_snapshot_version,
                model_version=model_version,
                odds_snapshot_version=odds_snapshot_version,
                allow_no_wide_odds=allow_no_wide_odds,
            )
            step_results["export_static"] = export
    except Exception as exc:
        status = "stop"
        stop_reason = f"exception:{exc.__class__.__name__}:{exc}"
        step_results["exception"] = {"type": exc.__class__.__name__, "message": str(exc)}

    # status to warn if non-stop and warnings exist
    if status != "stop" and len(warnings) > 0:
        status = "warn"

    warning_count = len(warnings)
    dataset_fingerprint = stable_fingerprint(
        {
            "race_date": race_date,
            "snapshot_version": snapshot_version,
            "feature_snapshot_version": feature_snapshot_version,
            "model_version": model_version,
            "odds_snapshot_version": odds_snapshot_version,
            "status": status,
            "warnings": warnings,
        }
    )

    summary = {
        "run_id": str(uuid.uuid4()),
        "race_date": race_date,
        "snapshot_version": snapshot_version,
        "feature_snapshot_version": feature_snapshot_version,
        "model_version": model_version,
        "odds_snapshot_version": odds_snapshot_version,
        "status": status,
        "stop_reason": stop_reason,
        "warning_count": warning_count,
        "warnings": warnings,
        "dataset_fingerprint": dataset_fingerprint,
        "created_at": dt.datetime.now().isoformat(timespec="seconds"),
        "steps": step_results,
    }
    _write_run_summary(run_summary_path, summary)

    # mandatory logging:
    # 1) dedicated race-day log
    write_race_day_run_log(
        db=db,
        race_date=race_date,
        snapshot_version=snapshot_version,
        feature_snapshot_version=feature_snapshot_version,
        model_version=model_version,
        odds_snapshot_version=odds_snapshot_version,
        status=status,
        stop_reason=stop_reason,
        warning_count=warning_count,
        warnings=warnings,
        run_summary_path=str(run_summary_path),
    )

    # 2) inference_log compatibility
    decision = step_results.get("decision", {})
    db.execute(
        """
        INSERT INTO inference_log(
          inference_id, race_date, inference_timestamp, feature_snapshot_version, model_version,
          odds_snapshot_version, dataset_fingerprint, stop_reason, buy_races, total_candidates, warnings_json
        ) VALUES (?, cast(? as DATE), cast(? as TIMESTAMP), ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            race_date,
            dt.datetime.now().isoformat(timespec="seconds"),
            feature_snapshot_version,
            model_version,
            odds_snapshot_version,
            dataset_fingerprint,
            stop_reason,
            int(decision.get("buy_races", 0)) if isinstance(decision, dict) else 0,
            int(decision.get("total_candidates", 0)) if isinstance(decision, dict) else 0,
            json.dumps({"warnings": warnings, "warning_count": warning_count}, ensure_ascii=False),
        ),
    )

    return summary
