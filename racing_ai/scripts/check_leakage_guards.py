from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd

from aikeiba.db.duckdb import DuckDb

RISKY_FEATURE_COLS = {
    "actual_top3",
    "finish_position",
    "payout",
    "return_yen",
    "hit_flag",
}


def _parse_date(s: str | None):
    if s is None or str(s).strip() == "":
        return None
    try:
        return dt.date.fromisoformat(str(s)[:10])
    except Exception:
        return None


def run_checks(
    *,
    db: DuckDb,
    model_version: str,
    models_root: Path | None = None,
    grid_metadata_path: Path | None = None,
    grid_param_application_path: Path | None = None,
    prediction_date: str | None = None,
) -> dict:
    issues: list[str] = []
    warns: list[str] = []

    ts = db.query_df(
        """
        WITH pred AS (
          SELECT race_id, horse_no, model_version, max(inference_timestamp) AS inference_timestamp
          FROM horse_predictions
          WHERE model_version = ?
          GROUP BY race_id, horse_no, model_version
        ),
        odds_latest AS (
          SELECT race_id, horse_no, max(captured_at) AS odds_timestamp
          FROM odds
          WHERE lower(odds_type) IN ('place','place_max')
          GROUP BY race_id, horse_no
        )
        SELECT count(*) AS c
        FROM pred p
        JOIN odds_latest o ON o.race_id=p.race_id AND o.horse_no=p.horse_no
        WHERE o.odds_timestamp > p.inference_timestamp
        """,
        (model_version,),
    )
    if int(ts.iloc[0]["c"]) > 0:
        issues.append(f"odds_timestamp_future_rows:{int(ts.iloc[0]['c'])}")

    cols = db.query_df("PRAGMA table_info('feature_store')")
    names = set(cols["name"].astype(str).tolist())
    if "source_race_date_max" not in names:
        issues.append("missing_feature_guard_column:source_race_date_max")

    overlap = sorted(names.intersection(RISKY_FEATURE_COLS))
    if overlap:
        issues.append(f"risky_columns_in_feature_store:{','.join(overlap)}")

    if models_root is None:
        models_root = Path("racing_ai/data/models_compare")
    meta_path = models_root / "top3" / model_version / "meta.json"
    if not meta_path.exists():
        warns.append(f"meta_not_found:{meta_path}")
    else:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        train_start = _parse_date(meta.get("train_start_date"))
        train_end = _parse_date(meta.get("train_end_date"))
        cal_start = _parse_date(meta.get("calibration_start_date"))
        cal_end = _parse_date(meta.get("calibration_end_date"))
        valid_start = _parse_date(meta.get("validation_start_date") or meta.get("valid_start_date"))
        valid_end = _parse_date(meta.get("validation_end_date") or meta.get("valid_end_date"))
        model_created = meta.get("model_created_at") or meta.get("created_at")
        feature_set_version = meta.get("feature_set_version") or meta.get("feature_snapshot_version")

        for key in [
            "train_start_date",
            "train_end_date",
            "calibration_start_date",
            "calibration_end_date",
            "validation_start_date",
            "validation_end_date",
            "row_count_train",
            "row_count_calibration",
            "row_count_validation",
        ]:
            if meta.get(key) is None:
                warns.append(f"meta_missing_field:{key}")

        if train_end is not None and valid_start is not None and not (train_end < valid_start):
            issues.append("meta_invalid_split:train_end_not_before_validation_start")
        if cal_start is not None and cal_end is not None and valid_start is not None and valid_end is not None:
            if not (cal_end < valid_start or valid_end < cal_start):
                warns.append("meta_warning:calibration_period_overlaps_validation_period")

        latest_pred_date = db.query_df(
            """
            SELECT max(cast(r.race_date as DATE)) AS max_race_date
            FROM horse_predictions hp
            JOIN races r ON r.race_id=hp.race_id
            WHERE hp.model_version=?
            """,
            (model_version,),
        ).iloc[0]["max_race_date"]
        if train_end is not None and latest_pred_date is not None:
            if pd.Timestamp(latest_pred_date).date() <= train_end:
                warns.append("prediction_target_date_not_after_train_end")

        cnt = db.query_df(
            "SELECT count(*) AS c FROM horse_predictions WHERE model_version=?",
            (model_version,),
        ).iloc[0]["c"]
        if int(cnt) == 0:
            warns.append("no_predictions_for_model_version")
        if str(meta.get("model_version", model_version)) != str(model_version):
            issues.append("meta_model_version_mismatch")
        if feature_set_version is None:
            warns.append("meta_feature_set_version_missing")
        if model_created is None:
            warns.append("meta_model_created_at_missing")
        else:
            pred_feat = db.query_df(
                """
                SELECT feature_snapshot_version, count(*) AS c
                FROM horse_predictions
                WHERE model_version=?
                GROUP BY feature_snapshot_version
                ORDER BY c DESC
                LIMIT 1
                """,
                (model_version,),
            )
            if len(pred_feat) > 0:
                pred_fs = str(pred_feat.iloc[0]["feature_snapshot_version"])
                if feature_set_version is not None and str(feature_set_version) != pred_fs:
                    warns.append(f"feature_set_version_mismatch:meta={feature_set_version},pred={pred_fs}")
        if train_start is None:
            warns.append("meta_warning:train_start_date_null")

    if grid_metadata_path is not None and grid_metadata_path.exists():
        try:
            gmeta = json.loads(grid_metadata_path.read_text(encoding="utf-8"))
            g_start = _parse_date(gmeta.get("grid_start_date"))
            g_end = _parse_date(gmeta.get("grid_end_date"))
            p_date = _parse_date(prediction_date) if prediction_date else None
            if p_date is not None and g_end is not None and not (g_end < p_date):
                warns.append("grid_search_period_not_before_prediction_date")
            if g_start is not None and g_end is not None and p_date is not None and g_start <= p_date <= g_end:
                warns.append("grid_search_same_period_as_prediction_risk")
            if gmeta.get("selected_best_params") is None:
                warns.append("grid_metadata_missing_selected_best_params")
        except Exception as exc:
            warns.append(f"grid_metadata_parse_error:{exc}")
    else:
        warns.append("grid_metadata_not_found")

    if grid_param_application_path is not None and grid_param_application_path.exists():
        try:
            app = json.loads(grid_param_application_path.read_text(encoding="utf-8"))
            is_safe = app.get("is_safe_temporal_application")
            if is_safe is False:
                warns.append("grid_param_application_not_safe")
            if app.get("manual_check_required"):
                warns.append("manual_check_required:grid_param_application_period")
        except Exception as exc:
            warns.append(f"grid_param_application_parse_error:{exc}")
    else:
        warns.append("grid_param_application_not_found")

    return {
        "status": "stop" if len(issues) > 0 else "ok",
        "issues": issues,
        "warnings": warns,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--model-version", required=True)
    ap.add_argument("--models-root", default="", help="Optional models root path")
    ap.add_argument("--grid-metadata-path", default="", help="Optional grid_search_metadata.json path")
    ap.add_argument("--grid-param-application-path", default="", help="Optional grid_param_application.json path")
    ap.add_argument("--prediction-date", default="", help="Optional prediction date YYYY-MM-DD")
    ap.add_argument("--out-json", default="reports/leakage_guard_report.json")
    args = ap.parse_args()

    db = DuckDb.connect(Path(args.db_path))
    models_root = Path(args.models_root) if str(args.models_root).strip() else None
    grid_meta = Path(args.grid_metadata_path) if str(args.grid_metadata_path).strip() else None
    grid_app = Path(args.grid_param_application_path) if str(args.grid_param_application_path).strip() else None
    if grid_app is None and grid_meta is not None:
        candidate = grid_meta.parent / "grid_param_application.json"
        if candidate.exists():
            grid_app = candidate
    pred_date = args.prediction_date if str(args.prediction_date).strip() else None

    report = run_checks(
        db=db,
        model_version=args.model_version,
        models_root=models_root,
        grid_metadata_path=grid_meta,
        grid_param_application_path=grid_app,
        prediction_date=pred_date,
    )
    out = Path(args.out_json)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
