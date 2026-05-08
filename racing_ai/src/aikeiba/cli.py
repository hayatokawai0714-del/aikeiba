from __future__ import annotations

import json
import hashlib
import datetime as dt
import shutil
import tempfile
from pathlib import Path
from typing import Optional

import typer
from rich import print

from aikeiba.db.duckdb import DuckDb
from aikeiba.db.migrations import apply_migrations

app = typer.Typer(no_args_is_help=True)


def _load_split_config(path: Path) -> tuple[dict, str]:
    if not path.exists():
        raise FileNotFoundError(f"split config not found: {path}")
    raw = path.read_bytes()
    split_hash = hashlib.sha256(raw).hexdigest()
    text = raw.decode("utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(text), split_hash
    try:
        import yaml  # type: ignore
    except Exception as exc:
        raise RuntimeError("YAML split config requires PyYAML. Use JSON split config if PyYAML is unavailable.") from exc
    obj = yaml.safe_load(text)
    if not isinstance(obj, dict):
        raise ValueError(f"split config must be object: {path}")
    return obj, split_hash


def _validate_split_config(cfg: dict, path: Path) -> list[str]:
    warnings: list[str] = []
    required_sections = ["train", "calibration", "validation"]
    for sec in required_sections:
        if sec not in cfg or not isinstance(cfg.get(sec), dict):
            raise ValueError(f"split config missing section '{sec}': {path}")

    train = cfg.get("train", {})
    calibration = cfg.get("calibration", {})
    validation = cfg.get("validation", {})
    train_end = train.get("end_date")
    cal_start = calibration.get("start_date")
    cal_end = calibration.get("end_date")
    val_start = validation.get("start_date")
    val_end = validation.get("end_date")

    def _pd(v: str | None):
        if v is None or str(v).strip() == "":
            return None
        return dt.date.fromisoformat(str(v)[:10])

    if train_end is None:
        warnings.append("split_config_warning:train.end_date_missing")
    if val_start is None or val_end is None:
        warnings.append("split_config_warning:validation.start_or_end_missing")

    d_train_end = _pd(train_end)
    d_cal_start = _pd(cal_start)
    d_cal_end = _pd(cal_end)
    d_val_start = _pd(val_start)

    if d_train_end and d_cal_start and not (d_train_end < d_cal_start):
        raise ValueError("invalid split date order: require train_end < calibration_start")
    if d_cal_end and d_val_start and not (d_cal_end < d_val_start):
        raise ValueError("invalid split date order: require calibration_end < validation_start")

    if (cal_start is None) != (cal_end is None):
        raise ValueError("calibration.start_date and calibration.end_date must be both set or both null")
    if cal_start is None and calibration.get("ratio_if_dates_missing") is None:
        warnings.append("split_config_warning:calibration_dates_missing_and_ratio_missing")

    return warnings


def _best_effort_log_raw_precheck_to_db(
    *,
    db_path: Path,
    command_name: str,
    race_date: str,
    model_version: str,
    precheck: dict,
    status: str,
    stop_reason: str | None,
    run_summary_path: Path | None,
    daily_cycle_summary_path: Path | None,
) -> None:
    try:
        from aikeiba.common.run_log import write_raw_precheck_to_daily_cycle_log

        db = DuckDb.connect(db_path)
        apply_migrations(db)
        write_raw_precheck_to_daily_cycle_log(
            db=db,
            run_id=None,
            command_name=command_name,
            race_date=race_date,
            model_version=model_version,
            raw_dir=precheck.get("raw_dir"),
            status=status,
            stop_reason=stop_reason,
            required_files=precheck.get("required_files", []),
            missing_files=precheck.get("missing_files", []),
            empty_files=precheck.get("empty_files", []),
            row_counts=precheck.get("row_counts", {}),
            raw_precheck_log_path=precheck.get("log_path"),
            run_summary_path=str(run_summary_path) if run_summary_path else None,
            daily_cycle_summary_path=str(daily_cycle_summary_path) if daily_cycle_summary_path else None,
            generated_at=precheck.get("generated_at"),
        )
    except Exception as exc:
        print(f"[yellow]WARN[/yellow] failed to write raw precheck to daily_cycle_run_log: {exc}")


@app.command()
def init_db(db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb"))):
    """Create DB and apply schema migrations."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = DuckDb.connect(db_path)
    apply_migrations(db)
    print(f"[green]OK[/green] initialized db: {db_path}")


@app.command()
def seed_demo(db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb"))):
    """Insert a tiny synthetic dataset to validate the MVP pipeline end-to-end."""
    from aikeiba.tools.seed_demo import seed_demo_dataset

    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = DuckDb.connect(db_path)
    apply_migrations(db)
    seed_demo_dataset(db)
    print("[green]OK[/green] seeded demo dataset")


@app.command()
def ingest_csv(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    in_dir: Path = typer.Option(..., help="Directory containing races.csv/entries.csv/..."),
):
    """Ingest normalized CSV files into DuckDB (idempotent delete+insert)."""
    from aikeiba.datalab.ingest_csv import ingest_from_csv_dir

    db = DuckDb.connect(db_path)
    apply_migrations(db)
    result = ingest_from_csv_dir(db=db, in_dir=in_dir)
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command()
def normalize_raw_jv(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    raw_dir: Path = typer.Option(..., help="JV-Link raw file directory (contains races.csv, entries.csv, ...)"),
    normalized_root: Path = typer.Option(Path("data/normalized")),
    race_date: str = typer.Option(..., help="YYYY-MM-DD"),
    snapshot_version: str = typer.Option(..., help="e.g. 20260414_0900"),
):
    """
    Normalize JV-Link raw files to normalized CSVs.
    raw -> normalized layer with quality gates.
    """
    from aikeiba.datalab.raw_pipeline import normalize_raw_jv_to_normalized

    db = DuckDb.connect(db_path)
    apply_migrations(db)
    result = normalize_raw_jv_to_normalized(
        raw_dir=raw_dir,
        normalized_root=normalized_root,
        target_race_date=race_date,
        snapshot_version=snapshot_version,
        db=db,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result["status"] == "stop":
        raise typer.Exit(code=2)


@app.command()
def ingest_normalized(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    normalized_dir: Path = typer.Option(..., help="Directory containing normalized races.csv/entries.csv/..."),
    race_date: str = typer.Option(..., help="YYYY-MM-DD"),
    snapshot_version: str = typer.Option(..., help="Snapshot version for audit log"),
):
    """
    Ingest normalized CSVs into warehouse (same final schema as ingest-csv).
    normalized -> warehouse layer.
    """
    from aikeiba.common.audit import log_pipeline_event
    from aikeiba.checks.data_quality import run_doctor
    from aikeiba.datalab.ingest_csv import ingest_from_csv_dir

    db = DuckDb.connect(db_path)
    apply_migrations(db)
    result = ingest_from_csv_dir(db=db, in_dir=normalized_dir)
    log_pipeline_event(
        db=db,
        stage="warehouse",
        snapshot_version=snapshot_version,
        target_race_date=race_date,
        status="ok",
        source_file_name="normalized_dir",
        source_file_path=str(normalized_dir),
        message="normalized csv ingested into warehouse",
        metrics=result,
    )
    doctor = run_doctor(db, race_date=race_date)
    status = "stop" if doctor["should_stop"] else "ok"
    log_pipeline_event(
        db=db,
        stage="warehouse",
        snapshot_version=snapshot_version,
        target_race_date=race_date,
        status=status,
        source_file_name="doctor",
        source_file_path=str(normalized_dir),
        message="post-ingest doctor check",
        metrics=doctor,
    )
    print(json.dumps({"ingest": result, "doctor": doctor}, ensure_ascii=False, indent=2))
    if doctor["should_stop"]:
        raise typer.Exit(code=2)


@app.command()
def jv_file_pipeline(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    raw_dir: Path = typer.Option(...),
    normalized_root: Path = typer.Option(Path("data/normalized")),
    race_date: str = typer.Option(..., help="YYYY-MM-DD"),
    snapshot_version: str = typer.Option(..., help="e.g. 20260414_0900"),
):
    """
    End-to-end file-based ingestion:
    raw -> normalized -> warehouse
    """
    from aikeiba.common.audit import log_pipeline_event
    from aikeiba.checks.data_quality import run_doctor
    from aikeiba.datalab.ingest_csv import ingest_from_csv_dir
    from aikeiba.datalab.raw_pipeline import normalize_raw_jv_to_normalized

    db = DuckDb.connect(db_path)
    apply_migrations(db)
    normalized = normalize_raw_jv_to_normalized(
        raw_dir=raw_dir,
        normalized_root=normalized_root,
        target_race_date=race_date,
        snapshot_version=snapshot_version,
        db=db,
    )
    if normalized["status"] == "stop":
        print(json.dumps(normalized, ensure_ascii=False, indent=2))
        raise typer.Exit(code=2)

    normalized_dir = Path(normalized["normalized_dir"])
    result = ingest_from_csv_dir(db=db, in_dir=normalized_dir)
    log_pipeline_event(
        db=db,
        stage="warehouse",
        snapshot_version=snapshot_version,
        target_race_date=race_date,
        status="warn" if normalized["status"] == "warn" else "ok",
        source_file_name="normalized_dir",
        source_file_path=str(normalized_dir),
        message="jv_file_pipeline ingest",
        metrics={"ingest_stats": result, "normalize_status": normalized["status"], "normalize_warns": normalized["warn_reasons"]},
    )
    doctor = run_doctor(db, race_date=race_date)
    final_status = "stop" if doctor["should_stop"] else ("warn" if normalized["status"] == "warn" else "ok")
    log_pipeline_event(
        db=db,
        stage="warehouse",
        snapshot_version=snapshot_version,
        target_race_date=race_date,
        status=final_status,
        source_file_name="doctor",
        source_file_path=str(normalized_dir),
        message="post-pipeline doctor check",
        metrics=doctor,
    )
    payload = {"normalize": normalized, "ingest": result, "doctor": doctor}
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if doctor["should_stop"]:
        raise typer.Exit(code=2)


@app.command()
def build_features(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    race_date: str = typer.Option(..., help="YYYY-MM-DD"),
    feature_snapshot_version: str = typer.Option("fs_v1"),
):
    """Build feature_store snapshot for a race_date (point-in-time safe)."""
    from aikeiba.features.assemble import build_feature_store_snapshot

    db = DuckDb.connect(db_path)
    result = build_feature_store_snapshot(db=db, race_date=race_date, feature_snapshot_version=feature_snapshot_version)
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command()
def build_features_range(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    start_date: str = typer.Option(..., help="YYYY-MM-DD"),
    end_date: str = typer.Option(..., help="YYYY-MM-DD"),
    feature_snapshot_version: str = typer.Option("fs_v1"),
):
    """Build feature_store snapshots for a date range (inclusive)."""
    from aikeiba.features.assemble import build_feature_store_snapshot

    db = DuckDb.connect(db_path)
    dates = db.query_df(
        "SELECT distinct cast(race_date as VARCHAR) AS d FROM races WHERE race_date BETWEEN cast(? as DATE) AND cast(? as DATE) ORDER BY d",
        (start_date, end_date),
    )["d"].tolist()

    out = []
    for d in dates:
        out.append(build_feature_store_snapshot(db=db, race_date=str(d), feature_snapshot_version=feature_snapshot_version))
    print(json.dumps({"dates": len(dates), "results": out}, ensure_ascii=False, indent=2))


@app.command()
def train_top3(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    models_root: Path = typer.Option(Path("data/models")),
    model_version: str = typer.Option("top3_v1"),
    feature_snapshot_version: str = typer.Option("fs_v1"),
    train_end_date: str = typer.Option(..., help="YYYY-MM-DD"),
    valid_start_date: str = typer.Option(..., help="YYYY-MM-DD"),
    valid_end_date: str = typer.Option(..., help="YYYY-MM-DD"),
    calibration_start_date: str = typer.Option("", help="Optional YYYY-MM-DD (default: auto split inside validation window)"),
    calibration_end_date: str = typer.Option("", help="Optional YYYY-MM-DD (default: auto split inside validation window)"),
    calibration_ratio: float = typer.Option(0.5, help="Auto split ratio for calibration inside validation window"),
    calibration_method: str = typer.Option("isotonic", help="isotonic|sigmoid|none"),
    split_config: Path | None = typer.Option(None, help="Optional split config file (.yaml/.json)"),
    test_period: str = typer.Option("", help="Optional YYYY-MM-DD..YYYY-MM-DD for dataset fingerprint alignment"),
    feature_set: str = typer.Option("stability", help="baseline or stability or stability_plus_pace"),
):
    """Train top3 model and save bundle (LightGBM + isotonic calibrator)."""
    from aikeiba.modeling.top3 import train_top3_bundle

    split_config_path = None
    split_config_hash = None
    if split_config is not None:
        cfg, cfg_hash = _load_split_config(split_config)
        cfg_warnings = _validate_split_config(cfg, split_config)
        for w in cfg_warnings:
            print(f"[yellow]WARN[/yellow] {w}")
        split_config_path = str(split_config)
        split_config_hash = cfg_hash
        train_cfg = cfg.get("train", {}) if isinstance(cfg, dict) else {}
        cal_cfg = cfg.get("calibration", {}) if isinstance(cfg, dict) else {}
        val_cfg = cfg.get("validation", {}) if isinstance(cfg, dict) else {}
        if train_cfg.get("end_date"):
            train_end_date = str(train_cfg.get("end_date"))
        if val_cfg.get("start_date"):
            valid_start_date = str(val_cfg.get("start_date"))
        if val_cfg.get("end_date"):
            valid_end_date = str(val_cfg.get("end_date"))
        if cal_cfg.get("start_date"):
            calibration_start_date = str(cal_cfg.get("start_date"))
        if cal_cfg.get("end_date"):
            calibration_end_date = str(cal_cfg.get("end_date"))
        if cal_cfg.get("ratio_if_dates_missing") is not None:
            calibration_ratio = float(cal_cfg.get("ratio_if_dates_missing"))

    db = DuckDb.connect(db_path)
    models_root.mkdir(parents=True, exist_ok=True)
    result = train_top3_bundle(
        db=db,
        models_root=models_root,
        model_version=model_version,
        feature_snapshot_version=feature_snapshot_version,
        train_end_date=train_end_date,
        valid_start_date=valid_start_date,
        valid_end_date=valid_end_date,
        calibration_start_date=calibration_start_date if calibration_start_date else None,
        calibration_end_date=calibration_end_date if calibration_end_date else None,
        calibration_ratio=calibration_ratio,
        calibration_method=calibration_method,
        split_config_path=split_config_path,
        split_config_hash=split_config_hash,
        test_period=test_period if test_period else None,
        feature_set=feature_set,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command()
def build_payouts_from_target_text(
    input_txt: Path = typer.Option(..., help="TARGET text export containing payout lines (e.g. payouts.txt)"),
    output_csv: Path = typer.Option(..., help="Output payouts.csv path"),
):
    """Convert TARGET text export (with payout lines) to payouts.csv for Aikeiba raw layer."""
    from aikeiba.datalab.target_text_payouts import build_payouts_csv_from_target_text

    result = build_payouts_csv_from_target_text(input_txt=input_txt, output_csv=output_csv)
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command()
def build_races_from_target_text(
    input_txt: Path = typer.Option(..., help="TARGET text export containing race blocks (e.g. payouts.txt)"),
    output_csv: Path = typer.Option(..., help="Output races.csv path"),
):
    """Convert TARGET text export to races.csv for Aikeiba raw layer."""
    from aikeiba.datalab.target_text_payouts import build_races_csv_from_target_text

    result = build_races_csv_from_target_text(input_txt=input_txt, output_csv=output_csv)
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command()
def build_entries_results_from_target_text(
    input_txt: Path = typer.Option(..., help="TARGET text export containing result tables (e.g. payouts.txt)"),
    out_dir: Path = typer.Option(..., help="Output directory to write entries.csv/results.csv"),
):
    """Convert TARGET payout text export to entries.csv/results.csv (horse_id will be empty)."""
    from aikeiba.datalab.target_text_payouts import build_entries_results_from_target_text as _build

    out_dir.mkdir(parents=True, exist_ok=True)
    result = _build(
        input_txt=input_txt,
        output_entries_csv=out_dir / "entries.csv",
        output_results_csv=out_dir / "results.csv",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command()
def build_results_from_target_text(
    input_txt: Path = typer.Option(..., help="TARGET text export: 成績（整形テキスト） (e.g. results.txt)"),
    races_csv: Path = typer.Option(..., help="Aikeiba raw races.csv path (used for race_id mapping)"),
    output_csv: Path = typer.Option(..., help="Output raw results.csv path"),
):
    """Convert TARGET result text export to Aikeiba raw results.csv."""
    from aikeiba.datalab.target_text_results import build_results_csv_from_target_result_text

    result = build_results_csv_from_target_result_text(input_txt=input_txt, races_csv=races_csv, output_csv=output_csv)
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command()
def build_raw_from_target_export(
    target_dir: Path = typer.Option(..., help="Directory containing TARGET exports (payouts.txt, entries.csv, odds_target.csv, results.txt, horse_master.csv)"),
    raw_dir: Path = typer.Option(..., help="Output raw directory (data/raw/YYYYMMDD_real)"),
    race_date: str = typer.Option(..., help="YYYY-MM-DD (for manifest)"),
    snapshot_version: str = typer.Option(..., help="Snapshot version label (e.g. 20260329_real)"),
    odds_snapshot_version: str = typer.Option(..., help="Odds snapshot version label (e.g. odds_target_20260329)"),
    overwrite: bool = typer.Option(False, help="Overwrite existing raw files"),
):
    """Build Aikeiba raw files from TARGET exports (no GUI automation)."""
    from aikeiba.datalab.target_export_bundle import build_raw_from_target_export as _build

    result = _build(
        target_dir=target_dir,
        raw_dir=raw_dir,
        race_date=race_date,
        snapshot_version=snapshot_version,
        odds_snapshot_version=odds_snapshot_version,
        overwrite=overwrite,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command()
def infer_top3(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    models_root: Path = typer.Option(Path("data/models")),
    race_date: str = typer.Option(..., help="YYYY-MM-DD"),
    feature_snapshot_version: str = typer.Option("fs_v1"),
    model_version: str = typer.Option("top3_v1"),
    odds_snapshot_version: str = typer.Option("odds_v1"),
):
    """Run top3 inference for a race_date and store horse_predictions rows."""
    from aikeiba.common.hashing import stable_fingerprint
    from aikeiba.inference.top3 import infer_top3_for_date

    db = DuckDb.connect(db_path)
    dataset_fingerprint = stable_fingerprint(
        {
            "race_date": race_date,
            "feature_snapshot_version": feature_snapshot_version,
            "model_version": model_version,
            "odds_snapshot_version": odds_snapshot_version,
        }
    )
    result = infer_top3_for_date(
        db=db,
        models_root=models_root,
        race_date=race_date,
        feature_snapshot_version=feature_snapshot_version,
        model_version=model_version,
        odds_snapshot_version=odds_snapshot_version,
        dataset_fingerprint=dataset_fingerprint,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command()
def export_static(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    race_date: str = typer.Option(..., help="YYYY-MM-DD"),
    out_dir: Path = typer.Option(Path("../data"), help="Static site data dir (default: repo root data/)"),
    feature_snapshot_version: str = typer.Option("fs_v1"),
    model_version: str = typer.Option("top3_v1"),
    odds_snapshot_version: str = typer.Option("odds_v1"),
    allow_no_wide_odds: bool = typer.Option(True, help="MVP: do not require wide odds snapshot"),
):
    """
    Export JSON files for the static dashboard.

    MVP: uses p_top3-centered candidates. EV integration is Phase 2.
    """
    from aikeiba.export.to_static import export_for_dashboard

    db = DuckDb.connect(db_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    result = export_for_dashboard(
        db=db,
        race_date=race_date,
        out_dir=out_dir,
        feature_snapshot_version=feature_snapshot_version,
        model_version=model_version,
        odds_snapshot_version=odds_snapshot_version,
        allow_no_wide_odds=allow_no_wide_odds,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command()
def doctor(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    race_date: Optional[str] = typer.Option(None, help="YYYY-MM-DD (optional)"),
):
    """Run data quality checks and show stop/warn reasons."""
    from aikeiba.checks.data_quality import run_doctor

    db = DuckDb.connect(db_path)
    report = run_doctor(db, race_date=race_date)
    print(json.dumps(report, ensure_ascii=False, indent=2))


@app.command()
def demo_mvp(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    models_root: Path = typer.Option(Path("data/models")),
    feature_snapshot_version: str = typer.Option("fs_v1"),
    model_version: str = typer.Option("top3_v1"),
    odds_snapshot_version: str = typer.Option("odds_v1"),
    out_dir: Path = typer.Option(Path("../data")),
):
    """Run an end-to-end demo MVP (seed -> features -> train -> infer -> export)."""
    from aikeiba.tools.seed_demo import seed_demo_dataset
    from aikeiba.features.assemble import build_feature_store_snapshot
    from aikeiba.modeling.top3 import train_top3_bundle
    from aikeiba.inference.top3 import infer_top3_for_date
    from aikeiba.common.hashing import stable_fingerprint
    from aikeiba.export.to_static import export_for_dashboard

    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = DuckDb.connect(db_path)
    apply_migrations(db)
    seed_demo_dataset(db)

    # Build features for historical + today (demo)
    dates = db.query_df("SELECT distinct cast(race_date as VARCHAR) AS d FROM races ORDER BY d")["d"].tolist()
    for d in dates:
        build_feature_store_snapshot(db=db, race_date=str(d), feature_snapshot_version=feature_snapshot_version)

    models_root.mkdir(parents=True, exist_ok=True)
    train_top3_bundle(
        db=db,
        models_root=models_root,
        model_version=model_version,
        feature_snapshot_version=feature_snapshot_version,
        train_end_date="2026-03-30",
        valid_start_date="2026-03-20",
        valid_end_date="2026-03-30",
    )

    dataset_fingerprint = stable_fingerprint(
        {
            "race_date": "2026-04-14",
            "feature_snapshot_version": feature_snapshot_version,
            "model_version": model_version,
            "odds_snapshot_version": odds_snapshot_version,
        }
    )
    infer_top3_for_date(
        db=db,
        models_root=models_root,
        race_date="2026-04-14",
        feature_snapshot_version=feature_snapshot_version,
        model_version=model_version,
        odds_snapshot_version=odds_snapshot_version,
        dataset_fingerprint=dataset_fingerprint,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    result = export_for_dashboard(
        db=db,
        race_date="2026-04-14",
        out_dir=out_dir,
        feature_snapshot_version=feature_snapshot_version,
        model_version=model_version,
        odds_snapshot_version=odds_snapshot_version,
        allow_no_wide_odds=True,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command("run-race-day")
def run_race_day(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    raw_dir: Path = typer.Option(..., help="JV raw dir"),
    allow_missing_raw: bool = typer.Option(False, help="If true, continue even when required raw files are missing"),
    normalized_root: Path = typer.Option(Path("data/normalized")),
    race_date: str = typer.Option(..., help="YYYY-MM-DD"),
    snapshot_version: str = typer.Option(..., help="raw snapshot version, e.g. 20260414_0900"),
    feature_snapshot_version: str = typer.Option("fs_v1"),
    model_version: str = typer.Option("top3_v1"),
    odds_snapshot_version: str = typer.Option("odds_v1"),
    models_root: Path = typer.Option(Path("data/models")),
    export_out_dir: Path = typer.Option(Path("../data")),
    run_summary_path: Path = typer.Option(Path("data/exports/run_summary.json")),
    auto_run_summary_dir: Path | None = typer.Option(None, help="Optional directory for automatic run_summary.v1 history"),
    allow_no_wide_odds: bool = typer.Option(True),
    results_csv_path: Path | None = typer.Option(None, help="Optional override path for results.csv"),
    payouts_csv_path: Path | None = typer.Option(None, help="Optional override path for payouts.csv"),
    decision_density_top3_max: float = typer.Option(1.35, help="Decision gate: density_top3_max"),
    decision_gap12_min: float = typer.Option(0.003, help="Decision gate: gap12_min"),
    decision_ai_weight: float = typer.Option(0.65, help="Decision fusion weight for AI probability (0-1)"),
    enable_value_skip: bool = typer.Option(False, help="Enable skip when value signal is weak"),
    min_ai_market_gap: float = typer.Option(0.0, help="Minimum required race max ai_market_gap for value skip"),
    enable_market_overrated_skip: bool = typer.Option(False, help="Enable skip by market-overrated top count"),
    max_market_overrated_top_count: int = typer.Option(3, help="Maximum allowed overrated horses in top3"),
    enable_pair_ev_skip: bool = typer.Option(False, help="Enable skip by top pair value score"),
    min_pair_value_score: float = typer.Option(0.0, help="Minimum top pair value score"),
    force_null_top3_for_test: bool = typer.Option(False, hidden=True),
    force_overlap_guard_fail_for_test: bool = typer.Option(False, hidden=True),
    skip_post_infer_gate: bool = typer.Option(False, help="Skip post-infer probability gate (unsafe; for debugging/experiments)"),
    skip_doctor_structured_stop: bool = typer.Option(False, help="Skip doctor structured stop (unsafe; for debugging/experiments)"),
    probability_gate_mode: str = typer.Option("strict", help="Probability gate mode: strict or warn-only"),
    race_meta_policy: str = typer.Option("skip", help="Race metadata policy: skip or warn-only"),
    race_day_out_root: Path = typer.Option(Path("data/race_day"), help="Root directory for race-day artifacts"),
    overwrite: bool = typer.Option(False, help="Overwrite fixed race-day artifact paths (default uses timestamp subdir)"),
    pair_model_root: Path = typer.Option(Path("racing_ai/data/models_compare/pair_reranker"), help="Root dir of pair shadow model bundles"),
    pair_model_version: str = typer.Option("pair_reranker_ts_v4", help="Pair reranker model version for shadow scoring"),
    model_dynamic_min_score: float = typer.Option(0.08, help="Shadow dynamic selection: minimum top score to buy"),
    model_dynamic_min_edge: float = typer.Option(0.0, help="Shadow dynamic selection: minimum top edge to buy"),
    model_dynamic_min_gap: float = typer.Option(0.01, help="Shadow dynamic selection: minimum top-vs-next gap to buy"),
    model_dynamic_default_k: int = typer.Option(5, help="Shadow dynamic selection: default selected pair count"),
    model_dynamic_min_k: int = typer.Option(1, help="Shadow dynamic selection: minimum selected pair count"),
    model_dynamic_max_k: int = typer.Option(5, help="Shadow dynamic selection: maximum selected pair count"),
    emit_expanded_candidates: bool = typer.Option(False, help="Emit evaluation-only expanded candidate pool artifact"),
    expanded_top_horse_n: int = typer.Option(10, help="Expanded pool: top fused-prob horse count"),
    expanded_ai_gap_horse_n: int = typer.Option(10, help="Expanded pool: ai-market-gap horse count"),
    expanded_max_pairs_per_race: int = typer.Option(45, help="Expanded pool: max pairs per race"),
):
    """
    Run race-day orchestration in one shot:
    1) jv-file-pipeline
    2) build-features
    3) doctor
    4) infer-top3
    5) decision (skip/candidates)
    6) export-static
    """
    from aikeiba.orchestration.race_day import run_race_day_pipeline
    from aikeiba.decision.skip_reasoning import SkipReasonConfig
    from aikeiba.common.raw_precheck import run_raw_precheck

    precheck = run_raw_precheck(raw_dir=raw_dir, race_date=race_date, model_version=model_version)
    missing_raw_files = list(precheck.get("missing_files", []))
    empty_raw_files = list(precheck.get("empty_files", []))
    precheck_stop_reason = precheck.get("stop_reason")
    precheck_should_stop = bool(precheck_stop_reason) and not allow_missing_raw
    _best_effort_log_raw_precheck_to_db(
        db_path=db_path,
        command_name="run-race-day",
        race_date=race_date,
        model_version=model_version,
        precheck=precheck,
        status="stop" if precheck_should_stop else "pass",
        stop_reason=str(precheck_stop_reason) if precheck_should_stop else None,
        run_summary_path=run_summary_path,
        daily_cycle_summary_path=None,
    )

    if precheck_should_stop:
        stop_summary = {
            "status": "stop",
            "stop_reason": precheck_stop_reason,
            "race_date": race_date,
            "model_version": model_version,
            "feature_snapshot_version": feature_snapshot_version,
            "created_at": dt.datetime.now().isoformat(timespec="seconds"),
            "raw_required_files_check": {
                "required_files": precheck.get("required_files", []),
                "missing_files": missing_raw_files,
                "empty_files": empty_raw_files,
                "row_counts": precheck.get("row_counts", {}),
                "status": "fail",
            },
            "raw_precheck_log_path": str(precheck.get("log_path")),
        }
        run_summary_path.parent.mkdir(parents=True, exist_ok=True)
        run_summary_path.write_text(json.dumps(stop_summary, ensure_ascii=False, indent=2), encoding="utf-8")
        if missing_raw_files:
            print(f"[yellow]WARN[/yellow] missing required raw files: {', '.join(missing_raw_files)}")
        for name in empty_raw_files:
            print(f"[yellow]WARN[/yellow] {name} exists but has 0 data rows")
        print(f"[yellow]WARN[/yellow] raw precheck log: {precheck.get('log_path')}")
        print(json.dumps(stop_summary, ensure_ascii=False, indent=2))
        raise typer.Exit(code=2)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = DuckDb.connect(db_path)
    apply_migrations(db)

    working_raw_dir = raw_dir
    with tempfile.TemporaryDirectory(prefix="aikeiba_raw_") as td:
        temp_dir = Path(td)
        if results_csv_path is not None or payouts_csv_path is not None:
            temp_raw = temp_dir / "raw"
            temp_raw.mkdir(parents=True, exist_ok=True)
            for src in raw_dir.glob("*.csv"):
                shutil.copy2(src, temp_raw / src.name)
            if results_csv_path is not None:
                shutil.copy2(results_csv_path, temp_raw / "results.csv")
            if payouts_csv_path is not None:
                shutil.copy2(payouts_csv_path, temp_raw / "payouts.csv")
            working_raw_dir = temp_raw

        summary = run_race_day_pipeline(
            db=db,
            raw_dir=working_raw_dir,
            normalized_root=normalized_root,
            race_date=race_date,
            snapshot_version=snapshot_version,
            feature_snapshot_version=feature_snapshot_version,
            model_version=model_version,
            odds_snapshot_version=odds_snapshot_version,
            models_root=models_root,
            export_out_dir=export_out_dir,
            run_summary_path=run_summary_path,
            auto_run_summary_dir=auto_run_summary_dir,
            allow_no_wide_odds=allow_no_wide_odds,
            decision_density_top3_max=decision_density_top3_max,
            decision_gap12_min=decision_gap12_min,
            decision_ai_weight=decision_ai_weight,
            skip_reason_config=SkipReasonConfig(
                density_top3_max=decision_density_top3_max,
                gap12_min=decision_gap12_min,
                min_ai_market_gap=min_ai_market_gap,
                max_market_overrated_top_count=max_market_overrated_top_count,
                min_pair_value_score=min_pair_value_score,
                enforce_no_value_horse=enable_value_skip,
                enforce_market_overrated_top_count=enable_market_overrated_skip,
                enforce_pair_value=enable_pair_ev_skip,
            ),
            force_null_top3_for_test=force_null_top3_for_test,
            force_overlap_guard_fail_for_test=force_overlap_guard_fail_for_test,
            skip_post_infer_gate=skip_post_infer_gate,
            skip_doctor_structured_stop=skip_doctor_structured_stop,
            probability_gate_mode=probability_gate_mode,
            race_meta_policy=race_meta_policy,
            race_day_out_root=race_day_out_root,
            overwrite_race_day_outputs=overwrite,
            pair_model_root=pair_model_root,
            pair_model_version=pair_model_version,
            model_dynamic_min_score=model_dynamic_min_score,
            model_dynamic_min_edge=model_dynamic_min_edge,
            model_dynamic_min_gap=model_dynamic_min_gap,
            model_dynamic_default_k=model_dynamic_default_k,
            model_dynamic_min_k=model_dynamic_min_k,
            model_dynamic_max_k=model_dynamic_max_k,
            emit_expanded_candidates=emit_expanded_candidates,
            expanded_top_horse_n=expanded_top_horse_n,
            expanded_ai_gap_horse_n=expanded_ai_gap_horse_n,
            expanded_max_pairs_per_race=expanded_max_pairs_per_race,
        )
    summary["raw_required_files_check"] = {
        "required_files": precheck.get("required_files", []),
        "missing_files": missing_raw_files,
        "empty_files": empty_raw_files,
        "row_counts": precheck.get("row_counts", {}),
        "status": "fail" if precheck_stop_reason else "pass",
        "stop_reason": precheck_stop_reason,
    }
    if summary.get("run_summary_path"):
        rs_path = Path(str(summary["run_summary_path"]))
        try:
            rs_path.parent.mkdir(parents=True, exist_ok=True)
            rs_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            print(f"[yellow]WARN[/yellow] failed to persist raw_required_files_check into run_summary: {exc}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if summary["status"] == "stop":
        raise typer.Exit(code=2)


@app.command("inspect-raw-dir")
def inspect_raw_dir_cmd(
    raw_dir: Path = typer.Option(..., help="Raw directory to inspect"),
    out_json: Path | None = typer.Option(None, help="Optional output json path"),
):
    from aikeiba.common.raw_inspect import inspect_raw_dir, write_raw_inspect

    report = inspect_raw_dir(raw_dir)
    if out_json is not None:
        write_raw_inspect(out_json, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


@app.command("build-real-raw-from-jv")
def build_real_raw_from_jv_cmd(
    source_dir: Path = typer.Option(..., help="JV-Link export directory"),
    target_date: str = typer.Option(..., help="YYYY-MM-DD"),
    out_raw_dir: Path = typer.Option(Path("data/raw/20260330_real"), help="Aikeiba raw output directory"),
    races_file: str = typer.Option("races.csv"),
    entries_file: str = typer.Option("entries.csv"),
    results_file: str = typer.Option("results.csv"),
    payouts_file: str = typer.Option("payouts.csv"),
):
    """
    Build Aikeiba raw csvs from JV-Link file export directory.
    Expected output:
      races.csv / entries.csv / results.csv / payouts.csv
    """
    from aikeiba.datalab.jvlink_collect import build_real_raw_from_jv_export

    result = build_real_raw_from_jv_export(
        source_dir=source_dir,
        target_date=target_date,
        out_raw_dir=out_raw_dir,
        races_file=races_file,
        entries_file=entries_file,
        results_file=results_file,
        payouts_file=payouts_file,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command("build-entries-from-target-image")
def build_entries_from_target_image_cmd(
    input_csv: Path = typer.Option(..., help="TARGET '出馬表・画面イメージ一括出力(CSV形式)' entries csv (no header)"),
    races_csv: Path = typer.Option(..., help="Aikeiba races.csv with race_id (headered)"),
    out_entries_csv: Path = typer.Option(..., help="Output Aikeiba raw entries.csv (headered)"),
    overwrite: bool = typer.Option(False),
):
    """
    Convert TARGET screen-image entries CSV into Aikeiba raw entries.csv schema.
    """
    from aikeiba.datalab.target_image_entries import build_entries_csv_from_target_image

    result = build_entries_csv_from_target_image(
        input_csv=input_csv,
        races_csv=races_csv,
        out_entries_csv=out_entries_csv,
        overwrite=overwrite,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command("fill-horse-id-from-target-results")
def fill_horse_id_from_target_results_cmd(
    entries_csv: Path = typer.Option(..., help="Aikeiba raw entries.csv (headered)"),
    target_results_csv: Path = typer.Option(..., help="TARGET 全馬成績CSV (no header, 52 fields)"),
    out_entries_csv: Path = typer.Option(..., help="Output entries.csv with horse_id filled"),
    overwrite: bool = typer.Option(False),
):
    """
    Fill entries.csv horse_id using TARGET 全馬成績CSV export (blood registration number).
    """
    from aikeiba.datalab.target_results_horse_id import fill_entries_horse_id_from_target_results

    result = fill_entries_horse_id_from_target_results(
        entries_csv=entries_csv,
        target_results_csv=target_results_csv,
        out_entries_csv=out_entries_csv,
        overwrite=overwrite,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command("fill-horse-id-from-target-horse-master")
def fill_horse_id_from_target_horse_master_cmd(
    entries_csv: Path = typer.Option(..., help="Aikeiba raw entries.csv (headered)"),
    horse_master_csv: Path = typer.Option(..., help="TARGET horse master csv (馬名,血統登録番号)"),
    out_entries_csv: Path = typer.Option(..., help="Output entries.csv with horse_id filled"),
    overwrite: bool = typer.Option(False),
):
    """
    Fill entries.csv horse_id using TARGET horse master export (recommended).
    """
    from aikeiba.datalab.target_horse_master import fill_entries_horse_id_from_target_horse_master

    result = fill_entries_horse_id_from_target_horse_master(
        entries_csv=entries_csv,
        horse_master_csv=horse_master_csv,
        out_entries_csv=out_entries_csv,
        overwrite=overwrite,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command("build-odds-from-target-csv")
def build_odds_from_target_csv_cmd(
    input_csv: Path = typer.Option(..., help="TARGET odds csv (ターゲット仕様)"),
    races_csv: Path = typer.Option(..., help="Aikeiba races.csv with race_id (headered)"),
    out_odds_csv: Path = typer.Option(..., help="Output Aikeiba raw odds.csv (headered)"),
    snapshot_version: str = typer.Option(..., help="raw snapshot version (source_version)"),
    odds_snapshot_version: str = typer.Option(..., help="odds_snapshot_version to store"),
    captured_at: str | None = typer.Option(None, help="odds captured_at timestamp (ISO). default: now"),
    overwrite: bool = typer.Option(False),
):
    """
    Convert TARGET odds CSV (ターゲット仕様) into Aikeiba raw odds.csv.
    """
    from aikeiba.datalab.target_odds import build_odds_csv_from_target_csv

    result = build_odds_csv_from_target_csv(
        input_csv=input_csv,
        races_csv=races_csv,
        out_odds_csv=out_odds_csv,
        snapshot_version=snapshot_version,
        odds_snapshot_version=odds_snapshot_version,
        captured_at=captured_at,
        overwrite=overwrite,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command("make-dataset-manifest")
def make_dataset_manifest_cmd(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    out_dir: Path = typer.Option(Path("data/datasets")),
    dataset_name: str = typer.Option(..., help="Dataset manifest name"),
    task_name: str = typer.Option("top3"),
    feature_snapshot_version: str = typer.Option("fs_v1"),
    train_period: str = typer.Option(..., help="YYYY-MM-DD..YYYY-MM-DD"),
    valid_period: str = typer.Option(..., help="YYYY-MM-DD..YYYY-MM-DD"),
    test_period: str = typer.Option(..., help="YYYY-MM-DD..YYYY-MM-DD"),
    filters_json: str = typer.Option("{}", help="JSON string"),
    excluded_rules_json: str = typer.Option("[]", help="JSON array string"),
):
    from aikeiba.evaluation.dataset_manifest import make_dataset_manifest

    db = DuckDb.connect(db_path)
    filters = json.loads(filters_json)
    excluded_rules = json.loads(excluded_rules_json)
    result = make_dataset_manifest(
        db=db,
        out_dir=out_dir,
        dataset_name=dataset_name,
        task_name=task_name,
        feature_snapshot_version=feature_snapshot_version,
        train_period=train_period,
        valid_period=valid_period,
        test_period=test_period,
        filters=filters,
        excluded_rules=excluded_rules,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command("compare-experiments")
def compare_experiments_cmd(
    dataset_manifest: Path = typer.Option(..., help="Path to dataset_manifest.json"),
    report_dir: Path = typer.Option(Path("data/reports")),
    experiment_spec: list[str] = typer.Option(
        ...,
        help="Repeatable spec: name|model_dir|optional_run_summary_path",
    ),
    strict_mismatch: bool = typer.Option(True, help="Exit non-zero when mismatch"),
    publish_latest: bool = typer.Option(True, help="Copy latest comparison files to static data dir"),
    latest_out_dir: Path = typer.Option(Path("../data"), help="Static site data directory for latest comparison files"),
):
    from aikeiba.evaluation.comparison_view import build_comparison_view, publish_latest_comparison_files
    from aikeiba.evaluation.comparison_report import make_comparison_report

    names: list[str] = []
    model_dirs: list[Path] = []
    run_paths: list[Path | None] = []
    for spec in experiment_spec:
        parts = [p.strip() for p in spec.split("|")]
        if len(parts) < 2:
            raise typer.BadParameter(f"invalid --experiment-spec: {spec}")
        names.append(parts[0])
        model_dirs.append(Path(parts[1]))
        run_paths.append(Path(parts[2]) if len(parts) >= 3 and parts[2] != "" else None)

    result = make_comparison_report(
        dataset_manifest_path=dataset_manifest,
        report_dir=report_dir,
        experiment_names=names,
        experiment_model_dirs=model_dirs,
        experiment_run_summary_paths=run_paths,
        strict_mismatch=strict_mismatch,
    )
    view_result = build_comparison_view(
        comparison_report_json_path=Path(result["comparison_report_json"]),
        dataset_manifest_path=dataset_manifest,
        comparison_report_csv_path=Path(result["comparison_report_csv"]),
        out_path=report_dir / "comparison_view.json",
    )
    result["comparison_view_json"] = view_result["comparison_view_json"]
    if publish_latest:
        latest_result = publish_latest_comparison_files(
            comparison_report_json_path=Path(result["comparison_report_json"]),
            comparison_view_json_path=Path(result["comparison_view_json"]),
            latest_dir=latest_out_dir,
        )
        result.update(latest_result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if strict_mismatch and result["comparison_status"] == "mismatch":
        raise typer.Exit(code=2)


@app.command("search-decision-thresholds")
def search_decision_thresholds_cmd(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    start_date: str = typer.Option(..., help="YYYY-MM-DD"),
    end_date: str = typer.Option(..., help="YYYY-MM-DD"),
    model_version: str = typer.Option(..., help="e.g. top3_stability_plus_pace_v3"),
    density_values: str = typer.Option("1.2,1.35,1.5,1.8,2.1,2.4", help="Comma separated float grid"),
    gap12_values: str = typer.Option("0.0,0.003,0.005,0.01,0.02", help="Comma separated float grid"),
    ai_weight_values: str = typer.Option("0.5,0.65,0.8", help="Comma separated AI weight grid"),
):
    from aikeiba.evaluation.decision_thresholds import search_decision_thresholds

    db = DuckDb.connect(db_path)
    dvals = [float(x.strip()) for x in density_values.split(",") if x.strip() != ""]
    gvals = [float(x.strip()) for x in gap12_values.split(",") if x.strip() != ""]
    wvals = [float(x.strip()) for x in ai_weight_values.split(",") if x.strip() != ""]
    result = search_decision_thresholds(
        db=db,
        start_date=start_date,
        end_date=end_date,
        model_version=model_version,
        density_values=dvals,
        gap12_values=gvals,
        ai_weight_values=wvals,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


@app.command("check-wide-label-coverage")
def check_wide_label_coverage_cmd(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    pairs_glob: str = typer.Option("racing_ai/data/bets/wide_pair_candidates_*.parquet"),
    pair_base_path: Path = typer.Option(Path("racing_ai/data/modeling/pair_learning_base.parquet")),
    out_md: Path = typer.Option(Path("racing_ai/reports/wide_label_coverage_report.md")),
):
    from aikeiba.evaluation.wide_label_coverage import build_wide_label_coverage_report

    res = build_wide_label_coverage_report(
        db_path=db_path,
        pairs_glob=pairs_glob,
        pair_base_path=pair_base_path,
        out_md=out_md,
    )
    print(json.dumps(res, ensure_ascii=False, indent=2))


@app.command("run-daily-cycle")
def run_daily_cycle_cmd(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    raw_dir: Path = typer.Option(..., help="JV raw dir"),
    allow_missing_raw: bool = typer.Option(False, help="If true, continue even when required raw files are missing"),
    normalized_root: Path = typer.Option(Path("data/normalized")),
    race_date: str = typer.Option(..., help="YYYY-MM-DD"),
    snapshot_version: str = typer.Option(..., help="raw snapshot version, e.g. 20260414_0900"),
    feature_snapshot_version: str = typer.Option("fs_v1"),
    model_version: str = typer.Option("top3_v1"),
    odds_snapshot_version: str = typer.Option("odds_v1"),
    models_root: Path = typer.Option(Path("data/models")),
    export_out_dir: Path = typer.Option(Path("../data")),
    run_summary_path: Path = typer.Option(Path("data/exports/run_summary.json")),
    allow_no_wide_odds: bool = typer.Option(True),
    dataset_manifest_path: Path | None = typer.Option(None, help="Optional fixed dataset manifest path for compare"),
    experiment_spec: list[str] = typer.Option(
        [],
        help="Repeatable spec: name|model_dir|optional_run_summary_path",
    ),
    experiment_task: str = typer.Option("top3", help="Auto discovery target task under models_root"),
    report_out_dir: Path = typer.Option(Path("data/reports/daily_cycle")),
    skip_compare: bool = typer.Option(False),
    publish_latest: bool = typer.Option(True),
    latest_out_dir: Path = typer.Option(Path("../data")),
    strict_compare_mismatch: bool = typer.Option(False, help="When true, treat compare mismatch as failed state"),
    daily_cycle_summary_path: Path = typer.Option(Path("data/exports/daily_cycle_summary.json")),
    run_label_coverage_check: bool = typer.Option(False, help="Run wide label coverage report after daily cycle"),
    label_coverage_pairs_glob: str = typer.Option("racing_ai/data/bets/wide_pair_candidates_*.parquet"),
    label_coverage_pair_base_path: Path = typer.Option(Path("racing_ai/data/modeling/pair_learning_base.parquet")),
    label_coverage_out_md: Path = typer.Option(Path("racing_ai/reports/wide_label_coverage_report.md")),
):
    from aikeiba.orchestration.daily_cycle import run_daily_cycle
    from aikeiba.common.raw_precheck import run_raw_precheck

    precheck = run_raw_precheck(raw_dir=raw_dir, race_date=race_date, model_version=model_version)
    missing_raw_files = list(precheck.get("missing_files", []))
    empty_raw_files = list(precheck.get("empty_files", []))
    precheck_stop_reason = precheck.get("stop_reason")
    precheck_block = bool(precheck_stop_reason) and not allow_missing_raw
    precheck_status = "not_ready" if precheck_stop_reason == "raw_files_empty" and precheck_block else ("stop" if precheck_block else "pass")
    _best_effort_log_raw_precheck_to_db(
        db_path=db_path,
        command_name="run-daily-cycle",
        race_date=race_date,
        model_version=model_version,
        precheck=precheck,
        status=precheck_status,
        stop_reason=str(precheck_stop_reason) if precheck_block else None,
        run_summary_path=run_summary_path,
        daily_cycle_summary_path=daily_cycle_summary_path,
    )
    if precheck_block:
        stop_payload = {
            "status": precheck_status,
            "stop_reason": precheck_stop_reason,
            "race_date": race_date,
            "model_version": model_version,
            "raw_dir": str(raw_dir),
            "required_files": precheck.get("required_files", []),
            "missing_files": missing_raw_files,
            "empty_files": empty_raw_files,
            "row_counts": precheck.get("row_counts", {}),
            "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
            "raw_precheck_log_path": str(precheck.get("log_path")),
        }
        daily_cycle_summary_path.parent.mkdir(parents=True, exist_ok=True)
        daily_cycle_summary_path.write_text(json.dumps(stop_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        if missing_raw_files:
            print(f"[yellow]WARN[/yellow] missing required raw files: {', '.join(missing_raw_files)}")
        for name in empty_raw_files:
            print(f"[yellow]WARN[/yellow] {name} exists but has 0 data rows")
        print(f"[yellow]WARN[/yellow] raw precheck log: {precheck.get('log_path')}")
        print(json.dumps(stop_payload, ensure_ascii=False, indent=2))
        raise typer.Exit(code=2)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = DuckDb.connect(db_path)
    apply_migrations(db)

    experiment_names: list[str] = []
    experiment_model_dirs: list[Path] = []
    experiment_run_paths: list[Path | None] = []
    for spec in experiment_spec:
        parts = [p.strip() for p in spec.split("|")]
        if len(parts) < 2:
            raise typer.BadParameter(f"invalid --experiment-spec: {spec}")
        experiment_names.append(parts[0])
        experiment_model_dirs.append(Path(parts[1]))
        experiment_run_paths.append(Path(parts[2]) if len(parts) >= 3 and parts[2] else None)

    summary = run_daily_cycle(
        db=db,
        raw_dir=raw_dir,
        normalized_root=normalized_root,
        race_date=race_date,
        snapshot_version=snapshot_version,
        feature_snapshot_version=feature_snapshot_version,
        model_version=model_version,
        odds_snapshot_version=odds_snapshot_version,
        models_root=models_root,
        export_out_dir=export_out_dir,
        run_summary_path=run_summary_path,
        allow_no_wide_odds=allow_no_wide_odds,
        dataset_manifest_path=dataset_manifest_path,
        experiment_names=experiment_names,
        experiment_model_dirs=experiment_model_dirs,
        experiment_run_summary_paths=experiment_run_paths,
        experiment_task=experiment_task,
        report_out_dir=report_out_dir,
        skip_compare=skip_compare,
        publish_latest=publish_latest,
        latest_out_dir=latest_out_dir,
        strict_compare_mismatch=strict_compare_mismatch,
        daily_cycle_summary_path=daily_cycle_summary_path,
    )
    if run_label_coverage_check:
        from aikeiba.evaluation.wide_label_coverage import build_wide_label_coverage_report

        try:
            coverage = build_wide_label_coverage_report(
                db_path=db_path,
                pairs_glob=label_coverage_pairs_glob,
                pair_base_path=label_coverage_pair_base_path,
                out_md=label_coverage_out_md,
            )
            summary["wide_label_coverage"] = coverage
        except Exception as exc:
            summary.setdefault("warnings", [])
            summary["warnings"].append(f"wide_label_coverage_failed:{exc}")
    summary["raw_required_files_check"] = {
        "required_files": precheck.get("required_files", []),
        "missing_files": missing_raw_files,
        "empty_files": empty_raw_files,
        "row_counts": precheck.get("row_counts", {}),
        "status": "fail" if precheck_stop_reason else "pass",
        "stop_reason": precheck_stop_reason,
    }
    try:
        daily_cycle_summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        print(f"[yellow]WARN[/yellow] failed to persist raw_required_files_check into daily_cycle_summary: {exc}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if summary["cycle_status"] in {"race_day_failed", "compare_failed", "stop"}:
        raise typer.Exit(code=2)


@app.command("run-baseline-vs-stability")
def run_baseline_vs_stability_cmd(
    db_path: Path = typer.Option(Path("data/warehouse/aikeiba.duckdb")),
    models_root: Path = typer.Option(Path("data/models")),
    feature_snapshot_version: str = typer.Option("fs_v1"),
    train_end_date: str = typer.Option(..., help="YYYY-MM-DD"),
    valid_start_date: str = typer.Option(..., help="YYYY-MM-DD"),
    valid_end_date: str = typer.Option(..., help="YYYY-MM-DD"),
    test_period: str = typer.Option(..., help="YYYY-MM-DD..YYYY-MM-DD"),
    baseline_model_version: str = typer.Option("top3_baseline_v1"),
    stability_model_version: str = typer.Option("top3_stability_v1"),
    baseline_feature_set: str = typer.Option("baseline", help="baseline/stability/stability_plus_pace"),
    stability_feature_set: str = typer.Option("stability", help="baseline/stability/stability_plus_pace"),
    baseline_experiment_name: str = typer.Option("exp_top3_baseline_v1"),
    stability_experiment_name: str = typer.Option("exp_top3_stability_v1"),
    dataset_manifest_path: Path = typer.Option(..., help="Fixed dataset manifest path"),
    report_dir: Path = typer.Option(Path("data/reports/baseline_vs_stability")),
    summary_json_path: Path = typer.Option(Path("data/reports/baseline_vs_stability/experiment_delta_summary.json")),
    summary_md_path: Path = typer.Option(Path("data/reports/baseline_vs_stability/experiment_delta_summary.md")),
    baseline_run_summary_path: Path | None = typer.Option(None, help="Optional baseline run_summary.json path"),
    stability_run_summary_path: Path | None = typer.Option(None, help="Optional stability run_summary.json path"),
    run_summary_search_dir: list[Path] = typer.Option(
        [],
        help="Repeatable search dir for auto run_summary linking (default: data/exports, ../data)",
    ),
    publish_latest: bool = typer.Option(True),
    latest_out_dir: Path = typer.Option(Path("../data")),
):
    from aikeiba.experiments.baseline_vs_stability import run_baseline_vs_stability_experiment

    db = DuckDb.connect(db_path)
    apply_migrations(db)
    result = run_baseline_vs_stability_experiment(
        db=db,
        models_root=models_root,
        feature_snapshot_version=feature_snapshot_version,
        train_end_date=train_end_date,
        valid_start_date=valid_start_date,
        valid_end_date=valid_end_date,
        test_period=test_period,
        baseline_model_version=baseline_model_version,
        stability_model_version=stability_model_version,
        baseline_feature_set=baseline_feature_set,
        stability_feature_set=stability_feature_set,
        baseline_experiment_name=baseline_experiment_name,
        stability_experiment_name=stability_experiment_name,
        dataset_manifest_path=dataset_manifest_path,
        report_dir=report_dir,
        summary_json_path=summary_json_path,
        summary_md_path=summary_md_path,
        publish_latest=publish_latest,
        latest_out_dir=latest_out_dir,
        baseline_run_summary_path=baseline_run_summary_path,
        stability_run_summary_path=stability_run_summary_path,
        run_summary_search_dirs=run_summary_search_dir or None,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    app()
