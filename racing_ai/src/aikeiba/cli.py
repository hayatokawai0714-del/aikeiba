from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer
from rich import print

from aikeiba.db.duckdb import DuckDb
from aikeiba.db.migrations import apply_migrations

app = typer.Typer(no_args_is_help=True)


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
):
    """Train top3 model and save bundle (LightGBM + isotonic calibrator)."""
    from aikeiba.modeling.top3 import train_top3_bundle

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
    force_null_top3_for_test: bool = typer.Option(False, hidden=True),
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

    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = DuckDb.connect(db_path)
    apply_migrations(db)

    summary = run_race_day_pipeline(
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
        force_null_top3_for_test=force_null_top3_for_test,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if summary["status"] == "stop":
        raise typer.Exit(code=2)
