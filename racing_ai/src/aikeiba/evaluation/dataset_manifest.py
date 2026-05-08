from __future__ import annotations

import csv
import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aikeiba.common.hashing import stable_fingerprint
from aikeiba.db.duckdb import DuckDb


@dataclass(frozen=True)
class DatasetManifestPaths:
    manifest_path: Path
    race_id_list_path: Path


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _atomic_write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    tmp.replace(path)


def _parse_period(period: str) -> tuple[str, str]:
    # Expected format: YYYY-MM-DD..YYYY-MM-DD
    if ".." not in period:
        raise ValueError(f"period must be YYYY-MM-DD..YYYY-MM-DD: {period}")
    a, b = period.split("..", 1)
    return a.strip(), b.strip()


def make_dataset_manifest(
    *,
    db: DuckDb,
    out_dir: Path,
    dataset_name: str,
    task_name: str,
    feature_snapshot_version: str,
    train_period: str,
    valid_period: str,
    test_period: str,
    filters: dict[str, Any] | None = None,
    excluded_rules: list[str] | None = None,
) -> dict[str, Any]:
    filters = filters or {}
    excluded_rules = excluded_rules or []

    train_start, train_end = _parse_period(train_period)
    valid_start, valid_end = _parse_period(valid_period)
    test_start, test_end = _parse_period(test_period)

    # Union of split ranges, preserving split labels for audit.
    q = """
    WITH train_r AS (
      SELECT distinct r.race_id, cast(r.race_date as VARCHAR) AS race_date, 'train' AS split_name
      FROM races r
      JOIN feature_store fs ON fs.race_id = r.race_id
      WHERE fs.feature_snapshot_version = ?
        AND r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
    ),
    valid_r AS (
      SELECT distinct r.race_id, cast(r.race_date as VARCHAR) AS race_date, 'valid' AS split_name
      FROM races r
      JOIN feature_store fs ON fs.race_id = r.race_id
      WHERE fs.feature_snapshot_version = ?
        AND r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
    ),
    test_r AS (
      SELECT distinct r.race_id, cast(r.race_date as VARCHAR) AS race_date, 'test' AS split_name
      FROM races r
      JOIN feature_store fs ON fs.race_id = r.race_id
      WHERE fs.feature_snapshot_version = ?
        AND r.race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
    )
    SELECT * FROM train_r
    UNION ALL
    SELECT * FROM valid_r
    UNION ALL
    SELECT * FROM test_r
    ORDER BY race_date, race_id, split_name
    """
    race_df = db.query_df(
        q,
        (
            feature_snapshot_version,
            train_start,
            train_end,
            feature_snapshot_version,
            valid_start,
            valid_end,
            feature_snapshot_version,
            test_start,
            test_end,
        ),
    )
    race_rows = race_df.to_dict("records")

    if len(race_rows) == 0:
        raise ValueError("no race_ids found for manifest periods/snapshot")

    # NOTE:
    # `train-top3` historically defines `dataset_fingerprint` using ONLY train+valid race_ids/row_count
    # (it does not include test split rows). Comparison tooling expects the manifest fingerprint to
    # match the model bundle's fingerprint, so we compute the fingerprint payload in the same way.
    # The manifest still records test split race_ids in `race_ids.csv` for audit.
    uniq_race_ids_all = sorted({str(r["race_id"]) for r in race_rows})
    race_count = len(uniq_race_ids_all)
    ids_sql_all = ",".join([f"'{rid}'" for rid in uniq_race_ids_all])
    row_count = int(
        db.query_df(
            f"""
            SELECT count(*) AS n
            FROM feature_store
            WHERE feature_snapshot_version = ?
              AND race_id IN ({ids_sql_all})
            """,
            (feature_snapshot_version,),
        ).iloc[0]["n"]
    )

    uniq_race_ids_train_valid = sorted(
        {str(r["race_id"]) for r in race_rows if str(r.get("split_name")) in {"train", "valid"}}
    )
    ids_sql_train_valid = ",".join([f"'{rid}'" for rid in uniq_race_ids_train_valid])
    row_count_train_valid = int(
        db.query_df(
            f"""
            SELECT count(*) AS n
            FROM feature_store
            WHERE feature_snapshot_version = ?
              AND race_id IN ({ids_sql_train_valid})
            """,
            (feature_snapshot_version,),
        ).iloc[0]["n"]
    )

    fingerprint_payload = {
        "task_name": task_name,
        "feature_snapshot_version": feature_snapshot_version,
        "train_period": train_period,
        "valid_period": valid_period,
        "test_period": test_period,
        "filters": filters,
        "excluded_rules": excluded_rules,
        "race_ids": uniq_race_ids_train_valid,
        "race_count": len(uniq_race_ids_train_valid),
        "row_count": row_count_train_valid,
    }
    dataset_fingerprint = stable_fingerprint(fingerprint_payload)
    created_at = dt.datetime.now().isoformat(timespec="seconds")

    dataset_dir = out_dir / dataset_name
    race_id_list_path = dataset_dir / "race_ids.csv"
    manifest_path = dataset_dir / "dataset_manifest.json"

    _atomic_write_csv(
        race_id_list_path,
        race_rows,
        fieldnames=["race_id", "race_date", "split_name"],
    )

    manifest = {
        "dataset_name": dataset_name,
        "task_name": task_name,
        "feature_snapshot_version": feature_snapshot_version,
        "train_period": train_period,
        "valid_period": valid_period,
        "test_period": test_period,
        "race_count": race_count,
        "row_count": row_count,
        "race_id_list_path": str(race_id_list_path),
        "filters": filters,
        "excluded_rules": excluded_rules,
        "dataset_fingerprint": dataset_fingerprint,
        "created_at": created_at,
    }
    _atomic_write_text(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2))

    return {
        "manifest_path": str(manifest_path),
        "race_id_list_path": str(race_id_list_path),
        "dataset_manifest": manifest,
    }
