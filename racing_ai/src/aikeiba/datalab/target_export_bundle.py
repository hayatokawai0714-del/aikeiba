from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from aikeiba.datalab.target_horse_master import fill_entries_horse_id_from_target_horse_master
from aikeiba.datalab.target_image_entries import build_entries_csv_from_target_image
from aikeiba.datalab.target_odds import build_odds_csv_from_target_csv
from aikeiba.datalab.target_text_payouts import build_payouts_csv_from_target_text, build_races_csv_from_target_text
from aikeiba.datalab.target_text_results import build_results_csv_from_target_result_text


def build_raw_from_target_export(
    *,
    target_dir: Path,
    raw_dir: Path,
    race_date: str,
    snapshot_version: str,
    odds_snapshot_version: str,
    overwrite: bool = False,
) -> dict[str, Any]:
    """
    Build Aikeiba raw layer files from TARGET frontier JV exports.

    This does NOT automate TARGET GUI operations. It assumes the user already exported files into target_dir.

    Expected inputs (minimum):
    - payouts.txt
    - entries.csv (出馬表★画面イメージ一括出力(CSV))
    - odds_target.csv (時系列オッズ(フルCSV))
    - results.txt (成績（整形テキスト）)
    - horse_master.csv (馬名,血統登録番号)
    """
    target_dir = Path(target_dir)
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    def require(p: Path) -> None:
        if not p.exists():
            raise FileNotFoundError(f"missing required input: {p}")

    payouts_txt = target_dir / "payouts.txt"
    entries_in = target_dir / "entries.csv"
    odds_in = target_dir / "odds_target.csv"
    results_txt = target_dir / "results.txt"
    horse_master = target_dir / "horse_master.csv"

    require(payouts_txt)
    require(entries_in)
    require(odds_in)
    require(results_txt)
    require(horse_master)

    races_csv = raw_dir / "races.csv"
    payouts_csv = raw_dir / "payouts.csv"
    entries_csv = raw_dir / "entries.csv"
    odds_csv = raw_dir / "odds.csv"
    results_csv = raw_dir / "results.csv"
    manifest_json = raw_dir / "target_export_manifest.json"

    written: list[str] = []

    if overwrite:
        for p in [races_csv, payouts_csv, entries_csv, odds_csv, results_csv, manifest_json]:
            if p.exists():
                p.unlink()

    r1 = build_races_csv_from_target_text(input_txt=payouts_txt, output_csv=races_csv)
    written.append(str(races_csv))
    r2 = build_payouts_csv_from_target_text(input_txt=payouts_txt, output_csv=payouts_csv)
    written.append(str(payouts_csv))

    r3 = build_entries_csv_from_target_image(
        input_csv=entries_in,
        races_csv=races_csv,
        out_entries_csv=entries_csv,
        overwrite=True,
    )
    written.append(str(entries_csv))

    r4 = fill_entries_horse_id_from_target_horse_master(
        entries_csv=entries_csv,
        horse_master_csv=horse_master,
        out_entries_csv=entries_csv,
        overwrite=True,
    )

    r5 = build_odds_csv_from_target_csv(
        input_csv=odds_in,
        races_csv=races_csv,
        out_odds_csv=odds_csv,
        snapshot_version=snapshot_version,
        odds_snapshot_version=odds_snapshot_version,
        overwrite=True,
    )
    written.append(str(odds_csv))

    r6 = build_results_csv_from_target_result_text(input_txt=results_txt, races_csv=races_csv, output_csv=results_csv)
    written.append(str(results_csv))

    payload = {
        "created_at": dt.datetime.now().isoformat(timespec="seconds"),
        "race_date": race_date,
        "snapshot_version": snapshot_version,
        "odds_snapshot_version": odds_snapshot_version,
        "target_dir": str(target_dir),
        "raw_dir": str(raw_dir),
        "inputs": {
            "payouts_txt": str(payouts_txt),
            "entries_csv": str(entries_in),
            "odds_target_csv": str(odds_in),
            "results_txt": str(results_txt),
            "horse_master_csv": str(horse_master),
        },
        "outputs": {
            "races_csv": str(races_csv),
            "entries_csv": str(entries_csv),
            "results_csv": str(results_csv),
            "payouts_csv": str(payouts_csv),
            "odds_csv": str(odds_csv),
        },
        "written_files": written,
        "steps": {
            "build_races": r1,
            "build_payouts": r2,
            "build_entries": r3,
            "fill_horse_id": r4,
            "build_odds": r5,
            "build_results": r6,
        },
    }

    manifest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    return payload
