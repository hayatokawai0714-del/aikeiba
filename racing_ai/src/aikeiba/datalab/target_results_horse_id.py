from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


def _read_csv_flexible(path: Path, *, header: int | None = "infer") -> pd.DataFrame:
    last_error: Exception | None = None
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return pd.read_csv(path, encoding=enc, header=header, dtype=str, keep_default_na=False)
        except Exception as exc:  # noqa: PERF203
            last_error = exc
    raise ValueError(f"failed to read csv: {path} ({last_error})")


@dataclass(frozen=True)
class FillHorseIdResult:
    created_at: str
    entries_in: str
    target_results_in: str
    entries_out: str
    mapped_count: int
    unmapped_count: int
    total_rows: int
    warnings: list[str]


def fill_entries_horse_id_from_target_results(
    *,
    entries_csv: Path,
    target_results_csv: Path,
    out_entries_csv: Path,
    overwrite: bool = False,
    horse_name_col_entries: str = "horse_name",
    horse_id_col_entries: str = "horse_id",
    horse_name_field_index: int = 13,
    horse_id_field_index: int = 37,
) -> dict[str, Any]:
    """
    Fill Aikeiba raw entries.csv horse_id using TARGET "全馬成績CSV" (52 fields, no header) export.

    Assumption (based on observed export):
      - target_results_csv is a no-header CSV with 52 fields.
      - field[13] = horse_name, field[37] = blood registration number (horse_id).
    """
    created_at = dt.datetime.now().isoformat(timespec="seconds")
    if out_entries_csv.exists() and not overwrite:
        raise FileExistsError(f"out_entries_csv already exists: {out_entries_csv}")

    entries = _read_csv_flexible(entries_csv)
    if horse_name_col_entries not in entries.columns or horse_id_col_entries not in entries.columns:
        raise ValueError(f"entries_csv must have columns: {horse_name_col_entries}, {horse_id_col_entries}")

    raw = _read_csv_flexible(target_results_csv, header=None)
    if raw.shape[1] <= max(horse_name_field_index, horse_id_field_index):
        raise ValueError(f"target_results_csv unexpected columns: {raw.shape[1]}")

    horse_name = raw.iloc[:, horse_name_field_index].astype(str).str.strip()
    horse_id = raw.iloc[:, horse_id_field_index].astype(str).str.strip()

    def norm_name(s: str) -> str:
        # Remove common decoration chars/spaces to improve matching across different TARGET exports.
        s2 = str(s)
        for ch in [" ", "　", "\t", "\r", "\n", "★", "☆", "*", "▲", "△", "◆", "◇", "(", ")", "（", "）", "[", "]", "【", "】"]:
            s2 = s2.replace(ch, "")
        return s2.strip()

    mapping: dict[str, str] = {}
    mapping_norm: dict[str, str] = {}
    for n, hid in zip(horse_name.tolist(), horse_id.tolist(), strict=False):
        if n == "" or hid == "":
            continue
        # Keep first seen mapping to remain stable.
        mapping.setdefault(n, hid)
        mapping_norm.setdefault(norm_name(n), hid)

    warnings: list[str] = []
    if len(mapping) == 0:
        warnings.append("no_horse_id_mapping_extracted_from_target_results")

    before_missing = entries[horse_id_col_entries].astype(str).str.strip().eq("").sum()
    # Only fill when empty
    fill_mask = entries[horse_id_col_entries].astype(str).str.strip().eq("")
    names = entries.loc[fill_mask, horse_name_col_entries].astype(str).str.strip()
    filled = names.map(mapping)
    # fallback normalized matching
    missing2 = filled.isna() | (filled.astype(str).str.strip() == "")
    if missing2.any():
        filled2 = names[missing2].map(lambda x: mapping_norm.get(norm_name(x), ""))
        filled.loc[missing2] = filled2
    entries.loc[fill_mask, horse_id_col_entries] = filled.fillna("")
    after_missing = entries[horse_id_col_entries].astype(str).str.strip().eq("").sum()

    mapped_count = int(before_missing - after_missing)
    unmapped_count = int(after_missing)

    out_entries_csv.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_entries_csv.with_suffix(out_entries_csv.suffix + ".tmp")
    entries.to_csv(tmp, index=False, encoding="utf-8")
    tmp.replace(out_entries_csv)

    result = FillHorseIdResult(
        created_at=created_at,
        entries_in=str(entries_csv),
        target_results_in=str(target_results_csv),
        entries_out=str(out_entries_csv),
        mapped_count=mapped_count,
        unmapped_count=unmapped_count,
        total_rows=int(len(entries)),
        warnings=warnings,
    )
    return json.loads(json.dumps(result.__dict__, ensure_ascii=False))
