from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


def _read_csv_flexible(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str, keep_default_na=False)
        except Exception as exc:  # noqa: PERF203
            last_error = exc
    raise ValueError(f"failed to read csv: {path} ({last_error})")


def _norm_name(s: str) -> str:
    s2 = str(s)
    for ch in [
        " ",
        "　",
        "\t",
        "\r",
        "\n",
        "★",
        "☆",
        "*",
        "▲",
        "△",
        "◆",
        "◇",
        "(",
        ")",
        "（",
        "）",
        "[",
        "]",
        "【",
        "】",
    ]:
        s2 = s2.replace(ch, "")
    return s2.strip()


@dataclass(frozen=True)
class FillHorseIdFromHorseMasterResult:
    created_at: str
    entries_in: str
    horse_master_in: str
    entries_out: str
    mapped_count: int
    unmapped_count: int
    total_rows: int
    warnings: list[str]


def fill_entries_horse_id_from_target_horse_master(
    *,
    entries_csv: Path,
    horse_master_csv: Path,
    out_entries_csv: Path,
    overwrite: bool = False,
    horse_name_col_entries: str = "horse_name",
    horse_id_col_entries: str = "horse_id",
    horse_name_col_master: str = "馬名",
    horse_id_col_master: str = "血統登録番号",
    replace_when_length_lte: int = 8,
) -> dict[str, Any]:
    """
    Fill Aikeiba raw entries.csv horse_id using TARGET horse master export.

    Expected horse_master_csv columns:
      - 馬名
      - 血統登録番号
    """
    created_at = dt.datetime.now().isoformat(timespec="seconds")
    if out_entries_csv.exists() and not overwrite:
        raise FileExistsError(f"out_entries_csv already exists: {out_entries_csv}")

    entries = _read_csv_flexible(entries_csv)
    if horse_name_col_entries not in entries.columns or horse_id_col_entries not in entries.columns:
        raise ValueError(f"entries_csv must have columns: {horse_name_col_entries}, {horse_id_col_entries}")

    hm = _read_csv_flexible(horse_master_csv)
    if horse_name_col_master not in hm.columns or horse_id_col_master not in hm.columns:
        raise ValueError(f"horse_master_csv must have columns: {horse_name_col_master}, {horse_id_col_master}")

    names = hm[horse_name_col_master].astype(str).map(_norm_name)
    ids = hm[horse_id_col_master].astype(str).str.strip()

    mapping_norm: dict[str, str] = {}
    for n, hid in zip(names.tolist(), ids.tolist(), strict=False):
        if n == "" or hid == "":
            continue
        mapping_norm.setdefault(n, hid)

    warnings: list[str] = []
    if len(mapping_norm) == 0:
        warnings.append("no_horse_id_mapping_extracted_from_horse_master")

    current = entries[horse_id_col_entries].astype(str).str.strip()
    before_missing = int(current.eq("").sum())

    # Replace blanks, and also replace short IDs (commonly mis-parsed like 8-digit values).
    replace_mask = current.eq("") | (current.str.len() <= replace_when_length_lte)
    ent_names = entries.loc[replace_mask, horse_name_col_entries].astype(str).map(_norm_name)
    mapped = ent_names.map(mapping_norm).fillna("")
    entries.loc[replace_mask & mapped.ne(""), horse_id_col_entries] = mapped[mapped.ne("")]

    current2 = entries[horse_id_col_entries].astype(str).str.strip()
    after_missing = int(current2.eq("").sum())
    mapped_count = int((current2.ne(current)).sum())
    unmapped_count = after_missing

    tmp = out_entries_csv.with_suffix(out_entries_csv.suffix + ".tmp")
    out_entries_csv.parent.mkdir(parents=True, exist_ok=True)
    entries.to_csv(tmp, index=False, encoding="utf-8")
    tmp.replace(out_entries_csv)

    result = FillHorseIdFromHorseMasterResult(
        created_at=created_at,
        entries_in=str(entries_csv),
        horse_master_in=str(horse_master_csv),
        entries_out=str(out_entries_csv),
        mapped_count=mapped_count,
        unmapped_count=unmapped_count,
        total_rows=int(len(entries)),
        warnings=warnings,
    )
    return json.loads(json.dumps(result.__dict__, ensure_ascii=False))
