from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class TargetImageEntriesBuildResult:
    input_csv: str
    races_csv: str
    out_entries_csv: str
    created_at: str
    race_count: int
    segment_count: int
    row_count_in: int
    row_count_out: int
    warnings: list[str]


def _read_noheader_csv(path: Path) -> pd.DataFrame:
    # TARGET frontier JV "画面イメージ一括出力(CSV)" often outputs CSV without header.
    # Keep empty strings; do not coerce to NaN too early.
    for enc in ("utf-8", "cp932"):
        try:
            return pd.read_csv(path, header=None, encoding=enc, dtype=str, keep_default_na=False)
        except UnicodeDecodeError:
            continue
    # last resort
    return pd.read_csv(path, header=None, encoding="utf-8", errors="replace", dtype=str, keep_default_na=False)


def _split_into_race_segments(df: pd.DataFrame) -> list[pd.DataFrame]:
    """
    Split TARGET entries rows into race segments.

    Assumption:
      - Each horse row has (waku, ..., umaban, ..., horse_name, sex, age, ..., jockey, weight, ..., odds, body_weight, ...)
      - New race starts when waku==1 and umaban==1 after having seen at least one row.
    """
    if len(df) == 0:
        return []

    # Column 0: waku, Column 2: umaban (based on observed exports).
    waku = df.iloc[:, 0].astype(str).str.strip()
    umaban = df.iloc[:, 2].astype(str).str.strip()

    starts = (waku == "1") & (umaban == "1")
    idx = list(df.index[starts])
    if not idx:
        return [df]

    # Ensure the first segment starts at 0
    if idx[0] != 0:
        idx = [0] + idx

    segments: list[pd.DataFrame] = []
    for i, s in enumerate(idx):
        e = idx[i + 1] if i + 1 < len(idx) else len(df)
        seg = df.iloc[s:e].copy()
        # Drop fully blank rows (rare)
        non_empty = seg.apply(lambda r: any(str(x).strip() != "" for x in r.tolist()), axis=1)
        seg = seg.loc[non_empty].copy()
        if len(seg) > 0:
            segments.append(seg)
    return segments


def build_entries_csv_from_target_image(
    *,
    input_csv: Path,
    races_csv: Path,
    out_entries_csv: Path,
    overwrite: bool = False,
) -> dict[str, Any]:
    """
    Convert TARGET frontier JV "出馬表・画面イメージ一括出力(CSV形式)" entries CSV
    into Aikeiba raw entries.csv schema (with headers).

    Output columns match aikeiba.datalab.raw_pipeline._normalize_entries aliases:
      race_id, horse_no, horse_id, horse_name, waku, sex, age, weight_carried, jockey_id, trainer_id, is_scratched

    Notes / Assumptions:
      - horse_id/jockey_id/trainer_id cannot be reliably extracted from this export; left blank.
      - race_id is assigned by matching the segment order to races_csv row order.
      - If segment_count != race_count, we still write output but include warnings.
    """
    created_at = dt.datetime.now().isoformat(timespec="seconds")
    if out_entries_csv.exists() and not overwrite:
        raise FileExistsError(f"out_entries_csv already exists: {out_entries_csv}")

    df_in = _read_noheader_csv(input_csv)
    races = pd.read_csv(races_csv, encoding="utf-8")
    if "race_id" not in races.columns:
        raise ValueError("races_csv must have 'race_id' column")
    race_ids = races["race_id"].astype(str).tolist()

    segments = _split_into_race_segments(df_in)
    warnings: list[str] = []
    if len(segments) != len(race_ids):
        warnings.append(f"segment_count_mismatch: segments={len(segments)} races={len(race_ids)}")

    rows: list[dict[str, Any]] = []
    for seg_i, seg in enumerate(segments):
        if seg_i >= len(race_ids):
            break
        rid = race_ids[seg_i]
        for _, r in seg.iterrows():
            # Observed column indices (0-based) from user's export sample:
            # 0 waku, 2 umaban, 7 horse_name, 9 sex, 10 age, 12 jockey_name, 13 weight_carried
            waku = str(r.iloc[0]).strip()
            umaban = str(r.iloc[2]).strip()
            horse_name = str(r.iloc[7]).strip() if seg.shape[1] > 7 else ""
            sex = str(r.iloc[9]).strip() if seg.shape[1] > 9 else ""
            age = str(r.iloc[10]).strip() if seg.shape[1] > 10 else ""
            weight_carried = str(r.iloc[13]).strip() if seg.shape[1] > 13 else ""

            # Skip rows that don't look like horse rows.
            if horse_name == "" or umaban == "":
                continue

            try:
                horse_no_int = int(umaban)
            except Exception:
                continue

            try:
                waku_int = int(waku) if waku != "" else None
            except Exception:
                waku_int = None

            rows.append(
                {
                    "race_id": rid,
                    "horse_no": horse_no_int,
                    "horse_id": "",
                    "horse_name": horse_name,
                    "waku": waku_int,
                    "sex": sex if sex != "" else None,
                    "age": int(age) if age.isdigit() else None,
                    "weight_carried": float(weight_carried) if weight_carried not in ("", None) else None,
                    "jockey_id": "",
                    "trainer_id": "",
                    "is_scratched": False,
                }
            )

    df_out = pd.DataFrame(rows)
    out_entries_csv.parent.mkdir(parents=True, exist_ok=True)
    # Atomic-ish write: write tmp then replace
    tmp = out_entries_csv.with_suffix(out_entries_csv.suffix + ".tmp")
    df_out.to_csv(tmp, index=False, encoding="utf-8")
    tmp.replace(out_entries_csv)

    result = TargetImageEntriesBuildResult(
        input_csv=str(input_csv),
        races_csv=str(races_csv),
        out_entries_csv=str(out_entries_csv),
        created_at=created_at,
        race_count=int(len(race_ids)),
        segment_count=int(len(segments)),
        row_count_in=int(len(df_in)),
        row_count_out=int(len(df_out)),
        warnings=warnings,
    )
    return json.loads(json.dumps(result.__dict__, ensure_ascii=False))
