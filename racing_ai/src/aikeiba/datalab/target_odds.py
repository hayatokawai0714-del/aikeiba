from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


def _read_csv_noheader_flexible(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return pd.read_csv(path, encoding=enc, header=None, dtype=str, keep_default_na=False)
        except Exception as exc:  # noqa: PERF203
            last_error = exc
    raise ValueError(f"failed to read csv: {path} ({last_error})")


@dataclass(frozen=True)
class BuildOddsResult:
    created_at: str
    input_csv: str
    races_csv: str
    out_odds_csv: str
    odds_snapshot_version: str
    captured_at: str
    race_count: int
    segment_count: int
    row_count_in: int
    row_count_out: int
    warnings: list[str]


def _split_into_race_segments(df: pd.DataFrame, *, horse_no_col: int) -> list[pd.DataFrame]:
    if len(df) == 0:
        return []
    horse_no = df.iloc[:, horse_no_col].astype(str).str.strip()
    starts = horse_no == "1"
    idx = list(df.index[starts])
    if not idx:
        return [df]
    if idx[0] != 0:
        idx = [0] + idx
    segments: list[pd.DataFrame] = []
    for i, s in enumerate(idx):
        e = idx[i + 1] if i + 1 < len(idx) else len(df)
        seg = df.iloc[s:e].copy()
        non_empty = seg.apply(lambda r: any(str(x).strip() != "" for x in r.tolist()), axis=1)
        seg = seg.loc[non_empty].copy()
        if len(seg) > 0:
            segments.append(seg)
    return segments


def build_odds_csv_from_target_csv(
    *,
    input_csv: Path,
    races_csv: Path,
    out_odds_csv: Path,
    snapshot_version: str,
    odds_snapshot_version: str,
    captured_at: str | None = None,
    overwrite: bool = False,
    # Column indices based on observed TARGET "オッズ一括出力（ターゲット仕様CSV）" sample.
    horse_no_col: int = 4,
    horse_name_col: int = 6,
    win_odds_col: int = 7,
) -> dict[str, Any]:
    """
    Convert TARGET odds CSV (ターゲット仕様) into Aikeiba raw odds.csv schema.

    Output columns (compatible with aikeiba.datalab.raw_pipeline._normalize_odds):
      race_id, odds_snapshot_version, captured_at, odds_type, horse_no, horse_no_a, horse_no_b, odds_value, source_version

    Assumption:
      - input_csv has no header.
      - column 4 = umaban (horse_no), column 6 = horse_name, column 7 = win odds.
      - new race begins when horse_no resets to 1; segments are aligned to races_csv order.
    """
    created_at = dt.datetime.now().isoformat(timespec="seconds")
    if out_odds_csv.exists() and not overwrite:
        raise FileExistsError(f"out_odds_csv already exists: {out_odds_csv}")

    df_in = _read_csv_noheader_flexible(input_csv)
    races = pd.read_csv(races_csv, encoding="utf-8-sig")
    if "race_id" not in races.columns:
        raise ValueError("races_csv must have 'race_id' column")
    race_ids = races["race_id"].astype(str).tolist()

    segments = _split_into_race_segments(df_in, horse_no_col=horse_no_col)
    warnings: list[str] = []
    if len(segments) != len(race_ids):
        warnings.append(f"segment_count_mismatch: segments={len(segments)} races={len(race_ids)}")

    cap = captured_at or dt.datetime.now().isoformat(timespec="seconds")

    rows: list[dict[str, Any]] = []
    for seg_i, seg in enumerate(segments):
        if seg_i >= len(race_ids):
            break
        rid = race_ids[seg_i]
        for _, r in seg.iterrows():
            horse_no_raw = str(r.iloc[horse_no_col]).strip() if seg.shape[1] > horse_no_col else ""
            if horse_no_raw == "":
                continue
            try:
                horse_no = int(horse_no_raw)
            except Exception:
                continue

            odds_raw = str(r.iloc[win_odds_col]).strip() if seg.shape[1] > win_odds_col else ""
            if odds_raw == "" or odds_raw == "0.0":
                odds_val = None
            else:
                try:
                    odds_val = float(odds_raw)
                except Exception:
                    odds_val = None

            rows.append(
                {
                    "race_id": rid,
                    "odds_snapshot_version": odds_snapshot_version,
                    "captured_at": cap,
                    "odds_type": "win",
                    "horse_no": horse_no,
                    "horse_no_a": -1,
                    "horse_no_b": -1,
                    "odds_value": odds_val,
                    "source_version": snapshot_version,
                }
            )

    df_out = pd.DataFrame(rows)
    out_odds_csv.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_odds_csv.with_suffix(out_odds_csv.suffix + ".tmp")
    df_out.to_csv(tmp, index=False, encoding="utf-8")
    tmp.replace(out_odds_csv)

    result = BuildOddsResult(
        created_at=created_at,
        input_csv=str(input_csv),
        races_csv=str(races_csv),
        out_odds_csv=str(out_odds_csv),
        odds_snapshot_version=odds_snapshot_version,
        captured_at=cap,
        race_count=int(len(race_ids)),
        segment_count=int(len(segments)),
        row_count_in=int(len(df_in)),
        row_count_out=int(len(df_out)),
        warnings=warnings,
    )
    return json.loads(json.dumps(result.__dict__, ensure_ascii=False))

