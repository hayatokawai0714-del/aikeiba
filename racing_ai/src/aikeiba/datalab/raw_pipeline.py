from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from aikeiba.common.audit import log_pipeline_event
from aikeiba.db.duckdb import DuckDb
from aikeiba.domain.ids import build_race_id_from_parts


REQUIRED_RAW_FILES = ("races.csv", "entries.csv")
OPTIONAL_RAW_FILES = ("results.csv", "odds.csv", "payouts.csv")


@dataclass(frozen=True)
class NormalizeThresholds:
    horse_id_missing_rate_stop: float = 0.02
    race_id_missing_rate_stop: float = 0.0
    min_entries_per_race_warn: int = 6
    max_entries_per_race_warn: int = 18
    min_entries_per_race_stop: int = 3
    max_entries_per_race_stop: int = 20


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8")


def _md5_file(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _norm_columns(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    df2.columns = [str(c).strip().lower() for c in df2.columns]
    return df2


def _pick(df: pd.DataFrame, aliases: list[str], default=None):
    for c in aliases:
        if c in df.columns:
            return df[c]
    return pd.Series([default] * len(df))


def _ensure_race_id(df: pd.DataFrame) -> pd.Series:
    race_id = _pick(df, ["race_id", "raceid"], default=None)
    has_race_id = race_id.notna() & (race_id.astype(str).str.strip() != "")
    if has_race_id.all():
        return race_id.astype(str)

    date_col = _pick(df, ["race_date", "date", "kaisai_date"], default=None).astype(str).str.replace("-", "")
    venue_col = _pick(df, ["venue", "place", "jyo", "track"], default="")
    race_no_col = _pick(df, ["race_no", "raceno", "r"], default=0).fillna(0).astype(int)

    built = []
    for i in range(len(df)):
        if has_race_id.iloc[i]:
            built.append(str(race_id.iloc[i]))
            continue
        d = str(date_col.iloc[i]).strip()
        v = str(venue_col.iloc[i]).strip()
        rn = int(race_no_col.iloc[i])
        if len(d) != 8 or rn <= 0 or v == "":
            built.append("")
        else:
            built.append(build_race_id_from_parts(d, v, rn))
    return pd.Series(built)


def _normalize_races(df_raw: pd.DataFrame, target_race_date: str, snapshot_version: str) -> pd.DataFrame:
    df = _norm_columns(df_raw)
    race_id = _ensure_race_id(df)

    out = pd.DataFrame(
        {
            "race_id": race_id.astype(str),
            "race_date": _pick(df, ["race_date", "date", "kaisai_date"], default=target_race_date).astype(str).str.replace("/", "-"),
            "venue": _pick(df, ["venue", "place", "jyo"], default=""),
            "race_no": _pick(df, ["race_no", "raceno", "r"], default=0).fillna(0).astype(int),
            "post_time": _pick(df, ["post_time", "hasso_time"], default=None),
            "surface": _pick(df, ["surface", "track_type"], default=None),
            "distance": _pick(df, ["distance"], default=None),
            "track_condition": _pick(df, ["track_condition", "baba"], default=None),
            "race_class": _pick(df, ["race_class", "class"], default=None),
            "field_size_expected": _pick(df, ["field_size_expected", "field_size", "head_count"], default=None),
        }
    )
    out["source_version"] = snapshot_version
    return out


def _normalize_entries(df_raw: pd.DataFrame, snapshot_version: str) -> pd.DataFrame:
    df = _norm_columns(df_raw)
    race_id = _ensure_race_id(df)
    out = pd.DataFrame(
        {
            "race_id": race_id.astype(str),
            "horse_no": _pick(df, ["horse_no", "umaban"], default=0).fillna(0).astype(int),
            "horse_id": _pick(df, ["horse_id", "ketto_num"], default=None),
            "horse_name": _pick(df, ["horse_name", "bamei"], default=None),
            "waku": _pick(df, ["waku", "wakuban"], default=None),
            "sex": _pick(df, ["sex"], default=None),
            "age": _pick(df, ["age"], default=None),
            "weight_carried": _pick(df, ["weight_carried", "futan"], default=None),
            "jockey_id": _pick(df, ["jockey_id", "kishu_id"], default=None),
            "trainer_id": _pick(df, ["trainer_id", "chokyoshi_id"], default=None),
            "is_scratched": _pick(df, ["is_scratched", "torikeshi"], default=False),
            "source_version": snapshot_version,
        }
    )
    return out


def _normalize_results(df_raw: pd.DataFrame, snapshot_version: str) -> pd.DataFrame:
    df = _norm_columns(df_raw)
    race_id = _ensure_race_id(df)
    out = pd.DataFrame(
        {
            "race_id": race_id.astype(str),
            "horse_no": _pick(df, ["horse_no", "umaban"], default=0).fillna(0).astype(int),
            "finish_position": _pick(df, ["finish_position", "chakujun"], default=None),
            "margin": _pick(df, ["margin", "chakusa"], default=None),
            "last3f_time": _pick(df, ["last3f_time", "agari3f"], default=None),
            "last3f_rank": _pick(df, ["last3f_rank", "agari_juni"], default=None),
            "corner_pos_1": _pick(df, ["corner_pos_1"], default=None),
            "corner_pos_2": _pick(df, ["corner_pos_2"], default=None),
            "corner_pos_3": _pick(df, ["corner_pos_3"], default=None),
            "corner_pos_4": _pick(df, ["corner_pos_4", "corner4"], default=None),
            "pop_rank": _pick(df, ["pop_rank", "ninki"], default=None),
            "odds_win_final": _pick(df, ["odds_win_final", "tansho_odds"], default=None),
            "source_version": snapshot_version,
        }
    )
    return out


def _normalize_odds(df_raw: pd.DataFrame, snapshot_version: str, default_captured_at: str) -> pd.DataFrame:
    df = _norm_columns(df_raw)
    race_id = _ensure_race_id(df)
    out = pd.DataFrame(
        {
            "race_id": race_id.astype(str),
            "odds_snapshot_version": _pick(df, ["odds_snapshot_version"], default=snapshot_version),
            "captured_at": _pick(df, ["captured_at", "odds_time"], default=default_captured_at),
            "odds_type": _pick(df, ["odds_type", "bet_type"], default="win"),
            "horse_no": _pick(df, ["horse_no", "umaban"], default=-1).fillna(-1).astype(int),
            "horse_no_a": _pick(df, ["horse_no_a"], default=-1).fillna(-1).astype(int),
            "horse_no_b": _pick(df, ["horse_no_b"], default=-1).fillna(-1).astype(int),
            "odds_value": _pick(df, ["odds_value", "odds"], default=None),
            "source_version": snapshot_version,
        }
    )
    return out


def _normalize_payouts(df_raw: pd.DataFrame, snapshot_version: str) -> pd.DataFrame:
    df = _norm_columns(df_raw)
    race_id = _ensure_race_id(df)
    out = pd.DataFrame(
        {
            "race_id": race_id.astype(str),
            "bet_type": _pick(df, ["bet_type"], default=None),
            "bet_key": _pick(df, ["bet_key", "pair"], default=None),
            "payout": _pick(df, ["payout"], default=None),
            "popularity": _pick(df, ["popularity"], default=None),
            "source_version": snapshot_version,
        }
    )
    return out


def _missing_rate(series: pd.Series) -> float:
    s = series.astype(str).str.strip()
    return float(((series.isna()) | (s == "")).mean())


def normalize_raw_jv_to_normalized(
    *,
    raw_dir: Path,
    normalized_root: Path,
    target_race_date: str,
    snapshot_version: str,
    thresholds: NormalizeThresholds = NormalizeThresholds(),
    db: DuckDb | None = None,
) -> dict[str, Any]:
    now = dt.datetime.now().isoformat(timespec="seconds")
    normalized_dir = normalized_root / snapshot_version / target_race_date
    normalized_dir.mkdir(parents=True, exist_ok=True)

    stops: list[str] = []
    warns: list[str] = []

    file_info: list[dict[str, Any]] = []
    for name in list(REQUIRED_RAW_FILES) + list(OPTIONAL_RAW_FILES):
        p = raw_dir / name
        exists = p.exists()
        row_count = None
        md5 = None
        if exists:
            df0 = _read_csv(p)
            row_count = int(len(df0))
            md5 = _md5_file(p)
        file_info.append(
            {
                "file_name": name,
                "path": str(p),
                "exists": exists,
                "row_count": row_count,
                "md5": md5,
                "raw_fetched_at": dt.datetime.fromtimestamp(p.stat().st_mtime).isoformat(timespec="seconds") if exists else None,
            }
        )
        if (name in REQUIRED_RAW_FILES) and (not exists):
            stops.append(f"missing_raw_file:{name}")

    races_df = pd.DataFrame()
    entries_df = pd.DataFrame()
    results_df = pd.DataFrame()
    odds_df = pd.DataFrame()
    payouts_df = pd.DataFrame()

    if len(stops) == 0:
        races_df = _normalize_races(_read_csv(raw_dir / "races.csv"), target_race_date=target_race_date, snapshot_version=snapshot_version)
        entries_df = _normalize_entries(_read_csv(raw_dir / "entries.csv"), snapshot_version=snapshot_version)
        if (raw_dir / "results.csv").exists():
            results_df = _normalize_results(_read_csv(raw_dir / "results.csv"), snapshot_version=snapshot_version)
        else:
            warns.append("missing_optional_raw_file:results.csv")
        if (raw_dir / "odds.csv").exists():
            odds_df = _normalize_odds(_read_csv(raw_dir / "odds.csv"), snapshot_version=snapshot_version, default_captured_at=now)
        else:
            warns.append("missing_optional_raw_file:odds.csv")
        if (raw_dir / "payouts.csv").exists():
            payouts_df = _normalize_payouts(_read_csv(raw_dir / "payouts.csv"), snapshot_version=snapshot_version)
        else:
            warns.append("missing_optional_raw_file:payouts.csv")

        # Quality gates
        if len(races_df) == 0:
            stops.append("zero_rows:races")
        if len(entries_df) == 0:
            stops.append("zero_rows:entries")

        race_id_missing_rate_entries = _missing_rate(entries_df["race_id"]) if len(entries_df) else 1.0
        horse_id_missing_rate_entries = _missing_rate(entries_df["horse_id"]) if len(entries_df) else 1.0
        if race_id_missing_rate_entries > thresholds.race_id_missing_rate_stop:
            stops.append("race_id_missing_rate_entries_high")
        if horse_id_missing_rate_entries > thresholds.horse_id_missing_rate_stop:
            stops.append("horse_id_missing_rate_entries_high")

        merged = entries_df.groupby("race_id", dropna=False).size().reset_index(name="n")
        if len(merged) > 0:
            min_n = int(merged["n"].min())
            max_n = int(merged["n"].max())
            if min_n < thresholds.min_entries_per_race_stop or max_n > thresholds.max_entries_per_race_stop:
                stops.append("entries_per_race_out_of_stop_range")
            elif min_n < thresholds.min_entries_per_race_warn or max_n > thresholds.max_entries_per_race_warn:
                warns.append("entries_per_race_out_of_warn_range")

    status = "stop" if len(stops) > 0 else ("warn" if len(warns) > 0 else "ok")

    # Persist normalized files only when no hard stop.
    written_files: list[str] = []
    if status != "stop":
        races_df.to_csv(normalized_dir / "races.csv", index=False, encoding="utf-8")
        entries_df.to_csv(normalized_dir / "entries.csv", index=False, encoding="utf-8")
        written_files.extend(["races.csv", "entries.csv"])
        if len(results_df) > 0:
            results_df.to_csv(normalized_dir / "results.csv", index=False, encoding="utf-8")
            written_files.append("results.csv")
        if len(odds_df) > 0:
            odds_df.to_csv(normalized_dir / "odds.csv", index=False, encoding="utf-8")
            written_files.append("odds.csv")
        if len(payouts_df) > 0:
            payouts_df.to_csv(normalized_dir / "payouts.csv", index=False, encoding="utf-8")
            written_files.append("payouts.csv")

    manifest = {
        "target_race_date": target_race_date,
        "snapshot_version": snapshot_version,
        "normalized_at": now,
        "status": status,
        "stop_reasons": stops,
        "warn_reasons": warns,
        "raw_files": file_info,
        "written_files": written_files,
    }
    (normalized_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    if db is not None:
        for info in file_info:
            log_pipeline_event(
                db=db,
                stage="raw",
                snapshot_version=snapshot_version,
                target_race_date=target_race_date,
                status=status,
                source_file_name=info["file_name"],
                source_file_path=info["path"],
                row_count=info["row_count"],
                message="raw file scan",
                metrics={"md5": info["md5"], "exists": info["exists"]},
            )
        log_pipeline_event(
            db=db,
            stage="normalized",
            snapshot_version=snapshot_version,
            target_race_date=target_race_date,
            status=status,
            source_file_name="manifest.json",
            source_file_path=str(normalized_dir / "manifest.json"),
            message="normalization completed",
            metrics={"stop_reasons": stops, "warn_reasons": warns, "written_files": written_files},
        )

    return {
        "normalized_dir": str(normalized_dir),
        "status": status,
        "stop_reasons": stops,
        "warn_reasons": warns,
        "manifest_path": str(normalized_dir / "manifest.json"),
        "written_files": written_files,
    }
