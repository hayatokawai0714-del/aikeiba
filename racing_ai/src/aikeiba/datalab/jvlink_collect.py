from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pandas as pd

from aikeiba.domain.ids import build_race_id_from_parts


def _read_csv_flexible(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as exc:  # noqa: PERF203
            last_error = exc
    raise ValueError(f"failed to read csv: {path} ({last_error})")


def _norm_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip().lower() for c in out.columns]
    return out


def _pick_series(df: pd.DataFrame, aliases: list[str], default=None) -> pd.Series:
    for c in aliases:
        key = c.strip().lower()
        if key in df.columns:
            return df[key]
    return pd.Series([default] * len(df))


def _to_date_yyyymmdd(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None
    s = s.replace("/", "").replace("-", "")
    if len(s) == 8 and s.isdigit():
        return s
    return None


def _ensure_race_id(
    df: pd.DataFrame,
    *,
    default_race_date: str | None = None,
) -> pd.Series:
    race_id = _pick_series(df, ["race_id", "raceid", "レースid", "レースID"], default=None)
    has = race_id.notna() & (race_id.astype(str).str.strip() != "")
    if has.all():
        return race_id.astype(str)

    date_col = _pick_series(df, ["race_date", "date", "kaisai_date", "開催日"], default=default_race_date)
    venue_col = _pick_series(df, ["venue", "place", "jyo", "track", "開催場", "競馬場"], default="")
    race_no_col = _pick_series(df, ["race_no", "raceno", "r", "レース番号", "race"], default=0)

    built: list[str] = []
    for i in range(len(df)):
        if bool(has.iloc[i]):
            built.append(str(race_id.iloc[i]))
            continue
        d = _to_date_yyyymmdd(date_col.iloc[i] if i < len(date_col) else default_race_date)
        v = str(venue_col.iloc[i]).strip() if i < len(venue_col) else ""
        try:
            rn = int(race_no_col.iloc[i])
        except Exception:
            rn = 0
        if d is None or v == "" or rn <= 0:
            built.append("")
        else:
            built.append(build_race_id_from_parts(d, v, rn))
    return pd.Series(built)


def _normalize_races(df_raw: pd.DataFrame, target_date: str) -> pd.DataFrame:
    df = _norm_columns(df_raw)
    race_id = _ensure_race_id(df, default_race_date=target_date)
    out = pd.DataFrame(
        {
            "race_id": race_id.astype(str),
            "race_date": _pick_series(df, ["race_date", "date", "kaisai_date", "開催日"], default=target_date),
            "venue": _pick_series(df, ["venue", "place", "jyo", "track", "開催場", "競馬場"], default=""),
            "race_no": _pick_series(df, ["race_no", "raceno", "r", "レース番号"], default=0),
            "post_time": _pick_series(df, ["post_time", "hasso_time", "発走時刻"], default=None),
            "surface": _pick_series(df, ["surface", "track_type", "芝ダ", "馬場種別"], default=None),
            "distance": _pick_series(df, ["distance", "距離"], default=None),
            "track_condition": _pick_series(df, ["track_condition", "baba", "馬場状態"], default=None),
            "race_class": _pick_series(df, ["race_class", "class", "クラス", "条件"], default=None),
            "field_size_expected": _pick_series(df, ["field_size_expected", "field_size", "head_count", "頭数"], default=None),
        }
    )
    out["race_date"] = out["race_date"].astype(str).str.replace("/", "-")
    out["race_no"] = pd.to_numeric(out["race_no"], errors="coerce").fillna(0).astype(int)
    return out


def _normalize_entries(df_raw: pd.DataFrame, target_date: str) -> pd.DataFrame:
    df = _norm_columns(df_raw)
    race_id = _ensure_race_id(df, default_race_date=target_date)
    out = pd.DataFrame(
        {
            "race_id": race_id.astype(str),
            "horse_no": _pick_series(df, ["horse_no", "umaban", "馬番"], default=0),
            "horse_id": _pick_series(df, ["horse_id", "ketto_num", "血統登録番号"], default=None),
            "horse_name": _pick_series(df, ["horse_name", "bamei", "馬名"], default=None),
            "waku": _pick_series(df, ["waku", "wakuban", "枠番"], default=None),
            "sex": _pick_series(df, ["sex", "性"], default=None),
            "age": _pick_series(df, ["age", "齢", "年齢"], default=None),
            "weight_carried": _pick_series(df, ["weight_carried", "futan", "斤量"], default=None),
            "jockey_id": _pick_series(df, ["jockey_id", "kishu_id", "騎手id"], default=None),
            "trainer_id": _pick_series(df, ["trainer_id", "chokyoshi_id", "調教師id"], default=None),
            "is_scratched": _pick_series(df, ["is_scratched", "torikeshi", "取消"], default=False),
        }
    )
    out["horse_no"] = pd.to_numeric(out["horse_no"], errors="coerce").fillna(0).astype(int)
    return out


def _normalize_results(df_raw: pd.DataFrame, target_date: str) -> pd.DataFrame:
    df = _norm_columns(df_raw)
    race_id = _ensure_race_id(df, default_race_date=target_date)
    out = pd.DataFrame(
        {
            "race_id": race_id.astype(str),
            "horse_no": _pick_series(df, ["horse_no", "umaban", "馬番"], default=0),
            "finish_position": _pick_series(df, ["finish_position", "chakujun", "着順"], default=None),
            "margin": _pick_series(df, ["margin", "chakusa", "着差"], default=None),
            "last3f_time": _pick_series(df, ["last3f_time", "agari3f", "上り3f"], default=None),
            "last3f_rank": _pick_series(df, ["last3f_rank", "agari_juni", "上り順位"], default=None),
            "corner_pos_1": _pick_series(df, ["corner_pos_1", "1角"], default=None),
            "corner_pos_2": _pick_series(df, ["corner_pos_2", "2角"], default=None),
            "corner_pos_3": _pick_series(df, ["corner_pos_3", "3角"], default=None),
            "corner_pos_4": _pick_series(df, ["corner_pos_4", "corner4", "4角"], default=None),
            "pop_rank": _pick_series(df, ["pop_rank", "ninki", "人気"], default=None),
            "odds_win_final": _pick_series(df, ["odds_win_final", "tansho_odds", "単勝オッズ"], default=None),
        }
    )
    out["horse_no"] = pd.to_numeric(out["horse_no"], errors="coerce").fillna(0).astype(int)
    return out


def _normalize_payouts(df_raw: pd.DataFrame, target_date: str) -> pd.DataFrame:
    df = _norm_columns(df_raw)
    race_id = _ensure_race_id(df, default_race_date=target_date)
    out = pd.DataFrame(
        {
            "race_id": race_id.astype(str),
            "bet_type": _pick_series(df, ["bet_type", "券種", "式別"], default=None),
            "bet_key": _pick_series(df, ["bet_key", "pair", "組番"], default=None),
            "payout": _pick_series(df, ["payout", "払戻", "払戻金"], default=None),
            "popularity": _pick_series(df, ["popularity", "人気"], default=None),
        }
    )
    out["bet_type"] = out["bet_type"].astype(str).str.strip().str.lower()
    return out


def build_real_raw_from_jv_export(
    *,
    source_dir: Path,
    target_date: str,
    out_raw_dir: Path,
    races_file: str = "races.csv",
    entries_file: str = "entries.csv",
    results_file: str = "results.csv",
    payouts_file: str = "payouts.csv",
) -> dict[str, Any]:
    source_dir = source_dir.resolve()
    out_raw_dir = out_raw_dir.resolve()
    out_raw_dir.mkdir(parents=True, exist_ok=True)

    src = {
        "races": source_dir / races_file,
        "entries": source_dir / entries_file,
        "results": source_dir / results_file,
        "payouts": source_dir / payouts_file,
    }
    missing = [name for name, path in src.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"missing source csv in {source_dir}: {missing}")

    races_df = _normalize_races(_read_csv_flexible(src["races"]), target_date)
    entries_df = _normalize_entries(_read_csv_flexible(src["entries"]), target_date)
    results_df = _normalize_results(_read_csv_flexible(src["results"]), target_date)
    payouts_df = _normalize_payouts(_read_csv_flexible(src["payouts"]), target_date)

    races_df.to_csv(out_raw_dir / "races.csv", index=False, encoding="utf-8")
    entries_df.to_csv(out_raw_dir / "entries.csv", index=False, encoding="utf-8")
    results_df.to_csv(out_raw_dir / "results.csv", index=False, encoding="utf-8")
    payouts_df.to_csv(out_raw_dir / "payouts.csv", index=False, encoding="utf-8")

    manifest = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "target_date": target_date,
        "source_dir": str(source_dir),
        "out_raw_dir": str(out_raw_dir),
        "source_files": {k: str(v) for k, v in src.items()},
        "row_counts": {
            "races": int(len(races_df)),
            "entries": int(len(entries_df)),
            "results": int(len(results_df)),
            "payouts": int(len(payouts_df)),
        },
        "notes": "JV-Link file export input normalized for Aikeiba raw layer",
    }
    (out_raw_dir / "raw_manifest_check.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest

