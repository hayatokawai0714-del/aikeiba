from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from aikeiba.db.duckdb import DuckDb
from aikeiba.db.migrations import apply_migrations


VENUE3_MAP = {
    "東京": "TOK",
    "中山": "NAK",
    "京都": "KYO",
    "阪神": "HAN",
    "中京": "CHU",
    "小倉": "KOK",
    "新潟": "NII",
    "福島": "FUK",
    "札幌": "SAP",
    "函館": "HAK",
}


def _to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.strip(), errors="coerce")


def _build_race_id(df: pd.DataFrame) -> pd.Series:
    race_date = pd.to_datetime(df["race_date"], errors="coerce").dt.strftime("%Y%m%d")
    venue = df["venue_name"].astype(str).str.strip().map(VENUE3_MAP)
    race_no = _to_int(df["race_no"]).astype(str).str.zfill(2)
    race_id = race_date + "-" + venue.fillna("UNK") + "-" + race_no + "R"
    return race_id


def import_population(
    *,
    db_path: Path,
    population_csv: Path,
    start_date: str,
    end_date: str,
    source_version: str,
) -> dict[str, Any]:
    db = DuckDb.connect(db_path)
    apply_migrations(db)

    df = pd.read_csv(population_csv, encoding="cp932", dtype=str)
    if "race_date" not in df.columns:
        raise RuntimeError("population csv missing race_date")
    dt_series = pd.to_datetime(df["race_date"], errors="coerce")
    m = (dt_series >= pd.to_datetime(start_date)) & (dt_series <= pd.to_datetime(end_date))
    df = df[m].copy()
    if len(df) == 0:
        return {"status": "warning", "message": "no rows in selected range", "rows": 0}

    df["race_id"] = _build_race_id(df)
    df["horse_no_i"] = _to_int(df.get("horse_no", pd.Series(index=df.index)))
    df["field_size_i"] = _to_int(df.get("field_size", pd.Series(index=df.index)))
    df["finish_i"] = _to_int(df.get("finish_position", pd.Series(index=df.index)))
    df["pop_rank_i"] = _to_int(df.get("pop_rank", pd.Series(index=df.index)))
    df["win_odds_f"] = _to_float(df.get("win_odds", pd.Series(index=df.index)))
    df["weight_f"] = _to_float(df.get("weight_carried", pd.Series(index=df.index)))
    df["margin_f"] = _to_float(df.get("margin_time", pd.Series(index=df.index)))

    races = (
        df.groupby("race_id", dropna=False)
        .agg(
            race_date=("race_date", "first"),
            venue=("venue_name", "first"),
            race_no=("race_no", "first"),
            surface=("surface", "first"),
            distance=("distance", "first"),
            track_condition=("track_condition", "first"),
            race_class=("class_code", "first"),
            field_size_expected=("field_size_i", "max"),
        )
        .reset_index()
    )
    races["post_time"] = None

    entries = pd.DataFrame(
        {
            "race_id": df["race_id"],
            "horse_no": df["horse_no_i"],
            "horse_id": df.get("horse_id", pd.Series(index=df.index)).astype(str).str.strip(),
            "horse_name": df.get("horse_name", pd.Series(index=df.index)),
            "waku": None,
            "sex": df.get("sex", pd.Series(index=df.index)),
            "age": _to_int(df.get("age", pd.Series(index=df.index))),
            "weight_carried": df["weight_f"],
            "jockey_id": df.get("jockey_code", pd.Series(index=df.index)).astype(str).str.strip(),
            "jockey_name_raw": df.get("jockey_name", pd.Series(index=df.index)),
            "pop_rank": df["pop_rank_i"],
            "trainer_id": df.get("trainer_code", pd.Series(index=df.index)).astype(str).str.strip(),
            "is_scratched": False,
            "source_version": source_version,
        }
    )

    results = pd.DataFrame(
        {
            "race_id": df["race_id"],
            "horse_no": df["horse_no_i"],
            "finish_position": df["finish_i"],
            "margin": df["margin_f"],
            "last3f_time": _to_float(df.get("last3f_time", pd.Series(index=df.index))),
            "last3f_rank": None,
            "corner_pos_1": _to_int(df.get("corner_pos_1", pd.Series(index=df.index))),
            "corner_pos_2": _to_int(df.get("corner_pos_2", pd.Series(index=df.index))),
            "corner_pos_3": _to_int(df.get("corner_pos_3", pd.Series(index=df.index))),
            "corner_pos_4": None,
            "pop_rank": df["pop_rank_i"],
            "odds_win_final": df["win_odds_f"],
            "source_version": source_version,
        }
    )

    races = races.dropna(subset=["race_id", "race_date"])
    entries = entries.dropna(subset=["race_id", "horse_no"])
    results = results.dropna(subset=["race_id", "horse_no"])

    db.con.register("tmp_hist_races", races)
    db.execute("DELETE FROM races WHERE race_id IN (SELECT race_id FROM tmp_hist_races)")
    db.execute(
        """
        INSERT INTO races(
          race_id, race_date, venue, race_no, post_time, surface, distance, track_condition, race_class, field_size_expected
        )
        SELECT
          race_id, cast(race_date as DATE), venue, cast(race_no as INTEGER), post_time, surface, cast(distance as INTEGER), track_condition, race_class, cast(field_size_expected as INTEGER)
        FROM tmp_hist_races
        """
    )
    db.con.unregister("tmp_hist_races")

    db.con.register("tmp_hist_entries", entries)
    db.execute(
        """
        DELETE FROM entries
        USING tmp_hist_entries
        WHERE entries.race_id = tmp_hist_entries.race_id
          AND entries.horse_no = cast(tmp_hist_entries.horse_no as INTEGER)
        """
    )
    db.execute(
        """
        INSERT INTO entries(
          race_id, horse_no, horse_id, horse_name, waku, sex, age, weight_carried, jockey_id, jockey_name_raw, pop_rank, trainer_id, is_scratched, source_version
        )
        SELECT
          race_id, cast(horse_no as INTEGER), nullif(horse_id,''), horse_name, waku, sex, cast(age as INTEGER), weight_carried, nullif(jockey_id,''), jockey_name_raw, cast(pop_rank as INTEGER), nullif(trainer_id,''), cast(is_scratched as BOOLEAN), source_version
        FROM tmp_hist_entries
        """
    )
    db.con.unregister("tmp_hist_entries")

    db.con.register("tmp_hist_results", results)
    db.execute(
        """
        DELETE FROM results
        USING tmp_hist_results
        WHERE results.race_id = tmp_hist_results.race_id
          AND results.horse_no = cast(tmp_hist_results.horse_no as INTEGER)
        """
    )
    db.execute(
        """
        INSERT INTO results(
          race_id, horse_no, finish_position, margin, last3f_time, last3f_rank,
          corner_pos_1, corner_pos_2, corner_pos_3, corner_pos_4, pop_rank, odds_win_final, source_version
        )
        SELECT
          race_id, cast(horse_no as INTEGER), cast(finish_position as INTEGER), margin, last3f_time, cast(last3f_rank as INTEGER),
          cast(corner_pos_1 as INTEGER), cast(corner_pos_2 as INTEGER), cast(corner_pos_3 as INTEGER), cast(corner_pos_4 as INTEGER), cast(pop_rank as INTEGER), odds_win_final, source_version
        FROM tmp_hist_results
        """
    )
    db.con.unregister("tmp_hist_results")

    return {
        "status": "ok",
        "population_csv": str(population_csv),
        "start_date": start_date,
        "end_date": end_date,
        "source_version": source_version,
        "rows_filtered": int(len(df)),
        "races_rows": int(len(races)),
        "entries_rows": int(len(entries)),
        "results_rows": int(len(results)),
        "payouts_rows": 0,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="racing_ai/data/warehouse/aikeiba.duckdb")
    ap.add_argument("--population-csv", default=r"C:\TXT\population_master_2021_2026_v1.csv")
    ap.add_argument("--start-date", default="2021-01-01")
    ap.add_argument("--end-date", default="2024-12-31")
    ap.add_argument("--source-version", default="target_population_2021_2024_v1")
    args = ap.parse_args()

    res = import_population(
        db_path=Path(args.db_path),
        population_csv=Path(args.population_csv),
        start_date=args.start_date,
        end_date=args.end_date,
        source_version=args.source_version,
    )
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
