from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from aikeiba.db.duckdb import DuckDb


def _load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8")


def ingest_from_csv_dir(*, db: DuckDb, in_dir: Path) -> dict[str, Any]:
    """
    MVP ingestion adapter.

    Expected files (UTF-8 CSV) with columns matching DB schema:
    - races.csv
    - entries.csv
    - results.csv (optional)
    - odds.csv (optional)
    - payouts.csv (optional)
    """
    stats: dict[str, Any] = {"in_dir": str(in_dir)}

    races_path = in_dir / "races.csv"
    if races_path.exists():
        races = _load_csv(races_path)
        db.con.register("tmp_races", races)
        db.execute("DELETE FROM races WHERE race_id IN (SELECT race_id FROM tmp_races)")
        db.execute(
            """
            INSERT INTO races(
              race_id, race_date, venue, race_no, post_time, surface, distance, track_condition, race_class, field_size_expected
            )
            SELECT
              race_id, cast(race_date as DATE), venue, race_no, post_time, surface, distance, track_condition, race_class, field_size_expected
            FROM tmp_races
            """
        )
        db.con.unregister("tmp_races")
        stats["races_rows"] = int(len(races))
    else:
        stats["races_rows"] = 0

    entries_path = in_dir / "entries.csv"
    if entries_path.exists():
        entries = _load_csv(entries_path)
        db.con.register("tmp_entries", entries)
        db.execute(
            """
            DELETE FROM entries
            USING tmp_entries
            WHERE entries.race_id = tmp_entries.race_id
              AND entries.horse_no = cast(tmp_entries.horse_no as INTEGER)
            """
        )
        db.execute(
            """
            INSERT INTO entries(
              race_id, horse_no, horse_id, horse_name, waku, sex, age, weight_carried, jockey_id, jockey_name_raw, pop_rank, trainer_id, is_scratched, source_version
            )
            SELECT
              race_id, cast(horse_no as INTEGER), horse_id, horse_name, waku, sex, age, weight_carried, jockey_id, jockey_name_raw, pop_rank, trainer_id, is_scratched, source_version
            FROM tmp_entries
            """
        )
        db.con.unregister("tmp_entries")
        stats["entries_rows"] = int(len(entries))
    else:
        stats["entries_rows"] = 0

    results_path = in_dir / "results.csv"
    if results_path.exists():
        results = _load_csv(results_path)
        db.con.register("tmp_results", results)
        db.execute(
            """
            DELETE FROM results
            USING tmp_results
            WHERE results.race_id = tmp_results.race_id
              AND results.horse_no = cast(tmp_results.horse_no as INTEGER)
            """
        )
        db.execute(
            """
            INSERT INTO results(
              race_id, horse_no, finish_position, margin, last3f_time, last3f_rank,
              corner_pos_1, corner_pos_2, corner_pos_3, corner_pos_4,
              pop_rank, odds_win_final, source_version
            )
            SELECT
              race_id, cast(horse_no as INTEGER), finish_position, margin, last3f_time, last3f_rank,
              corner_pos_1, corner_pos_2, corner_pos_3, corner_pos_4,
              pop_rank, odds_win_final, source_version
            FROM tmp_results
            """
        )
        db.con.unregister("tmp_results")
        stats["results_rows"] = int(len(results))
    else:
        stats["results_rows"] = 0

    odds_path = in_dir / "odds.csv"
    if odds_path.exists():
        odds = _load_csv(odds_path)
        for c in ["horse_no", "horse_no_a", "horse_no_b"]:
            if c in odds.columns:
                odds[c] = odds[c].fillna(-1).astype(int)
        db.con.register("tmp_odds", odds)
        db.execute(
            """
            DELETE FROM odds
            USING tmp_odds
            WHERE odds.race_id = tmp_odds.race_id
              AND odds.odds_snapshot_version = tmp_odds.odds_snapshot_version
              AND odds.odds_type = tmp_odds.odds_type
              AND odds.horse_no = cast(tmp_odds.horse_no as INTEGER)
              AND odds.horse_no_a = cast(tmp_odds.horse_no_a as INTEGER)
              AND odds.horse_no_b = cast(tmp_odds.horse_no_b as INTEGER)
            """
        )
        db.execute(
            """
            INSERT INTO odds(
              race_id, odds_snapshot_version, captured_at, odds_type, horse_no, horse_no_a, horse_no_b, odds_value, source_version
            )
            SELECT
              race_id, odds_snapshot_version, cast(captured_at as TIMESTAMP), odds_type, horse_no, horse_no_a, horse_no_b, odds_value, source_version
            FROM tmp_odds
            """
        )
        db.con.unregister("tmp_odds")
        stats["odds_rows"] = int(len(odds))
    else:
        stats["odds_rows"] = 0

    payouts_path = in_dir / "payouts.csv"
    if payouts_path.exists():
        payouts = _load_csv(payouts_path)
        db.con.register("tmp_payouts", payouts)
        db.execute(
            """
            DELETE FROM payouts
            USING tmp_payouts
            WHERE payouts.race_id = tmp_payouts.race_id
              AND payouts.bet_type = cast(tmp_payouts.bet_type as VARCHAR)
              AND payouts.bet_key = cast(tmp_payouts.bet_key as VARCHAR)
            """
        )
        db.execute(
            """
            INSERT INTO payouts(
              race_id, bet_type, bet_key, payout, popularity, source_version
            )
            SELECT
              race_id, cast(bet_type as VARCHAR), cast(bet_key as VARCHAR), payout, popularity, source_version
            FROM tmp_payouts
            """
        )
        db.con.unregister("tmp_payouts")
        stats["payouts_rows"] = int(len(payouts))
    else:
        stats["payouts_rows"] = 0

    return stats
