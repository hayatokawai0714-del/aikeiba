from __future__ import annotations

import datetime as dt
import random

import pandas as pd

from aikeiba.db.duckdb import DuckDb


def seed_demo_dataset(db: DuckDb) -> None:
    """
    Small synthetic dataset to validate the pipeline.
    - Not intended for real training quality.
    - Deterministic with fixed RNG seed.
    """
    random.seed(42)

    # Generate 30 historical races + 3 today's races
    start = dt.date(2026, 3, 1)
    venues = ["中山", "阪神", "福島"]

    races_rows = []
    entries_rows = []
    results_rows = []

    horse_counter = 1000
    jockeys = [f"J{n:03d}" for n in range(1, 21)]
    trainers = [f"T{n:03d}" for n in range(1, 21)]

    def race_id(d: dt.date, venue: str, race_no: int) -> str:
        venue_code = {"中山": "NAK", "阪神": "HAN", "福島": "FUK"}[venue]
        return f"{d.strftime('%Y%m%d')}-{venue_code}-{race_no:02d}R"

    for i in range(30):
        d = start + dt.timedelta(days=i)
        venue = venues[i % len(venues)]
        race_no = (i % 12) + 1
        rid = race_id(d, venue, race_no)
        surface = "芝" if i % 2 == 0 else "ダート"
        distance = 1200 + (i % 5) * 200
        field_size = 12 + (i % 5)
        races_rows.append(
            {
                "race_id": rid,
                "race_date": d.isoformat(),
                "venue": venue,
                "race_no": race_no,
                "post_time": "15:00",
                "surface": surface,
                "distance": distance,
                "track_condition": "良",
                "race_class": "1勝クラス",
                "field_size_expected": field_size,
            }
        )

        base_strengths = [random.random() for _ in range(field_size)]
        order = sorted(range(field_size), key=lambda k: base_strengths[k], reverse=True)

        for hn in range(1, field_size + 1):
            horse_id = f"H{horse_counter + hn:06d}"
            horse_name = f"DEMO_HORSE_{horse_counter + hn}"
            jockey_id = random.choice(jockeys)
            trainer_id = random.choice(trainers)
            entries_rows.append(
                {
                    "race_id": rid,
                    "horse_no": hn,
                    "horse_id": horse_id,
                    "horse_name": horse_name,
                    "waku": ((hn - 1) // 2) + 1,
                    "sex": "牡",
                    "age": 4,
                    "weight_carried": 57.0,
                    "jockey_id": jockey_id,
                    "trainer_id": trainer_id,
                    "is_scratched": False,
                    "source_version": "demo",
                }
            )

        # results based on base_strengths (best strength gets pos=1)
        for pos, idx in enumerate(order, start=1):
            hn = idx + 1
            # margin: lower is better; pos 1 near 0.
            margin = max(0.0, (pos - 1) * 0.15 + random.random() * 0.05)
            last3f_rank = min(field_size, max(1, int(round(pos + random.random() * 2 - 1))))
            corner4 = min(field_size, max(1, int(round(pos + random.random() * 3 - 1))))
            pop_rank = min(field_size, max(1, int(round(pos + random.random() * 3 - 1))))
            results_rows.append(
                {
                    "race_id": rid,
                    "horse_no": hn,
                    "finish_position": pos,
                    "margin": float(margin),
                    "last3f_time": 35.0 + random.random(),
                    "last3f_rank": int(last3f_rank),
                    "corner_pos_1": None,
                    "corner_pos_2": None,
                    "corner_pos_3": None,
                    "corner_pos_4": int(corner4),
                    "pop_rank": int(pop_rank),
                    "odds_win_final": 2.0 + pos * 0.8,
                    "source_version": "demo",
                }
            )

        horse_counter += field_size

    # "Today" races with entries only (no results)
    today = dt.date(2026, 4, 14)
    for venue, race_no in [("中山", 11), ("阪神", 10), ("福島", 9)]:
        rid = race_id(today, venue, race_no)
        surface = "芝" if venue != "阪神" else "ダート"
        distance = 1800 if venue == "中山" else 1400 if venue == "阪神" else 1200
        field_size = 14 if venue == "中山" else 16 if venue == "阪神" else 12
        races_rows.append(
            {
                "race_id": rid,
                "race_date": today.isoformat(),
                "venue": venue,
                "race_no": race_no,
                "post_time": "15:30",
                "surface": surface,
                "distance": distance,
                "track_condition": "良",
                "race_class": "2勝クラス",
                "field_size_expected": field_size,
            }
        )
        for hn in range(1, field_size + 1):
            horse_id = f"H{horse_counter + hn:06d}"
            horse_name = f"TODAY_HORSE_{horse_counter + hn}"
            entries_rows.append(
                {
                    "race_id": rid,
                    "horse_no": hn,
                    "horse_id": horse_id,
                    "horse_name": horse_name,
                    "waku": ((hn - 1) // 2) + 1,
                    "sex": "牡",
                    "age": 4,
                    "weight_carried": 57.0,
                    "jockey_id": random.choice(jockeys),
                    "trainer_id": random.choice(trainers),
                    "is_scratched": False,
                    "source_version": "demo",
                }
            )
        horse_counter += field_size

    # Insert
    db.con.register("tmp_races", pd.DataFrame(races_rows))
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

    db.con.register("tmp_entries", pd.DataFrame(entries_rows))
    db.execute("DELETE FROM entries WHERE (race_id, horse_no) IN (SELECT race_id, horse_no FROM tmp_entries)")
    db.execute(
        """
        INSERT INTO entries(
          race_id, horse_no, horse_id, horse_name, waku, sex, age, weight_carried, jockey_id, trainer_id, is_scratched, source_version
        )
        SELECT
          race_id, horse_no, horse_id, horse_name, waku, sex, age, weight_carried, jockey_id, trainer_id, is_scratched, source_version
        FROM tmp_entries
        """
    )
    db.con.unregister("tmp_entries")

    db.con.register("tmp_results", pd.DataFrame(results_rows))
    db.execute("DELETE FROM results WHERE (race_id, horse_no) IN (SELECT race_id, horse_no FROM tmp_results)")
    db.execute(
        """
        INSERT INTO results(
          race_id, horse_no, finish_position, margin, last3f_time, last3f_rank,
          corner_pos_1, corner_pos_2, corner_pos_3, corner_pos_4,
          pop_rank, odds_win_final, source_version
        )
        SELECT
          race_id, horse_no, finish_position, margin, last3f_time, last3f_rank,
          corner_pos_1, corner_pos_2, corner_pos_3, corner_pos_4,
          pop_rank, odds_win_final, source_version
        FROM tmp_results
        """
    )
    db.con.unregister("tmp_results")
