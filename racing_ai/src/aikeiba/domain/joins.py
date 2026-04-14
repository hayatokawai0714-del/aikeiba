from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from aikeiba.db.duckdb import DuckDb


@dataclass(frozen=True)
class PastPerformanceRow:
    race_date: str
    race_id: str
    venue: str | None
    surface: str | None
    distance: int | None
    finish_position: int | None
    margin: float | None
    last3f_rank: int | None
    corner_pos_4: int | None
    pop_rank: int | None
    jockey_id: str | None


def load_past_performances(
    *,
    db: DuckDb,
    horse_id: str,
    max_race_date: str,
    limit: int = 20,
) -> list[PastPerformanceRow]:
    """
    Point-in-time safe history loader:
    - Uses results joined with races/entries for a horse_id.
    - Restricts to races strictly before max_race_date (inclusive by caller choice).
    """
    df = db.query_df(
        """
        SELECT
          cast(r.race_date as VARCHAR) AS race_date,
          r.race_id,
          r.venue,
          r.surface,
          r.distance,
          res.finish_position,
          res.margin,
          res.last3f_rank,
          res.corner_pos_4,
          res.pop_rank,
          e.jockey_id
        FROM results res
        JOIN races r ON r.race_id = res.race_id
        JOIN entries e ON e.race_id = res.race_id AND e.horse_no = res.horse_no
        WHERE e.horse_id = ?
          AND r.race_date <= ?
        ORDER BY r.race_date DESC
        LIMIT ?
        """,
        (horse_id, max_race_date, limit),
    )

    rows: list[PastPerformanceRow] = []
    for rec in df.to_dict("records"):
        rows.append(PastPerformanceRow(**rec))
    return rows


def jockey_top3_rate_lookback(
    *,
    db: DuckDb,
    jockey_id: str,
    asof_date: str,
    lookback_days: int = 365,
) -> Optional[float]:
    import datetime as dt

    min_date = (dt.date.fromisoformat(asof_date) - dt.timedelta(days=lookback_days)).isoformat()
    df = db.query_df(
        """
        SELECT
          avg(CASE WHEN res.finish_position IS NOT NULL AND res.finish_position <= 3 THEN 1.0 ELSE 0.0 END) AS rate
        FROM results res
        JOIN races r ON r.race_id = res.race_id
        JOIN entries e ON e.race_id = res.race_id AND e.horse_no = res.horse_no
        WHERE e.jockey_id = ?
          AND r.race_date < cast(? as DATE)
          AND r.race_date >= cast(? as DATE)
        """,
        (jockey_id, asof_date, min_date),
    )
    v = df.iloc[0]["rate"]
    if v is None:
        return None
    return float(v)


def field_size_for_race(db: DuckDb, race_id: str) -> int:
    df = db.query_df("SELECT count(*) AS n FROM entries WHERE race_id = ? AND (is_scratched IS NULL OR is_scratched=FALSE)", (race_id,))
    return int(df.iloc[0]["n"])
