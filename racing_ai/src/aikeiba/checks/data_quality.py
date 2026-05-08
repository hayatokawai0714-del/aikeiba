from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from aikeiba.db.duckdb import DuckDb


@dataclass(frozen=True)
class DoctorReport:
    should_stop: bool
    stop_reasons: list[str]
    warn_reasons: list[str]
    stats: dict[str, Any]


def run_doctor(db: DuckDb, race_date: Optional[str]) -> dict[str, Any]:
    # Keep it safe: only lightweight, auditable checks for now.
    stop_reasons: list[str] = []
    warn_reasons: list[str] = []

    stats: dict[str, Any] = {}
    if race_date is not None:
        stats["race_date"] = race_date
        n_races = int(db.query_df("SELECT count(*) AS n FROM races WHERE race_date = ?", (race_date,)).iloc[0]["n"])
        n_entries = int(
            db.query_df(
                "SELECT count(*) AS n FROM entries e JOIN races r ON r.race_id=e.race_id WHERE r.race_date = ?",
                (race_date,),
            ).iloc[0]["n"]
        )
        stats["n_races"] = n_races
        stats["n_entries"] = n_entries

        if n_races == 0:
            stop_reasons.append("no_races_for_date")
        if n_entries == 0:
            stop_reasons.append("no_entries_for_date")

        # horse_id attach rate
        attach = db.query_df(
            """
            SELECT
              avg(CASE WHEN e.horse_id IS NULL OR e.horse_id='' THEN 0.0 ELSE 1.0 END) AS attach_rate
            FROM entries e
            JOIN races r ON r.race_id=e.race_id
            WHERE r.race_date = ?
            """,
            (race_date,),
        ).iloc[0]["attach_rate"]
        stats["horse_id_attach_rate"] = float(attach) if attach is not None else None
        if attach is not None and float(attach) < 0.98:
            stop_reasons.append("horse_id_attach_rate_low")

        # odds attach proxy (rows / entries)
        n_odds = int(
            db.query_df(
                """
                SELECT count(*) AS n
                FROM odds o
                JOIN races r ON r.race_id=o.race_id
                WHERE r.race_date = ?
                """,
                (race_date,),
            ).iloc[0]["n"]
        )
        odds_attach_rate = float(n_odds) / float(n_entries) if n_entries > 0 else None
        stats["odds_rows"] = n_odds
        stats["odds_attach_rate"] = odds_attach_rate
        if odds_attach_rate is None or odds_attach_rate < 0.5:
            warn_reasons.append("odds_attach_rate_low")

        # field size outlier check
        field_df = db.query_df(
            """
            SELECT e.race_id, count(*) AS field_size
            FROM entries e
            JOIN races r ON r.race_id=e.race_id
            WHERE r.race_date = ?
              AND (e.is_scratched IS NULL OR e.is_scratched=FALSE)
            GROUP BY e.race_id
            """,
            (race_date,),
        )
        if len(field_df) > 0:
            min_field = int(field_df["field_size"].min())
            max_field = int(field_df["field_size"].max())
            stats["field_size_min"] = min_field
            stats["field_size_max"] = max_field
            if min_field < 3 or max_field > 20:
                stop_reasons.append("field_size_outlier_stop")
            elif min_field < 6 or max_field > 18:
                warn_reasons.append("field_size_outlier_warn")

    report = DoctorReport(
        should_stop=len(stop_reasons) > 0,
        stop_reasons=stop_reasons,
        warn_reasons=warn_reasons,
        stats=stats,
    )
    return {
        "should_stop": report.should_stop,
        "stop_reasons": report.stop_reasons,
        "warn_reasons": report.warn_reasons,
        "stats": report.stats,
    }
