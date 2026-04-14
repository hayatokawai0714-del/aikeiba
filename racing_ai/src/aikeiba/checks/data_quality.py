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
