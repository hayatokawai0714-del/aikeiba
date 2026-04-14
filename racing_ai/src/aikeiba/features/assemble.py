from __future__ import annotations

import datetime as dt
from dataclasses import asdict
from typing import Any

import numpy as np
import pandas as pd

from aikeiba.common.hashing import stable_fingerprint
from aikeiba.db.duckdb import DuckDb
from aikeiba.domain.joins import (
    field_size_for_race,
    jockey_top3_rate_lookback,
    load_past_performances,
)
from aikeiba.features.base import SnapshotMeta


def _now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _date_minus_days(date_str: str, days: int) -> str:
    d = dt.date.fromisoformat(date_str)
    return (d - dt.timedelta(days=days)).isoformat()


def build_feature_store_snapshot(
    *,
    db: DuckDb,
    race_date: str,
    feature_snapshot_version: str,
    source_race_date_max: str | None = None,
) -> dict[str, Any]:
    """
    Build MVP features for a single race_date and write to feature_store table.
    Point-in-time:
      - source_race_date_max defaults to race_date - 1 day.
      - histories and aggregates must not use races after that.
    """
    if source_race_date_max is None:
        source_race_date_max = _date_minus_days(race_date, 1)

    races = db.query_df("SELECT race_id, venue, surface, distance FROM races WHERE race_date = ?", (race_date,)).to_dict("records")
    entries = db.query_df(
        """
        SELECT r.race_id, r.venue, r.surface, r.distance, e.horse_no, e.waku, e.horse_id, e.jockey_id
        FROM entries e
        JOIN races r ON r.race_id=e.race_id
        WHERE r.race_date = ?
          AND (e.is_scratched IS NULL OR e.is_scratched=FALSE)
        """,
        (race_date,),
    ).to_dict("records")

    dataset_fp = stable_fingerprint(
        {
            "race_date": race_date,
            "source_race_date_max": source_race_date_max,
            "counts": {
                "races": len(races),
                "entries": len(entries),
            },
        }
    )

    meta = SnapshotMeta(
        feature_generated_at=_now_iso(),
        source_race_date_max=source_race_date_max,
        feature_snapshot_version=feature_snapshot_version,
        dataset_fingerprint=dataset_fp,
        odds_snapshot_version=None,
    )

    # Precompute field sizes per race_id.
    field_sizes: dict[str, int] = {}
    for r in races:
        field_sizes[r["race_id"]] = field_size_for_race(db, r["race_id"])

    rows: list[dict[str, Any]] = []
    for e in entries:
        race_id = e["race_id"]
        horse_id = e["horse_id"]
        if horse_id is None or str(horse_id).strip() == "":
            # We'll stop upstream if attach rate is low; keep row with nulls.
            history = []
        else:
            history = load_past_performances(db=db, horse_id=horse_id, max_race_date=source_race_date_max, limit=20)

        prev = history[0] if len(history) > 0 else None
        last3 = history[:3]
        last5 = history[:5]
        last10 = history[:10]

        def mean_int(vals: list[int | None]) -> float | None:
            xs = [v for v in vals if v is not None]
            if len(xs) == 0:
                return None
            return float(np.mean(xs))

        def mean_float(vals: list[float | None]) -> float | None:
            xs = [v for v in vals if v is not None]
            if len(xs) == 0:
                return None
            return float(np.mean(xs))

        def std_float(vals: list[float | None]) -> float | None:
            xs = [v for v in vals if v is not None]
            if len(xs) < 2:
                return None
            return float(np.std(xs, ddof=1))

        prev_last3f_rank = prev.last3f_rank if prev else None
        avg_last3f_rank_3 = mean_int([r.last3f_rank for r in last3])
        best_last3f_count = int(sum(1 for r in last10 if r.last3f_rank == 1))

        prev_margin = prev.margin if prev else None
        avg_margin_3 = mean_float([r.margin for r in last3])
        margin_std_3 = std_float([r.margin for r in last3])

        prev_corner4_pos = prev.corner_pos_4 if prev else None
        avg_corner4_pos_3 = mean_int([r.corner_pos_4 for r in last3])

        dist_change = None
        course_change = None
        surface_change = None
        if prev and e["distance"] is not None and prev.distance is not None:
            dist_change = int(e["distance"] - prev.distance)
            course_change = bool(e["venue"] != prev.venue)
            surface_change = bool(e["surface"] != prev.surface)

        finish_pos_std_5 = std_float([float(r.finish_position) if r.finish_position is not None else None for r in last5])
        big_loss_count_10 = int(sum(1 for r in last10 if r.margin is not None and r.margin >= 2.0))
        itb_rate_10 = mean_float([1.0 if (r.finish_position is not None and r.finish_position <= 5) else 0.0 for r in last10]) if len(last10) > 0 else None

        field_size = field_sizes.get(race_id, 0)
        horse_no_rel = float(e["horse_no"]) / float(field_size) if field_size else None

        jockey_rate = None
        if e["jockey_id"] is not None and str(e["jockey_id"]).strip() != "":
            jockey_rate = jockey_top3_rate_lookback(db=db, jockey_id=e["jockey_id"], asof_date=race_date, lookback_days=365)

        rows.append(
            {
                "race_id": race_id,
                "horse_no": int(e["horse_no"]),
                "feature_snapshot_version": feature_snapshot_version,
                "race_date": race_date,
                "venue": e["venue"],
                "surface": e["surface"],
                "distance": e["distance"],
                "field_size": field_size,
                "prev_last3f_rank": prev_last3f_rank,
                "avg_last3f_rank_3": avg_last3f_rank_3,
                "best_last3f_count": best_last3f_count,
                "prev_margin": prev_margin,
                "avg_margin_3": avg_margin_3,
                "margin_std_3": margin_std_3,
                "prev_corner4_pos": prev_corner4_pos,
                "avg_corner4_pos_3": avg_corner4_pos_3,
                "dist_change": dist_change,
                "course_change": course_change,
                "surface_change": surface_change,
                "finish_pos_std_5": finish_pos_std_5,
                "big_loss_count_10": big_loss_count_10,
                "itb_rate_10": itb_rate_10,
                "waku": e["waku"],
                "horse_no_rel": horse_no_rel,
                "jockey_top3_rate_1y": jockey_rate,
                **asdict(meta),
            }
        )

    # Write rows into feature_store with idempotent delete+insert for the date+snapshot.
    db.execute(
        "DELETE FROM feature_store WHERE race_date = cast(? as DATE) AND feature_snapshot_version = ?",
        (race_date, feature_snapshot_version),
    )
    if len(rows) > 0:
        # Use DuckDB's relation insert via pandas conversion.
        df = pd.DataFrame(rows)
        db.con.register("tmp_feature_rows", df)
        db.execute("INSERT INTO feature_store SELECT * FROM tmp_feature_rows")
        db.con.unregister("tmp_feature_rows")

    return {"race_date": race_date, "rows": len(rows), "meta": asdict(meta)}
