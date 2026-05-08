from __future__ import annotations

from typing import Any

import numpy as np

from aikeiba.domain.joins import PastPerformanceRow


def _mean(vals: list[float | None]) -> float | None:
    xs = [v for v in vals if v is not None]
    if not xs:
        return None
    return float(np.mean(xs))


def _std(vals: list[float | None]) -> float | None:
    xs = [v for v in vals if v is not None]
    if len(xs) < 2:
        return None
    return float(np.std(xs, ddof=1))


def _rate(rows: list[PastPerformanceRow], fn) -> float | None:
    if len(rows) == 0:
        return None
    vals = [1.0 if fn(r) else 0.0 for r in rows]
    return float(np.mean(vals))


def _worst_finish(rows: list[PastPerformanceRow]) -> int | None:
    vals: list[int] = []
    for r in rows:
        v = r.finish_position
        if v is None:
            continue
        try:
            fv = float(v)
        except Exception:
            continue
        if np.isnan(fv):
            continue
        vals.append(int(fv))
    if len(vals) == 0:
        return None
    return int(max(vals))


def _consecutive(rows: list[PastPerformanceRow], fn) -> int:
    c = 0
    for r in rows:
        if fn(r):
            c += 1
        else:
            break
    return c


def _distance_bucket(distance: int | None) -> str | None:
    if distance is None:
        return None
    if distance <= 1400:
        return "sprint"
    if distance <= 1800:
        return "mile"
    if distance <= 2200:
        return "middle"
    return "long"


def build_stability_features(
    *,
    history: list[PastPerformanceRow],
    current_distance: int | None,
    current_venue: str | None,
    min_history: int = 5,
) -> dict[str, Any]:
    last5 = history[:5]
    last10 = history[:10]
    finish_last5 = [float(r.finish_position) if r.finish_position is not None else None for r in last5]
    finish_last10 = [float(r.finish_position) if r.finish_position is not None else None for r in last10]
    margin_last5 = [r.margin for r in last5]
    margin_last10 = [r.margin for r in last10]

    current_bucket = _distance_bucket(current_distance)
    same_bucket = [r for r in history if _distance_bucket(r.distance) == current_bucket] if current_bucket else []
    same_course = [r for r in history if current_venue is not None and r.venue == current_venue]

    out = {
        "finish_pos_std_last5": _std(finish_last5),
        "finish_pos_std_last10": _std(finish_last10),
        "margin_std_last5": _std(margin_last5),
        "margin_std_last10": _std(margin_last10),
        "top3_rate_last5": _rate(last5, lambda r: r.finish_position is not None and r.finish_position <= 3),
        "top3_rate_last10": _rate(last10, lambda r: r.finish_position is not None and r.finish_position <= 3),
        "board_rate_last5": _rate(last5, lambda r: r.finish_position is not None and r.finish_position <= 5),
        "board_rate_last10": _rate(last10, lambda r: r.finish_position is not None and r.finish_position <= 5),
        "big_loss_rate_last5": _rate(last5, lambda r: r.margin is not None and r.margin >= 2.0),
        "big_loss_rate_last10": _rate(last10, lambda r: r.margin is not None and r.margin >= 2.0),
        "worst_finish_last5": _worst_finish(last5),
        "worst_finish_last10": _worst_finish(last10),
        "top3_rate_same_distance_bucket": _rate(same_bucket, lambda r: r.finish_position is not None and r.finish_position <= 3),
        "top3_rate_same_course": _rate(same_course, lambda r: r.finish_position is not None and r.finish_position <= 3),
        "finish_pos_std_same_course": _std([float(r.finish_position) if r.finish_position is not None else None for r in same_course]),
        "margin_std_same_course": _std([r.margin for r in same_course]),
        "consecutive_bad_runs": _consecutive(history, lambda r: r.finish_position is not None and r.finish_position >= 10),
        "consecutive_top3_runs": _consecutive(history, lambda r: r.finish_position is not None and r.finish_position <= 3),
        "avg_finish_pos_last5": _mean(finish_last5),
        "avg_margin_last5": _mean(margin_last5),
        "min_history_flag": len(history) < min_history,
    }
    return out
