from __future__ import annotations

from typing import Any

import numpy as np

from aikeiba.domain.joins import PastPerformanceRow


def _mean(values: list[float | None]) -> float | None:
    xs = [v for v in values if v is not None]
    if len(xs) == 0:
        return None
    return float(np.mean(xs))


def _std(values: list[float | None]) -> float | None:
    xs = [v for v in values if v is not None]
    if len(xs) < 2:
        return None
    return float(np.std(xs, ddof=1))


def _rate(rows: list[PastPerformanceRow], fn) -> float | None:
    if len(rows) == 0:
        return None
    return float(np.mean([1.0 if fn(row) else 0.0 for row in rows]))


def build_pace_features(
    *,
    history: list[PastPerformanceRow],
    min_history: int = 5,
) -> dict[str, Any]:
    last5 = history[:5]
    corner4_last5 = [float(row.corner_pos_4) if row.corner_pos_4 is not None else None for row in last5]
    last3f_rank_last5 = [float(row.last3f_rank) if row.last3f_rank is not None else None for row in last5]
    pace_finish_delta_last5 = [
        float(row.corner_pos_4 - row.finish_position)
        if row.corner_pos_4 is not None and row.finish_position is not None
        else None
        for row in last5
    ]

    avg_corner4_pos_last5 = _mean(corner4_last5)

    return {
        "avg_corner4_pos_last5": avg_corner4_pos_last5,
        "corner4_pos_std_last5": _std(corner4_last5),
        "front_runner_rate_last5": _rate(last5, lambda row: row.corner_pos_4 is not None and row.corner_pos_4 <= 4),
        "closer_rate_last5": _rate(last5, lambda row: row.corner_pos_4 is not None and row.corner_pos_4 >= 10),
        "avg_last3f_rank_last5": _mean(last3f_rank_last5),
        "pace_finish_delta_last5": _mean(pace_finish_delta_last5),
        "pace_min_history_flag": len(history) < min_history,
    }
