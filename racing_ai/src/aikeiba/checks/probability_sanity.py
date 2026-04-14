from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass(frozen=True)
class ProbSanity:
    should_stop: bool
    stop_reasons: list[str]
    stats: dict[str, Any]


def check_top3_probs(p_top3: np.ndarray) -> ProbSanity:
    stop: list[str] = []
    stats: dict[str, Any] = {}

    if p_top3.size == 0:
        stop.append("no_probs")
        return ProbSanity(should_stop=True, stop_reasons=stop, stats=stats)

    if np.any(np.isnan(p_top3)):
        stop.append("p_top3_has_nan")

    mn = float(np.nanmin(p_top3))
    mx = float(np.nanmax(p_top3))
    stats["p_top3_min"] = mn
    stats["p_top3_max"] = mx
    if mn < -1e-6 or mx > 1.0 + 1e-6:
        stop.append("p_top3_out_of_range")

    return ProbSanity(should_stop=len(stop) > 0, stop_reasons=stop, stats=stats)
