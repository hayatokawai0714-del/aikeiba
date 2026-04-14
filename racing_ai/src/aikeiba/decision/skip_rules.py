from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class SkipDecision:
    buy_flag: bool
    reason: str
    density_top3: float
    gap12: float


def decide_buy_or_skip(
    *,
    p_top3: list[float],
    density_top3_max: float = 1.35,
    gap12_min: float = 0.003,
) -> SkipDecision:
    """
    Race-level decision based on calibrated p_top3.
    - density_top3: sum of top-3 p_top3
    - gap12: top1 - top2
    """
    arr = np.asarray(p_top3, dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return SkipDecision(buy_flag=False, reason="no_probs", density_top3=float("nan"), gap12=float("nan"))

    top = np.sort(arr)[::-1]
    density = float(np.sum(top[:3])) if top.size >= 3 else float(np.sum(top))
    gap12 = float(top[0] - top[1]) if top.size >= 2 else float("nan")

    reasons = []
    if density > density_top3_max:
        reasons.append("density_top3_excess")
    if not np.isnan(gap12) and gap12 < gap12_min:
        reasons.append("gap12_shortage")

    if len(reasons) > 0:
        return SkipDecision(buy_flag=False, reason=" + ".join(reasons), density_top3=density, gap12=gap12)
    return SkipDecision(buy_flag=True, reason="ok", density_top3=density, gap12=gap12)
