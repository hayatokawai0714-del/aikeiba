from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True)
class ValueInputs:
    p_win: float | None
    p_top3: float | None
    market_p_win: float | None
    market_p_top3: float | None


def compute_market_prob_from_odds(odds: float) -> float:
    # Simple conversion. Production should handle takeout/overround explicitly.
    if odds <= 0:
        return 0.0
    return 1.0 / odds


def compute_market_top3_prob_from_place_odds(place_min: float | None, place_max: float | None) -> float | None:
    """
    Build a robust proxy of market top3 probability from place odds.
    If both min/max are available, use inverse geometric mean.
    """
    if place_min is None and place_max is None:
        return None
    vals = []
    for v in (place_min, place_max):
        if v is None:
            continue
        if v > 0:
            vals.append(float(v))
    if len(vals) == 0:
        return None
    if len(vals) == 1:
        return 1.0 / vals[0]
    gmean = math.sqrt(vals[0] * vals[1])
    if gmean <= 0:
        return None
    return 1.0 / gmean


def blend_ai_market_prob(
    *,
    p_ai: float | None,
    p_market: float | None,
    ai_weight: float = 0.65,
) -> float | None:
    """
    Convex blend for market-aware probability.
    If market is missing, fallback to AI-only.
    """
    if p_ai is None:
        return None
    if p_market is None:
        return float(p_ai)
    w = min(1.0, max(0.0, float(ai_weight)))
    p = w * float(p_ai) + (1.0 - w) * float(p_market)
    return min(1.0, max(0.0, p))


def value_score(inputs: ValueInputs) -> float | None:
    """
    Phase 2: odds-aware value score.
    Must NOT be used inside ability/strength model inputs.
    """
    if inputs.p_top3 is None or inputs.market_p_top3 is None:
        return None
    return float(inputs.p_top3) - float(inputs.market_p_top3)
