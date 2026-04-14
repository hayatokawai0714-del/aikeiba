from __future__ import annotations

from dataclasses import dataclass


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


def value_score(inputs: ValueInputs) -> float | None:
    """
    Phase 2: odds-aware value score.
    Must NOT be used inside ability/strength model inputs.
    """
    if inputs.p_top3 is None or inputs.market_p_top3 is None:
        return None
    return float(inputs.p_top3) - float(inputs.market_p_top3)
