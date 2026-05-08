from __future__ import annotations

from aikeiba.decision.value_model import (
    blend_ai_market_prob,
    compute_market_top3_prob_from_place_odds,
)


def test_compute_market_top3_prob_from_place_odds_geometric_mean() -> None:
    p = compute_market_top3_prob_from_place_odds(2.0, 4.5)
    assert p is not None
    assert abs(p - (1.0 / 3.0)) < 1e-9


def test_blend_ai_market_prob_fallback_and_blend() -> None:
    assert blend_ai_market_prob(p_ai=0.4, p_market=None, ai_weight=0.65) == 0.4
    b = blend_ai_market_prob(p_ai=0.4, p_market=0.2, ai_weight=0.65)
    assert b is not None
    assert abs(b - (0.65 * 0.4 + 0.35 * 0.2)) < 1e-12

