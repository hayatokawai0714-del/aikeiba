from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PairScoreRow:
    race_id: str
    pair: str
    horse1_no: int
    horse2_no: int
    horse1_p_top3_fused: float | None
    horse2_p_top3_fused: float | None
    pair_prob_naive: float | None
    horse1_market_top3_proxy: float | None
    horse2_market_top3_proxy: float | None
    horse1_ai_market_gap: float | None
    horse2_ai_market_gap: float | None
    pair_value_score: float | None
    pair_missing_flag: bool


def simple_pair_value_score(
    *,
    p1: float | None,
    p2: float | None,
    gap1: float | None,
    gap2: float | None,
) -> tuple[float | None, float | None, bool]:
    if p1 is None or p2 is None:
        return None, None, True
    pair_prob_naive = float(p1) * float(p2)
    g1 = max(float(gap1), 0.0) if gap1 is not None else 0.0
    g2 = max(float(gap2), 0.0) if gap2 is not None else 0.0
    score = pair_prob_naive + 0.5 * g1 + 0.5 * g2
    return pair_prob_naive, score, False

