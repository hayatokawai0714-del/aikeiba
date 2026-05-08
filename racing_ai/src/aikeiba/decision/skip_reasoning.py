from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SkipReasonConfig:
    density_low_min: float | None = None
    gap12_min: float = 0.003
    density_top3_max: float = 1.35
    min_positive_ai_gap_count: int = 0
    min_candidate_count: int = 0
    market_top1_max: float | None = None
    min_pair_value_score: float | None = None
    min_ai_market_gap: float | None = None
    max_market_overrated_top_count: int | None = None
    enforce_no_value_horse: bool = False
    enforce_market_too_strong: bool = False
    enforce_not_enough_candidates: bool = False
    enforce_pair_value: bool = False
    enforce_market_overrated_top_count: bool = False


def decide_skip_reason(
    *,
    density_top3: float | None,
    gap12: float | None,
    positive_ai_gap_count: int,
    candidate_count: int,
    market_top1: float | None,
    top_pair_value_score: float | None,
    race_ai_market_gap_max: float | None = None,
    market_overrated_top_count: int = 0,
    config: SkipReasonConfig,
) -> str:
    if density_top3 is None or gap12 is None:
        return "SKIP_ODDS_MISSING"
    if density_top3 > config.density_top3_max:
        return "SKIP_DENSITY_TOO_HIGH"
    if config.density_low_min is not None and density_top3 < config.density_low_min:
        return "SKIP_DENSITY_TOO_LOW"
    if gap12 < config.gap12_min:
        return "SKIP_GAP_TOO_SMALL"
    if config.enforce_no_value_horse and positive_ai_gap_count < config.min_positive_ai_gap_count:
        return "SKIP_NO_VALUE_HORSE"
    if config.enforce_no_value_horse and config.min_ai_market_gap is not None:
        if race_ai_market_gap_max is None or race_ai_market_gap_max < config.min_ai_market_gap:
            return "SKIP_NO_VALUE_HORSE"
    if config.enforce_market_too_strong and config.market_top1_max is not None and market_top1 is not None and market_top1 > config.market_top1_max:
        return "SKIP_MARKET_TOO_STRONG"
    if config.enforce_market_overrated_top_count and config.max_market_overrated_top_count is not None:
        if market_overrated_top_count > config.max_market_overrated_top_count:
            return "SKIP_MARKET_TOO_STRONG"
    if config.enforce_not_enough_candidates and candidate_count < config.min_candidate_count:
        return "SKIP_NOT_ENOUGH_CANDIDATES"
    if config.enforce_pair_value and config.min_pair_value_score is not None:
        if top_pair_value_score is None or top_pair_value_score < config.min_pair_value_score:
            return "SKIP_LOW_PAIR_VALUE"
    return "BUY_OK"
