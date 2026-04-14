from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WideCandidate:
    race_id: str
    axis_horse_no: int
    partner_horse_no: int
    selected_stage: str

    @property
    def pair(self) -> str:
        a, b = sorted((self.axis_horse_no, self.partner_horse_no))
        return f"{a}-{b}"


def generate_wide_candidates_rule_based(
    *,
    race_id: str,
    horse_nos: list[int],
    p_top3: dict[int, float],
    ability_rank: dict[int, int] | None = None,
    stability: dict[int, float] | None = None,
    axis_k: int = 1,
    partner_k: int = 6,
) -> list[WideCandidate]:
    """
    MVP: p_top3-centered rule-based wide candidates.
    - Two-stage mindset: axis vs partners.
    - EV integration is Phase 2 (wide odds snapshot optional).
    """
    ability_rank = ability_rank or {}
    stability = stability or {}

    def score_axis(h: int) -> float:
        # prioritize high p_top3, then stability, then ability rank.
        return float(p_top3.get(h, 0.0)) * 100.0 + float(stability.get(h, 0.0)) * 10.0 - float(ability_rank.get(h, 999)) * 0.1

    def score_partner(h: int) -> float:
        return float(p_top3.get(h, 0.0)) * 100.0 - float(ability_rank.get(h, 999)) * 0.05

    axis_sorted = sorted(horse_nos, key=score_axis, reverse=True)[: max(axis_k, 1)]
    partner_sorted = sorted(horse_nos, key=score_partner, reverse=True)[: max(partner_k, 2)]

    candidates: list[WideCandidate] = []
    for a in axis_sorted:
        for b in partner_sorted:
            if a == b:
                continue
            stage = "A" if b in partner_sorted[:3] else "B"
            candidates.append(WideCandidate(race_id=race_id, axis_horse_no=a, partner_horse_no=b, selected_stage=stage))

    # de-dup pairs (axis changes can generate same pair)
    uniq: dict[str, WideCandidate] = {}
    for c in candidates:
        uniq.setdefault(f"{c.race_id}:{c.pair}:{c.selected_stage}", c)
    return list(uniq.values())
