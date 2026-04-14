from __future__ import annotations


def overlap_guard_pairs(
    *,
    today_wide_candidates: set[tuple[str, str]],
    today_pipeline_bets: set[tuple[str, str]],
    result_detail: set[tuple[str, str]] | None = None,
) -> dict[str, object]:
    """
    Minimum overlap guard:
    - Keys are (race_id, pair).
    - If overlap is 0, caller can stop or warn.
    """
    result_detail = result_detail or set()

    a = today_wide_candidates
    b = today_pipeline_bets
    ab = a.intersection(b)

    br = b.intersection(result_detail) if result_detail else set()
    ar = a.intersection(result_detail) if result_detail else set()

    return {
        "overlap_candidates_vs_bets": len(ab),
        "overlap_bets_vs_results": len(br),
        "overlap_candidates_vs_results": len(ar),
        "should_stop": len(a) > 0 and len(b) > 0 and len(ab) == 0,
    }
