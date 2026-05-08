from __future__ import annotations

import pandas as pd

from aikeiba.inference.top3 import _shrink_race_sum_top3


def test_shrink_race_sum_top3_shrinks_only_when_sum_exceeds_target() -> None:
    df = pd.DataFrame(
        [
            {"race_id": "R1", "horse_no": 1, "p_top3": 0.9},
            {"race_id": "R1", "horse_no": 2, "p_top3": 0.8},
            {"race_id": "R1", "horse_no": 3, "p_top3": 0.7},
            {"race_id": "R1", "horse_no": 4, "p_top3": 0.6},
            {"race_id": "R1", "horse_no": 5, "p_top3": 0.5},
            {"race_id": "R2", "horse_no": 1, "p_top3": 0.5},
            {"race_id": "R2", "horse_no": 2, "p_top3": 0.4},
            {"race_id": "R2", "horse_no": 3, "p_top3": 0.3},
        ]
    )

    out = _shrink_race_sum_top3(df, target_sum=3.0)

    sum_r1 = float(out.loc[out["race_id"] == "R1", "p_top3"].sum())
    sum_r2 = float(out.loc[out["race_id"] == "R2", "p_top3"].sum())

    assert abs(sum_r1 - 3.0) < 1e-9
    assert abs(sum_r2 - 1.2) < 1e-9
