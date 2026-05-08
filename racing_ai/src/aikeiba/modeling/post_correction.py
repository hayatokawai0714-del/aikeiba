from __future__ import annotations

import numpy as np
import pandas as pd


def apply_post_correction(
    df: pd.DataFrame,
    *,
    race_col: str,
    prob_col: str,
    method: str,
    field_size_col: str | None = None,
    clip_max: float = 0.95,
) -> pd.DataFrame:
    out = df.copy()
    m = str(method or "none").strip().lower()
    if m == "none":
        out[prob_col] = np.clip(out[prob_col].to_numpy(dtype=float), 0.0, clip_max)
        return out

    sums = out.groupby(race_col)[prob_col].sum(min_count=1).rename("_sum")
    out = out.merge(sums, left_on=race_col, right_index=True, how="left")
    arr = out[prob_col].to_numpy(dtype=float)
    s = out["_sum"].to_numpy(dtype=float)
    scale = np.ones(len(out), dtype=float)
    valid = np.isfinite(s) & (s > 0)

    if m == "current_shrink_only":
        over = valid & (s > 3.0)
        scale[over] = 3.0 / s[over]
    elif m == "scale_to_3":
        scale[valid] = 3.0 / s[valid]
    elif m == "scale_to_expected_top3_clip":
        if field_size_col is None or field_size_col not in out.columns:
            raise ValueError("field_size_col required for scale_to_expected_top3_clip")
        fs = pd.to_numeric(out[field_size_col], errors="coerce").to_numpy(dtype=float)
        expected = np.minimum(3.0, np.where(np.isfinite(fs), fs, 3.0))
        expected = np.maximum(expected, 1.0)
        scale[valid] = expected[valid] / s[valid]
    else:
        raise ValueError(f"unknown post-correction method: {method}")

    out[prob_col] = np.clip(arr * scale, 0.0, clip_max)
    return out.drop(columns=["_sum"])
