from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from sklearn.isotonic import IsotonicRegression


@dataclass(frozen=True)
class Calibrator:
    method: str
    model: object

    def predict(self, p_raw: np.ndarray) -> np.ndarray:
        if self.method == "isotonic":
            p = self.model.predict(p_raw)
            return np.clip(p, 0.0, 1.0)
        raise ValueError(f"unknown calibrator method: {self.method}")


def fit_isotonic(p_raw: np.ndarray, y: np.ndarray) -> Calibrator:
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(p_raw, y)
    return Calibrator(method="isotonic", model=iso)
