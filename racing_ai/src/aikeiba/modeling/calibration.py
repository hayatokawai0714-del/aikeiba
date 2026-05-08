from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression


@dataclass(frozen=True)
class Calibrator:
    method: str
    model: object

    def predict(self, p_raw: np.ndarray) -> np.ndarray:
        if self.method == "isotonic":
            p = self.model.predict(p_raw)
            return np.clip(p, 0.0, 1.0)
        if self.method == "sigmoid":
            x = np.asarray(p_raw, dtype=float).reshape(-1, 1)
            p = self.model.predict_proba(x)[:, 1]
            return np.clip(p, 0.0, 1.0)
        if self.method == "none":
            return np.clip(np.asarray(p_raw, dtype=float), 0.0, 1.0)
        raise ValueError(f"unknown calibrator method: {self.method}")


def fit_isotonic(p_raw: np.ndarray, y: np.ndarray) -> Calibrator:
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(p_raw, y)
    return Calibrator(method="isotonic", model=iso)


def fit_sigmoid(p_raw: np.ndarray, y: np.ndarray) -> Calibrator:
    x = np.asarray(p_raw, dtype=float).reshape(-1, 1)
    yy = np.asarray(y, dtype=int)
    clf = LogisticRegression(solver="lbfgs", max_iter=1000)
    clf.fit(x, yy)
    return Calibrator(method="sigmoid", model=clf)


def fit_none() -> Calibrator:
    return Calibrator(method="none", model=None)
