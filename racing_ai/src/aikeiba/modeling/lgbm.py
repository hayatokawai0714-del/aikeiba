from __future__ import annotations

from dataclasses import dataclass

import lightgbm as lgb
import numpy as np


@dataclass(frozen=True)
class LgbmModel:
    booster: lgb.Booster
    feature_names: list[str]

    def predict(self, X) -> np.ndarray:
        return self.booster.predict(X[self.feature_names])


def train_binary_lgbm(
    *,
    X_train,
    y_train,
    X_valid,
    y_valid,
    seed: int = 42,
) -> LgbmModel:
    feature_names = list(X_train.columns)

    dtrain = lgb.Dataset(X_train, label=y_train, feature_name=feature_names, free_raw_data=False)
    dvalid = lgb.Dataset(X_valid, label=y_valid, feature_name=feature_names, free_raw_data=False)

    params = {
        "objective": "binary",
        "metric": ["binary_logloss", "auc"],
        "learning_rate": 0.05,
        "num_leaves": 63,
        "min_data_in_leaf": 50,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 1,
        "seed": seed,
        "verbose": -1,
    }

    booster = lgb.train(
        params,
        dtrain,
        num_boost_round=4000,
        valid_sets=[dvalid],
        callbacks=[lgb.early_stopping(stopping_rounds=200, verbose=False)],
    )

    return LgbmModel(booster=booster, feature_names=feature_names)
