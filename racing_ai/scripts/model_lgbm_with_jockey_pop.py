from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class EvalResult:
    name: str
    test_rows: int
    positives: int
    accuracy: float
    auc: float


def _auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    # Avoid hard dependency on sklearn for AUC calculation.
    # Computes ROC AUC via rank statistic (equivalent to Mann–Whitney U).
    y_true = y_true.astype(int)
    pos = y_true == 1
    neg = ~pos
    n_pos = int(pos.sum())
    n_neg = int(neg.sum())
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    ranks = pd.Series(y_score).rank(method="average").to_numpy()
    sum_ranks_pos = float(ranks[pos].sum())
    auc = (sum_ranks_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)
    return float(auc)


def _accuracy(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float((y_true == y_pred).mean())


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize column names expected in this pipeline.
    required = [
        "race_id_norm",
        "race_date",
        "horse_no",
        "単勝オッズ",
        "距離",
        "人気",
        "騎手",
        "target_finish",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"missing required columns: {missing}")

    out = df[required].copy()
    out["race_date"] = pd.to_datetime(out["race_date"], errors="coerce")
    out["horse_no"] = pd.to_numeric(out["horse_no"], errors="coerce").astype("Int64")
    out["単勝オッズ"] = pd.to_numeric(out["単勝オッズ"], errors="coerce")
    out["距離"] = pd.to_numeric(out["距離"], errors="coerce").astype("Int64")
    out["人気"] = pd.to_numeric(out["人気"], errors="coerce").astype("Int64")
    out["target_finish"] = pd.to_numeric(out["target_finish"], errors="coerce").astype("Int64")
    out["騎手"] = out["騎手"].astype("string")
    return out


def _split_train_test(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    year = df["race_date"].dt.year
    train = df[year == 2025].copy()
    test = df[year == 2026].copy()
    return train, test


def _apply_missing_rules(train: pd.DataFrame, test: pd.DataFrame, *, drop_odds_missing: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    # jockey: fill UNKNOWN
    train["騎手"] = train["騎手"].fillna("UNKNOWN").replace("", "UNKNOWN")
    test["騎手"] = test["騎手"].fillna("UNKNOWN").replace("", "UNKNOWN")

    # pop_rank: median impute from train
    pop_median = int(np.nanmedian(train["人気"].astype("float").to_numpy())) if train["人気"].notna().any() else 0
    train["人気"] = train["人気"].fillna(pop_median)
    test["人気"] = test["人気"].fillna(pop_median)

    if drop_odds_missing:
        train = train[train["単勝オッズ"].notna()].copy()
        test = test[test["単勝オッズ"].notna()].copy()
    else:
        odds_median = float(np.nanmedian(train["単勝オッズ"].to_numpy())) if train["単勝オッズ"].notna().any() else 0.0
        train["単勝オッズ"] = train["単勝オッズ"].fillna(odds_median)
        test["単勝オッズ"] = test["単勝オッズ"].fillna(odds_median)

    # distance: median impute from train (distance can be missing in 2026)
    dist_median = int(np.nanmedian(train["距離"].astype("float").to_numpy())) if train["距離"].notna().any() else 0
    train["距離"] = train["距離"].fillna(dist_median)
    test["距離"] = test["距離"].fillna(dist_median)

    # finish must exist for target
    train = train[train["target_finish"].notna()].copy()
    test = test[test["target_finish"].notna()].copy()
    return train, test


def _eval_binary(y_true: np.ndarray, y_score: np.ndarray, threshold: float) -> tuple[float, float]:
    y_pred = (y_score >= threshold).astype(int)
    return _accuracy(y_true, y_pred), _auc(y_true, y_score)


def _fit_lightgbm(train: pd.DataFrame, test: pd.DataFrame, *, seed: int) -> tuple[EvalResult, pd.Series]:
    try:
        import lightgbm as lgb  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"lightgbm is not available ({e}). Install with: pip install lightgbm") from e

    X_train = train[["単勝オッズ", "距離", "人気", "騎手"]].copy()
    X_test = test[["単勝オッズ", "距離", "人気", "騎手"]].copy()

    # Ensure numeric columns are plain floats/ints (avoid pandas nullable dtypes).
    for c in ["単勝オッズ", "距離", "人気"]:
        X_train[c] = pd.to_numeric(X_train[c], errors="coerce").astype(float)
        X_test[c] = pd.to_numeric(X_test[c], errors="coerce").astype(float)

    # categorical handling
    X_train["騎手"] = X_train["騎手"].astype("category")
    X_test["騎手"] = X_test["騎手"].astype("category")

    y_train = (train["target_finish"].astype(int) == 1).astype(int).to_numpy()
    y_test = (test["target_finish"].astype(int) == 1).astype(int).to_numpy()

    clf = lgb.LGBMClassifier(
        n_estimators=1200,
        learning_rate=0.03,
        num_leaves=63,
        min_data_in_leaf=50,
        subsample=0.9,
        colsample_bytree=0.9,
        reg_lambda=1.0,
        class_weight="balanced",
        random_state=seed,
    )
    clf.fit(X_train, y_train, categorical_feature=["騎手"])
    prob = clf.predict_proba(X_test)[:, 1]
    acc, auc = _eval_binary(y_test, prob, threshold=0.5)

    imp = pd.Series(clf.feature_importances_, index=X_train.columns).sort_values(ascending=False)
    res = EvalResult(
        name="lightgbm(win)",
        test_rows=int(len(test)),
        positives=int(y_test.sum()),
        accuracy=float(acc),
        auc=float(auc),
    )
    return res, imp


def _fit_baseline_logreg(train: pd.DataFrame, test: pd.DataFrame) -> EvalResult:
    from sklearn.compose import ColumnTransformer  # type: ignore
    from sklearn.linear_model import LogisticRegression  # type: ignore
    from sklearn.pipeline import Pipeline  # type: ignore
    from sklearn.preprocessing import OneHotEncoder  # type: ignore

    X_train = train[["単勝オッズ", "距離"]].copy()
    X_test = test[["単勝オッズ", "距離"]].copy()
    # Convert pandas nullable dtypes to plain float to avoid pandas.NA issues in sklearn.
    X_train["距離"] = pd.to_numeric(X_train["距離"], errors="coerce").astype(float)
    X_test["距離"] = pd.to_numeric(X_test["距離"], errors="coerce").astype(float)
    X_train["単勝オッズ"] = pd.to_numeric(X_train["単勝オッズ"], errors="coerce").astype(float)
    X_test["単勝オッズ"] = pd.to_numeric(X_test["単勝オッズ"], errors="coerce").astype(float)
    y_train = (train["target_finish"].astype(int) == 1).astype(int).to_numpy()
    y_test = (test["target_finish"].astype(int) == 1).astype(int).to_numpy()

    preprocess = ColumnTransformer(
        [("num", "passthrough", ["単勝オッズ", "距離"])],
        remainder="drop",
    )

    model = Pipeline(
        steps=[
            ("preprocess", preprocess),
            ("clf", LogisticRegression(max_iter=2000, n_jobs=1)),
        ]
    )
    model.fit(X_train, y_train)
    prob = model.predict_proba(X_test)[:, 1]
    acc, auc = _eval_binary(y_test, prob, threshold=0.5)
    return EvalResult(
        name="baseline_logreg(win,odds+distance)",
        test_rows=int(len(test)),
        positives=int(y_test.sum()),
        accuracy=float(acc),
        auc=float(auc),
    )


def _fit_logreg_with_pop_jockey(train: pd.DataFrame, test: pd.DataFrame) -> EvalResult:
    from sklearn.compose import ColumnTransformer  # type: ignore
    from sklearn.linear_model import LogisticRegression  # type: ignore
    from sklearn.pipeline import Pipeline  # type: ignore
    from sklearn.preprocessing import OneHotEncoder  # type: ignore

    X_train = train[["単勝オッズ", "距離", "人気", "騎手"]].copy()
    X_test = test[["単勝オッズ", "距離", "人気", "騎手"]].copy()

    for c in ["単勝オッズ", "距離", "人気"]:
        X_train[c] = pd.to_numeric(X_train[c], errors="coerce").astype(float)
        X_test[c] = pd.to_numeric(X_test[c], errors="coerce").astype(float)
    X_train["騎手"] = X_train["騎手"].fillna("UNKNOWN").astype(str)
    X_test["騎手"] = X_test["騎手"].fillna("UNKNOWN").astype(str)

    y_train = (train["target_finish"].astype(int) == 1).astype(int).to_numpy()
    y_test = (test["target_finish"].astype(int) == 1).astype(int).to_numpy()

    preprocess = ColumnTransformer(
        [
            ("num", "passthrough", ["単勝オッズ", "距離", "人気"]),
            ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=5), ["騎手"]),
        ],
        remainder="drop",
        sparse_threshold=0.3,
    )

    model = Pipeline(
        steps=[
            ("preprocess", preprocess),
            ("clf", LogisticRegression(max_iter=4000, solver="saga")),
        ]
    )
    model.fit(X_train, y_train)
    prob = model.predict_proba(X_test)[:, 1]
    acc, auc = _eval_binary(y_test, prob, threshold=0.5)
    return EvalResult(
        name="logreg(win,odds+distance+pop+onehot_jockey)",
        test_rows=int(len(test)),
        positives=int(y_test.sum()),
        accuracy=float(acc),
        auc=float(auc),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Train a stronger model using pop_rank + jockey categorical.")
    ap.add_argument("--input", required=True, help=r"e.g. C:\TXT\population_all_2025_2026.csv")
    ap.add_argument("--encoding", default="cp932")
    ap.add_argument("--drop-odds-missing", action="store_true", default=False)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    df = pd.read_csv(Path(args.input), encoding=args.encoding, dtype=str)
    df = _prep(df)

    train, test = _split_train_test(df)
    if len(train) == 0 or len(test) == 0:
        raise RuntimeError("train/test split resulted in empty set; check race_date parsing")

    train, test = _apply_missing_rules(train, test, drop_odds_missing=args.drop_odds_missing)

    print(f"[data] train_rows={len(train)} test_rows={len(test)}")
    print(f"[data] test_positive(win)={int((test['target_finish'].astype(int)==1).sum())}")

    base = _fit_baseline_logreg(train, test)
    print(f"\n[{base.name}] accuracy={base.accuracy:.6f} auc={base.auc:.6f} test_rows={base.test_rows}")

    ext = _fit_logreg_with_pop_jockey(train, test)
    print(f"\n[{ext.name}] accuracy={ext.accuracy:.6f} auc={ext.auc:.6f} test_rows={ext.test_rows}")
    print(f"[delta vs baseline] auc={ext.auc - base.auc:+.6f} accuracy={ext.accuracy - base.accuracy:+.6f}")

    try:
        res, imp = _fit_lightgbm(train, test, seed=args.seed)
        print(f"\n[{res.name}] accuracy={res.accuracy:.6f} auc={res.auc:.6f} test_rows={res.test_rows}")
        print(f"[delta] auc={res.auc - base.auc:+.6f} accuracy={res.accuracy - base.accuracy:+.6f}")
        print("\n[feature_importance]")
        print(imp.to_string())
    except RuntimeError as e:
        print(f"\n[lightgbm] SKIP: {e}")
        print("[lightgbm] Falling back to baseline only.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
