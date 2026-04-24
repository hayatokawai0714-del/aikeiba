import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke-test model on population_all_2025_2026.csv")
    ap.add_argument("--input", required=True, help=r"e.g. C:\TXT\population_all_2025_2026.csv")
    ap.add_argument("--output", required=True, help=r"e.g. C:\TXT\population_model_input.csv")
    ap.add_argument("--encoding", default="cp932")
    ap.add_argument("--missing-drop-threshold", type=float, default=0.20, help="Drop columns with missing rate >= this")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(str(in_path))

    df = pd.read_csv(in_path, encoding=args.encoding)

    required = ["race_id_norm", "race_date", "horse_no", "target_finish"]
    for c in required:
        if c not in df.columns:
            raise RuntimeError(f"Missing required column: {c}")

    # Recompute missing rates
    missing_rate = df.isna().mean().sort_values(ascending=False)
    print("[smoke] missing_rate (top 20):")
    print((missing_rate.head(20) * 100).round(3).to_string())

    # Drop high-missing columns (excluding required columns)
    drop_cols = [
        c for c, r in missing_rate.items() if (r >= args.missing_drop_threshold and c not in required)
    ]
    kept_cols = [c for c in df.columns if c not in drop_cols]
    print("\n[smoke] dropped_cols (missing>=%.1f%%): %d" % (args.missing_drop_threshold * 100, len(drop_cols)))
    if drop_cols:
        print(", ".join(drop_cols))

    df = df[kept_cols].copy()

    # Basic typing
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df["horse_no"] = pd.to_numeric(df["horse_no"], errors="coerce").astype("Int64")
    df["target_finish"] = pd.to_numeric(df["target_finish"], errors="coerce").astype("Int64")

    # Filter invalid rows for minimal run
    before = len(df)
    df = df.dropna(subset=["race_id_norm", "race_date", "horse_no", "target_finish"])
    after = len(df)
    print(f"\n[smoke] rows_valid_required: {after}/{before} (dropped {before-after})")

    # Targets
    df["win"] = (df["target_finish"] == 1).astype(int)
    df["top3"] = (df["target_finish"] <= 3).astype(int)

    # Choose minimal features among candidates that survived dropping
    feature_candidates = ["単勝オッズ", "距離", "馬場状態"]
    features = [c for c in feature_candidates if c in df.columns]
    # 馬場状態 is categorical; include only if low-missing and not too many uniques
    if "馬場状態" in features:
        nunique = df["馬場状態"].nunique(dropna=True)
        miss = df["馬場状態"].isna().mean()
        if miss >= args.missing_drop_threshold or nunique > 20:
            features.remove("馬場状態")

    print("\n[smoke] using_features =", ", ".join(features) if features else "(none)")
    if not features:
        raise RuntimeError("No usable features left; cannot run model.")

    # Build model input frame (save for inspection)
    out_cols = required + features + ["win", "top3"]
    model_df = df[out_cols].copy()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    model_df.to_csv(out_path, index=False, encoding=args.encoding)
    print("[smoke] wrote model_input =", str(out_path))

    # Split train/test by year
    train = model_df[model_df["race_date"].dt.year == 2025].copy()
    test = model_df[model_df["race_date"].dt.year == 2026].copy()
    print(f"\n[smoke] split: train_rows={len(train)} test_rows={len(test)}")

    # If some rows have missing features, impute simple median (numeric) / mode (categorical)
    X_train = train[features].copy()
    X_test = test[features].copy()

    # Encode categorical if any (only 馬場状態)
    if "馬場状態" in features:
        X_train = pd.get_dummies(X_train, columns=["馬場状態"], dummy_na=True)
        X_test = pd.get_dummies(X_test, columns=["馬場状態"], dummy_na=True)
        X_test = X_test.reindex(columns=X_train.columns, fill_value=0)

    # Impute numeric
    for c in X_train.columns:
        if X_train[c].dtype.kind in "biufc":
            med = np.nanmedian(X_train[c].to_numpy(dtype=float))
            X_train[c] = X_train[c].astype(float).fillna(med)
            X_test[c] = X_test[c].astype(float).fillna(med)

    y_train = train["win"].astype(int).to_numpy()
    y_test = test["win"].astype(int).to_numpy()

    # Model: logistic regression (smoke test)
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score, roc_auc_score

    clf = LogisticRegression(max_iter=2000, n_jobs=None)
    clf.fit(X_train, y_train)

    proba = clf.predict_proba(X_test)[:, 1]
    pred = (proba >= 0.5).astype(int)
    acc = accuracy_score(y_test, pred)
    try:
        auc = roc_auc_score(y_test, proba)
    except ValueError:
        auc = float("nan")

    print("\n[smoke] evaluation (win):")
    print("  test_rows =", len(y_test))
    print("  positives =", int(y_test.sum()))
    print("  accuracy =", round(float(acc), 6))
    print("  auc =", round(float(auc), 6) if not np.isnan(auc) else "nan")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

