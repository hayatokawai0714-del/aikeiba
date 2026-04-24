import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score


TRAIN_YEARS = {2021, 2022, 2023, 2024}
VALID_YEARS = {2025}
TEST_YEARS = {2026}


@dataclass
class EvalResult:
    name: str
    rows: int
    auc: float
    logloss: float
    brier: float
    accuracy: float
    actual_top3_rate: float
    pred_mean: float
    pred_minus_actual: float


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Train minimal top3 LightGBM model and evaluate probability quality.")
    ap.add_argument("--input", default=r"C:\TXT\dataset_top3_2021_2026_v1_clean.csv")
    ap.add_argument("--pred-out", default=r"C:\TXT\top3_model_predictions_2026_v1.csv")
    ap.add_argument("--model-out", default=r"C:\TXT\top3_model_lgbm_v1.txt")
    ap.add_argument("--report-out", default=r"C:\TXT\top3_model_report_v1.txt")
    ap.add_argument("--encoding", default="cp932")
    ap.add_argument("--seed", type=int, default=42)
    return ap.parse_args()


def safe_auc(y_true: pd.Series, y_prob: np.ndarray) -> float:
    unique = pd.Series(y_true).nunique(dropna=False)
    if unique < 2:
        return float("nan")
    return float(roc_auc_score(y_true, y_prob))


def evaluate(name: str, y_true: pd.Series, y_prob: np.ndarray) -> EvalResult:
    y_true_arr = y_true.astype(int).to_numpy()
    y_prob_arr = np.clip(np.asarray(y_prob, dtype=float), 1e-8, 1 - 1e-8)
    pred_label = (y_prob_arr >= 0.5).astype(int)

    actual_rate = float(np.mean(y_true_arr)) if len(y_true_arr) else 0.0
    pred_mean = float(np.mean(y_prob_arr)) if len(y_prob_arr) else 0.0

    return EvalResult(
        name=name,
        rows=int(len(y_true_arr)),
        auc=safe_auc(y_true_arr, y_prob_arr),
        logloss=float(log_loss(y_true_arr, y_prob_arr, labels=[0, 1])),
        brier=float(brier_score_loss(y_true_arr, y_prob_arr)),
        accuracy=float(accuracy_score(y_true_arr, pred_label)),
        actual_top3_rate=actual_rate,
        pred_mean=pred_mean,
        pred_minus_actual=pred_mean - actual_rate,
    )


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def format_eval(result: EvalResult) -> str:
    return (
        f"[{result.name}] rows={result.rows} "
        f"auc={result.auc:.6f} logloss={result.logloss:.6f} "
        f"brier={result.brier:.6f} accuracy={result.accuracy:.6f} "
        f"actual_top3_rate={result.actual_top3_rate:.6f} "
        f"pred_mean={result.pred_mean:.6f} "
        f"pred_minus_actual={result.pred_minus_actual:+.6f}"
    )


def main() -> int:
    args = parse_args()

    input_path = Path(args.input)
    pred_out_path = Path(args.pred_out)
    model_out_path = Path(args.model_out)
    report_out_path = Path(args.report_out)

    df = pd.read_csv(input_path, encoding=args.encoding, low_memory=False)

    required_cols = [
        "race_date",
        "race_id_raw",
        "horse_no",
        "horse_name",
        "jockey_name",
        "top3",
        "win_odds",
        "pop_rank",
        "distance",
        "field_size",
        "track_condition",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")

    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df[df["race_date"].notna()].copy()
    df["year"] = df["race_date"].dt.year

    for col in ["win_odds", "pop_rank", "distance", "field_size", "top3"]:
        df[col] = to_float(df[col])

    before_drop_win_odds = len(df)
    df = df[df["win_odds"].notna()].copy()
    dropped_win_odds = before_drop_win_odds - len(df)

    train_mask = df["year"].isin(TRAIN_YEARS)
    valid_mask = df["year"].isin(VALID_YEARS)
    test_mask = df["year"].isin(TEST_YEARS)

    train_df = df[train_mask].copy()
    valid_df = df[valid_mask].copy()
    test_df = df[test_mask].copy()

    if train_df.empty or valid_df.empty or test_df.empty:
        raise SystemExit(
            "Split produced empty subset. "
            f"train={len(train_df)} valid={len(valid_df)} test={len(test_df)}"
        )

    med_pop_rank = float(train_df["pop_rank"].median())
    med_distance = float(train_df["distance"].median())
    med_field_size = float(train_df["field_size"].median())

    for split_df in [train_df, valid_df, test_df]:
        split_df["pop_rank"] = split_df["pop_rank"].fillna(med_pop_rank)
        split_df["distance"] = split_df["distance"].fillna(med_distance)
        split_df["field_size"] = split_df["field_size"].fillna(med_field_size)
        split_df["track_condition"] = split_df["track_condition"].fillna("UNKNOWN").astype(str)
        split_df["jockey_name"] = split_df["jockey_name"].fillna("UNKNOWN").astype(str)

        split_df["log_win_odds"] = np.log(split_df["win_odds"].clip(lower=1e-6))
        split_df["pop_rank_rate"] = split_df["pop_rank"] / split_df["field_size"].replace(0, np.nan)

    med_pop_rank_rate = float(train_df["pop_rank_rate"].median())
    for split_df in [train_df, valid_df, test_df]:
        split_df["pop_rank_rate"] = split_df["pop_rank_rate"].fillna(med_pop_rank_rate)

    features_num = ["log_win_odds", "distance", "field_size", "pop_rank", "pop_rank_rate"]
    features_cat = ["track_condition", "jockey_name"]
    features = features_num + features_cat

    for split_df in [train_df, valid_df, test_df]:
        for col in features_cat:
            split_df[col] = split_df[col].astype("category")

    x_train = train_df[features].copy()
    y_train = train_df["top3"].astype(int)
    x_valid = valid_df[features].copy()
    y_valid = valid_df["top3"].astype(int)
    x_test = test_df[features].copy()
    y_test = test_df["top3"].astype(int)

    model = lgb.LGBMClassifier(
        objective="binary",
        n_estimators=4000,
        learning_rate=0.03,
        num_leaves=31,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=args.seed,
        n_jobs=-1,
    )

    model.fit(
        x_train,
        y_train,
        eval_set=[(x_valid, y_valid)],
        eval_metric=["auc", "binary_logloss"],
        callbacks=[lgb.early_stopping(200), lgb.log_evaluation(200)],
        categorical_feature=features_cat,
    )

    valid_raw = model.predict_proba(x_valid)[:, 1]
    test_raw = model.predict_proba(x_test)[:, 1]

    valid_eval_raw = evaluate("valid_raw", y_valid, valid_raw)
    test_eval_raw = evaluate("test_raw", y_test, test_raw)

    # Calibration on valid predictions only.
    platt = LogisticRegression(solver="lbfgs")
    platt.fit(valid_raw.reshape(-1, 1), y_valid)
    valid_platt = platt.predict_proba(valid_raw.reshape(-1, 1))[:, 1]
    test_platt = platt.predict_proba(test_raw.reshape(-1, 1))[:, 1]
    valid_eval_platt = evaluate("valid_platt", y_valid, valid_platt)
    test_eval_platt = evaluate("test_platt", y_test, test_platt)

    isotonic = IsotonicRegression(out_of_bounds="clip")
    isotonic.fit(valid_raw, y_valid)
    valid_isotonic = isotonic.predict(valid_raw)
    test_isotonic = isotonic.predict(test_raw)
    valid_eval_isotonic = evaluate("valid_isotonic", y_valid, valid_isotonic)
    test_eval_isotonic = evaluate("test_isotonic", y_test, test_isotonic)

    calibrator_name = "platt"
    test_calibrated = test_platt
    chosen_valid_eval = valid_eval_platt
    chosen_test_eval = test_eval_platt

    if valid_eval_isotonic.logloss < valid_eval_platt.logloss:
        calibrator_name = "isotonic"
        test_calibrated = test_isotonic
        chosen_valid_eval = valid_eval_isotonic
        chosen_test_eval = test_eval_isotonic

    prediction_cols = [
        "race_date",
        "race_id_raw",
        "horse_no",
        "horse_name",
        "jockey_name",
        "top3",
        "win_odds",
        "pop_rank",
    ]
    pred_df = test_df[prediction_cols].copy()
    pred_df["pred_top3_raw"] = test_raw
    pred_df["pred_top3_calibrated"] = test_calibrated
    pred_df = pred_df[
        [
            "race_date",
            "race_id_raw",
            "horse_no",
            "horse_name",
            "jockey_name",
            "top3",
            "pred_top3_raw",
            "pred_top3_calibrated",
            "win_odds",
            "pop_rank",
        ]
    ]

    pred_out_path.parent.mkdir(parents=True, exist_ok=True)
    model_out_path.parent.mkdir(parents=True, exist_ok=True)
    report_out_path.parent.mkdir(parents=True, exist_ok=True)

    pred_df.to_csv(pred_out_path, index=False, encoding=args.encoding)
    model.booster_.save_model(str(model_out_path))

    lines: list[str] = []
    lines.append("top3 LightGBM minimal model report")
    lines.append("")
    lines.append(f"input={input_path}")
    lines.append(f"dropped_win_odds_missing={dropped_win_odds}")
    lines.append(f"split_rows train={len(train_df)} valid={len(valid_df)} test={len(test_df)}")
    lines.append("")
    lines.append("features_numeric=" + ", ".join(features_num))
    lines.append("features_categorical=" + ", ".join(features_cat))
    lines.append("")
    lines.append("imputation(train medians)")
    lines.append(f"- pop_rank median={med_pop_rank:.6f}")
    lines.append(f"- distance median={med_distance:.6f}")
    lines.append(f"- field_size median={med_field_size:.6f}")
    lines.append(f"- pop_rank_rate median={med_pop_rank_rate:.6f}")
    lines.append("- track_condition missing -> UNKNOWN")
    lines.append("- jockey_name missing -> UNKNOWN")
    lines.append("")
    lines.append("raw metrics")
    lines.append(format_eval(valid_eval_raw))
    lines.append(format_eval(test_eval_raw))
    lines.append("")
    lines.append("calibration metrics")
    lines.append(format_eval(valid_eval_platt))
    lines.append(format_eval(test_eval_platt))
    lines.append(format_eval(valid_eval_isotonic))
    lines.append(format_eval(test_eval_isotonic))
    lines.append("")
    lines.append(f"chosen_calibrator={calibrator_name} (selected by valid logloss)")
    lines.append(format_eval(chosen_valid_eval))
    lines.append(format_eval(chosen_test_eval))

    report_out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("=== done ===")
    print(f"input: {input_path}")
    print(f"predictions: {pred_out_path}")
    print(f"model: {model_out_path}")
    print(f"report: {report_out_path}")
    print("")
    print("=== split rows ===")
    print(f"train={len(train_df)} valid={len(valid_df)} test={len(test_df)}")
    print("")
    print("=== raw ===")
    print(format_eval(valid_eval_raw))
    print(format_eval(test_eval_raw))
    print("")
    print("=== calibration ===")
    print(format_eval(valid_eval_platt))
    print(format_eval(test_eval_platt))
    print(format_eval(valid_eval_isotonic))
    print(format_eval(test_eval_isotonic))
    print("")
    print(f"chosen_calibrator={calibrator_name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
