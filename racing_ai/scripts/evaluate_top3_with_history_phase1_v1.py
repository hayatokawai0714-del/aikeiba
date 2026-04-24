import argparse
import re
from dataclasses import dataclass
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score


TRAIN_YEARS = {2021, 2022, 2023, 2024}
VALID_YEARS = {2025}
TEST_YEARS = {2026}


@dataclass
class Metrics:
    rows: int
    auc: float
    logloss: float
    brier: float
    accuracy: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate top3 model with Phase-1 history features.")
    parser.add_argument("--input", default=r"C:\TXT\dataset_top3_with_history_phase1.csv")
    parser.add_argument("--baseline-report", default=r"C:\TXT\top3_model_report_v1.txt")
    parser.add_argument("--pred-out", default=r"C:\TXT\top3_model_with_history_predictions_2026_v1.csv")
    parser.add_argument("--report-out", default=r"C:\TXT\top3_model_with_history_report_v1.txt")
    parser.add_argument("--encoding", default="cp932")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def calc_metrics(y_true: pd.Series, y_prob: np.ndarray) -> Metrics:
    y = y_true.astype(int).to_numpy()
    p = np.clip(np.asarray(y_prob, dtype=float), 1e-8, 1 - 1e-8)
    pred = (p >= 0.5).astype(int)
    auc = float("nan")
    if pd.Series(y).nunique(dropna=False) >= 2:
        auc = float(roc_auc_score(y, p))
    return Metrics(
        rows=int(len(y)),
        auc=auc,
        logloss=float(log_loss(y, p, labels=[0, 1])),
        brier=float(brier_score_loss(y, p)),
        accuracy=float(accuracy_score(y, pred)),
    )


def parse_baseline_test_raw(report_path: Path) -> Metrics | None:
    if not report_path.exists():
        return None
    text = report_path.read_text(encoding="utf-8", errors="ignore")
    pattern = re.compile(
        r"\[test_raw\]\s+rows=(?P<rows>\d+)\s+auc=(?P<auc>[-+0-9.]+)\s+"
        r"logloss=(?P<logloss>[-+0-9.]+)\s+brier=(?P<brier>[-+0-9.]+)\s+accuracy=(?P<accuracy>[-+0-9.]+)"
    )
    match = pattern.search(text)
    if not match:
        return None
    return Metrics(
        rows=int(match.group("rows")),
        auc=float(match.group("auc")),
        logloss=float(match.group("logloss")),
        brier=float(match.group("brier")),
        accuracy=float(match.group("accuracy")),
    )


def check_years(name: str, split_df: pd.DataFrame, expected: set[int]) -> str:
    years = sorted(split_df["year"].dropna().unique().tolist())
    mixed = set(years) - expected
    status = "OK" if len(mixed) == 0 else "NG"
    return f"{name}: years={years} expected={sorted(expected)} status={status}"


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    baseline_report_path = Path(args.baseline_report)
    pred_out_path = Path(args.pred_out)
    report_out_path = Path(args.report_out)

    df = pd.read_csv(input_path, encoding=args.encoding, low_memory=False)
    required_cols = [
        "race_date",
        "top3",
        "win_odds",
        "pop_rank",
        "distance",
        "field_size",
        "track_condition",
        "jockey_name",
        "prev_finish_position",
        "avg_finish_last3",
        "same_distance_win_rate",
    ]
    missing = [column for column in required_cols if column not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")

    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df[df["race_date"].notna()].copy()
    df["year"] = df["race_date"].dt.year

    for col in [
        "top3",
        "win_odds",
        "pop_rank",
        "distance",
        "field_size",
        "prev_finish_position",
        "avg_finish_last3",
        "same_distance_win_rate",
    ]:
        df[col] = to_float(df[col])

    before_drop = len(df)
    df = df[df["win_odds"].notna()].copy()
    dropped_win_odds = before_drop - len(df)

    train_df = df[df["year"].isin(TRAIN_YEARS)].copy()
    valid_df = df[df["year"].isin(VALID_YEARS)].copy()
    test_df = df[df["year"].isin(TEST_YEARS)].copy()

    if train_df.empty or valid_df.empty or test_df.empty:
        raise SystemExit(
            f"Empty split detected: train={len(train_df)} valid={len(valid_df)} test={len(test_df)}"
        )

    med_pop_rank = float(train_df["pop_rank"].median())
    med_distance = float(train_df["distance"].median())
    med_field_size = float(train_df["field_size"].median())

    for split in [train_df, valid_df, test_df]:
        split["pop_rank"] = split["pop_rank"].fillna(med_pop_rank)
        split["distance"] = split["distance"].fillna(med_distance)
        split["field_size"] = split["field_size"].fillna(med_field_size)
        split["track_condition"] = split["track_condition"].fillna("UNKNOWN").astype(str)
        split["jockey_name"] = split["jockey_name"].fillna("UNKNOWN").astype(str)
        split["log_win_odds"] = np.log(split["win_odds"].clip(lower=1e-6))

    features_num = [
        "log_win_odds",
        "distance",
        "field_size",
        "pop_rank",
        "prev_finish_position",
        "avg_finish_last3",
        "same_distance_win_rate",
    ]
    features_cat = ["track_condition", "jockey_name"]
    features = features_num + features_cat

    for split in [train_df, valid_df, test_df]:
        for col in features_cat:
            split[col] = split[col].astype("category")

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

    test_pred = model.predict_proba(x_test)[:, 1]
    test_metrics = calc_metrics(y_test, test_pred)
    baseline_metrics = parse_baseline_test_raw(baseline_report_path)

    test_df = test_df.copy()
    test_df["pred_top3"] = test_pred
    test_df["market_prob"] = 1.0 / test_df["win_odds"].replace(0, np.nan)
    test_df["value_gap"] = test_df["pred_top3"] - test_df["market_prob"]

    pred_cols = [
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
    pred_df = test_df.copy()
    pred_df["pred_top3_raw"] = pred_df["pred_top3"]
    pred_df["pred_top3_calibrated"] = pred_df["pred_top3"]
    pred_df = pred_df[pred_cols]

    report_lines: list[str] = []
    report_lines.append("top3 model with history phase1 report")
    report_lines.append("")
    report_lines.append(f"input={input_path}")
    report_lines.append(f"baseline_report={baseline_report_path}")
    report_lines.append(f"dropped_win_odds_missing={dropped_win_odds}")
    report_lines.append("")
    report_lines.append("split_check")
    report_lines.append(check_years("train", train_df, TRAIN_YEARS))
    report_lines.append(check_years("valid", valid_df, VALID_YEARS))
    report_lines.append(check_years("test", test_df, TEST_YEARS))
    report_lines.append(
        f"rows train={len(train_df)} valid={len(valid_df)} test={len(test_df)}"
    )
    report_lines.append("")
    report_lines.append("features")
    report_lines.append("numeric=" + ", ".join(features_num))
    report_lines.append("categorical=" + ", ".join(features_cat))
    report_lines.append("")
    report_lines.append("test_metrics_phase1")
    report_lines.append(
        f"AUC={test_metrics.auc:.6f} logloss={test_metrics.logloss:.6f} "
        f"Brier={test_metrics.brier:.6f} accuracy={test_metrics.accuracy:.6f}"
    )
    report_lines.append("")
    report_lines.append("baseline_comparison(test_raw)")
    if baseline_metrics is None:
        report_lines.append("情報不足: baseline test_raw metrics could not be parsed.")
    else:
        report_lines.append(
            f"baseline AUC={baseline_metrics.auc:.6f} logloss={baseline_metrics.logloss:.6f} "
            f"Brier={baseline_metrics.brier:.6f} accuracy={baseline_metrics.accuracy:.6f}"
        )
        report_lines.append(
            f"delta AUC={test_metrics.auc - baseline_metrics.auc:+.6f} "
            f"logloss={test_metrics.logloss - baseline_metrics.logloss:+.6f} "
            f"Brier={test_metrics.brier - baseline_metrics.brier:+.6f}"
        )
    report_lines.append("")
    report_lines.append("value_gap_summary(test_2026)")
    report_lines.append(f"rows={len(test_df)}")
    report_lines.append(f"pred_top3_mean={float(test_df['pred_top3'].mean()):.6f}")
    report_lines.append(f"market_prob_mean={float(test_df['market_prob'].mean()):.6f}")
    report_lines.append(f"value_gap_mean={float(test_df['value_gap'].mean()):.6f}")
    report_lines.append(f"value_gap_std={float(test_df['value_gap'].std(ddof=0)):.6f}")
    report_lines.append("value_gap_quantiles=" + ", ".join(
        [f"q{int(q*100)}={float(test_df['value_gap'].quantile(q)):.6f}" for q in [0.1, 0.25, 0.5, 0.75, 0.9]]
    ))

    pred_out_path.parent.mkdir(parents=True, exist_ok=True)
    pred_df.to_csv(pred_out_path, index=False, encoding=args.encoding)

    report_out_path.parent.mkdir(parents=True, exist_ok=True)
    report_out_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    print("=== done ===")
    print(f"predictions: {pred_out_path}")
    print(f"report: {report_out_path}")
    print(
        f"test AUC={test_metrics.auc:.6f} logloss={test_metrics.logloss:.6f} "
        f"Brier={test_metrics.brier:.6f} accuracy={test_metrics.accuracy:.6f}"
    )
    if baseline_metrics is not None:
        print(
            f"delta AUC={test_metrics.auc - baseline_metrics.auc:+.6f} "
            f"logloss={test_metrics.logloss - baseline_metrics.logloss:+.6f} "
            f"Brier={test_metrics.brier - baseline_metrics.brier:+.6f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
