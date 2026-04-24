import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Phase-1 history features without leakage.")
    parser.add_argument("--input", default=r"C:\TXT\dataset_top3_2021_2026_v1_clean.csv")
    parser.add_argument("--output", default=r"C:\TXT\dataset_top3_with_history_phase1.csv")
    parser.add_argument("--encoding", default="cp932")
    return parser.parse_args()


def to_numeric(df: pd.DataFrame, cols: list[str]) -> None:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    df = pd.read_csv(input_path, encoding=args.encoding, low_memory=False)

    required = ["horse_id", "race_date", "race_id_raw", "finish_position", "distance", "win"]
    missing_cols = [col for col in required if col not in df.columns]
    if missing_cols:
        raise SystemExit(f"Missing required columns: {missing_cols}")

    to_numeric(df, ["finish_position", "distance", "win"])
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    if df["race_date"].isna().any():
        raise SystemExit("race_date contains invalid values after datetime conversion.")

    sort_cols = ["horse_id", "race_date", "race_id_raw", "horse_no"]
    existing_sort_cols = [col for col in sort_cols if col in df.columns]
    df = df.sort_values(existing_sort_cols, ascending=True, kind="mergesort").reset_index(drop=True)

    horse_group = df.groupby("horse_id", sort=False, dropna=False)
    df["prev_finish_position"] = horse_group["finish_position"].shift(1)
    df["avg_finish_last3"] = horse_group["finish_position"].transform(
        lambda values: values.shift(1).rolling(window=3, min_periods=1).mean()
    )

    hd_group = df.groupby(["horse_id", "distance"], sort=False, dropna=False)
    prior_starts = hd_group.cumcount()
    prior_wins = hd_group["win"].cumsum() - df["win"]
    df["same_distance_win_rate"] = np.where(prior_starts > 0, prior_wins / prior_starts, np.nan)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding=args.encoding)

    feature_cols = ["prev_finish_position", "avg_finish_last3", "same_distance_win_rate"]
    print("=== done ===")
    print(f"input:  {input_path}")
    print(f"output: {output_path}")
    print("rows:", len(df))
    print("\nmissing_rate")
    for col in feature_cols:
        miss_rate = float(df[col].isna().mean())
        print(f"- {col}: {miss_rate:.6f}")

    sample_cols = ["race_date", "race_id_raw", "horse_id", "distance", "finish_position", "win"] + feature_cols
    print("\nsample_rows")
    print(df[sample_cols].head(10).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
