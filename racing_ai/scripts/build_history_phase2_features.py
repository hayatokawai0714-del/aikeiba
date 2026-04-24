import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Phase-2 history features with strict no-leakage rules.")
    parser.add_argument("--input", default=r"C:\TXT\dataset_top3_with_history_phase1.csv")
    parser.add_argument("--population", default=r"C:\TXT\population_master_2021_2026_v1.csv")
    parser.add_argument("--output", default=r"C:\TXT\dataset_top3_with_history_phase2.csv")
    parser.add_argument("--encoding", default="cp932")
    return parser.parse_args()


def to_numeric(df: pd.DataFrame, cols: list[str]) -> None:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


def normalize_race_id_raw(series: pd.Series) -> pd.Series:
    return series.astype(str).str.split(".").str[0]


def ensure_margin_last3f_columns(base_df: pd.DataFrame, pop_path: Path, encoding: str) -> pd.DataFrame:
    if {"margin_time", "last3f_time"}.issubset(base_df.columns):
        return base_df

    if not pop_path.exists():
        raise SystemExit(
            "margin_time / last3f_time are missing in input and population file does not exist: "
            f"{pop_path}"
        )

    pop = pd.read_csv(pop_path, encoding=encoding, low_memory=False)
    required_pop = {"race_id_raw", "horse_no", "margin_time", "last3f_time"}
    missing = required_pop - set(pop.columns)
    if missing:
        raise SystemExit(f"population file missing columns: {sorted(missing)}")

    pop = pop[["race_id_raw", "horse_no", "margin_time", "last3f_time"]].copy()
    pop["race_id_raw"] = normalize_race_id_raw(pop["race_id_raw"])
    pop["horse_no"] = pd.to_numeric(pop["horse_no"], errors="coerce")
    pop = pop.dropna(subset=["horse_no"]).copy()
    pop["horse_no"] = pop["horse_no"].astype(int)

    merged = base_df.copy()
    merged["race_id_raw"] = normalize_race_id_raw(merged["race_id_raw"])
    merged["horse_no"] = pd.to_numeric(merged["horse_no"], errors="coerce")
    merged = merged.dropna(subset=["horse_no"]).copy()
    merged["horse_no"] = merged["horse_no"].astype(int)

    merged = merged.merge(pop, on=["race_id_raw", "horse_no"], how="left", suffixes=("", "_pop"))
    if "margin_time_pop" in merged.columns and "margin_time" not in merged.columns:
        merged = merged.rename(columns={"margin_time_pop": "margin_time"})
    if "last3f_time_pop" in merged.columns and "last3f_time" not in merged.columns:
        merged = merged.rename(columns={"last3f_time_pop": "last3f_time"})
    return merged


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    pop_path = Path(args.population)
    output_path = Path(args.output)

    df = pd.read_csv(input_path, encoding=args.encoding, low_memory=False)
    input_cols = set(df.columns)
    required_base = {"race_date", "race_id_raw", "horse_id", "horse_no"}
    missing_base = required_base - set(df.columns)
    if missing_base:
        raise SystemExit(f"Missing required columns in input: {sorted(missing_base)}")

    df = ensure_margin_last3f_columns(df, pop_path, args.encoding)

    req_after = {"margin_time", "last3f_time"}
    missing_after = req_after - set(df.columns)
    if missing_after:
        raise SystemExit(f"Required columns unavailable after join: {sorted(missing_after)}")

    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    if df["race_date"].isna().any():
        raise SystemExit("race_date has invalid values after datetime conversion.")

    to_numeric(df, ["horse_no", "margin_time", "last3f_time"])
    df = df.dropna(subset=["horse_no"]).copy()
    df["horse_no"] = df["horse_no"].astype(int)

    sort_cols = ["race_id_raw", "race_date", "horse_no"]
    df = df.sort_values(sort_cols, ascending=True, kind="mergesort").reset_index(drop=True)

    df["last3f_rank_raw"] = (
        df.groupby("race_id_raw", dropna=False)["last3f_time"]
        .rank(method="first", ascending=True)
    )

    horse_sort_cols = ["horse_id", "race_date", "race_id_raw", "horse_no"]
    df = df.sort_values(horse_sort_cols, ascending=True, kind="mergesort").reset_index(drop=True)

    horse_group = df.groupby("horse_id", sort=False, dropna=False)

    # Phase-2 features (all use past-only data with shift(1))
    df["prev_margin"] = horse_group["margin_time"].shift(1)
    df["avg_margin_last3"] = horse_group["margin_time"].transform(
        lambda values: values.shift(1).rolling(window=3, min_periods=1).mean()
    )
    df["prev_last3f_rank"] = horse_group["last3f_rank_raw"].shift(1)
    df["last3f_best_count"] = horse_group["last3f_rank_raw"].transform(
        lambda values: (values.eq(1).astype(float)).shift(1).rolling(window=3, min_periods=1).sum()
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    drop_cols = ["last3f_rank_raw"]
    if "margin_time" not in input_cols:
        drop_cols.append("margin_time")
    if "last3f_time" not in input_cols:
        drop_cols.append("last3f_time")
    out_df = df.drop(columns=[c for c in drop_cols if c in df.columns])
    out_df.to_csv(output_path, index=False, encoding=args.encoding)

    added_cols = ["prev_margin", "avg_margin_last3", "prev_last3f_rank", "last3f_best_count"]
    print("=== done ===")
    print(f"input:  {input_path}")
    print(f"output: {output_path}")
    print("rows:", len(out_df))
    print("\nmissing_rate")
    for col in added_cols:
        miss_rate = float(out_df[col].isna().mean())
        print(f"- {col}: {miss_rate:.6f}")

    sample_cols = ["race_date", "race_id_raw", "horse_id", "horse_no", "margin_time", "last3f_time"] + added_cols
    sample_cols = [c for c in sample_cols if c in out_df.columns]
    print("\nsample_rows")
    print(out_df[sample_cols].head(10).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
