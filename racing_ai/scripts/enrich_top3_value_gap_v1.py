import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Enrich value_gap with race-relative temporary value score features.")
    ap.add_argument("--input", default=r"C:\TXT\top3_value_gap_detail_2026_v1.csv")
    ap.add_argument("--output", default=r"C:\TXT\top3_value_gap_enriched_2026_v1.csv")
    ap.add_argument("--report", default=r"C:\TXT\top3_value_gap_enriched_report_v1.txt")
    ap.add_argument("--encoding", default="cp932")
    return ap.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def enrich_by_race(df: pd.DataFrame) -> pd.DataFrame:
    group = df.groupby("race_id_raw", dropna=False)

    df["value_gap_rank"] = group["value_gap"].rank(method="first", ascending=False).astype(int)

    gap_mean = group["value_gap"].transform("mean")
    gap_std = group["value_gap"].transform(lambda x: x.std(ddof=0))
    gap_std = gap_std.fillna(0.0)

    df["gap_std"] = gap_std
    df["value_gap_z"] = np.where(gap_std > 0, (df["value_gap"] - gap_mean) / gap_std, 0.0)

    race_max = group["value_gap"].transform("max")
    df["gap_top_diff"] = race_max - df["value_gap"]

    return df


def rank_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("value_gap_rank", dropna=False)
        .agg(
            count=("top3", "size"),
            actual_top3_rate=("top3", "mean"),
            avg_value_gap=("value_gap", "mean"),
            avg_pred_top3=("pred_top3_raw", "mean"),
        )
        .reset_index()
        .sort_values("value_gap_rank")
    )
    return summary


def zbin_summary(df: pd.DataFrame) -> pd.DataFrame:
    z_bins = [-np.inf, -1.0, -0.5, 0.0, 0.5, 1.0, np.inf]
    z_labels = ["z<-1", "-1<=z<-0.5", "-0.5<=z<0", "0<=z<0.5", "0.5<=z<1", "1<=z"]
    z_cat = pd.cut(df["value_gap_z"], bins=z_bins, labels=z_labels, right=False, include_lowest=True)

    summary = (
        df.assign(value_gap_z_bin=z_cat)
        .groupby("value_gap_z_bin", dropna=False, observed=False)
        .agg(
            count=("top3", "size"),
            actual_top3_rate=("top3", "mean"),
            avg_value_gap_z=("value_gap_z", "mean"),
            avg_value_gap=("value_gap", "mean"),
        )
        .reset_index()
    )
    return summary


def gap_std_distribution(df: pd.DataFrame) -> pd.Series:
    race_std = df.groupby("race_id_raw", dropna=False)["gap_std"].first()
    return race_std.describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99])


def race_buyability_summary(df: pd.DataFrame) -> pd.DataFrame:
    race_df = (
        df.sort_values(["race_id_raw", "value_gap_rank"])
        .groupby("race_id_raw", dropna=False)
        .agg(
            gap_std=("gap_std", "first"),
            race_size=("horse_no", "size"),
            rank1_top3_hit=("top3", "first"),
            rank1_value_gap=("value_gap", "first"),
        )
        .reset_index()
    )

    q33 = float(race_df["gap_std"].quantile(0.33))
    q66 = float(race_df["gap_std"].quantile(0.66))

    race_df["gap_std_tier"] = "high_var"
    race_df.loc[race_df["gap_std"] <= q33, "gap_std_tier"] = "low_var"
    race_df.loc[(race_df["gap_std"] > q33) & (race_df["gap_std"] <= q66), "gap_std_tier"] = "mid_var"

    summary = (
        race_df.groupby("gap_std_tier", dropna=False)
        .agg(
            races=("race_id_raw", "size"),
            avg_gap_std=("gap_std", "mean"),
            rank1_top3_hit_rate=("rank1_top3_hit", "mean"),
            avg_rank1_value_gap=("rank1_value_gap", "mean"),
        )
        .reset_index()
    )

    order = ["high_var", "mid_var", "low_var"]
    summary["gap_std_tier"] = pd.Categorical(summary["gap_std_tier"], categories=order, ordered=True)
    summary = summary.sort_values("gap_std_tier").reset_index(drop=True)

    return summary


def main() -> int:
    args = parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    report_path = Path(args.report)

    df = pd.read_csv(in_path, encoding=args.encoding, low_memory=False)

    required_cols = ["race_id_raw", "value_gap", "top3", "pred_top3_raw", "win_odds"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")

    df["value_gap"] = to_float(df["value_gap"])
    df["top3"] = to_float(df["top3"]).fillna(0).astype(int)
    df["pred_top3_raw"] = to_float(df["pred_top3_raw"])
    df["win_odds"] = to_float(df["win_odds"])

    before = len(df)
    df = df[df["value_gap"].notna()].copy()
    dropped = before - len(df)

    if df.empty:
        raise SystemExit("No rows after filtering value_gap non-null")

    df = enrich_by_race(df)

    sort_cols = [c for c in ["race_date", "race_id_raw", "value_gap_rank", "horse_no"] if c in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    rank_stats = rank_summary(df)
    z_stats = zbin_summary(df)
    std_dist = gap_std_distribution(df)
    buyability = race_buyability_summary(df)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(out_path, index=False, encoding=args.encoding)

    lines: list[str] = []
    lines.append("top3 value_gap enriched analysis report (2026)")
    lines.append("")
    lines.append(f"input={in_path}")
    lines.append(f"output={out_path}")
    lines.append(f"rows_input={before}")
    lines.append(f"rows_output={len(df)}")
    lines.append(f"dropped_missing_value_gap={dropped}")
    lines.append("")
    lines.append("added columns")
    lines.append("- value_gap_rank: race内 value_gap 降順順位(1=最高)")
    lines.append("- value_gap_z: race内標準化(value_gap - mean) / std")
    lines.append("- gap_top_diff: race内max(value_gap)との差")
    lines.append("- gap_std: race内 value_gap 標準偏差")
    lines.append("")
    lines.append("(1) value_gap_rank別 top3率")
    lines.extend(rank_stats.to_string(index=False).splitlines())
    lines.append("")
    lines.append("(2) value_gap_z別 top3率")
    lines.append("《推測》z区分は [z<-1, -1<=z<-0.5, -0.5<=z<0, 0<=z<0.5, 0.5<=z<1, 1<=z] を採用")
    lines.extend(z_stats.to_string(index=False).splitlines())
    lines.append("")
    lines.append("(3) raceごとの gap_std 分布")
    for idx, val in std_dist.items():
        lines.append(f"- {idx}: {float(val):.8f}")
    lines.append("")
    lines.append("上位馬の精度")
    top1 = rank_stats.loc[rank_stats["value_gap_rank"] == 1]
    top2 = rank_stats.loc[rank_stats["value_gap_rank"] == 2]
    top3 = rank_stats.loc[rank_stats["value_gap_rank"] == 3]
    if not top1.empty:
        lines.append(f"- rank1 top3率: {float(top1['actual_top3_rate'].iloc[0]):.6f}")
    if not top2.empty:
        lines.append(f"- rank2 top3率: {float(top2['actual_top3_rate'].iloc[0]):.6f}")
    if not top3.empty:
        lines.append(f"- rank3 top3率: {float(top3['actual_top3_rate'].iloc[0]):.6f}")

    lines.append("")
    lines.append("レースごとのばらつき")
    lines.extend(buyability.to_string(index=False).splitlines())
    lines.append("")
    lines.append("買えるレースの特徴（暫定）")
    lines.append("《推測》gap_stdが高いレースほど rank1_value_gap が大きく、優位差を取りやすい可能性があります。")
    lines.append("《推測》rank1_top3_hit_rate が高い階層を優先し、将来は複勝/ワイドオッズで検証するのが妥当です。")

    report_path.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"input: {in_path}")
    print(f"output: {out_path}")
    print(f"report: {report_path}")
    print("")
    print("=== rank head ===")
    print(rank_stats.head(10).to_string(index=False))
    print("")
    print("=== z-bin summary ===")
    print(z_stats.to_string(index=False))
    print("")
    print("=== gap_std distribution ===")
    print(std_dist.to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
