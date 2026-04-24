import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Create temporary race-relative value score from enriched value_gap data.")
    ap.add_argument("--input", default=r"C:\TXT\top3_value_gap_enriched_2026_v1.csv")
    ap.add_argument("--output", default=r"C:\TXT\top3_value_score_2026_v1.csv")
    ap.add_argument("--report", default=r"C:\TXT\top3_value_score_report_2026_v1.txt")
    ap.add_argument("--encoding", default="cp932")
    return ap.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def build_rank_score(rank_series: pd.Series) -> pd.Series:
    rank = to_float(rank_series)
    score = pd.Series(0.2, index=rank_series.index, dtype=float)
    score[rank == 1] = 1.0
    score[rank == 2] = 0.8
    score[rank == 3] = 0.6
    score[rank == 4] = 0.4
    return score


def summarize_by_rank(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("value_score_rank", dropna=False)
        .agg(
            count=("top3", "size"),
            actual_top3_rate=("top3", "mean"),
            avg_value_score_v1=("value_score_v1", "mean"),
            avg_value_gap=("value_gap", "mean"),
        )
        .reset_index()
        .sort_values("value_score_rank")
    )


def summarize_top_segments(df: pd.DataFrame) -> pd.DataFrame:
    q90 = float(df["value_score_v1"].quantile(0.90))
    q80 = float(df["value_score_v1"].quantile(0.80))

    top10 = df[df["value_score_v1"] >= q90]
    top20 = df[df["value_score_v1"] >= q80]

    rows = [
        {
            "segment": "top10%",
            "rows": int(len(top10)),
            "actual_top3_rate": float(top10["top3"].mean()),
            "avg_value_score_v1": float(top10["value_score_v1"].mean()),
            "avg_value_gap": float(top10["value_gap"].mean()),
        },
        {
            "segment": "top20%",
            "rows": int(len(top20)),
            "actual_top3_rate": float(top20["top3"].mean()),
            "avg_value_score_v1": float(top20["value_score_v1"].mean()),
            "avg_value_gap": float(top20["value_gap"].mean()),
        },
    ]
    return pd.DataFrame(rows)


def summarize_race_level(df: pd.DataFrame) -> pd.DataFrame:
    race_rows: list[dict[str, float | int | str]] = []

    for race_id, race_df in df.groupby("race_id_raw", dropna=False):
        race_sorted = race_df.sort_values("value_score_rank", ascending=True)
        top1 = race_sorted.iloc[0]
        top2 = race_sorted.head(2)

        race_rows.append(
            {
                "race_id_raw": race_id,
                "race_size": int(len(race_df)),
                "value_score_max": float(race_df["value_score_v1"].max()),
                "value_score_top2_mean": float(top2["value_score_v1"].mean()),
                "gap_std": float(race_df["gap_std"].iloc[0]) if "gap_std" in race_df.columns else float("nan"),
                "rank1_top3": int(top1["top3"]),
                "rank1_value_score_v1": float(top1["value_score_v1"]),
                "rank1_value_gap": float(top1["value_gap"]),
            }
        )

    return pd.DataFrame(race_rows)


def main() -> int:
    args = parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    report_path = Path(args.report)

    df = pd.read_csv(in_path, encoding=args.encoding, low_memory=False)

    required_cols = ["race_id_raw", "top3", "value_gap", "value_gap_rank", "value_gap_z"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")

    df["top3"] = to_float(df["top3"]).fillna(0).astype(int)
    df["value_gap"] = to_float(df["value_gap"])
    df["value_gap_rank"] = to_float(df["value_gap_rank"])
    df["value_gap_z"] = to_float(df["value_gap_z"])

    before = len(df)
    df = df[df["value_gap"].notna() & df["value_gap_rank"].notna() & df["value_gap_z"].notna()].copy()
    dropped = before - len(df)

    if df.empty:
        raise SystemExit("No rows after filtering required score columns")

    df["rank_score"] = build_rank_score(df["value_gap_rank"])
    df["value_score_v1"] = 0.6 * df["value_gap_z"] + 0.4 * df["rank_score"]

    df["value_score_rank"] = (
        df.groupby("race_id_raw", dropna=False)["value_score_v1"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    sort_cols = [c for c in ["race_date", "race_id_raw", "value_score_rank", "horse_no"] if c in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    rank_summary = summarize_by_rank(df)
    top_segments = summarize_top_segments(df)
    score_dist = df["value_score_v1"].describe(percentiles=[0.01, 0.05, 0.1, 0.2, 0.5, 0.8, 0.9, 0.95, 0.99])

    race_level = summarize_race_level(df)
    race_level_summary = pd.DataFrame(
        [
            {
                "races": int(len(race_level)),
                "avg_value_score_max": float(race_level["value_score_max"].mean()),
                "avg_value_score_top2_mean": float(race_level["value_score_top2_mean"].mean()),
                "avg_gap_std": float(race_level["gap_std"].mean()),
                "rank1_top3_rate": float(race_level["rank1_top3"].mean()),
            }
        ]
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(out_path, index=False, encoding=args.encoding)

    top_rank_rows = rank_summary[rank_summary["value_score_rank"].isin([1, 2, 3])]
    score_rank1 = rank_summary.loc[rank_summary["value_score_rank"] == 1, "actual_top3_rate"]
    score_rank1_rate = float(score_rank1.iloc[0]) if not score_rank1.empty else float("nan")

    top10_rate = float(top_segments.loc[top_segments["segment"] == "top10%", "actual_top3_rate"].iloc[0])
    top20_rate = float(top_segments.loc[top_segments["segment"] == "top20%", "actual_top3_rate"].iloc[0])

    proceed_flag = "GO"
    if score_rank1_rate < 0.45 or top10_rate < 0.40:
        proceed_flag = "HOLD"

    lines: list[str] = []
    lines.append("top3 temporary value score report (2026)")
    lines.append("")
    lines.append(f"input={in_path}")
    lines.append(f"output={out_path}")
    lines.append(f"rows_input={before}")
    lines.append(f"rows_output={len(df)}")
    lines.append(f"dropped_rows={dropped}")
    lines.append("")
    lines.append("score definition")
    lines.append("value_score_v1 = 0.6 * value_gap_z + 0.4 * rank_score")
    lines.append("rank_score: rank1=1.0 rank2=0.8 rank3=0.6 rank4=0.4 rank5+=0.2")
    lines.append("value_score_rank: race内 value_score_v1 降順順位")
    lines.append("")
    lines.append("value_score_rank別 top3率")
    lines.extend(rank_summary.to_string(index=False).splitlines())
    lines.append("")
    lines.append("value_score_v1 上位セグメント")
    lines.extend(top_segments.to_string(index=False).splitlines())
    lines.append("")
    lines.append("value_score_v1 分布")
    for idx, val in score_dist.items():
        lines.append(f"- {idx}: {float(val):.8f}")
    lines.append("")
    lines.append("レース単位集計（全体要約）")
    lines.extend(race_level_summary.to_string(index=False).splitlines())
    lines.append("")
    lines.append("レース単位集計（先頭20件）")
    lines.extend(race_level.head(20).to_string(index=False).splitlines())
    lines.append("")
    lines.append("判断材料")
    lines.append(f"- value_score_rank1 の top3率: {score_rank1_rate:.6f}")
    lines.append("- value_score_rank1-3 の top3率:")
    lines.extend(top_rank_rows.to_string(index=False).splitlines())
    lines.append(f"- 上位10% top3率: {top10_rate:.6f}")
    lines.append(f"- 上位20% top3率: {top20_rate:.6f}")
    lines.append(f"- ワイド候補生成への暫定判定: {proceed_flag}")
    lines.append("《推測》GO/HOLD は rank1率と上位10%率の閾値による暫定判定です。")

    report_path.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"input: {in_path}")
    print(f"output: {out_path}")
    print(f"report: {report_path}")
    print("")
    print("=== value_score_rank head ===")
    print(rank_summary.head(10).to_string(index=False))
    print("")
    print("=== top segments ===")
    print(top_segments.to_string(index=False))
    print("")
    print("=== decision ===")
    print(f"value_score_rank1_top3_rate={score_rank1_rate:.6f}")
    print(f"top10_rate={top10_rate:.6f}")
    print(f"proceed={proceed_flag}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
