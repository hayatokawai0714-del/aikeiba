import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Analyze value gap between AI top3 probability and market proxy probability.")
    ap.add_argument("--input", default=r"C:\TXT\top3_model_predictions_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\top3_value_gap_report_2026_v1.txt")
    ap.add_argument("--out-detail", default=r"C:\TXT\top3_value_gap_detail_2026_v1.csv")
    ap.add_argument("--out-summary", default=r"C:\TXT\top3_value_gap_tier_summary_2026_v1.csv")
    ap.add_argument("--encoding", default="cp932")
    return ap.parse_args()


def safe_to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def build_tier_labels(df: pd.DataFrame) -> pd.Series:
    q90 = float(df["value_gap"].quantile(0.90))
    q80 = float(df["value_gap"].quantile(0.80))
    q20 = float(df["value_gap"].quantile(0.20))

    tier = pd.Series("", index=df.index, dtype="object")
    tier[df["value_gap"] >= q90] = "上位10%"
    tier[(df["value_gap"] < q90) & (df["value_gap"] >= q80)] = "上位20%(10-20%)"
    tier[(df["value_gap"] < q80) & (df["value_gap"] >= q20)] = "中位(20-80%)"
    tier[df["value_gap"] < q20] = "下位(80-100%)"

    return tier


def tier_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    order = ["上位10%", "上位20%(10-20%)", "中位(20-80%)", "下位(80-100%)"]
    summary = (
        df.groupby("value_gap_tier", dropna=False)
        .agg(
            count=("top3", "size"),
            actual_top3_rate=("top3", "mean"),
            avg_pred_top3=("pred_top3_raw", "mean"),
            avg_win_odds=("win_odds", "mean"),
            avg_market_win_prob=("market_win_prob", "mean"),
            avg_value_gap=("value_gap", "mean"),
        )
        .reset_index()
    )
    summary["value_gap_tier"] = pd.Categorical(summary["value_gap_tier"], categories=order, ordered=True)
    summary = summary.sort_values("value_gap_tier").reset_index(drop=True)
    return summary


def main() -> int:
    args = parse_args()

    input_path = Path(args.input)
    out_report = Path(args.out_report)
    out_detail = Path(args.out_detail)
    out_summary = Path(args.out_summary)

    df = pd.read_csv(input_path, encoding=args.encoding, low_memory=False)

    required = ["pred_top3_raw", "win_odds", "top3"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")

    df["pred_top3_raw"] = safe_to_float(df["pred_top3_raw"])
    df["win_odds"] = safe_to_float(df["win_odds"])
    df["top3"] = safe_to_float(df["top3"]).fillna(0).astype(int)

    before = len(df)
    df = df[df["pred_top3_raw"].notna()].copy()

    df["market_win_prob"] = np.where(df["win_odds"] > 0, 1.0 / df["win_odds"], np.nan)
    drop_invalid_odds = int(df["market_win_prob"].isna().sum())
    df = df[df["market_win_prob"].notna()].copy()

    if df.empty:
        raise SystemExit("No rows after filtering invalid pred_top3_raw / win_odds")

    df["value_gap"] = df["pred_top3_raw"] - df["market_win_prob"]
    df["value_gap_tier"] = build_tier_labels(df)

    dist = df["value_gap"].describe(percentiles=[0.01, 0.05, 0.1, 0.2, 0.5, 0.8, 0.9, 0.95, 0.99])

    top_stats = []
    for q in [0.99, 0.95, 0.90, 0.80]:
        threshold = float(df["value_gap"].quantile(q))
        sub = df[df["value_gap"] >= threshold]
        top_stats.append(
            {
                "segment": f"top_{int(round((1 - q) * 100))}%",
                "rows": int(len(sub)),
                "actual_top3_rate": float(sub["top3"].mean()) if len(sub) else float("nan"),
                "avg_pred_top3": float(sub["pred_top3_raw"].mean()) if len(sub) else float("nan"),
                "avg_win_odds": float(sub["win_odds"].mean()) if len(sub) else float("nan"),
                "avg_market_win_prob": float(sub["market_win_prob"].mean()) if len(sub) else float("nan"),
                "avg_value_gap": float(sub["value_gap"].mean()) if len(sub) else float("nan"),
            }
        )

    top_stats_df = pd.DataFrame(top_stats)
    tier_summary = tier_aggregate(df)

    out_detail.parent.mkdir(parents=True, exist_ok=True)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    detail_cols = [
        "race_date",
        "race_id_raw",
        "horse_no",
        "horse_name",
        "jockey_name",
        "top3",
        "pred_top3_raw",
        "win_odds",
        "market_win_prob",
        "value_gap",
        "value_gap_tier",
    ]
    available_cols = [c for c in detail_cols if c in df.columns]
    df[available_cols].to_csv(out_detail, index=False, encoding=args.encoding)
    tier_summary.to_csv(out_summary, index=False, encoding=args.encoding)

    lines: list[str] = []
    lines.append("top3 value-gap analysis report (2026 test)")
    lines.append("")
    lines.append(f"input={input_path}")
    lines.append(f"rows_input={before}")
    lines.append(f"rows_output={len(df)}")
    lines.append(f"dropped_invalid_odds={drop_invalid_odds}")
    lines.append("")
    lines.append("definition")
    lines.append("market_win_prob = 1 / win_odds")
    lines.append("value_gap = pred_top3_raw - market_win_prob")
    lines.append("tier = 上位10% / 上位20%(10-20%) / 中位(20-80%) / 下位(80-100%)")
    lines.append("")
    lines.append("value_gap distribution")
    for idx, val in dist.items():
        if isinstance(val, (int, float, np.floating)):
            lines.append(f"- {idx}: {float(val):.8f}")
        else:
            lines.append(f"- {idx}: {val}")
    lines.append("")
    lines.append("top segment performance")
    lines.extend(top_stats_df.to_string(index=False).splitlines())
    lines.append("")
    lines.append("tier summary")
    lines.extend(tier_summary.to_string(index=False).splitlines())

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"input: {input_path}")
    print(f"detail: {out_detail}")
    print(f"summary: {out_summary}")
    print(f"report: {out_report}")
    print("")
    print("=== tier summary ===")
    print(tier_summary.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
