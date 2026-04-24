import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd


RATES = [0.15, 0.20, 0.25]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Optimize race selection with fixed retained rates (15/20/25%).")
    ap.add_argument("--input", default=r"C:\TXT\wide_race_selection_2026_v1.csv")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_race_selection_fixed_rate_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_race_selection_fixed_rate_report_2026_v1.txt")
    ap.add_argument("--encoding", default="cp932")
    return ap.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def zscore(series: pd.Series) -> pd.Series:
    std = float(series.std(ddof=0))
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=series.index)
    return (series - float(series.mean())) / std


def evaluate_subset(df: pd.DataFrame, rate: float, n_total: int, base_top1: float, base_top3: float) -> dict:
    keep = max(1, int(math.ceil(n_total * rate)))
    sub = df.head(keep).copy()

    top1 = float(sub["top1_wide_hit"].mean())
    top3 = float(sub["top3_any_hit"].mean())

    return {
        "rate": rate,
        "label": f"top_{int(rate * 100)}%",
        "races": int(len(sub)),
        "top1_hit_rate": top1,
        "top3_hit_rate": top3,
        "top1_lift": top1 - base_top1,
        "top3_lift": top3 - base_top3,
        "top1_drop_from_base": base_top1 - top1,
        "top3_drop_from_base": base_top3 - top3,
    }


def choose_recommended(summary: pd.DataFrame) -> pd.Series:
    # 《推測》バランス指標: top1/top3の改善を重視しつつ、20%からの乖離に軽いペナルティ。
    out = summary.copy()
    out["balance_score"] = (
        out["top1_lift"] * 0.55
        + out["top3_lift"] * 0.35
        - (out["rate"] - 0.20).abs() * 0.10
    )
    out = out.sort_values(["balance_score", "races"], ascending=[False, False]).reset_index(drop=True)
    return out.iloc[0]


def main() -> int:
    args = parse_args()

    in_path = Path(args.input)
    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)

    df = pd.read_csv(in_path, encoding=args.encoding, low_memory=False)

    required = ["race_id_raw", "value_score_max", "gap_std", "top1_wide_hit", "top3_any_hit"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")

    df["value_score_max"] = to_float(df["value_score_max"])
    df["gap_std"] = to_float(df["gap_std"])
    df["top1_wide_hit"] = to_float(df["top1_wide_hit"])
    df["top3_any_hit"] = to_float(df["top3_any_hit"])

    before = len(df)
    df = df[
        df["value_score_max"].notna()
        & df["gap_std"].notna()
        & df["top1_wide_hit"].notna()
        & df["top3_any_hit"].notna()
    ].copy()
    dropped = before - len(df)

    if df.empty:
        raise SystemExit("No races after filtering required columns")

    df["z_value_score_max"] = zscore(df["value_score_max"])
    df["z_gap_std"] = zscore(df["gap_std"])

    # 《推測》合成スコア重み
    df["race_select_score_v1"] = 0.6 * df["z_value_score_max"] + 0.4 * df["z_gap_std"]
    df = df.sort_values("race_select_score_v1", ascending=False).reset_index(drop=True)
    df["race_select_rank"] = np.arange(1, len(df) + 1)

    n_total = int(len(df))
    base_top1 = float(df["top1_wide_hit"].mean())
    base_top3 = float(df["top3_any_hit"].mean())

    rows = [evaluate_subset(df, rate, n_total, base_top1, base_top3) for rate in RATES]
    summary = pd.DataFrame(rows)

    rec = choose_recommended(summary)
    rec_rate = float(rec["rate"])
    rec_keep = int(rec["races"])

    df["selected_top15"] = df["race_select_rank"] <= int(math.ceil(n_total * 0.15))
    df["selected_top20"] = df["race_select_rank"] <= int(math.ceil(n_total * 0.20))
    df["selected_top25"] = df["race_select_rank"] <= int(math.ceil(n_total * 0.25))
    df["selected_recommended"] = df["race_select_rank"] <= rec_keep
    df["recommended_rate"] = rec_rate

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(out_csv, index=False, encoding=args.encoding)

    # races/day estimate
    races_per_day_text = "N/A"
    if "race_date" in df.columns:
        dates = pd.to_datetime(df["race_date"], errors="coerce")
        day_count = int(dates.dt.date.nunique()) if dates.notna().any() else 0
        if day_count > 0:
            est = rec_keep / day_count
            races_per_day_text = f"{est:.2f} races/day ({rec_keep} races / {day_count} days)"

    lines: list[str] = []
    lines.append("wide race selection fixed-rate report (2026)")
    lines.append("")
    lines.append(f"input={in_path}")
    lines.append(f"output={out_csv}")
    lines.append(f"races_input={before}")
    lines.append(f"races_used={n_total}")
    lines.append(f"dropped_rows={dropped}")
    lines.append("")
    lines.append("score definition")
    lines.append("《推測》race_select_score_v1 = 0.6*z(value_score_max) + 0.4*z(gap_std)")
    lines.append("- race_select_score_v1 の降順でレース選別")
    lines.append("")
    lines.append("フィルタ前ベース")
    lines.append(f"- top1_hit_rate={base_top1:.6f}")
    lines.append(f"- top3_hit_rate={base_top3:.6f}")
    lines.append("")
    lines.append("15% / 20% / 25% 比較")
    lines.extend(summary.to_string(index=False).splitlines())
    lines.append("")
    lines.append("推奨残存率")
    lines.append(f"- 推奨: {int(rec_rate * 100)}% ({rec_keep} races)")
    lines.append(f"- top1_hit_rate={float(rec['top1_hit_rate']):.6f} (diff {float(rec['top1_lift']):+.6f})")
    lines.append(f"- top3_hit_rate={float(rec['top3_hit_rate']):.6f} (diff {float(rec['top3_lift']):+.6f})")
    lines.append("- 理由: hit率改善と件数確保のバランスが最も良い")
    lines.append("")
    lines.append("実運用想定")
    lines.append(f"- 《推測》1日あたり対象レース数: {races_per_day_text}")

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print(f"base_top1={base_top1:.6f} base_top3={base_top3:.6f}")
    print("\n=== fixed-rate summary ===")
    print(summary.to_string(index=False))
    print("\n=== recommended ===")
    print(f"rate={rec_rate:.2%} races={rec_keep} top1={float(rec['top1_hit_rate']):.6f} top3={float(rec['top3_hit_rate']):.6f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
