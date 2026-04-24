import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Simulate wide ROI on fixed 15% selected races.")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--race", default=r"C:\TXT\wide_race_selection_fixed_rate_2026_v1.csv")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_roi_simulation_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_roi_report_2026_v1.txt")
    ap.add_argument("--encoding", default="cp932")
    ap.add_argument("--stake", type=int, default=100)
    ap.add_argument("--assumed-wide-odds", type=float, default=8.0)
    return ap.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def simulate_pattern(df: pd.DataFrame, pattern: str, stake: int, assumed_odds: float) -> dict:
    work = df.copy()

    if pattern == "top1":
        work = work[work["pair_rank_score"] == 1].copy()
    elif pattern == "top3":
        work = work[work["pair_rank_score"] <= 3].copy()
    else:
        raise ValueError(f"Unknown pattern: {pattern}")

    if work.empty:
        return {
            "pattern": pattern,
            "races": 0,
            "bets": 0,
            "stake_total": 0,
            "return_total": 0.0,
            "roi_pct": np.nan,
            "bet_hit_rate": np.nan,
            "race_hit_rate": np.nan,
            "avg_payout_multiple": np.nan,
            "bets_per_race": np.nan,
            "expected_value_per_race": np.nan,
            "expected_value_per_bet": np.nan,
        }

    payout_mult = np.where(work["wide_hit"] == 1, assumed_odds, 0.0)
    returns = payout_mult * stake

    races = int(work["race_id_raw"].nunique())
    bets = int(len(work))
    stake_total = int(bets * stake)
    return_total = float(returns.sum())

    bet_hit_rate = float(work["wide_hit"].mean())
    race_hit_rate = float(work.groupby("race_id_raw", dropna=False)["wide_hit"].max().mean())

    hit_rows = work[work["wide_hit"] == 1]
    avg_payout_multiple = float(assumed_odds) if len(hit_rows) else 0.0

    expected_value_per_bet = float(return_total / stake_total) if stake_total else np.nan
    expected_value_per_race = float((return_total - stake_total) / races) if races else np.nan

    return {
        "pattern": pattern,
        "races": races,
        "bets": bets,
        "stake_total": stake_total,
        "return_total": return_total,
        "roi_pct": float((return_total / stake_total) * 100.0) if stake_total else np.nan,
        "bet_hit_rate": bet_hit_rate,
        "race_hit_rate": race_hit_rate,
        "avg_payout_multiple": avg_payout_multiple,
        "bets_per_race": float(bets / races) if races else np.nan,
        "expected_value_per_race": expected_value_per_race,
        "expected_value_per_bet": expected_value_per_bet,
    }


def main() -> int:
    args = parse_args()

    wide_path = Path(args.wide)
    race_path = Path(args.race)
    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)

    wide_df = pd.read_csv(wide_path, encoding=args.encoding, low_memory=False)
    race_df = pd.read_csv(race_path, encoding=args.encoding, low_memory=False)

    required_wide = ["race_id_raw", "pair_rank_score", "wide_hit"]
    required_race = ["race_id_raw", "selected_top15", "race_date"]
    miss_wide = [c for c in required_wide if c not in wide_df.columns]
    miss_race = [c for c in required_race if c not in race_df.columns]
    if miss_wide:
        raise SystemExit(f"Missing wide columns: {miss_wide}")
    if miss_race:
        raise SystemExit(f"Missing race columns: {miss_race}")

    wide_df["pair_rank_score"] = to_float(wide_df["pair_rank_score"])
    wide_df["wide_hit"] = to_float(wide_df["wide_hit"]).fillna(0).astype(int)

    selected_race_df = race_df[race_df["selected_top15"] == True].copy()  # noqa: E712
    if selected_race_df.empty:
        raise SystemExit("No races selected by top15 filter")

    selected_races = set(selected_race_df["race_id_raw"].astype(str))
    wide_df["race_id_raw"] = wide_df["race_id_raw"].astype(str)
    target_wide = wide_df[wide_df["race_id_raw"].isin(selected_races)].copy()

    if target_wide.empty:
        raise SystemExit("No wide candidates matched selected_top15 races")

    summary_rows = [
        simulate_pattern(target_wide, "top1", args.stake, args.assumed_wide_odds),
        simulate_pattern(target_wide, "top3", args.stake, args.assumed_wide_odds),
    ]
    summary = pd.DataFrame(summary_rows)

    # Baseline daily operating stats.
    race_dates = pd.to_datetime(selected_race_df["race_date"], errors="coerce")
    active_days = int(race_dates.dt.date.nunique()) if race_dates.notna().any() else 0
    races_count = int(selected_race_df["race_id_raw"].nunique())
    races_per_day = float(races_count / active_days) if active_days else np.nan

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    summary.to_csv(out_csv, index=False, encoding=args.encoding)

    top1 = summary[summary["pattern"] == "top1"].iloc[0]
    top3 = summary[summary["pattern"] == "top3"].iloc[0]

    lines: list[str] = []
    lines.append("wide ROI simulation report (2026, fixed 15% races)")
    lines.append("")
    lines.append(f"input_wide={wide_path}")
    lines.append(f"input_race_selection={race_path}")
    lines.append(f"output={out_csv}")
    lines.append("")
    lines.append("simulation settings")
    lines.append("- race filter: selected_top15 == True")
    lines.append(f"- stake_per_bet={args.stake}")
    lines.append(f"- 《推測》hit payout multiple={args.assumed_wide_odds:.2f}x (wide実配当データ未使用)")
    lines.append("")
    lines.append("ROI比較 (top1 vs top3)")
    lines.extend(summary.to_string(index=False).splitlines())
    lines.append("")
    lines.append("主要評価")
    lines.append(f"- top1 ROI={float(top1['roi_pct']):.2f}% / hit_rate={float(top1['bet_hit_rate']):.6f}")
    lines.append(f"- top3 ROI={float(top3['roi_pct']):.2f}% / hit_rate={float(top3['bet_hit_rate']):.6f}")
    lines.append(f"- 《推測》1日あたり購入レース数={races_per_day:.2f} ({races_count} races / {active_days} days)")
    lines.append(f"- top1 1レース期待値(円)={float(top1['expected_value_per_race']):.2f}")
    lines.append(f"- top3 1レース期待値(円)={float(top3['expected_value_per_race']):.2f}")
    lines.append("")
    lines.append("ROI>100% 判定")
    lines.append(f"- top1: {'YES' if float(top1['roi_pct']) > 100 else 'NO'}")
    lines.append(f"- top3: {'YES' if float(top3['roi_pct']) > 100 else 'NO'}")
    lines.append("")
    lines.append("改善余地")
    lines.append("《推測》実配当（ワイド払戻）を接続し、人気帯別に配当分布を使うとROI精度が上がります。")
    lines.append("《推測》pair_value_score_v1 との併用選別、または上位3内の点数削減で期待値改善余地があります。")

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print("\n=== summary ===")
    print(summary.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
