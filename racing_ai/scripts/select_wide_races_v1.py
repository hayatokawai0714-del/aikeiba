import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build temporary race-selection(skip) rule for wide candidates.")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--score", default=r"C:\TXT\top3_value_score_2026_v1.csv")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_race_selection_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_race_selection_report_2026_v1.txt")
    ap.add_argument("--encoding", default="cp932")
    return ap.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def build_race_features(score_df: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for race_id, race_df in score_df.groupby("race_id_raw", dropna=False):
        race_sorted = race_df.sort_values("value_score_rank", ascending=True)
        top2 = race_sorted.head(2)

        top1_vs_top2_diff = np.nan
        if len(top2) >= 2:
            top1_vs_top2_diff = float(top2.iloc[0]["value_score_v1"] - top2.iloc[1]["value_score_v1"])

        gap_std = float(race_df["gap_std"].iloc[0]) if "gap_std" in race_df.columns else float(race_df["value_gap"].std(ddof=0))

        frames.append(
            {
                "race_id_raw": race_id,
                "race_date": race_df["race_date"].iloc[0] if "race_date" in race_df.columns else "",
                "value_score_max": float(race_df["value_score_v1"].max()),
                "value_score_top2_mean": float(top2["value_score_v1"].mean()) if len(top2) else float("nan"),
                "gap_std": gap_std,
                "top1_top2_diff": top1_vs_top2_diff,
                "horse_count": int(len(race_df)),
            }
        )

    return pd.DataFrame(frames)


def build_wide_features(wide_df: pd.DataFrame) -> pd.DataFrame:
    top1 = (
        wide_df[wide_df["pair_rank_score"] == 1]
        .groupby("race_id_raw", dropna=False)
        .agg(
            top1_pair_score_v1=("pair_score_v1", "first"),
            top1_wide_hit=("wide_hit", "first"),
        )
        .reset_index()
    )

    top3 = (
        wide_df[wide_df["pair_rank_score"] <= 3]
        .groupby("race_id_raw", dropna=False)
        .agg(top3_any_hit=("wide_hit", "max"))
        .reset_index()
    )

    return top1.merge(top3, on="race_id_raw", how="outer")


def classify_races(df: pd.DataFrame) -> pd.DataFrame:
    q_max_hi = float(df["value_score_max"].quantile(0.67))
    q_max_lo = float(df["value_score_max"].quantile(0.33))
    q_std_hi = float(df["gap_std"].quantile(0.67))
    q_std_lo = float(df["gap_std"].quantile(0.33))

    group = pd.Series("普通", index=df.index, dtype="object")
    group[(df["value_score_max"] >= q_max_hi) & (df["gap_std"] >= q_std_hi)] = "強い"
    group[(df["value_score_max"] <= q_max_lo) & (df["gap_std"] <= q_std_lo)] = "弱い"

    out = df.copy()
    out["race_group"] = group
    out["group_value_score_max_hi_th"] = q_max_hi
    out["group_value_score_max_lo_th"] = q_max_lo
    out["group_gap_std_hi_th"] = q_std_hi
    out["group_gap_std_lo_th"] = q_std_lo
    return out


def summarize_groups(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("race_group", dropna=False)
        .agg(
            races=("race_id_raw", "size"),
            top1_hit_rate=("top1_wide_hit", "mean"),
            top3_hit_rate=("top3_any_hit", "mean"),
            avg_value_score_max=("value_score_max", "mean"),
            avg_gap_std=("gap_std", "mean"),
        )
        .reset_index()
        .sort_values("race_group")
    )


def threshold_sweep(df: pd.DataFrame) -> pd.DataFrame:
    base_top1 = float(df["top1_wide_hit"].mean())
    base_top3 = float(df["top3_any_hit"].mean())

    max_qs = [0.50, 0.60, 0.70, 0.80]
    std_qs = [0.50, 0.60, 0.70, 0.80]

    rows = []
    for mq in max_qs:
        x = float(df["value_score_max"].quantile(mq))
        for sq in std_qs:
            y = float(df["gap_std"].quantile(sq))
            sub = df[(df["value_score_max"] > x) & (df["gap_std"] > y)]
            if len(sub) == 0:
                continue
            rows.append(
                {
                    "x_quantile": mq,
                    "y_quantile": sq,
                    "value_score_max_th": x,
                    "gap_std_th": y,
                    "races_after": int(len(sub)),
                    "race_ratio": float(len(sub) / len(df)),
                    "top1_hit_rate": float(sub["top1_wide_hit"].mean()),
                    "top3_hit_rate": float(sub["top3_any_hit"].mean()),
                    "top1_lift": float(sub["top1_wide_hit"].mean() - base_top1),
                    "top3_lift": float(sub["top3_any_hit"].mean() - base_top3),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out = out.sort_values(["top1_lift", "races_after"], ascending=[False, False]).reset_index(drop=True)
    return out


def main() -> int:
    args = parse_args()

    wide_path = Path(args.wide)
    score_path = Path(args.score)
    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)

    wide_df = pd.read_csv(wide_path, encoding=args.encoding, low_memory=False)
    score_df = pd.read_csv(score_path, encoding=args.encoding, low_memory=False)

    for col in ["pair_rank_score", "pair_score_v1", "wide_hit"]:
        wide_df[col] = to_float(wide_df[col])
    for col in ["value_score_v1", "value_score_rank", "gap_std", "value_gap"]:
        if col in score_df.columns:
            score_df[col] = to_float(score_df[col])

    wide_df = wide_df[wide_df["pair_rank_score"].notna() & wide_df["pair_score_v1"].notna() & wide_df["wide_hit"].notna()].copy()
    score_df = score_df[score_df["value_score_v1"].notna() & score_df["value_score_rank"].notna()].copy()

    race_feat = build_race_features(score_df)
    wide_feat = build_wide_features(wide_df)

    race_df = race_feat.merge(wide_feat, on="race_id_raw", how="inner")
    race_df["top1_wide_hit"] = race_df["top1_wide_hit"].astype(int)
    race_df["top3_any_hit"] = race_df["top3_any_hit"].astype(int)

    race_df = classify_races(race_df)
    group_summary = summarize_groups(race_df)
    sweep = threshold_sweep(race_df)

    base_races = int(len(race_df))
    base_top1 = float(race_df["top1_wide_hit"].mean())
    base_top3 = float(race_df["top3_any_hit"].mean())

    best = None
    if not sweep.empty:
        # 《推測》レース数が減りすぎないように最低件数を設定して採用。
        min_races_primary = max(100, int(base_races * 0.15))
        min_races_secondary = max(50, int(base_races * 0.08))

        viable = sweep[sweep["races_after"] >= min_races_primary].copy()
        if viable.empty:
            viable = sweep[sweep["races_after"] >= min_races_secondary].copy()
        if viable.empty:
            viable = sweep.copy()
        best = viable.iloc[0]

        race_df["selected_by_best_filter"] = (
            (race_df["value_score_max"] > float(best["value_score_max_th"]))
            & (race_df["gap_std"] > float(best["gap_std_th"]))
        )
    else:
        race_df["selected_by_best_filter"] = False

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    race_df.to_csv(out_csv, index=False, encoding=args.encoding)

    lines: list[str] = []
    lines.append("wide race selection report (2026)")
    lines.append("")
    lines.append(f"input_wide={wide_path}")
    lines.append(f"input_score={score_path}")
    lines.append(f"output={out_csv}")
    lines.append("")
    lines.append("フィルタ前")
    lines.append(f"- races={base_races}")
    lines.append(f"- top1_hit_rate={base_top1:.6f}")
    lines.append(f"- top3_hit_rate={base_top3:.6f}")
    lines.append("")
    lines.append("レース分類 (強い/普通/弱い)")
    lines.append("《推測》閾値は value_score_max と gap_std の33/67パーセンタイルを使用")
    lines.extend(group_summary.to_string(index=False).splitlines())
    lines.append("")
    lines.append("閾値スイープ (value_score_max > X AND gap_std > Y)")
    if sweep.empty:
        lines.append("- 該当なし")
    else:
        lines.extend(sweep.head(20).to_string(index=False).splitlines())

    lines.append("")
    lines.append("どの条件が効いたか")
    if best is None:
        lines.append("- 閾値候補が作れなかったため判定保留")
    else:
        x = float(best["value_score_max_th"])
        y = float(best["gap_std_th"])
        sub = race_df[race_df["selected_by_best_filter"]]
        lines.append("《推測》採用条件は top1_lift 最大（最低レース数: 全体の15%、なければ8%）で選択")
        lines.append(f"- 採用閾値: value_score_max > {x:.6f}, gap_std > {y:.6f}")
        lines.append(f"- フィルタ後レース数: {len(sub)} / {base_races} ({len(sub)/base_races:.2%})")
        lines.append(f"- フィルタ後 top1_hit_rate: {float(sub['top1_wide_hit'].mean()):.6f}")
        lines.append(f"- フィルタ後 top3_hit_rate: {float(sub['top3_any_hit'].mean()):.6f}")
        lines.append(f"- top1_hit_rate 変化: {float(sub['top1_wide_hit'].mean() - base_top1):+.6f}")
        lines.append(f"- top3_hit_rate 変化: {float(sub['top3_any_hit'].mean() - base_top3):+.6f}")
        lines.append("")
        lines.append("暫定の買い条件")
        lines.append(f"- value_score_max > {x:.6f}")
        lines.append(f"- gap_std > {y:.6f}")
        lines.append("- 上記を満たすレースのみ、pair_rank_score 上位を候補にする")

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print(f"races_before={base_races} top1={base_top1:.6f} top3={base_top3:.6f}")
    if best is not None:
        print(
            "best_filter="
            f"value_score_max>{float(best['value_score_max_th']):.6f},"
            f"gap_std>{float(best['gap_std_th']):.6f},"
            f"races_after={int(best['races_after'])},"
            f"top1_lift={float(best['top1_lift']):+.6f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
