import argparse
from pathlib import Path

import numpy as np
import pandas as pd

JYO_TO_VENUE = {
    "01": "SAP",
    "02": "HAK",
    "03": "FUK",
    "04": "NII",
    "05": "TOK",
    "06": "NAK",
    "07": "CHU",
    "08": "KYO",
    "09": "HAN",
    "10": "KOK",
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Analyze losing races for wide strategy (no rule changes).")
    ap.add_argument("--roi", default=r"C:\TXT\wide_roi_real_2026_v1.csv")
    ap.add_argument("--race", default=r"C:\TXT\wide_race_selection_2026_v1.csv")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--score", default=r"C:\TXT\top3_value_score_2026_v1.csv")
    ap.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_loss_analysis_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_loss_analysis_report_2026_v1.txt")
    ap.add_argument("--encoding", default="cp932")
    ap.add_argument("--stake", type=int, default=100)
    return ap.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def raw_to_race_id(raw: str) -> str:
    s = str(raw).split(".")[0].zfill(16)
    date = s[:8]
    jyo = s[8:10]
    race_no = int(s[-2:])
    venue = JYO_TO_VENUE.get(jyo, "UNK")
    return f"{date}-{venue}-{race_no:02d}R"


def normalize_pair_key(a: int, b: int) -> str:
    x, y = sorted((int(a), int(b)))
    return f"{x:02d}-{y:02d}"


def load_wide_payouts(payout_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for fp in payout_root.rglob("payouts.csv"):
        if "2026" not in str(fp):
            continue
        try:
            df = pd.read_csv(fp, encoding="utf-8-sig", low_memory=False)
        except Exception:
            df = pd.read_csv(fp, encoding="cp932", low_memory=False)

        if not {"race_id", "bet_type", "bet_key", "payout"}.issubset(df.columns):
            continue

        sub = df[df["bet_type"].astype(str).str.upper() == "WIDE"][["race_id", "bet_key", "payout"]].copy()
        sub["payout_yen"] = to_float(sub["payout"])
        sub = sub[sub["payout_yen"].notna()].copy()

        def _norm(v: str) -> str:
            parts = [p for p in str(v).strip().split("-") if p]
            if len(parts) != 2:
                return ""
            try:
                return normalize_pair_key(int(parts[0]), int(parts[1]))
            except Exception:
                return ""

        sub["pair_key"] = sub["bet_key"].map(_norm)
        sub = sub[sub["pair_key"] != ""]
        frames.append(sub[["race_id", "pair_key", "payout_yen"]])

    if not frames:
        return pd.DataFrame(columns=["race_id", "pair_key", "payout_yen"])

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["race_id", "pair_key"], keep="last")
    return out.reset_index(drop=True)


def summarize_vs_label(df: pd.DataFrame, label_col: str, cols: list[str]) -> pd.DataFrame:
    rows = []
    for c in cols:
        for group_value, name in [(1, "勝ち"), (0, "負け")]:
            sub = df[df[label_col] == group_value]
            rows.append(
                {
                    "feature": c,
                    "group": name,
                    "count": int(len(sub)),
                    "mean": float(sub[c].mean()) if len(sub) else np.nan,
                    "median": float(sub[c].median()) if len(sub) else np.nan,
                }
            )
        win_mean = rows[-2]["mean"]
        lose_mean = rows[-1]["mean"]
        rows.append(
            {
                "feature": c,
                "group": "差分(勝ち-負け)",
                "count": int(len(df)),
                "mean": float(win_mean - lose_mean) if pd.notna(win_mean) and pd.notna(lose_mean) else np.nan,
                "median": np.nan,
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    args = parse_args()

    # 指定入力の存在確認（集計の参照元）
    _ = pd.read_csv(Path(args.roi), encoding=args.encoding, low_memory=False)

    race_df = pd.read_csv(Path(args.race), encoding=args.encoding, low_memory=False)
    wide_df = pd.read_csv(Path(args.wide), encoding=args.encoding, low_memory=False)
    score_df = pd.read_csv(Path(args.score), encoding=args.encoding, low_memory=False)

    for c in ["horse_no_1", "horse_no_2", "pair_rank_score", "pair_score_v1"]:
        wide_df[c] = to_float(wide_df[c])
    score_df["win_odds"] = to_float(score_df["win_odds"])

    # 条件は変更しない: wide_race_selection_2026_v1 の selected_by_best_filter があればそれを利用
    if "selected_by_best_filter" in race_df.columns:
        target_races = race_df[race_df["selected_by_best_filter"] == True].copy()  # noqa: E712
        if target_races.empty:
            target_races = race_df.copy()
            selection_note = "《推測》selected_by_best_filter が空のため全レースで分析"
        else:
            selection_note = "selected_by_best_filter=True のレースを分析"
    else:
        target_races = race_df.copy()
        selection_note = "《推測》選別列がないため全レースで分析"

    target_races["race_id_raw"] = target_races["race_id_raw"].astype(str)
    race_ids = set(target_races["race_id_raw"])

    wide_df["race_id_raw"] = wide_df["race_id_raw"].astype(str)
    wide_df = wide_df[wide_df["race_id_raw"].isin(race_ids)].copy()
    wide_df = wide_df[wide_df["horse_no_1"].notna() & wide_df["horse_no_2"].notna() & wide_df["pair_rank_score"].notna()]

    payout_df = load_wide_payouts(Path(args.payout_root))
    if payout_df.empty:
        raise SystemExit("No 2026 wide payout rows found.")

    wide_df["race_id"] = wide_df["race_id_raw"].map(raw_to_race_id)
    wide_df["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(wide_df["horse_no_1"], wide_df["horse_no_2"])]

    joined = wide_df.merge(payout_df, on=["race_id", "pair_key"], how="left")
    joined["return_yen"] = joined["payout_yen"].fillna(0.0)

    # win_odds平均（レース単位）
    score_df["race_id_raw"] = score_df["race_id_raw"].astype(str)
    odds_by_race = (
        score_df[score_df["race_id_raw"].isin(race_ids)]
        .groupby("race_id_raw", dropna=False)
        .agg(win_odds_mean=("win_odds", "mean"))
        .reset_index()
    )

    race_base_cols = [c for c in ["race_id_raw", "race_date", "value_score_max", "gap_std", "top1_top2_diff", "top1_pair_score_v1"] if c in target_races.columns]
    race_base = target_races[race_base_cols].drop_duplicates(subset=["race_id_raw"]).copy()
    race_base = race_base.merge(odds_by_race, on="race_id_raw", how="left")

    pattern_results: list[pd.DataFrame] = []
    for pattern, cond in [("top1", joined["pair_rank_score"] == 1), ("top3", joined["pair_rank_score"] <= 3)]:
        sub = joined[cond].copy()
        if sub.empty:
            continue

        race_profit = (
            sub.groupby("race_id_raw", dropna=False)
            .agg(
                races_bets=("pair_rank_score", "size"),
                payout_total=("return_yen", "sum"),
                pair_score_v1=("pair_score_v1", "max"),
                hit_any=("payout_yen", lambda s: int(s.notna().any())),
            )
            .reset_index()
        )
        race_profit["stake_total"] = race_profit["races_bets"] * args.stake
        race_profit["profit"] = race_profit["payout_total"] - race_profit["stake_total"]
        race_profit["is_win_race"] = (race_profit["profit"] > 0).astype(int)
        race_profit["pattern"] = pattern

        merged = race_profit.merge(race_base, on="race_id_raw", how="left")
        if "race_date" in merged.columns:
            merged["month"] = pd.to_datetime(merged["race_date"], errors="coerce").dt.strftime("%Y-%m")
        else:
            merged["month"] = ""
        pattern_results.append(merged)

    if not pattern_results:
        raise SystemExit("No race-level results generated.")

    out_df = pd.concat(pattern_results, ignore_index=True)

    feature_cols = [c for c in ["value_score_max", "gap_std", "top1_top2_diff", "pair_score_v1", "win_odds_mean"] if c in out_df.columns]

    report_lines: list[str] = []
    report_lines.append("wide loss analysis report (2026)")
    report_lines.append("")
    report_lines.append(f"input_roi={args.roi}")
    report_lines.append(f"input_race={args.race}")
    report_lines.append(f"output={args.out_csv}")
    report_lines.append(f"selection_scope={selection_note}")

    for pattern in sorted(out_df["pattern"].unique()):
        p_df = out_df[out_df["pattern"] == pattern].copy()
        win_cnt = int((p_df["is_win_race"] == 1).sum())
        lose_cnt = int((p_df["is_win_race"] == 0).sum())

        report_lines.append("")
        report_lines.append(f"[{pattern}] レース損益概要")
        report_lines.append(f"- races={len(p_df)} win_races={win_cnt} lose_races={lose_cnt}")
        report_lines.append(f"- avg_profit={float(p_df['profit'].mean()):.2f} median_profit={float(p_df['profit'].median()):.2f}")

        cmp_all = summarize_vs_label(p_df, "is_win_race", feature_cols)
        report_lines.append("- 勝ち vs 負け（平均・中央値）")
        report_lines.extend(cmp_all.to_string(index=False).splitlines())

        april = p_df[p_df["month"] == "2026-04"].copy()
        report_lines.append("")
        report_lines.append("- 2026-04 負けレース分析")
        if april.empty:
            report_lines.append("  2026-04 データなし")
        else:
            april_cmp = summarize_vs_label(april, "is_win_race", feature_cols)
            report_lines.extend(april_cmp.to_string(index=False).splitlines())
            lose_apr = april[april["is_win_race"] == 0]
            report_lines.append("  《推測》2026-04の負けレース共通点は、勝ち側との差分がマイナスに出る特徴量を優先確認")
            report_lines.append(f"  lose_races_2026_04={len(lose_apr)} / april_races={len(april)}")

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    out_df.to_csv(out_csv, index=False, encoding=args.encoding)
    out_report.write_text("\n".join(report_lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print("\n=== head ===")
    print(out_df.head(10).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
