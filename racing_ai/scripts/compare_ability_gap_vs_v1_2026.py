import argparse
from pathlib import Path

import numpy as np
import pandas as pd


MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
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
    parser = argparse.ArgumentParser(description="Compare ability_gap method vs v1 under aligned conditions.")
    parser.add_argument("--ability-roi", default=r"C:\TXT\wide_roi_with_ability_gap_v1.csv")
    parser.add_argument("--v1-roi", default=r"C:\TXT\wide_roi_real_2026_v1.csv")
    parser.add_argument("--v1-spec", default=r"C:\TXT\wide_v1_strategy_spec_2026.txt")
    parser.add_argument("--v1-monthly", default=r"C:\TXT\wide_roi_monthly_2026_v1.csv")
    parser.add_argument("--ability-wide", default=r"C:\TXT\wide_candidates_2026_ability_gap_v1.csv")
    parser.add_argument("--ability-race", default=r"C:\TXT\wide_race_selection_fixed_rate_2026_ability_gap_v1.csv")
    parser.add_argument("--v1-wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    parser.add_argument("--v1-race", default=r"C:\TXT\wide_race_selection_fixed_rate_2026_v1.csv")
    parser.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    parser.add_argument("--out-csv", default=r"C:\TXT\ability_gap_vs_v1_comparison_2026.csv")
    parser.add_argument("--out-report", default=r"C:\TXT\ability_gap_vs_v1_comparison_report.txt")
    parser.add_argument("--encoding", default="cp932")
    parser.add_argument("--stake", type=int, default=100)
    return parser.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def normalize_pair_key(a: int, b: int) -> str:
    x, y = sorted((int(a), int(b)))
    return f"{x:02d}-{y:02d}"


def raw_to_race_id(raw: str) -> str:
    s = str(raw).split(".")[0].zfill(16)
    date = s[:8]
    jyo = s[8:10]
    race_no = int(s[-2:])
    venue = JYO_TO_VENUE.get(jyo, "UNK")
    return f"{date}-{venue}-{race_no:02d}R"


def load_payouts(root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for fp in root.rglob("payouts.csv"):
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


def build_selected_bets(wide_path: Path, race_path: Path, pattern: str, encoding: str) -> pd.DataFrame:
    wide = pd.read_csv(wide_path, encoding=encoding, low_memory=False)
    race = pd.read_csv(race_path, encoding=encoding, low_memory=False)

    wide["race_id_raw"] = wide["race_id_raw"].astype(str)
    wide["pair_rank_score"] = to_float(wide["pair_rank_score"])
    wide["horse_no_1"] = to_float(wide["horse_no_1"])
    wide["horse_no_2"] = to_float(wide["horse_no_2"])
    wide = wide[wide["pair_rank_score"].notna() & wide["horse_no_1"].notna() & wide["horse_no_2"].notna()].copy()

    race["race_id_raw"] = race["race_id_raw"].astype(str)
    selected_ids = set(race.loc[race["selected_top15"] == True, "race_id_raw"])  # noqa: E712
    wide = wide[wide["race_id_raw"].isin(selected_ids)].copy()

    if pattern == "top1":
        wide = wide[wide["pair_rank_score"] == 1].copy()
    else:
        wide = wide[wide["pair_rank_score"] <= 3].copy()

    wide["horse_no_1"] = wide["horse_no_1"].astype(int)
    wide["horse_no_2"] = wide["horse_no_2"].astype(int)
    wide["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(wide["horse_no_1"], wide["horse_no_2"])]
    wide["race_id"] = wide["race_id_raw"].map(raw_to_race_id)
    return wide[["race_id_raw", "race_id", "pair_key", "race_date"]].drop_duplicates().reset_index(drop=True)


def summarize_switch(df: pd.DataFrame, stake: int, label: str) -> dict:
    bets = len(df)
    races = df["race_id_raw"].nunique() if bets else 0
    stake_total = bets * stake
    ret = float(df["payout_yen"].fillna(0.0).sum()) if bets else 0.0
    hit = float(df["payout_yen"].notna().mean()) if bets else np.nan
    roi = float((ret / stake_total) * 100.0) if stake_total else np.nan
    return {
        "section": "switch",
        "item": label,
        "races": int(races),
        "bets": int(bets),
        "hit_rate": hit,
        "roi_pct": roi,
        "return_total": ret,
        "stake_total": int(stake_total),
    }


def stability_from_monthly(monthly_df: pd.DataFrame, pattern: str) -> tuple[float, float]:
    sub = monthly_df[(monthly_df["pattern"] == pattern) & (monthly_df["month"] != "ALL")].copy()
    vals = sub["roi_pct"].astype(float).dropna()
    if vals.empty:
        return np.nan, np.nan
    return float(vals.min()), float(vals.std(ddof=0))


def main() -> int:
    args = parse_args()

    ability = pd.read_csv(Path(args.ability_roi), encoding=args.encoding, low_memory=False)
    v1 = pd.read_csv(Path(args.v1_roi), encoding=args.encoding, low_memory=False)
    v1_monthly = pd.read_csv(Path(args.v1_monthly), encoding=args.encoding, low_memory=False)

    ability_all = ability[ability["month"] == "ALL"].copy()
    if ability_all.empty:
        raise SystemExit("ability ROI file missing month=ALL rows.")

    v1_all = v1.copy()
    if "month" in v1_all.columns:
        v1_all = v1_all[v1_all["month"] == "ALL"].copy()
    needed = {"pattern", "roi_pct", "hit_rate", "races", "bets"}
    if not needed.issubset(v1_all.columns):
        raise SystemExit(f"v1 ROI missing required columns: {sorted(needed - set(v1_all.columns))}")

    rows = []
    for pattern in ["top1", "top3"]:
        n = ability_all[ability_all["pattern"] == pattern].iloc[0]
        o = v1_all[v1_all["pattern"] == pattern].iloc[0]
        n_worst, n_std = stability_from_monthly(ability, pattern)
        o_worst, o_std = stability_from_monthly(v1_monthly, pattern)
        rows.append(
            {
                "section": "summary",
                "item": pattern,
                "roi_pct_new": float(n["roi_pct"]),
                "roi_pct_old": float(o["roi_pct"]),
                "roi_diff": float(n["roi_pct"] - o["roi_pct"]),
                "hit_rate_new": float(n["hit_rate"]),
                "hit_rate_old": float(o["hit_rate"]),
                "hit_diff": float(n["hit_rate"] - o["hit_rate"]),
                "worst_month_new": n_worst,
                "worst_month_old": o_worst,
                "worst_month_diff": n_worst - o_worst if pd.notna(n_worst) and pd.notna(o_worst) else np.nan,
                "roi_std_new": n_std,
                "roi_std_old": o_std,
                "roi_std_diff": n_std - o_std if pd.notna(n_std) and pd.notna(o_std) else np.nan,
                "races_new": int(n["races"]),
                "races_old": int(o["races"]),
                "bets_new": int(n["bets"]),
                "bets_old": int(o["bets"]),
            }
        )

    payout = load_payouts(Path(args.payout_root))
    if payout.empty:
        raise SystemExit("No payout data found for switch analysis.")

    switch_rows = []
    for pattern in ["top1", "top3"]:
        old_bets = build_selected_bets(Path(args.v1_wide), Path(args.v1_race), pattern, args.encoding)
        new_bets = build_selected_bets(Path(args.ability_wide), Path(args.ability_race), pattern, args.encoding)

        old_key = old_bets.assign(_k=old_bets["race_id_raw"] + "_" + old_bets["pair_key"])
        new_key = new_bets.assign(_k=new_bets["race_id_raw"] + "_" + new_bets["pair_key"])

        dropped = old_key[~old_key["_k"].isin(set(new_key["_k"]))].drop(columns=["_k"])
        added = new_key[~new_key["_k"].isin(set(old_key["_k"]))].drop(columns=["_k"])

        dropped = dropped.merge(payout, on=["race_id", "pair_key"], how="left")
        added = added.merge(payout, on=["race_id", "pair_key"], how="left")

        d = summarize_switch(dropped, args.stake, f"{pattern}_dropped_from_v1")
        a = summarize_switch(added, args.stake, f"{pattern}_added_by_ability")
        switch_rows.extend([d, a])

    switch_df = pd.DataFrame(switch_rows)
    summary_df = pd.DataFrame(rows)

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    combined = pd.concat([summary_df, switch_df], ignore_index=True, sort=False)
    combined.to_csv(out_csv, index=False, encoding=args.encoding)

    report_lines: list[str] = []
    report_lines.append("ability_gap vs v1 comparison report (2026)")
    report_lines.append("")
    report_lines.append(f"input_ability={args.ability_roi}")
    report_lines.append(f"input_v1={args.v1_roi}")
    report_lines.append(f"input_v1_spec={args.v1_spec}")
    report_lines.append("")
    report_lines.append("summary metrics")
    report_lines.extend(summary_df.to_string(index=False).splitlines())
    report_lines.append("")

    for pattern in ["top1", "top3"]:
        r = summary_df[summary_df["item"] == pattern].iloc[0]
        decision = "REJECT"
        if r["roi_diff"] > 0 and r["worst_month_diff"] >= 0 and r["roi_std_diff"] <= 0:
            decision = "ADOPT"
        elif r["roi_diff"] > 0:
            decision = "CONDITIONAL"
        report_lines.append(f"{pattern} decision={decision}")
        report_lines.append(
            f"- ROI diff={r['roi_diff']:+.6f}, hit diff={r['hit_diff']:+.6f}, "
            f"worst_month diff={r['worst_month_diff']:+.6f}, roi_std diff={r['roi_std_diff']:+.6f}"
        )
        report_lines.append("")

    report_lines.append("switched bets (dropped/added)")
    report_lines.extend(switch_df.to_string(index=False).splitlines())
    report_lines.append("")
    report_lines.append("monthly ROI comparison")
    report_lines.append("[v1]")
    report_lines.extend(v1_monthly.to_string(index=False).splitlines())
    report_lines.append("")
    report_lines.append("[ability_gap]")
    report_lines.extend(ability[ability["month"] != "ALL"].to_string(index=False).splitlines())
    report_lines.append("")
    report_lines.append("《推測》2026単年比較は過剰最適化リスクがあるため、採用判断は複数年検証で再確認が必要。")

    out_report.write_text("\n".join(report_lines) + "\n", encoding=args.encoding)
    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
