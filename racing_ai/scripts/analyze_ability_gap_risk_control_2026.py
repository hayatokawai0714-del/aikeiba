import argparse
import math
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
    parser = argparse.ArgumentParser(description="Risk control analysis for ability_gap method.")
    parser.add_argument("--ability-roi", default=r"C:\TXT\wide_roi_with_ability_gap_v1.csv")
    parser.add_argument("--compare-csv", default=r"C:\TXT\ability_gap_vs_v1_comparison_2026.csv")
    parser.add_argument("--ability-wide", default=r"C:\TXT\wide_candidates_2026_ability_gap_v1.csv")
    parser.add_argument("--ability-race", default=r"C:\TXT\wide_race_selection_fixed_rate_2026_ability_gap_v1.csv")
    parser.add_argument("--dataset", default=r"C:\TXT\dataset_top3_with_history_phase1.csv")
    parser.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    parser.add_argument("--out-csv", default=r"C:\TXT\ability_gap_risk_control_2026.csv")
    parser.add_argument("--out-report", default=r"C:\TXT\ability_gap_risk_control_report_2026.txt")
    parser.add_argument("--encoding", default="cp932")
    parser.add_argument("--stake", type=int, default=100)
    return parser.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def normalize_race_id_raw(v: object) -> str:
    return str(v).split(".")[0]


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
    frames = []
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


def prepare_bets(args: argparse.Namespace) -> pd.DataFrame:
    wide = pd.read_csv(Path(args.ability_wide), encoding=args.encoding, low_memory=False)
    race = pd.read_csv(Path(args.ability_race), encoding=args.encoding, low_memory=False)
    data = pd.read_csv(Path(args.dataset), encoding=args.encoding, low_memory=False)
    payout = load_payouts(Path(args.payout_root))
    if payout.empty:
        raise SystemExit("No payout data found.")

    wide["race_id_raw"] = wide["race_id_raw"].map(normalize_race_id_raw)
    for c in ["horse_no_1", "horse_no_2", "pair_rank_score", "pair_value_score_v1"]:
        wide[c] = to_float(wide[c])
    wide = wide[
        wide["horse_no_1"].notna() & wide["horse_no_2"].notna() & wide["pair_rank_score"].notna() & wide["pair_value_score_v1"].notna()
    ].copy()
    wide["horse_no_1"] = wide["horse_no_1"].astype(int)
    wide["horse_no_2"] = wide["horse_no_2"].astype(int)

    race["race_id_raw"] = race["race_id_raw"].map(normalize_race_id_raw)
    selected_ids = set(race.loc[race["selected_top15"] == True, "race_id_raw"])  # noqa: E712
    race_feat = race[["race_id_raw", "gap_std"]].drop_duplicates()
    race_feat["gap_std"] = to_float(race_feat["gap_std"])

    bets = wide[wide["race_id_raw"].isin(selected_ids)].copy()
    bets = bets.merge(race_feat, on="race_id_raw", how="left")

    data["race_date"] = pd.to_datetime(data["race_date"], errors="coerce")
    data = data[data["race_date"].dt.year == 2026].copy()
    data["race_id_raw"] = data["race_id_raw"].map(normalize_race_id_raw)
    data["horse_no"] = to_float(data["horse_no"]).astype("Int64")
    data["win_odds"] = to_float(data["win_odds"])
    odds = data[["race_id_raw", "horse_no", "win_odds"]].drop_duplicates()

    o1 = odds.rename(columns={"horse_no": "horse_no_1", "win_odds": "win_odds_1"})
    o2 = odds.rename(columns={"horse_no": "horse_no_2", "win_odds": "win_odds_2"})
    bets = bets.merge(o1, on=["race_id_raw", "horse_no_1"], how="left")
    bets = bets.merge(o2, on=["race_id_raw", "horse_no_2"], how="left")

    bets["pair_odds_geo"] = np.sqrt(bets["win_odds_1"] * bets["win_odds_2"])
    bets["race_id"] = bets["race_id_raw"].map(raw_to_race_id)
    bets["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(bets["horse_no_1"], bets["horse_no_2"])]
    bets = bets.merge(payout, on=["race_id", "pair_key"], how="left")
    bets["wide_hit_real"] = bets["payout_yen"].notna().astype(int)
    bets["return_yen"] = bets["payout_yen"].fillna(0.0)
    bets["race_month"] = pd.to_datetime(bets["race_date"], errors="coerce").dt.strftime("%Y-%m")

    q90 = float(bets["pair_value_score_v1"].quantile(0.90))
    q80 = float(bets["pair_value_score_v1"].quantile(0.80))
    bets["value_gap_seg"] = "other80%"
    bets.loc[bets["pair_value_score_v1"] >= q80, "value_gap_seg"] = "top20%"
    bets.loc[bets["pair_value_score_v1"] >= q90, "value_gap_seg"] = "top10%"

    bets["odds_seg"] = "20+"
    bets.loc[bets["pair_odds_geo"] < 20, "odds_seg"] = "5-20"
    bets.loc[bets["pair_odds_geo"] < 5, "odds_seg"] = "~5"

    gap_median = float(bets["gap_std"].median())
    bets["gap_std_seg"] = np.where(bets["gap_std"] >= gap_median, "high", "low")
    return bets


def calc_metrics(df: pd.DataFrame, stake: int) -> dict:
    bets = len(df)
    races = int(df["race_id_raw"].nunique()) if bets else 0
    stake_total = bets * stake
    ret = float(df["return_yen"].sum()) if bets else 0.0
    roi = float((ret / stake_total) * 100.0) if stake_total else np.nan
    hit = float(df["wide_hit_real"].mean()) if bets else np.nan
    ret_var = float(df["return_yen"].var(ddof=0)) if bets else np.nan

    month_vals = []
    for month in MONTHS:
        mdf = df[df["race_month"] == month]
        m_stake = len(mdf) * stake
        m_roi = float((mdf["return_yen"].sum() / m_stake) * 100.0) if m_stake else np.nan
        month_vals.append(m_roi)
    arr = np.array([x for x in month_vals if pd.notna(x)], dtype=float)
    worst = float(arr.min()) if len(arr) else np.nan
    std = float(arr.std(ddof=0)) if len(arr) else np.nan
    return {
        "races": races,
        "bets": int(bets),
        "hit_rate": hit,
        "roi_pct": roi,
        "return_var": ret_var,
        "worst_month_roi": worst,
        "roi_monthly_std": std,
    }


def segment_metrics(bets: pd.DataFrame, pattern: str, dim: str, stake: int) -> pd.DataFrame:
    if pattern == "top1":
        sub = bets[bets["pair_rank_score"] == 1].copy()
    else:
        sub = bets[bets["pair_rank_score"] <= 3].copy()
    rows = []
    for key, g in sub.groupby(dim, dropna=False):
        m = calc_metrics(g, stake)
        rows.append({"section": "segment", "pattern": pattern, "dimension": dim, "segment": key, **m})
    return pd.DataFrame(rows)


def evaluate_filters(bets: pd.DataFrame, stake: int) -> pd.DataFrame:
    filter_defs = {
        "baseline": np.ones(len(bets), dtype=bool),
        "exclude_value_top10": bets["value_gap_seg"] != "top10%",
        "exclude_odds_20plus": bets["odds_seg"] != "20+",
        "exclude_low_gapstd": bets["gap_std_seg"] != "low",
        "safe_combo": (bets["value_gap_seg"] != "top10%") & (bets["odds_seg"] != "20+") & (bets["gap_std_seg"] != "low"),
    }
    rows = []
    for name, mask in filter_defs.items():
        f = bets[mask].copy()
        for pattern in ["top1", "top3"]:
            if pattern == "top1":
                p = f[f["pair_rank_score"] == 1].copy()
            else:
                p = f[f["pair_rank_score"] <= 3].copy()
            m = calc_metrics(p, stake)
            rows.append({"section": "filter", "filter_name": name, "pattern": pattern, **m})
    return pd.DataFrame(rows)


def choose_filter(filter_df: pd.DataFrame) -> str:
    pivot = filter_df.pivot_table(index="filter_name", columns="pattern", values=["roi_pct", "hit_rate", "roi_monthly_std"], aggfunc="first")
    pivot.columns = [f"{a}_{b}" for a, b in pivot.columns]
    pivot = pivot.reset_index()
    base = pivot[pivot["filter_name"] == "baseline"].iloc[0]
    pivot["score"] = (
        (pivot["roi_pct_top1"] - base["roi_pct_top1"]) * 2.0
        + (pivot["hit_rate_top1"] - base["hit_rate_top1"]) * 100.0
        + (base["roi_monthly_std_top3"] - pivot["roi_monthly_std_top3"]) * 0.5
    )
    pivot = pivot.sort_values("score", ascending=False).reset_index(drop=True)
    return str(pivot.iloc[0]["filter_name"])


def main() -> int:
    args = parse_args()
    _ = pd.read_csv(Path(args.ability_roi), encoding=args.encoding, low_memory=False)
    _ = pd.read_csv(Path(args.compare_csv), encoding=args.encoding, low_memory=False)

    bets = prepare_bets(args)

    seg_frames = []
    for pattern in ["top1", "top3"]:
        seg_frames.append(segment_metrics(bets, pattern, "value_gap_seg", args.stake))
        seg_frames.append(segment_metrics(bets, pattern, "odds_seg", args.stake))
        seg_frames.append(segment_metrics(bets, pattern, "gap_std_seg", args.stake))
    seg_df = pd.concat(seg_frames, ignore_index=True)

    filter_df = evaluate_filters(bets, args.stake)
    best_filter = choose_filter(filter_df)
    best_rows = filter_df[filter_df["filter_name"] == best_filter].copy()
    base_rows = filter_df[filter_df["filter_name"] == "baseline"].copy()
    merged = best_rows.merge(
        base_rows[["pattern", "roi_pct", "hit_rate", "roi_monthly_std", "worst_month_roi"]],
        on="pattern",
        suffixes=("_best", "_base"),
    )
    merged["roi_diff"] = merged["roi_pct_best"] - merged["roi_pct_base"]
    merged["hit_diff"] = merged["hit_rate_best"] - merged["hit_rate_base"]
    merged["std_diff"] = merged["roi_monthly_std_best"] - merged["roi_monthly_std_base"]
    merged["worst_diff"] = merged["worst_month_roi_best"] - merged["worst_month_roi_base"]

    seg_with_flag = seg_df.copy()
    seg_with_flag["risk_flag"] = (
        (seg_with_flag["roi_pct"] >= seg_with_flag.groupby(["pattern", "dimension"])["roi_pct"].transform("mean"))
        & (seg_with_flag["hit_rate"] < seg_with_flag.groupby(["pattern", "dimension"])["hit_rate"].transform("mean"))
    ) | (
        seg_with_flag["return_var"] >= seg_with_flag.groupby(["pattern", "dimension"])["return_var"].transform("quantile", 0.75)
    ) | (seg_with_flag["worst_month_roi"] < 60.0)

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    out_df = pd.concat([seg_with_flag, filter_df], ignore_index=True, sort=False)
    out_df.to_csv(out_csv, index=False, encoding=args.encoding)

    report_lines: list[str] = []
    report_lines.append("ability_gap risk control report (2026)")
    report_lines.append("")
    report_lines.append(f"input_ability_roi={args.ability_roi}")
    report_lines.append(f"input_compare={args.compare_csv}")
    report_lines.append("")
    report_lines.append("1) segment metrics")
    report_lines.extend(seg_with_flag.to_string(index=False).splitlines())
    report_lines.append("")
    report_lines.append("2) dangerous segments")
    danger = seg_with_flag[seg_with_flag["risk_flag"] == True].copy()  # noqa: E712
    if danger.empty:
        report_lines.append("none")
    else:
        report_lines.extend(danger.to_string(index=False).splitlines())
    report_lines.append("")
    report_lines.append("3) filter candidates")
    report_lines.extend(filter_df.to_string(index=False).splitlines())
    report_lines.append("")
    report_lines.append("4) selected filter and re-evaluation")
    report_lines.append(f"selected_filter={best_filter}")
    report_lines.extend(merged.to_string(index=False).splitlines())
    report_lines.append("")
    report_lines.append("《推測》filter選定は top1維持を最優先し、top3の月次分散抑制を副目的としてスコア化。")
    report_lines.append("《推測》2026単年最適化回避のため、複数年で同フィルタの再検証が必要。")

    out_report.write_text("\n".join(report_lines) + "\n", encoding=args.encoding)
    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print(f"selected_filter={best_filter}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
