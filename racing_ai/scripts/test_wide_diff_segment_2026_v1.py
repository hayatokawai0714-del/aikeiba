import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd

MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
JYO_TO_VENUE = {
    "01": "SAP", "02": "HAK", "03": "FUK", "04": "NII", "05": "TOK",
    "06": "NAK", "07": "CHU", "08": "KYO", "09": "HAN", "10": "KOK",
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Test conditional usefulness of diff in wide race selection (2026).")
    ap.add_argument("--race", default=r"C:\TXT\wide_race_selection_2026_v1.csv")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--roi", default=r"C:\TXT\wide_roi_real_2026_v1.csv")
    ap.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_diff_segment_test_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_diff_segment_report_2026_v1.txt")
    ap.add_argument("--encoding", default="cp932")
    ap.add_argument("--stake", type=int, default=100)
    ap.add_argument("--retain-rate", type=float, default=0.15)
    return ap.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def zscore(series: pd.Series) -> pd.Series:
    std = float(series.std(ddof=0))
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=series.index)
    return (series - float(series.mean())) / std


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


def load_wide_payouts(root: Path) -> pd.DataFrame:
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


def summarize(sub: pd.DataFrame, stake: int) -> dict:
    races = int(sub["race_id_raw"].nunique()) if len(sub) else 0
    bets = int(len(sub))
    stake_total = bets * stake
    ret = float(sub["return_yen"].sum()) if len(sub) else 0.0
    roi = float((ret / stake_total) * 100.0) if stake_total else np.nan
    hit = float(sub["wide_hit_real"].mean()) if len(sub) else np.nan

    month_roi = {}
    for m in MONTHS:
        ms = sub[sub["race_month"] == m]
        ms_stake = len(ms) * stake
        month_roi[m] = float((ms["return_yen"].sum() / ms_stake) * 100.0) if ms_stake else np.nan

    return {
        "races": races,
        "bets": bets,
        "hit_rate": hit,
        "roi_pct": roi,
        "avg_payout_yen": float(sub.loc[sub["wide_hit_real"] == 1, "payout_yen"].mean()) if (len(sub) and (sub["wide_hit_real"] == 1).any()) else np.nan,
        "roi_2026_01": month_roi["2026-01"],
        "roi_2026_02": month_roi["2026-02"],
        "roi_2026_03": month_roi["2026-03"],
        "roi_2026_04": month_roi["2026-04"],
    }


def main() -> int:
    args = parse_args()

    _ = pd.read_csv(Path(args.roi), encoding=args.encoding, low_memory=False)

    race = pd.read_csv(Path(args.race), encoding=args.encoding, low_memory=False)
    wide = pd.read_csv(Path(args.wide), encoding=args.encoding, low_memory=False)

    required = ["race_id_raw", "race_date", "value_score_max", "gap_std", "top1_top2_diff", "horse_count"]
    miss = [c for c in required if c not in race.columns]
    if miss:
        raise SystemExit(f"Missing race columns: {miss}")

    race = race.copy()
    race["race_id_raw"] = race["race_id_raw"].astype(str)
    for c in ["value_score_max", "gap_std", "top1_top2_diff", "horse_count"]:
        race[c] = to_float(race[c])

    race = race[race["value_score_max"].notna() & race["gap_std"].notna() & race["top1_top2_diff"].notna() & race["horse_count"].notna()].copy()

    race["z_value_score_max"] = zscore(race["value_score_max"])
    race["z_gap_std"] = zscore(race["gap_std"])
    race["z_top1_top2_diff"] = zscore(race["top1_top2_diff"])

    # v1 / v2 (w_diff=0.05 fixed)
    race["score_v1"] = 0.6 * race["z_value_score_max"] + 0.4 * race["z_gap_std"]
    race["score_v2"] = 0.55 * race["z_value_score_max"] + 0.40 * race["z_gap_std"] + 0.05 * race["z_top1_top2_diff"]

    gap_med = float(race["gap_std"].median())
    val_med = float(race["value_score_max"].median())
    field_med = float(race["horse_count"].median())

    segments = {
        "all": pd.Series(True, index=race.index),
        "gap_std_high": race["gap_std"] > gap_med,
        "gap_std_low": race["gap_std"] <= gap_med,
        "value_score_max_high": race["value_score_max"] > val_med,
        "value_score_max_low": race["value_score_max"] <= val_med,
        "field_size_high": race["horse_count"] > field_med,
        "field_size_low": race["horse_count"] <= field_med,
    }

    wide = wide.copy()
    wide["race_id_raw"] = wide["race_id_raw"].astype(str)
    for c in ["horse_no_1", "horse_no_2", "pair_rank_score"]:
        wide[c] = to_float(wide[c])
    wide = wide[wide["horse_no_1"].notna() & wide["horse_no_2"].notna() & wide["pair_rank_score"].notna()].copy()

    race_dates = race[["race_id_raw", "race_date"]].drop_duplicates()
    pairs = wide.merge(race_dates, on="race_id_raw", how="inner", suffixes=("", "_sel"))
    if "race_date" not in pairs.columns and "race_date_sel" in pairs.columns:
        pairs["race_date"] = pairs["race_date_sel"]

    pairs["race_id"] = pairs["race_id_raw"].map(raw_to_race_id)
    pairs["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(pairs["horse_no_1"], pairs["horse_no_2"])]

    payout = load_wide_payouts(Path(args.payout_root))
    if payout.empty:
        raise SystemExit("No 2026 wide payout data")

    pairs = pairs.merge(payout, on=["race_id", "pair_key"], how="left")
    pairs["wide_hit_real"] = pairs["payout_yen"].notna().astype(int)
    pairs["return_yen"] = pairs["payout_yen"].fillna(0.0)
    pairs["race_month"] = pd.to_datetime(pairs["race_date"], errors="coerce").dt.strftime("%Y-%m")

    out_rows = []

    for seg_name, seg_mask in segments.items():
        seg_race = race[seg_mask].copy()
        if seg_race.empty:
            continue

        keep_n = max(1, int(math.ceil(len(seg_race) * args.retain_rate)))
        for version, score_col in [("v1", "score_v1"), ("v2", "score_v2")]:
            selected = seg_race.sort_values(score_col, ascending=False).head(keep_n)
            ids = set(selected["race_id_raw"])
            sub_pairs = pairs[pairs["race_id_raw"].isin(ids)].copy()

            for pattern, cond in [("top1", sub_pairs["pair_rank_score"] == 1), ("top3", sub_pairs["pair_rank_score"] <= 3)]:
                eval_df = sub_pairs[cond].copy()
                stats = summarize(eval_df, args.stake)
                out_rows.append(
                    {
                        "segment": seg_name,
                        "version": version,
                        "pattern": pattern,
                        "segment_races": int(len(seg_race)),
                        "selected_races_target": keep_n,
                        "gap_std_median": gap_med,
                        "value_score_max_median": val_med,
                        "field_size_median": field_med,
                        **stats,
                    }
                )

    out_df = pd.DataFrame(out_rows)

    # comparison table v2-v1
    pv = out_df.pivot_table(
        index=["segment", "pattern", "segment_races", "selected_races_target"],
        columns="version",
        values=["roi_pct", "hit_rate", "races", "roi_2026_04"],
        aggfunc="first",
    )
    pv.columns = [f"{a}_{b}" for a, b in pv.columns]
    pv = pv.reset_index()
    for metric in ["roi_pct", "hit_rate", "races", "roi_2026_04"]:
        v1_col = f"{metric}_v1"
        v2_col = f"{metric}_v2"
        if v1_col in pv.columns and v2_col in pv.columns:
            pv[f"{metric}_diff_v2_minus_v1"] = pv[v2_col] - pv[v1_col]

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    final_out = out_df.merge(
        pv[[
            "segment", "pattern",
            "roi_pct_diff_v2_minus_v1",
            "hit_rate_diff_v2_minus_v1",
            "roi_2026_04_diff_v2_minus_v1",
        ]],
        on=["segment", "pattern"],
        how="left",
    )
    final_out.to_csv(out_csv, index=False, encoding=args.encoding)

    improve = pv[(pv.get("roi_pct_diff_v2_minus_v1", 0) > 0) & (pv.get("roi_2026_04_diff_v2_minus_v1", 0) > 0)]
    worsen = pv[pv.get("roi_pct_diff_v2_minus_v1", 0) < 0]

    lines = []
    lines.append("wide diff segment test report (2026)")
    lines.append("")
    lines.append(f"input_race={args.race}")
    lines.append(f"input_wide={args.wide}")
    lines.append(f"input_roi={args.roi}")
    lines.append(f"output={out_csv}")
    lines.append("")
    lines.append("settings")
    lines.append(f"- retain_rate={args.retain_rate:.2%} (segment内上位採用)")
    lines.append("- v1=0.60*z(value_score_max)+0.40*z(gap_std)")
    lines.append("- v2=0.55*z(value_score_max)+0.40*z(gap_std)+0.05*z(top1_top2_diff)")
    lines.append("- 《推測》field_sizeは raceの horse_count を代理利用")
    lines.append("")
    lines.append("segment medians")
    lines.append(f"- gap_std_median={gap_med:.6f}")
    lines.append(f"- value_score_max_median={val_med:.6f}")
    lines.append(f"- field_size_median={field_med:.6f}")
    lines.append("")
    lines.append("comparison (v2-v1)")
    lines.extend(pv.to_string(index=False).splitlines())
    lines.append("")
    lines.append("diffが効く条件")
    if len(improve):
        lines.extend(improve[["segment", "pattern", "roi_pct_diff_v2_minus_v1", "roi_2026_04_diff_v2_minus_v1"]].to_string(index=False).splitlines())
    else:
        lines.append("- 該当なし")
    lines.append("")
    lines.append("diffが悪化する条件")
    if len(worsen):
        lines.extend(worsen[["segment", "pattern", "roi_pct_diff_v2_minus_v1", "roi_2026_04_diff_v2_minus_v1"]].to_string(index=False).splitlines())
    else:
        lines.append("- 該当なし")
    lines.append("")
    lines.append("実運用ルール提案")
    lines.append("《推測》v2がROI改善するセグメントのみに diff を有効化し、それ以外はv1を維持する条件分岐が有効です。")

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print("\n=== comparison ===")
    print(pv.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
