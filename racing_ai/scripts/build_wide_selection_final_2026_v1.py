import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd

MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
RATES = [0.15, 0.20]
JYO_TO_VENUE = {
    "01": "SAP", "02": "HAK", "03": "FUK", "04": "NII", "05": "TOK",
    "06": "NAK", "07": "CHU", "08": "KYO", "09": "HAN", "10": "KOK",
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build final conditional wide selection score and evaluate ROI.")
    ap.add_argument("--race", default=r"C:\TXT\wide_race_selection_2026_v1.csv")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_selection_final_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_selection_final_report_2026_v1.txt")
    ap.add_argument("--encoding", default="cp932")
    ap.add_argument("--stake", type=int, default=100)
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

    month_roi = {}
    for m in MONTHS:
        ms = sub[sub["race_month"] == m].copy()
        ms_stake = len(ms) * stake
        month_roi[m] = float((ms["return_yen"].sum() / ms_stake) * 100.0) if ms_stake else np.nan

    vals = np.array([v for v in month_roi.values() if pd.notna(v)], dtype=float)
    worst = float(vals.min()) if len(vals) else np.nan
    std = float(vals.std(ddof=0)) if len(vals) else np.nan

    return {
        "races": races,
        "bets": bets,
        "hit_rate": float(sub["wide_hit_real"].mean()) if len(sub) else np.nan,
        "roi_pct": roi,
        "worst_month_roi": worst,
        "roi_monthly_std": std,
        "roi_2026_01": month_roi["2026-01"],
        "roi_2026_02": month_roi["2026-02"],
        "roi_2026_03": month_roi["2026-03"],
        "roi_2026_04": month_roi["2026-04"],
    }


def evaluate(race_df: pd.DataFrame, pairs: pd.DataFrame, score_col: str, rate: float, pattern: str, stake: int) -> dict:
    keep_n = int(math.ceil(len(race_df) * rate))
    selected = race_df.sort_values(score_col, ascending=False).head(keep_n)
    ids = set(selected["race_id_raw"])

    sub = pairs[pairs["race_id_raw"].isin(ids)].copy()
    if pattern == "top1":
        sub = sub[sub["pair_rank_score"] == 1]
    else:
        sub = sub[sub["pair_rank_score"] <= 3]

    stats = summarize(sub, stake)
    return {
        "score_type": score_col,
        "rate": rate,
        "pattern": pattern,
        "retained_rate": float(keep_n / len(race_df)),
        **stats,
    }


def main() -> int:
    args = parse_args()

    race = pd.read_csv(Path(args.race), encoding=args.encoding, low_memory=False)
    wide = pd.read_csv(Path(args.wide), encoding=args.encoding, low_memory=False)

    need = ["race_id_raw", "race_date", "value_score_max", "gap_std", "top1_top2_diff"]
    missing = [c for c in need if c not in race.columns]
    if missing:
        raise SystemExit(f"Missing columns in race input: {missing}")

    race = race.copy()
    race["race_id_raw"] = race["race_id_raw"].astype(str)
    for c in ["value_score_max", "gap_std", "top1_top2_diff"]:
        race[c] = to_float(race[c])
    race = race[race["value_score_max"].notna() & race["gap_std"].notna() & race["top1_top2_diff"].notna()].copy()

    race["z_value_score_max"] = zscore(race["value_score_max"])
    race["z_gap_std"] = zscore(race["gap_std"])
    race["z_top1_top2_diff"] = zscore(race["top1_top2_diff"])

    gap_median = float(race["gap_std"].median())
    value_q70 = float(race["value_score_max"].quantile(0.70))

    race["score_v1"] = 0.55 * race["z_value_score_max"] + 0.45 * race["z_gap_std"]
    race["score_v2"] = 0.52 * race["z_value_score_max"] + 0.40 * race["z_gap_std"] + 0.08 * race["z_top1_top2_diff"]

    race["use_diff"] = (race["gap_std"] < gap_median) & (race["value_score_max"] < value_q70)
    race["score_final"] = np.where(race["use_diff"], race["score_v2"], race["score_v1"])

    race["rank_v1"] = race["score_v1"].rank(method="first", ascending=False).astype(int)
    race["rank_final"] = race["score_final"].rank(method="first", ascending=False).astype(int)
    race["selected_final_top15"] = race["rank_final"] <= int(math.ceil(len(race) * 0.15))
    race["selected_final_top20"] = race["rank_final"] <= int(math.ceil(len(race) * 0.20))

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
        raise SystemExit("No 2026 payout data found")

    pairs = pairs.merge(payout, on=["race_id", "pair_key"], how="left")
    pairs["wide_hit_real"] = pairs["payout_yen"].notna().astype(int)
    pairs["return_yen"] = pairs["payout_yen"].fillna(0.0)
    pairs["race_month"] = pd.to_datetime(pairs["race_date"], errors="coerce").dt.strftime("%Y-%m")

    eval_rows = []
    for rate in RATES:
        for pattern in ["top1", "top3"]:
            eval_rows.append(evaluate(race, pairs, "score_v1", rate, pattern, args.stake))
            eval_rows.append(evaluate(race, pairs, "score_final", rate, pattern, args.stake))

    eval_df = pd.DataFrame(eval_rows)

    pv = eval_df.pivot_table(index=["rate", "pattern"], columns="score_type", values=["roi_pct", "worst_month_roi", "roi_monthly_std", "roi_2026_04"], aggfunc="first")
    pv.columns = [f"{a}_{b}" for a, b in pv.columns]
    pv = pv.reset_index()
    for m in ["roi_pct", "worst_month_roi", "roi_monthly_std", "roi_2026_04"]:
        if f"{m}_score_v1" in pv.columns and f"{m}_score_final" in pv.columns:
            pv[f"{m}_diff_final_minus_v1"] = pv[f"{m}_score_final"] - pv[f"{m}_score_v1"]

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    race_out = race[
        [
            "race_date", "race_id_raw", "value_score_max", "gap_std", "top1_top2_diff",
            "z_value_score_max", "z_gap_std", "z_top1_top2_diff",
            "score_v1", "score_v2", "use_diff", "score_final", "rank_v1", "rank_final",
            "selected_final_top15", "selected_final_top20",
        ]
    ].copy()
    race_out.to_csv(out_csv, index=False, encoding=args.encoding)

    lines = []
    lines.append("wide final conditional selection report (2026)")
    lines.append("")
    lines.append(f"input_race={args.race}")
    lines.append(f"input_wide={args.wide}")
    lines.append(f"output={out_csv}")
    lines.append("")
    lines.append("rule")
    lines.append(f"- gap_std median={gap_median:.6f}")
    lines.append(f"- value_score_max 70%ile={value_q70:.6f}")
    lines.append("- use_diff = (gap_std < median) AND (value_score_max < 70%ile)")
    lines.append("- score_v1 = 0.55*z(value_score_max) + 0.45*z(gap_std)")
    lines.append("- score_v2 = 0.52*z(value_score_max) + 0.40*z(gap_std) + 0.08*z(top1_top2_diff)")
    lines.append("- score_final = use_diff ? score_v2 : score_v1")
    lines.append(f"- use_diff races={int(race['use_diff'].sum())}/{len(race)} ({race['use_diff'].mean():.2%})")
    lines.append("")
    lines.append("evaluation")
    lines.extend(eval_df.to_string(index=False).splitlines())
    lines.append("")
    lines.append("v1 comparison")
    lines.extend(pv.to_string(index=False).splitlines())
    lines.append("")
    lines.append("summary")
    lines.append("- 4月改善は roi_2026_04_diff_final_minus_v1 で確認")
    lines.append("- 安定性は worst_month_roi と roi_monthly_std で確認")
    lines.append("《推測》diff条件分岐で改善が限定的なら、条件閾値（中央値/70%ile）の再調整が必要です。")

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print("\n=== v1 vs final ===")
    print(pv.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
