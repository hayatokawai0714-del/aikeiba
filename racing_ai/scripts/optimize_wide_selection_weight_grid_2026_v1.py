import argparse
import itertools
import math
from pathlib import Path

import numpy as np
import pandas as pd

RATES = [0.15, 0.20, 0.25]
MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
JYO_TO_VENUE = {
    "01": "SAP", "02": "HAK", "03": "FUK", "04": "NII", "05": "TOK",
    "06": "NAK", "07": "CHU", "08": "KYO", "09": "HAN", "10": "KOK",
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Grid-search wide race-selection weights with real ROI evaluation.")
    ap.add_argument("--race", default=r"C:\TXT\wide_race_selection_2026_v1.csv")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--roi", default=r"C:\TXT\wide_roi_real_2026_v1.csv")
    ap.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_selection_weight_grid_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_selection_weight_grid_report_2026_v1.txt")
    ap.add_argument("--encoding", default="cp932")
    ap.add_argument("--stake", type=int, default=100)
    ap.add_argument("--step", type=float, default=0.05)
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


def summarize(sub: pd.DataFrame, stake: int) -> dict:
    races = int(sub["race_id_raw"].nunique()) if len(sub) else 0
    bets = int(len(sub))
    stake_total = bets * stake
    ret = float(sub["return_yen"].sum()) if len(sub) else 0.0
    roi = float((ret / stake_total) * 100.0) if stake_total else np.nan
    hit_rate = float(sub["wide_hit_real"].mean()) if len(sub) else np.nan
    race_hit = float(sub.groupby("race_id_raw", dropna=False)["wide_hit_real"].max().mean()) if len(sub) else np.nan
    avg_payout = float(sub.loc[sub["wide_hit_real"] == 1, "payout_yen"].mean()) if (len(sub) and (sub["wide_hit_real"] == 1).any()) else np.nan
    return {
        "races": races,
        "bets": bets,
        "hit_rate": hit_rate,
        "race_hit_rate": race_hit,
        "roi_pct": roi,
        "avg_payout_yen": avg_payout,
        "stake_total": int(stake_total),
        "return_total": ret,
    }


def frange(start: float, stop: float, step: float) -> list[float]:
    vals = []
    cur = start
    while cur <= stop + 1e-9:
        vals.append(round(cur, 10))
        cur += step
    return vals


def evaluate_setting(joined: pd.DataFrame, race_df: pd.DataFrame, score_col: str, rate: float, pattern: str, stake: int) -> dict:
    n = len(race_df)
    keep_n = int(math.ceil(n * rate))
    selected = race_df.sort_values(score_col, ascending=False).head(keep_n)
    selected_ids = set(selected["race_id_raw"])

    sub = joined[joined["race_id_raw"].isin(selected_ids)].copy()
    if pattern == "top1":
        sub = sub[sub["pair_rank_score"] == 1].copy()
    elif pattern == "top3":
        sub = sub[sub["pair_rank_score"] <= 3].copy()

    overall = summarize(sub, stake)

    month_rois = {}
    for m in MONTHS:
        month_sub = sub[sub["race_month"] == m].copy()
        month_rois[m] = summarize(month_sub, stake)["roi_pct"]

    roi_vals = np.array([v for v in month_rois.values() if pd.notna(v)], dtype=float)
    worst_month_roi = float(np.min(roi_vals)) if len(roi_vals) else np.nan
    roi_std = float(np.std(roi_vals, ddof=0)) if len(roi_vals) else np.nan

    return {
        "rate": rate,
        "pattern": pattern,
        **overall,
        "retained_rate": float(keep_n / n),
        "roi_2026_01": month_rois["2026-01"],
        "roi_2026_02": month_rois["2026-02"],
        "roi_2026_03": month_rois["2026-03"],
        "roi_2026_04": month_rois["2026-04"],
        "worst_month_roi": worst_month_roi,
        "roi_monthly_std": roi_std,
    }


def main() -> int:
    args = parse_args()

    _ = pd.read_csv(Path(args.roi), encoding=args.encoding, low_memory=False)

    race = pd.read_csv(Path(args.race), encoding=args.encoding, low_memory=False)
    wide = pd.read_csv(Path(args.wide), encoding=args.encoding, low_memory=False)

    required = ["race_id_raw", "race_date", "value_score_max", "gap_std", "top1_top2_diff"]
    missing = [c for c in required if c not in race.columns]
    if missing:
        raise SystemExit(f"Missing race columns: {missing}")

    race = race.copy()
    race["race_id_raw"] = race["race_id_raw"].astype(str)
    for c in ["value_score_max", "gap_std", "top1_top2_diff"]:
        race[c] = to_float(race[c])
    race = race[race["value_score_max"].notna() & race["gap_std"].notna() & race["top1_top2_diff"].notna()].copy()

    race["z_value_score_max"] = zscore(race["value_score_max"])
    race["z_gap_std"] = zscore(race["gap_std"])
    race["z_top1_top2_diff"] = zscore(race["top1_top2_diff"])

    wide = wide.copy()
    wide["race_id_raw"] = wide["race_id_raw"].astype(str)
    for c in ["horse_no_1", "horse_no_2", "pair_rank_score"]:
        wide[c] = to_float(wide[c])
    wide = wide[wide["horse_no_1"].notna() & wide["horse_no_2"].notna() & wide["pair_rank_score"].notna()].copy()

    race_date_map = race[["race_id_raw", "race_date"]].drop_duplicates()
    joined = wide.merge(race_date_map, on="race_id_raw", how="inner", suffixes=("", "_sel"))
    if "race_date" not in joined.columns:
        if "race_date_sel" in joined.columns:
            joined["race_date"] = joined["race_date_sel"]
        elif "race_date_x" in joined.columns and "race_date_y" in joined.columns:
            joined["race_date"] = joined["race_date_x"].fillna(joined["race_date_y"])

    joined["race_id"] = joined["race_id_raw"].map(raw_to_race_id)
    joined["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(joined["horse_no_1"], joined["horse_no_2"])]

    payout = load_wide_payouts(Path(args.payout_root))
    if payout.empty:
        raise SystemExit("No 2026 wide payout rows found")

    joined = joined.merge(payout, on=["race_id", "pair_key"], how="left")
    joined["wide_hit_real"] = joined["payout_yen"].notna().astype(int)
    joined["return_yen"] = joined["payout_yen"].fillna(0.0)
    joined["race_month"] = pd.to_datetime(joined["race_date"], errors="coerce").dt.strftime("%Y-%m")

    # v1 baseline
    race["score_v1"] = 0.6 * race["z_value_score_max"] + 0.4 * race["z_gap_std"]

    grid_rows = []

    # Baseline entries (for direct comparison)
    for rate, pattern in itertools.product(RATES, ["top1", "top3"]):
        base = evaluate_setting(joined, race, "score_v1", rate, pattern, args.stake)
        base.update(
            {
                "w_value_raw": 0.6,
                "w_gap_raw": 0.4,
                "w_diff_raw": 0.0,
                "w_value": 0.6,
                "w_gap": 0.4,
                "w_diff": 0.0,
                "is_v1": True,
            }
        )
        grid_rows.append(base)

    value_ws = frange(0.50, 0.80, args.step)
    gap_ws = frange(0.15, 0.40, args.step)
    diff_ws = frange(0.00, 0.25, args.step)

    for wv, wg, wd in itertools.product(value_ws, gap_ws, diff_ws):
        raw_sum = wv + wg + wd
        if raw_sum <= 0:
            continue

        nv = wv / raw_sum
        ng = wg / raw_sum
        nd = wd / raw_sum

        race["score_grid"] = nv * race["z_value_score_max"] + ng * race["z_gap_std"] + nd * race["z_top1_top2_diff"]

        for rate, pattern in itertools.product(RATES, ["top1", "top3"]):
            res = evaluate_setting(joined, race, "score_grid", rate, pattern, args.stake)
            res.update(
                {
                    "w_value_raw": wv,
                    "w_gap_raw": wg,
                    "w_diff_raw": wd,
                    "w_value": nv,
                    "w_gap": ng,
                    "w_diff": nd,
                    "is_v1": False,
                }
            )
            grid_rows.append(res)

    out_df = pd.DataFrame(grid_rows)

    # baseline lookup for each (rate, pattern)
    baseline = out_df[out_df["is_v1"]].set_index(["rate", "pattern"])

    def baseline_col(row: pd.Series, col: str) -> float:
        return float(baseline.loc[(row["rate"], row["pattern"]), col])

    out_df["baseline_roi"] = out_df.apply(lambda r: baseline_col(r, "roi_pct"), axis=1)
    out_df["baseline_worst_month_roi"] = out_df.apply(lambda r: baseline_col(r, "worst_month_roi"), axis=1)
    out_df["baseline_roi_std"] = out_df.apply(lambda r: baseline_col(r, "roi_monthly_std"), axis=1)
    out_df["roi_diff_vs_v1"] = out_df["roi_pct"] - out_df["baseline_roi"]
    out_df["worst_month_diff_vs_v1"] = out_df["worst_month_roi"] - out_df["baseline_worst_month_roi"]
    out_df["roi_std_diff_vs_v1"] = out_df["roi_monthly_std"] - out_df["baseline_roi_std"]

    # adoption screen
    out_df["passes_main_criteria"] = (
        (out_df["roi_pct"] >= out_df["baseline_roi"] - 1e-9)
        & (out_df["worst_month_roi"] >= out_df["baseline_worst_month_roi"] - 1e-9)
        & (out_df["retained_rate"] >= 0.15)
    )

    candidates = out_df[(~out_df["is_v1"]) & (out_df["passes_main_criteria"])].copy()
    if not candidates.empty:
        candidates = candidates.sort_values(
            ["roi_diff_vs_v1", "worst_month_diff_vs_v1", "roi_monthly_std"],
            ascending=[False, False, True],
        )
        best = candidates.iloc[0]
    else:
        # fallback: closest balanced candidate
        pool = out_df[~out_df["is_v1"]].copy()
        pool["balance_score"] = pool["roi_diff_vs_v1"] * 0.5 + pool["worst_month_diff_vs_v1"] * 0.4 - pool["roi_monthly_std"] * 0.02
        pool = pool.sort_values("balance_score", ascending=False)
        best = pool.iloc[0]

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_csv, index=False, encoding=args.encoding)

    lines: list[str] = []
    lines.append("wide selection weight grid report (2026)")
    lines.append("")
    lines.append(f"input_race={args.race}")
    lines.append(f"input_wide={args.wide}")
    lines.append(f"input_roi={args.roi}")
    lines.append(f"output={out_csv}")
    lines.append("grid")
    lines.append(f"- value_score_max raw: 0.50..0.80 step {args.step}")
    lines.append(f"- gap_std raw: 0.15..0.40 step {args.step}")
    lines.append(f"- top1_top2_diff raw: 0.00..0.25 step {args.step}")
    lines.append("- raw重みは毎回 sum=1 に正規化")
    lines.append("")

    summary_cols = [
        "is_v1", "rate", "pattern", "w_value", "w_gap", "w_diff",
        "races", "roi_pct", "worst_month_roi", "roi_monthly_std", "roi_diff_vs_v1", "worst_month_diff_vs_v1"
    ]

    lines.append("v1 baseline")
    lines.extend(out_df[out_df["is_v1"]][summary_cols].to_string(index=False).splitlines())
    lines.append("")

    top_non_v1 = out_df[~out_df["is_v1"]].sort_values(["roi_diff_vs_v1", "worst_month_diff_vs_v1"], ascending=[False, False]).head(20)
    lines.append("top candidates (non-v1)")
    lines.extend(top_non_v1[summary_cols + ["passes_main_criteria"]].to_string(index=False).splitlines())
    lines.append("")

    lines.append("best setting")
    lines.append(best[summary_cols + ["passes_main_criteria"]].to_string())
    lines.append("")

    has_better = bool((~out_df["is_v1"] & out_df["passes_main_criteria"]).any())
    lines.append(f"v1を超えた設定があるか: {'YES' if has_better else 'NO'}")
    lines.append(f"best通期ROI={float(best['roi_pct']):.4f}")
    lines.append(f"best最悪月ROI={float(best['worst_month_roi']):.4f}")
    lines.append(f"best月次ROI標準偏差={float(best['roi_monthly_std']):.4f}")
    lines.append("《推測》2026単年での最適化は過学習リスクがあるため、採用前に期間外検証が必要です。")

    if has_better:
        lines.append("diff採用判断: 条件付きで採用候補あり")
    else:
        lines.append("diff採用判断: 現時点はv1維持が無難")

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print(f"has_better_than_v1={has_better}")
    print("\n=== best ===")
    print(best[summary_cols + ["passes_main_criteria"]].to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
