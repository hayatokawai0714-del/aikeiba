import argparse
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
    ap = argparse.ArgumentParser(description="Re-optimize wide race selection with diff-included score and real ROI.")
    ap.add_argument("--race", default=r"C:\TXT\wide_race_selection_2026_v1.csv")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_selection_with_diff_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_selection_with_diff_report_2026_v1.txt")
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


def evaluate_config(joined: pd.DataFrame, race_df: pd.DataFrame, score_col: str, rate: float, pattern: str, stake: int) -> tuple[dict, list[dict]]:
    n = len(race_df)
    keep_n = int(math.ceil(n * rate))
    selected = race_df.sort_values(score_col, ascending=False).head(keep_n).copy()
    selected_ids = set(selected["race_id_raw"])

    sub = joined[joined["race_id_raw"].isin(selected_ids)].copy()
    if pattern == "top1":
        sub = sub[sub["pair_rank_score"] == 1].copy()
    elif pattern == "top3":
        sub = sub[sub["pair_rank_score"] <= 3].copy()
    else:
        raise ValueError(pattern)

    overall = summarize(sub, stake)
    overall_row = {
        "row_type": "overall",
        "score_version": score_col,
        "rate": rate,
        "pattern": pattern,
        "month": "ALL",
        "retained_rate": float(keep_n / n),
        **overall,
    }

    monthly_rows: list[dict] = []
    for m in MONTHS:
        ms = sub[sub["race_month"] == m].copy()
        monthly = summarize(ms, stake)
        monthly_rows.append(
            {
                "row_type": "monthly",
                "score_version": score_col,
                "rate": rate,
                "pattern": pattern,
                "month": m,
                "retained_rate": float(keep_n / n),
                **monthly,
            }
        )

    return overall_row, monthly_rows


def main() -> int:
    args = parse_args()

    race = pd.read_csv(Path(args.race), encoding=args.encoding, low_memory=False)
    wide = pd.read_csv(Path(args.wide), encoding=args.encoding, low_memory=False)

    required_race = ["race_id_raw", "race_date", "value_score_max", "gap_std", "top1_top2_diff"]
    miss = [c for c in required_race if c not in race.columns]
    if miss:
        raise SystemExit(f"Missing race columns: {miss}")

    race = race.copy()
    race["race_id_raw"] = race["race_id_raw"].astype(str)
    for c in ["value_score_max", "gap_std", "top1_top2_diff"]:
        race[c] = to_float(race[c])
    race = race[race["value_score_max"].notna() & race["gap_std"].notna() & race["top1_top2_diff"].notna()].copy()

    race["z_value_score_max"] = zscore(race["value_score_max"])
    race["z_gap_std"] = zscore(race["gap_std"])
    race["z_top1_top2_diff"] = zscore(race["top1_top2_diff"])

    race["race_select_score_v1"] = 0.6 * race["z_value_score_max"] + 0.4 * race["z_gap_std"]
    race["race_select_score_v2"] = 0.5 * race["z_value_score_max"] + 0.3 * race["z_gap_std"] + 0.2 * race["z_top1_top2_diff"]

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

    rows: list[dict] = []
    for score_col in ["race_select_score_v1", "race_select_score_v2"]:
        for rate in RATES:
            for pattern in ["top1", "top3"]:
                ov, mo = evaluate_config(joined, race, score_col, rate, pattern, args.stake)
                rows.append(ov)
                rows.extend(mo)

    out_df = pd.DataFrame(rows)

    # recommendation per pattern for v2
    rec_rows = []
    for pattern in ["top1", "top3"]:
        p = out_df[(out_df["row_type"] == "overall") & (out_df["score_version"] == "race_select_score_v2") & (out_df["pattern"] == pattern)].copy()
        if p.empty:
            continue
        april_roi = {}
        roi_std = {}
        for rate in RATES:
            april_roi[rate] = float(
                out_df[(out_df["row_type"] == "monthly") & (out_df["score_version"] == "race_select_score_v2") & (out_df["pattern"] == pattern) & (out_df["rate"] == rate) & (out_df["month"] == "2026-04")]["roi_pct"].iloc[0]
            )
            roi_std[rate] = float(
                out_df[(out_df["row_type"] == "monthly") & (out_df["score_version"] == "race_select_score_v2") & (out_df["pattern"] == pattern) & (out_df["rate"] == rate)]["roi_pct"].std(ddof=0)
            )

        p["april_roi"] = p["rate"].map(april_roi)
        p["roi_std"] = p["rate"].map(roi_std)
        p["score"] = p["april_roi"] * 0.5 + p["roi_pct"] * 0.4 - p["roi_std"] * 0.2
        best = p.sort_values("score", ascending=False).iloc[0]
        rec_rows.append(
            {
                "pattern": pattern,
                "recommended_rate": float(best["rate"]),
                "roi_pct": float(best["roi_pct"]),
                "april_roi": float(best["april_roi"]),
                "roi_std": float(best["roi_std"]),
                "races": int(best["races"]),
            }
        )

    rec_df = pd.DataFrame(rec_rows)

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_csv, index=False, encoding=args.encoding)

    lines: list[str] = []
    lines.append("wide selection with diff report (2026)")
    lines.append("")
    lines.append(f"input={args.race}")
    lines.append(f"output={out_csv}")
    lines.append("score_v2 = 0.5*z(value_score_max) + 0.3*z(gap_std) + 0.2*z(top1_top2_diff)")
    lines.append("score_v1 = 0.6*z(value_score_max) + 0.4*z(gap_std)")
    lines.append("《推測》v1比較は同一データ上で再計算した参照値です。")
    lines.append("")

    for pattern in ["top1", "top3"]:
        lines.append(f"[{pattern}] overall comparison (v1 vs v2)")
        p = out_df[(out_df["row_type"] == "overall") & (out_df["pattern"] == pattern)].copy()
        lines.extend(p[["score_version", "rate", "races", "retained_rate", "hit_rate", "roi_pct", "avg_payout_yen"]].to_string(index=False).splitlines())
        lines.append("- monthly ROI")
        m = out_df[(out_df["row_type"] == "monthly") & (out_df["pattern"] == pattern)].copy()
        lines.extend(m[["score_version", "rate", "month", "roi_pct", "races"]].to_string(index=False).splitlines())
        lines.append("")

    lines.append("推奨設定（v2）")
    lines.append("《推測》選定基準は 全体ROI・2026-04ROI・月次標準偏差 のバランス")
    lines.extend(rec_df.to_string(index=False).splitlines())
    lines.append("")
    lines.append("diff追加の効果")
    lines.append("《推測》v2がv1よりROI/安定性で上回る rate・pattern を採用候補とするのが妥当です。")
    lines.append("《推測》係数最適化は本評価では固定（0.5/0.3/0.2）で、次段でグリッド探索が有効です。")

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print("\n=== recommended (v2) ===")
    print(rec_df.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
