import argparse
from pathlib import Path

import numpy as np
import pandas as pd

THRESHOLDS = [0.02, 0.04, 0.06, 0.08, 0.10]
MONTHS = ["2026-01", "2026-02", "2026-03", "2026-04"]
JYO_TO_VENUE = {
    "01": "SAP", "02": "HAK", "03": "FUK", "04": "NII", "05": "TOK",
    "06": "NAK", "07": "CHU", "08": "KYO", "09": "HAN", "10": "KOK",
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Optimize top1_top2_diff skip filter with fixed selected_top15 races.")
    ap.add_argument("--roi", default=r"C:\TXT\wide_roi_real_2026_v1.csv")
    ap.add_argument("--race", default=r"C:\TXT\wide_race_selection_2026_v1.csv")
    ap.add_argument("--fixed", default=r"C:\TXT\wide_race_selection_fixed_rate_2026_v1.csv")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_diff_filter_optimization_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_diff_filter_report_2026_v1.txt")
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
    return {
        "races": races,
        "bets": bets,
        "hit_rate": float(sub["wide_hit_real"].mean()) if len(sub) else np.nan,
        "race_hit_rate": float(sub.groupby("race_id_raw", dropna=False)["wide_hit_real"].max().mean()) if len(sub) else np.nan,
        "roi_pct": float((ret / stake_total) * 100.0) if stake_total else np.nan,
        "stake_total": int(stake_total),
        "return_total": ret,
        "avg_payout_yen": float(sub.loc[sub["wide_hit_real"] == 1, "payout_yen"].mean()) if (len(sub) and (sub["wide_hit_real"] == 1).any()) else np.nan,
    }


def eval_pattern(joined: pd.DataFrame, selected_race_df: pd.DataFrame, pattern: str, threshold: float, stake: int, base_races: int) -> tuple[dict, list[dict]]:
    race_keep = selected_race_df[selected_race_df["top1_top2_diff"] > threshold]["race_id_raw"].astype(str)
    race_keep_set = set(race_keep)
    filt = joined[joined["race_id_raw"].isin(race_keep_set)].copy()

    if pattern == "top1":
        filt = filt[filt["pair_rank_score"] == 1].copy()
    elif pattern == "top3":
        filt = filt[filt["pair_rank_score"] <= 3].copy()
    else:
        raise ValueError(pattern)

    agg = summarize(filt, stake)
    agg_row = {
        "row_type": "overall",
        "pattern": pattern,
        "threshold": threshold,
        "month": "ALL",
        "base_races": base_races,
        "retained_rate": float(agg["races"] / base_races) if base_races else np.nan,
        **agg,
    }

    monthly_rows: list[dict] = []
    for month in MONTHS:
        m = filt[filt["race_month"] == month].copy()
        m_agg = summarize(m, stake)
        monthly_rows.append(
            {
                "row_type": "monthly",
                "pattern": pattern,
                "threshold": threshold,
                "month": month,
                "base_races": base_races,
                "retained_rate": float(m_agg["races"] / base_races) if base_races else np.nan,
                **m_agg,
            }
        )

    return agg_row, monthly_rows


def main() -> int:
    args = parse_args()

    _ = pd.read_csv(Path(args.roi), encoding=args.encoding, low_memory=False)

    race = pd.read_csv(Path(args.race), encoding=args.encoding, low_memory=False)
    fixed = pd.read_csv(Path(args.fixed), encoding=args.encoding, low_memory=False)
    wide = pd.read_csv(Path(args.wide), encoding=args.encoding, low_memory=False)

    race["race_id_raw"] = race["race_id_raw"].astype(str)
    fixed["race_id_raw"] = fixed["race_id_raw"].astype(str)

    if "selected_top15" in race.columns:
        race_sel = race.copy()
        selection_note = "selected_top15 を race 入力から使用"
    else:
        race_sel = race.merge(fixed[["race_id_raw", "selected_top15"]], on="race_id_raw", how="left")
        selection_note = "《推測》selected_top15 が race 入力に無いため fixed_rate から補完"

    for c in ["top1_top2_diff"]:
        race_sel[c] = to_float(race_sel[c])

    selected = race_sel[race_sel["selected_top15"] == True].copy()  # noqa: E712
    selected = selected[selected["top1_top2_diff"].notna()].copy()

    if selected.empty:
        raise SystemExit("No selected_top15 races with valid top1_top2_diff")

    base_races = int(selected["race_id_raw"].nunique())
    diff_stats = selected["top1_top2_diff"].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9])
    race_date_map = selected[["race_id_raw", "race_date", "top1_top2_diff"]].drop_duplicates()

    for c in ["horse_no_1", "horse_no_2", "pair_rank_score"]:
        wide[c] = to_float(wide[c])
    wide["race_id_raw"] = wide["race_id_raw"].astype(str)

    joined = wide[wide["race_id_raw"].isin(set(selected["race_id_raw"]))].copy()
    joined = joined[joined["horse_no_1"].notna() & joined["horse_no_2"].notna() & joined["pair_rank_score"].notna()].copy()
    joined = joined.merge(race_date_map, on="race_id_raw", how="left")
    if "race_date" not in joined.columns:
        if "race_date_x" in joined.columns and "race_date_y" in joined.columns:
            joined["race_date"] = joined["race_date_x"].fillna(joined["race_date_y"])
        elif "race_date_x" in joined.columns:
            joined["race_date"] = joined["race_date_x"]
        elif "race_date_y" in joined.columns:
            joined["race_date"] = joined["race_date_y"]
    joined["race_id"] = joined["race_id_raw"].map(raw_to_race_id)
    joined["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(joined["horse_no_1"], joined["horse_no_2"])]

    payouts = load_wide_payouts(Path(args.payout_root))
    if payouts.empty:
        raise SystemExit("No real 2026 wide payouts found")

    joined = joined.merge(payouts, on=["race_id", "pair_key"], how="left")
    joined["wide_hit_real"] = joined["payout_yen"].notna().astype(int)
    joined["return_yen"] = joined["payout_yen"].fillna(0.0)
    joined["race_month"] = pd.to_datetime(joined["race_date"], errors="coerce").dt.strftime("%Y-%m")

    overall_rows: list[dict] = []
    monthly_rows: list[dict] = []

    for pattern in ["top1", "top3"]:
        base_overall, base_monthly = eval_pattern(joined, selected, pattern, threshold=-1.0, stake=args.stake, base_races=base_races)
        base_overall["threshold"] = 0.0
        for row in base_monthly:
            row["threshold"] = 0.0
        overall_rows.append(base_overall)
        monthly_rows.extend(base_monthly)

        for th in THRESHOLDS:
            ov, mo = eval_pattern(joined, selected, pattern, threshold=th, stake=args.stake, base_races=base_races)
            overall_rows.append(ov)
            monthly_rows.extend(mo)

    out_df = pd.DataFrame(overall_rows + monthly_rows)

    # comparison table for report
    overall = out_df[out_df["row_type"] == "overall"].copy()

    rec_rows = []
    for pattern in ["top1", "top3"]:
        p = overall[overall["pattern"] == pattern].copy()
        base = p[p["threshold"] == 0.0].iloc[0]

        cand = p[p["threshold"] > 0.0].copy()
        cand["april_roi"] = [
            float(
                out_df[
                    (out_df["row_type"] == "monthly")
                    & (out_df["pattern"] == pattern)
                    & (out_df["threshold"] == th)
                    & (out_df["month"] == "2026-04")
                ]["roi_pct"].iloc[0]
            )
            for th in cand["threshold"]
        ]
        cand["roi_std"] = [
            float(
                out_df[
                    (out_df["row_type"] == "monthly")
                    & (out_df["pattern"] == pattern)
                    & (out_df["threshold"] == th)
                ]["roi_pct"].std(ddof=0)
            )
            for th in cand["threshold"]
        ]
        cand["april_improve"] = cand["april_roi"] - float(
            out_df[
                (out_df["row_type"] == "monthly")
                & (out_df["pattern"] == pattern)
                & (out_df["threshold"] == 0.0)
                & (out_df["month"] == "2026-04")
            ]["roi_pct"].iloc[0]
        )
        cand["stability_score"] = cand["april_improve"] * 0.6 + (cand["roi_pct"] - float(base["roi_pct"])) * 0.3 - (1.0 - cand["retained_rate"]) * 20.0
        best = cand.sort_values("stability_score", ascending=False).iloc[0]
        rec_rows.append(
            {
                "pattern": pattern,
                "best_threshold": float(best["threshold"]),
                "best_roi": float(best["roi_pct"]),
                "best_april_roi": float(best["april_roi"]),
                "best_retained_rate": float(best["retained_rate"]),
                "best_roi_std": float(best["roi_std"]),
            }
        )

    rec_df = pd.DataFrame(rec_rows)

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    out_df.to_csv(out_csv, index=False, encoding=args.encoding)

    lines: list[str] = []
    lines.append("wide diff filter optimization report (2026)")
    lines.append("")
    lines.append(f"input_roi={args.roi}")
    lines.append(f"input_race={args.race}")
    lines.append(f"output={out_csv}")
    lines.append(f"selection_note={selection_note}")
    lines.append("- 他条件は固定、top1_top2_diff 閾値のみ検証")
    lines.append("")
    lines.append("top1_top2_diff 分布（selected_top15 内）")
    for idx, val in diff_stats.items():
        lines.append(f"- {idx}: {float(val):.6f}")
    for th in THRESHOLDS:
        kept = int((selected["top1_top2_diff"] > th).sum())
        lines.append(f"- diff>{th:.2f}: {kept}/{base_races} races ({kept/base_races:.2%})")
    lines.append("")

    for pattern in ["top1", "top3"]:
        lines.append(f"[{pattern}] threshold comparison")
        p = overall[(overall["pattern"] == pattern) & (overall["threshold"] >= 0.0)].copy()
        lines.extend(p[["threshold", "races", "retained_rate", "hit_rate", "roi_pct", "avg_payout_yen"]].to_string(index=False).splitlines())
        lines.append("- monthly ROI")
        m = out_df[(out_df["row_type"] == "monthly") & (out_df["pattern"] == pattern) & (out_df["threshold"] >= 0.0)].copy()
        lines.extend(m[["threshold", "month", "roi_pct", "races", "hit_rate"]].to_string(index=False).splitlines())
        lines.append("")

    lines.append("最適閾値（安定性重視）")
    lines.append("《推測》評価は 2026-04 ROI改善 + 全体ROI + 残存率ペナルティ の合成スコア")
    lines.extend(rec_df.to_string(index=False).splitlines())

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print("\n=== recommendations ===")
    print(rec_df.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
