import argparse
from pathlib import Path

import numpy as np
import pandas as pd

JYO_TO_VENUE = {
    "01": "SAP", "02": "HAK", "03": "FUK", "04": "NII", "05": "TOK",
    "06": "NAK", "07": "CHU", "08": "KYO", "09": "HAN", "10": "KOK",
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Use diff-related metrics as risk filter for wide ROI.")
    ap.add_argument("--race", default=r"C:\TXT\wide_race_selection_2026_v1.csv")
    ap.add_argument("--horse", default=r"C:\TXT\top3_value_score_2026_v1.csv")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_diff_risk_filter_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_diff_risk_filter_report_2026_v1.txt")
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


def summarize_strategy(df: pd.DataFrame, stake: int, col_return: str, bets_per_race: int) -> dict:
    races = int(len(df))
    stake_total = races * bets_per_race * stake
    ret_total = float(df[col_return].sum())
    roi = float((ret_total / stake_total) * 100.0) if stake_total else np.nan
    hit = float((df[col_return] > 0).mean()) if races else np.nan
    avg_payout = float(df.loc[df[col_return] > 0, col_return].mean() / bets_per_race) if (df[col_return] > 0).any() else np.nan
    return {
        "races": races,
        "stake_total": int(stake_total),
        "return_total": ret_total,
        "roi_pct": roi,
        "hit_rate": hit,
        "avg_payout_per_ticket_yen": avg_payout,
    }


def main() -> int:
    args = parse_args()

    race = pd.read_csv(Path(args.race), encoding=args.encoding, low_memory=False)
    horse = pd.read_csv(Path(args.horse), encoding=args.encoding, low_memory=False)
    wide = pd.read_csv(Path(args.wide), encoding=args.encoding, low_memory=False)

    for col in ["top1_top2_diff"]:
        race[col] = to_float(race[col])

    horse["race_id_raw"] = horse["race_id_raw"].astype(str)
    horse["value_gap"] = to_float(horse["value_gap"])
    horse["value_gap_rank"] = to_float(horse["value_gap_rank"])

    race["race_id_raw"] = race["race_id_raw"].astype(str)
    race = race[race["top1_top2_diff"].notna()].copy()

    # value_gap top3 features
    top3 = horse[horse["race_id_raw"].isin(set(race["race_id_raw"]))].copy()
    top3 = top3[top3["value_gap"].notna()].copy()
    top3["value_gap_rank"] = top3.groupby("race_id_raw", dropna=False)["value_gap"].rank(method="first", ascending=False)
    top3 = top3[top3["value_gap_rank"] <= 3].copy()

    top3_feat = (
        top3.groupby("race_id_raw", dropna=False)
        .agg(
            value_gap_top3_var=("value_gap", lambda s: float(np.var(s, ddof=0))),
            value_gap_top3_min=("value_gap", "min"),
        )
        .reset_index()
    )

    risk_df = race.merge(top3_feat, on="race_id_raw", how="left")

    # baseline pair returns
    wide["race_id_raw"] = wide["race_id_raw"].astype(str)
    for c in ["horse_no_1", "horse_no_2", "pair_rank_score"]:
        wide[c] = to_float(wide[c])
    wide = wide[wide["race_id_raw"].isin(set(risk_df["race_id_raw"]))].copy()

    wide["race_id"] = wide["race_id_raw"].map(raw_to_race_id)
    wide["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(wide["horse_no_1"], wide["horse_no_2"])]

    payout = load_wide_payouts(Path(args.payout_root))
    if payout.empty:
        raise SystemExit("No payout data found")

    wide = wide.merge(payout, on=["race_id", "pair_key"], how="left")
    wide["return_yen"] = wide["payout_yen"].fillna(0.0)

    race_ret = (
        wide.groupby("race_id_raw", dropna=False)
        .agg(
            top1_return=("return_yen", lambda s: float(s[wide.loc[s.index, "pair_rank_score"] == 1].sum())),
            top3_return=("return_yen", lambda s: float(s[wide.loc[s.index, "pair_rank_score"] <= 3].sum())),
        )
        .reset_index()
    )

    risk_df = risk_df.merge(race_ret, on="race_id_raw", how="left")
    risk_df["top1_return"] = risk_df["top1_return"].fillna(0.0)
    risk_df["top3_return"] = risk_df["top3_return"].fillna(0.0)

    # group by diff median
    diff_median = float(risk_df["top1_top2_diff"].median())
    risk_df["diff_group"] = np.where(risk_df["top1_top2_diff"] < diff_median, "diff_small", "diff_large")

    # dangerous rule sweep
    diff_qs = [0.15, 0.20, 0.25, 0.30, 0.35]
    var_qs = [0.15, 0.20, 0.25, 0.30, 0.35]

    base_top1 = summarize_strategy(risk_df, args.stake, "top1_return", 1)
    base_top3 = summarize_strategy(risk_df, args.stake, "top3_return", 3)

    sweep_rows = []
    for dq in diff_qs:
        diff_th = float(risk_df["top1_top2_diff"].quantile(dq))
        for vq in var_qs:
            var_th = float(risk_df["value_gap_top3_var"].quantile(vq))
            danger = (risk_df["top1_top2_diff"] < diff_th) & (risk_df["value_gap_top3_var"] < var_th)
            kept = risk_df[~danger].copy()
            top1 = summarize_strategy(kept, args.stake, "top1_return", 1)
            top3 = summarize_strategy(kept, args.stake, "top3_return", 3)
            sweep_rows.append(
                {
                    "diff_quantile": dq,
                    "var_quantile": vq,
                    "diff_threshold": diff_th,
                    "var_threshold": var_th,
                    "danger_races": int(danger.sum()),
                    "kept_races": int(len(kept)),
                    "kept_rate": float(len(kept) / len(risk_df)) if len(risk_df) else np.nan,
                    "top1_roi": top1["roi_pct"],
                    "top1_hit": top1["hit_rate"],
                    "top1_roi_diff_vs_base": top1["roi_pct"] - base_top1["roi_pct"],
                    "top3_roi": top3["roi_pct"],
                    "top3_hit": top3["hit_rate"],
                    "top3_roi_diff_vs_base": top3["roi_pct"] - base_top3["roi_pct"],
                }
            )

    sweep_df = pd.DataFrame(sweep_rows)
    best = sweep_df.sort_values(["top1_roi_diff_vs_base", "top3_roi_diff_vs_base", "kept_races"], ascending=[False, False, False]).iloc[0]

    best_danger = (risk_df["top1_top2_diff"] < float(best["diff_threshold"])) & (risk_df["value_gap_top3_var"] < float(best["var_threshold"]))
    risk_df["is_danger_race"] = best_danger

    # group comparison
    group_rows = []
    for grp, gdf in risk_df.groupby("diff_group", dropna=False):
        t1 = summarize_strategy(gdf, args.stake, "top1_return", 1)
        t3 = summarize_strategy(gdf, args.stake, "top3_return", 3)
        group_rows.append({"group": grp, "pattern": "top1", **t1})
        group_rows.append({"group": grp, "pattern": "top3", **t3})
    group_df = pd.DataFrame(group_rows)

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    risk_df.to_csv(out_csv, index=False, encoding=args.encoding)

    lines = []
    lines.append("wide diff risk filter report (2026)")
    lines.append("")
    lines.append(f"input_race={args.race}")
    lines.append(f"output={out_csv}")
    lines.append("- diffはスコアに使わず、危険レース除外フィルタとしてのみ評価")
    lines.append("")
    lines.append("baseline")
    lines.append(f"- top1 ROI={base_top1['roi_pct']:.6f}, hit={base_top1['hit_rate']:.6f}")
    lines.append(f"- top3 ROI={base_top3['roi_pct']:.6f}, hit={base_top3['hit_rate']:.6f}")
    lines.append("")
    lines.append("diff group comparison")
    lines.extend(group_df.to_string(index=False).splitlines())
    lines.append("")
    lines.append("best danger rule")
    lines.append("《推測》danger = (top1_top2_diff < X) AND (value_gap_top3_var < Y)")
    lines.append(best.to_string())
    lines.append("")
    lines.append("top sweep candidates")
    lines.extend(sweep_df.sort_values(["top1_roi_diff_vs_base", "top3_roi_diff_vs_base"], ascending=[False, False]).head(20).to_string(index=False).splitlines())

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print("\n=== best ===")
    print(best.to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
