import argparse
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd

JYO_TO_VENUE = {
    "01": "SAP", "02": "HAK", "03": "FUK", "04": "NII", "05": "TOK",
    "06": "NAK", "07": "CHU", "08": "KYO", "09": "HAN", "10": "KOK",
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Evaluate horse-selection diff usage on fixed v1 race selection.")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--horse", default=r"C:\TXT\top3_value_score_2026_v1.csv")
    ap.add_argument("--roi", default=r"C:\TXT\wide_roi_real_2026_v1.csv")
    ap.add_argument("--race-selection", default=r"C:\TXT\wide_race_selection_fixed_rate_2026_v1.csv")
    ap.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_horse_selection_with_diff_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_horse_selection_with_diff_report_2026_v1.txt")
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
    roi = float((ret / stake_total) * 100.0) if stake_total else np.nan

    return {
        "races": races,
        "bets": bets,
        "stake_total": stake_total,
        "return_total": ret,
        "roi_pct": roi,
        "hit_rate": float(sub["wide_hit_real"].mean()) if len(sub) else np.nan,
        "race_hit_rate": float(sub.groupby("race_id_raw", dropna=False)["wide_hit_real"].max().mean()) if len(sub) else np.nan,
        "avg_payout_hit": float(sub.loc[sub["wide_hit_real"] == 1, "payout_yen"].mean()) if (len(sub) and (sub["wide_hit_real"] == 1).any()) else np.nan,
    }


def main() -> int:
    args = parse_args()

    # input existence check
    _ = pd.read_csv(Path(args.wide), encoding=args.encoding, low_memory=False)
    _ = pd.read_csv(Path(args.roi), encoding=args.encoding, low_memory=False)

    horse = pd.read_csv(Path(args.horse), encoding=args.encoding, low_memory=False)
    race_sel = pd.read_csv(Path(args.race_selection), encoding=args.encoding, low_memory=False)

    req_horse = ["race_id_raw", "race_date", "horse_no", "horse_name", "pred_top3_raw", "value_score_v1", "value_gap_z"]
    miss = [c for c in req_horse if c not in horse.columns]
    if miss:
        raise SystemExit(f"Missing horse columns: {miss}")
    if "selected_top15" not in race_sel.columns:
        raise SystemExit("race_selection must contain selected_top15")

    horse = horse.copy()
    horse["race_id_raw"] = horse["race_id_raw"].astype(str)
    for c in ["horse_no", "pred_top3_raw", "value_score_v1", "value_gap_z"]:
        horse[c] = to_float(horse[c])
    horse = horse[horse["horse_no"].notna() & horse["pred_top3_raw"].notna() & horse["value_score_v1"].notna() & horse["value_gap_z"].notna()].copy()

    race_sel["race_id_raw"] = race_sel["race_id_raw"].astype(str)
    selected_ids = set(race_sel[race_sel["selected_top15"] == True]["race_id_raw"])  # noqa: E712
    if not selected_ids:
        raise SystemExit("No selected_top15 races found")

    horse = horse[horse["race_id_raw"].isin(selected_ids)].copy()
    if horse.empty:
        raise SystemExit("No horse rows after fixed race selection")

    horse["horse_score_v2"] = 0.7 * horse["value_score_v1"] + 0.3 * horse["value_gap_z"]
    horse["horse_rank_v2"] = horse.groupby("race_id_raw", dropna=False)["horse_score_v2"].rank(method="first", ascending=False).astype(int)

    top3_horse = horse[horse["horse_rank_v2"] <= 3].copy()

    pair_rows = []
    for race_id, rdf in top3_horse.groupby("race_id_raw", dropna=False):
        recs = rdf.to_dict("records")
        if len(recs) < 2:
            continue
        for a, b in combinations(recs, 2):
            pair_rows.append(
                {
                    "race_id_raw": str(race_id),
                    "race_date": a["race_date"],
                    "horse_no_1": int(a["horse_no"]),
                    "horse_no_2": int(b["horse_no"]),
                    "horse_name_1": a.get("horse_name", ""),
                    "horse_name_2": b.get("horse_name", ""),
                    "pred_top3_1": float(a["pred_top3_raw"]),
                    "pred_top3_2": float(b["pred_top3_raw"]),
                    "horse_score_v2_1": float(a["horse_score_v2"]),
                    "horse_score_v2_2": float(b["horse_score_v2"]),
                    "pair_score_v2": float(a["pred_top3_raw"] * b["pred_top3_raw"]),
                    "pair_horse_score_v2": float(a["horse_score_v2"] + b["horse_score_v2"]),
                }
            )

    pairs = pd.DataFrame(pair_rows)
    if pairs.empty:
        raise SystemExit("No pairs generated from top3 horses")

    pairs["pair_rank_v2"] = pairs.groupby("race_id_raw", dropna=False)["pair_score_v2"].rank(method="first", ascending=False).astype(int)

    pairs["race_id"] = pairs["race_id_raw"].map(raw_to_race_id)
    pairs["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(pairs["horse_no_1"], pairs["horse_no_2"])]

    payout = load_wide_payouts(Path(args.payout_root))
    if payout.empty:
        raise SystemExit("No payout data found")

    pairs = pairs.merge(payout, on=["race_id", "pair_key"], how="left")
    pairs["wide_hit_real"] = pairs["payout_yen"].notna().astype(int)
    pairs["return_yen"] = pairs["payout_yen"].fillna(0.0)

    top1_df = pairs[pairs["pair_rank_v2"] == 1].copy()
    top3_df = pairs[pairs["pair_rank_v2"] <= 3].copy()

    res_top1 = summarize(top1_df, args.stake)
    res_top3 = summarize(top3_df, args.stake)

    baseline = pd.read_csv(Path(args.roi), encoding=args.encoding, low_memory=False)
    base_top1 = baseline[baseline["pattern"] == "top1"].iloc[0]
    base_top3 = baseline[baseline["pattern"] == "top3"].iloc[0]

    pairs["selected_top1"] = pairs["pair_rank_v2"] == 1
    pairs["selected_top3"] = pairs["pair_rank_v2"] <= 3

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    pairs.to_csv(out_csv, index=False, encoding=args.encoding)

    lines = []
    lines.append("wide horse selection with diff report (2026)")
    lines.append("")
    lines.append(f"input_wide={args.wide}")
    lines.append(f"input_horse={args.horse}")
    lines.append(f"input_roi_baseline={args.roi}")
    lines.append(f"output={out_csv}")
    lines.append("- レース選別は selected_top15 固定（変更なし）")
    lines.append("- horse_score_v2 = 0.7*value_score_v1 + 0.3*value_gap_z")
    lines.append("- 各レース上位3頭からペア生成")
    lines.append("《推測》pair順位は pair_score_v2=pred_top3_1*pred_top3_2 の降順")
    lines.append("")
    lines.append("result (new horse selection)")
    lines.append(f"- top1 ROI={res_top1['roi_pct']:.6f}, hit_rate={res_top1['hit_rate']:.6f}, avg_payout={res_top1['avg_payout_hit']:.2f}")
    lines.append(f"- top3 ROI={res_top3['roi_pct']:.6f}, hit_rate={res_top3['hit_rate']:.6f}, avg_payout={res_top3['avg_payout_hit']:.2f}")
    lines.append("")
    lines.append("v1 comparison")
    lines.append(f"- top1 ROI: {float(base_top1['roi_pct']):.6f} -> {res_top1['roi_pct']:.6f} (diff {res_top1['roi_pct']-float(base_top1['roi_pct']):+.6f})")
    lines.append(f"- top1 hit_rate: {float(base_top1['hit_rate']):.6f} -> {res_top1['hit_rate']:.6f} (diff {res_top1['hit_rate']-float(base_top1['hit_rate']):+.6f})")
    lines.append(f"- top3 ROI: {float(base_top3['roi_pct']):.6f} -> {res_top3['roi_pct']:.6f} (diff {res_top3['roi_pct']-float(base_top3['roi_pct']):+.6f})")
    lines.append(f"- top3 hit_rate: {float(base_top3['hit_rate']):.6f} -> {res_top3['hit_rate']:.6f} (diff {res_top3['hit_rate']-float(base_top3['hit_rate']):+.6f})")
    lines.append("")
    lines.append("diffの寄与")
    lines.append("《推測》value_gap_zを馬選択に混ぜることで、同一レース選別内の馬順位を再配列し、ペア構成の期待値を変える効果を狙っています。")

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print("\n=== summary ===")
    print(f"top1_roi={res_top1['roi_pct']:.6f} top1_hit={res_top1['hit_rate']:.6f}")
    print(f"top3_roi={res_top3['roi_pct']:.6f} top3_hit={res_top3['hit_rate']:.6f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
