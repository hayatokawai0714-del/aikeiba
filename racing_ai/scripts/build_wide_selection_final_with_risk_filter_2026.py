import argparse
import itertools
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
    ap = argparse.ArgumentParser(description="Build final v1 + diff-risk-filter wide pipeline (2026).")
    ap.add_argument("--race", default=r"C:\TXT\wide_race_selection_2026_v1.csv")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--horse", default=r"C:\TXT\top3_value_score_2026_v1.csv")
    ap.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_selection_final_with_risk_filter_2026.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_selection_final_with_risk_filter_report_2026.txt")
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


def normalize_selected_flag(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    s = series.astype(str).str.strip().str.lower()
    return s.isin({"true", "1", "t", "yes", "y"})


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


def build_value_gap_top3_features(horse: pd.DataFrame) -> pd.DataFrame:
    horse = horse.copy()
    horse["race_id_raw"] = horse["race_id_raw"].astype(str)
    horse["value_gap"] = to_float(horse["value_gap"])
    horse = horse[horse["value_gap"].notna()].copy()
    horse["value_gap_rank_calc"] = horse.groupby("race_id_raw", dropna=False)["value_gap"].rank(method="first", ascending=False)
    top3 = horse[horse["value_gap_rank_calc"] <= 3].copy()
    feat = (
        top3.groupby("race_id_raw", dropna=False)
        .agg(
            value_gap_top3_var=("value_gap", lambda s: float(np.var(s, ddof=0))),
            value_gap_top3_min=("value_gap", "min"),
        )
        .reset_index()
    )
    return feat


def build_pairs_from_horse(horse: pd.DataFrame, race_ids: set[str]) -> pd.DataFrame:
    cols = ["race_date", "race_id_raw", "horse_no", "horse_name", "pred_top3_raw", "value_score_v1", "value_score_rank", "top3"]
    df = horse[cols].copy()
    df["race_id_raw"] = df["race_id_raw"].astype(str)
    df = df[df["race_id_raw"].isin(race_ids)].copy()

    for c in ["horse_no", "pred_top3_raw", "value_score_v1", "value_score_rank", "top3"]:
        df[c] = to_float(df[c])
    df = df[df["horse_no"].notna() & df["pred_top3_raw"].notna() & df["value_score_v1"].notna() & df["top3"].notna()].copy()

    df["pred_rank_calc"] = df.groupby("race_id_raw", dropna=False)["pred_top3_raw"].rank(method="first", ascending=False)
    df["candidate_flag"] = (df["value_score_rank"] <= 3) | (df["pred_rank_calc"] <= 3)

    pair_rows = []
    for race_id, grp in df.groupby("race_id_raw", dropna=False):
        cand = grp[grp["candidate_flag"]].copy()
        if len(cand) < 2:
            continue
        records = cand.to_dict("records")
        for left, right in itertools.combinations(records, 2):
            pair_rows.append(
                {
                    "race_date": left["race_date"],
                    "race_id_raw": race_id,
                    "horse_no_1": int(left["horse_no"]),
                    "horse_no_2": int(right["horse_no"]),
                    "horse_name_1": left["horse_name"],
                    "horse_name_2": right["horse_name"],
                    "pred_top3_1": float(left["pred_top3_raw"]),
                    "pred_top3_2": float(right["pred_top3_raw"]),
                    "value_score_1": float(left["value_score_v1"]),
                    "value_score_2": float(right["value_score_v1"]),
                    "top3_1": int(left["top3"]),
                    "top3_2": int(right["top3"]),
                }
            )

    if not pair_rows:
        return pd.DataFrame(
            columns=[
                "race_date",
                "race_id_raw",
                "horse_no_1",
                "horse_no_2",
                "horse_name_1",
                "horse_name_2",
                "pred_top3_1",
                "pred_top3_2",
                "value_score_1",
                "value_score_2",
                "top3_1",
                "top3_2",
                "pair_score_v1",
                "pair_value_score_v1",
                "wide_hit",
                "pair_rank_score",
            ]
        )

    pairs = pd.DataFrame(pair_rows)
    pairs["pair_score_v1"] = pairs["pred_top3_1"] * pairs["pred_top3_2"]
    pairs["pair_value_score_v1"] = pairs["value_score_1"] + pairs["value_score_2"]
    pairs["wide_hit"] = ((pairs["top3_1"] == 1) & (pairs["top3_2"] == 1)).astype(int)

    pairs = pairs.sort_values(["race_id_raw", "pair_score_v1", "pair_value_score_v1"], ascending=[True, False, False]).copy()
    pairs["pair_rank_score"] = pairs.groupby("race_id_raw", dropna=False).cumcount() + 1
    pairs = pairs[pairs["pair_rank_score"] <= 3].copy()

    pairs["race_id"] = pairs["race_id_raw"].map(raw_to_race_id)
    pairs["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(pairs["horse_no_1"], pairs["horse_no_2"])]
    return pairs.reset_index(drop=True)


def evaluate_roi(pairs: pd.DataFrame, selected_ids: set[str], stake: int) -> dict:
    sub = pairs[pairs["race_id_raw"].isin(selected_ids)].copy()
    top1 = sub[sub["pair_rank_score"] == 1].copy()
    top3 = sub[sub["pair_rank_score"] <= 3].copy()

    def _calc(df: pd.DataFrame, bets_per_race: int) -> dict:
        races = int(df["race_id_raw"].nunique()) if len(df) else 0
        bets = int(len(df))
        stake_total = bets * stake
        ret = float(df["return_yen"].sum()) if len(df) else 0.0
        roi = float((ret / stake_total) * 100.0) if stake_total else np.nan
        hit = float(df["wide_hit_real"].mean()) if len(df) else np.nan

        month_roi = {}
        month_col = pd.to_datetime(df["race_date"], errors="coerce").dt.strftime("%Y-%m") if len(df) else pd.Series(dtype=object)
        for m in MONTHS:
            ms = df[month_col == m] if len(df) else df
            ms_stake = len(ms) * stake
            month_roi[m] = float((ms["return_yen"].sum() / ms_stake) * 100.0) if ms_stake else np.nan

        vals = np.array([v for v in month_roi.values() if pd.notna(v)], dtype=float)
        worst = float(vals.min()) if len(vals) else np.nan

        return {
            "races": races,
            "bets": bets,
            "stake_total": int(stake_total),
            "return_total": ret,
            "roi_pct": roi,
            "hit_rate": hit,
            "worst_month_roi": worst,
            "roi_2026_01": month_roi["2026-01"],
            "roi_2026_02": month_roi["2026-02"],
            "roi_2026_03": month_roi["2026-03"],
            "roi_2026_04": month_roi["2026-04"],
        }

    return {"top1": _calc(top1, 1), "top3": _calc(top3, 3)}


def main() -> int:
    args = parse_args()

    race = pd.read_csv(Path(args.race), encoding=args.encoding, low_memory=False)
    wide = pd.read_csv(Path(args.wide), encoding=args.encoding, low_memory=False)
    horse = pd.read_csv(Path(args.horse), encoding=args.encoding, low_memory=False)

    race["race_id_raw"] = race["race_id_raw"].astype(str)
    race["top1_top2_diff"] = to_float(race["top1_top2_diff"])
    race["selected_by_best_filter"] = normalize_selected_flag(race["selected_by_best_filter"])

    top3_feat = build_value_gap_top3_features(horse)
    race = race.merge(top3_feat, on="race_id_raw", how="left")

    diff_th = 0.206736
    var_th = 0.006311
    race["is_danger_race"] = (race["top1_top2_diff"] < diff_th) & (race["value_gap_top3_var"] < var_th)

    race["selected_v1"] = race["selected_by_best_filter"]
    race["selected_v1_risk_filtered"] = race["selected_v1"] & (~race["is_danger_race"])

    selected_v1_ids = set(race.loc[race["selected_v1"], "race_id_raw"])
    selected_filtered_ids = set(race.loc[race["selected_v1_risk_filtered"], "race_id_raw"])

    pairs = build_pairs_from_horse(horse, selected_v1_ids)
    if pairs.empty:
        raise SystemExit("No pair candidates generated from horse data.")

    payout = load_wide_payouts(Path(args.payout_root))
    if payout.empty:
        raise SystemExit("No 2026 payout data found.")

    pairs = pairs.merge(payout, on=["race_id", "pair_key"], how="left")
    pairs["return_yen"] = pairs["payout_yen"].fillna(0.0)
    pairs["wide_hit_real"] = pairs["payout_yen"].notna().astype(int)

    eval_v1 = evaluate_roi(pairs, selected_v1_ids, args.stake)
    eval_rf = evaluate_roi(pairs, selected_filtered_ids, args.stake)

    top1_race_return = (
        pairs[pairs["pair_rank_score"] == 1]
        .groupby("race_id_raw", dropna=False)["return_yen"]
        .sum()
        .rename("top1_return")
        .reset_index()
    )
    top3_race_return = (
        pairs[pairs["pair_rank_score"] <= 3]
        .groupby("race_id_raw", dropna=False)["return_yen"]
        .sum()
        .rename("top3_return")
        .reset_index()
    )
    race_out = race.merge(top1_race_return, on="race_id_raw", how="left").merge(top3_race_return, on="race_id_raw", how="left")
    race_out["top1_return"] = race_out["top1_return"].fillna(0.0)
    race_out["top3_return"] = race_out["top3_return"].fillna(0.0)
    race_out["race_date"] = pd.to_datetime(race_out["race_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    race_out[
        [
            "race_date",
            "race_id_raw",
            "selected_v1",
            "selected_v1_risk_filtered",
            "is_danger_race",
            "top1_top2_diff",
            "value_gap_top3_var",
            "value_gap_top3_min",
            "value_score_max",
            "gap_std",
            "top1_return",
            "top3_return",
        ]
    ].to_csv(out_csv, index=False, encoding=args.encoding)

    rows = []
    rows.append("wide final pipeline with diff risk filter (2026)")
    rows.append("")
    rows.append(f"input_race={args.race}")
    rows.append(f"input_wide={args.wide}")
    rows.append(f"input_horse={args.horse}")
    rows.append(f"output={out_csv}")
    rows.append("")
    rows.append("risk rule")
    rows.append(f"- top1_top2_diff < {diff_th:.6f}")
    rows.append(f"- value_gap_top3_var < {var_th:.6f}")
    rows.append("- both true => exclude")
    rows.append("")
    rows.append("counts")
    rows.append(f"- v1 selected races={len(selected_v1_ids)}")
    rows.append(f"- excluded races={int((race['selected_v1'] & race['is_danger_race']).sum())}")
    rows.append(f"- kept races={len(selected_filtered_ids)}")
    rows.append("")
    rows.append("metrics")
    for name, res in [("v1_only", eval_v1), ("v1_plus_risk_filter", eval_rf)]:
        rows.append(f"[{name}]")
        for pat in ["top1", "top3"]:
            r = res[pat]
            rows.append(
                f"- {pat}: ROI={r['roi_pct']:.6f}, hit={r['hit_rate']:.6f}, races={r['races']}, worst_month={r['worst_month_roi']:.6f}, "
                f"m01={r['roi_2026_01']:.6f}, m02={r['roi_2026_02']:.6f}, m03={r['roi_2026_03']:.6f}, m04={r['roi_2026_04']:.6f}"
            )
        rows.append("")

    rows.append("comparison (risk_filter - v1)")
    for pat in ["top1", "top3"]:
        rv1 = eval_v1[pat]
        rrf = eval_rf[pat]
        rows.append(
            f"- {pat}: ROI_diff={rrf['roi_pct'] - rv1['roi_pct']:.6f}, hit_diff={rrf['hit_rate'] - rv1['hit_rate']:.6f}, "
            f"worst_month_diff={rrf['worst_month_roi'] - rv1['worst_month_roi']:.6f}, m04_diff={rrf['roi_2026_04'] - rv1['roi_2026_04']:.6f}"
        )

    out_report.write_text("\n".join(rows) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print("=== comparison ===")
    for pat in ["top1", "top3"]:
        rv1 = eval_v1[pat]
        rrf = eval_rf[pat]
        print(
            f"{pat}: roi_v1={rv1['roi_pct']:.6f}, roi_rf={rrf['roi_pct']:.6f}, hit_v1={rv1['hit_rate']:.6f}, hit_rf={rrf['hit_rate']:.6f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
