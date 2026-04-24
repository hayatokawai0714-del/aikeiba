import argparse
import math
from itertools import combinations
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd


TRAIN_YEARS = {2021, 2022, 2023, 2024}
VALID_YEARS = {2025}
TEST_YEARS = {2026}
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
    parser = argparse.ArgumentParser(description="Apply separated ability-gap model to existing wide flow.")
    parser.add_argument("--dataset", default=r"C:\TXT\dataset_top3_with_history_phase1.csv")
    parser.add_argument("--base-score", default=r"C:\TXT\top3_value_score_2026_v1.csv")
    parser.add_argument("--base-roi", default=r"C:\TXT\wide_roi_real_2026_v1.csv")
    parser.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    parser.add_argument("--out-csv", default=r"C:\TXT\wide_roi_with_ability_gap_v1.csv")
    parser.add_argument("--out-report", default=r"C:\TXT\wide_roi_with_ability_gap_report.txt")
    parser.add_argument("--out-wide", default=r"C:\TXT\wide_candidates_2026_ability_gap_v1.csv")
    parser.add_argument("--out-race", default=r"C:\TXT\wide_race_selection_fixed_rate_2026_ability_gap_v1.csv")
    parser.add_argument("--encoding", default="cp932")
    parser.add_argument("--seed", type=int, default=42)
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


def zscore(series: pd.Series) -> pd.Series:
    std = float(series.std(ddof=0))
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=series.index)
    return (series - float(series.mean())) / std


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


def train_ability_model(dataset_path: Path, encoding: str, seed: int):
    df = pd.read_csv(dataset_path, encoding=encoding, low_memory=False)
    req = [
        "race_date",
        "race_id_raw",
        "horse_no",
        "win_odds",
        "top3",
        "prev_finish_position",
        "avg_finish_last3",
        "same_distance_win_rate",
    ]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing dataset columns: {missing}")

    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df[df["race_date"].notna()].copy()
    df["year"] = df["race_date"].dt.year
    df["race_id_raw"] = df["race_id_raw"].map(normalize_race_id_raw)
    df["horse_no"] = to_float(df["horse_no"]).astype("Int64")

    for c in ["win_odds", "top3", "prev_finish_position", "avg_finish_last3", "same_distance_win_rate"]:
        df[c] = to_float(df[c])

    train = df[df["year"].isin(TRAIN_YEARS)].copy()
    valid = df[df["year"].isin(VALID_YEARS)].copy()
    test = df[df["year"].isin(TEST_YEARS)].copy()
    if train.empty or valid.empty or test.empty:
        raise SystemExit(f"Split empty: train={len(train)} valid={len(valid)} test={len(test)}")

    feats = ["prev_finish_position", "avg_finish_last3", "same_distance_win_rate"]
    medians = {f: float(train[f].median()) for f in feats}
    for split in [train, valid, test]:
        for f in feats:
            split[f] = split[f].fillna(medians[f])

    model = lgb.LGBMClassifier(
        objective="binary",
        n_estimators=4000,
        learning_rate=0.03,
        num_leaves=31,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=seed,
        n_jobs=-1,
    )
    model.fit(
        train[feats],
        train["top3"].astype(int),
        eval_set=[(valid[feats], valid["top3"].astype(int))],
        eval_metric=["auc", "binary_logloss"],
        callbacks=[lgb.early_stopping(200), lgb.log_evaluation(200)],
    )

    test = test.copy()
    test["ability_top3_prob"] = model.predict_proba(test[feats])[:, 1]
    test["market_prob"] = np.where(test["win_odds"] > 0, 1.0 / test["win_odds"], np.nan)
    test["ability_gap"] = test["ability_top3_prob"] - test["market_prob"]
    return test[
        [
            "race_date",
            "race_id_raw",
            "horse_no",
            "top3",
            "win_odds",
            "ability_top3_prob",
            "market_prob",
            "ability_gap",
        ]
    ].copy()


def build_score_v2(base_score_path: Path, ability_df: pd.DataFrame, encoding: str) -> pd.DataFrame:
    base = pd.read_csv(base_score_path, encoding=encoding, low_memory=False)
    need = ["race_id_raw", "horse_no", "value_score_v1", "pred_top3_raw", "top3"]
    missing = [c for c in need if c not in base.columns]
    if missing:
        raise SystemExit(f"Missing base score columns: {missing}")

    base["race_id_raw"] = base["race_id_raw"].map(normalize_race_id_raw)
    base["horse_no"] = to_float(base["horse_no"]).astype("Int64")
    base["value_score_v1"] = to_float(base["value_score_v1"])
    base["pred_top3_raw"] = to_float(base["pred_top3_raw"])
    base["top3"] = to_float(base["top3"]).fillna(0).astype(int)

    merged = base.merge(
        ability_df[["race_id_raw", "horse_no", "ability_top3_prob", "market_prob", "ability_gap"]],
        on=["race_id_raw", "horse_no"],
        how="left",
    )
    merged["value_score_v2"] = 0.7 * merged["value_score_v1"] + 0.3 * merged["ability_gap"]
    merged = merged[merged["value_score_v2"].notna() & merged["pred_top3_raw"].notna()].copy()
    merged["value_score_rank"] = (
        merged.groupby("race_id_raw", dropna=False)["value_score_v2"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    merged["pred_top3_rank"] = (
        merged.groupby("race_id_raw", dropna=False)["pred_top3_raw"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    race_mean = merged.groupby("race_id_raw", dropna=False)["value_score_v2"].transform("mean")
    race_std = merged.groupby("race_id_raw", dropna=False)["value_score_v2"].transform(lambda s: s.std(ddof=0)).fillna(0.0)
    merged["value_gap_z"] = np.where(race_std > 0, (merged["value_score_v2"] - race_mean) / race_std, 0.0)
    merged["gap_std"] = race_std
    merged["value_gap"] = merged["value_score_v2"]

    merged["value_score_v1_base"] = merged["value_score_v1"]
    merged["value_score_v1"] = merged["value_score_v2"]
    return merged


def build_wide_candidates(score_df: pd.DataFrame) -> pd.DataFrame:
    score = score_df.copy()
    score["is_candidate"] = (score["value_score_rank"] <= 3) | (score["pred_top3_rank"] <= 3)
    cand = score[score["is_candidate"]].copy()

    rows: list[dict] = []
    for race_id, race_df in cand.groupby("race_id_raw", dropna=False):
        if len(race_df) < 2:
            continue
        records = race_df.to_dict("records")
        for left, right in combinations(records, 2):
            ordered = sorted([left, right], key=lambda r: (-float(r["pred_top3_raw"]), int(r["horse_no"])))
            first, second = ordered[0], ordered[1]
            p1 = float(first["pred_top3_raw"])
            p2 = float(second["pred_top3_raw"])
            v1 = float(first["value_score_v2"])
            v2 = float(second["value_score_v2"])
            t1 = int(first["top3"])
            t2 = int(second["top3"])
            rows.append(
                {
                    "race_date": first.get("race_date", second.get("race_date")),
                    "race_id_raw": race_id,
                    "horse_no_1": int(first["horse_no"]),
                    "horse_no_2": int(second["horse_no"]),
                    "horse_name_1": first.get("horse_name", ""),
                    "horse_name_2": second.get("horse_name", ""),
                    "pred_top3_1": p1,
                    "pred_top3_2": p2,
                    "value_score_1": v1,
                    "value_score_2": v2,
                    "pair_score_v1": p1 * p2,
                    "pair_value_score_v1": v1 + v2,
                    "top3_1": t1,
                    "top3_2": t2,
                    "wide_hit": int(t1 == 1 and t2 == 1),
                    "candidate_horse_count": int(len(race_df)),
                }
            )

    if not rows:
        raise SystemExit("No candidate pairs generated.")
    pairs = pd.DataFrame(rows)
    pairs["pair_rank_score"] = (
        pairs.groupby("race_id_raw", dropna=False)["pair_score_v1"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    pairs["pair_rank_value"] = (
        pairs.groupby("race_id_raw", dropna=False)["pair_value_score_v1"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    selected = pairs[(pairs["pair_rank_score"] <= 3) | (pairs["pair_rank_value"] <= 3)].copy()
    selected = selected.sort_values(["race_date", "race_id_raw", "pair_rank_score", "pair_rank_value"]).reset_index(drop=True)
    return selected


def build_race_selection(score_df: pd.DataFrame, wide_df: pd.DataFrame) -> pd.DataFrame:
    race_rows = []
    for race_id, race_df in score_df.groupby("race_id_raw", dropna=False):
        sorted_df = race_df.sort_values("value_score_rank", ascending=True)
        top2 = sorted_df.head(2)
        diff = float("nan")
        if len(top2) >= 2:
            diff = float(top2.iloc[0]["value_score_v2"] - top2.iloc[1]["value_score_v2"])
        race_rows.append(
            {
                "race_id_raw": race_id,
                "race_date": race_df["race_date"].iloc[0] if "race_date" in race_df.columns else "",
                "value_score_max": float(race_df["value_score_v2"].max()),
                "value_score_top2_mean": float(top2["value_score_v2"].mean()) if len(top2) else np.nan,
                "gap_std": float(race_df["gap_std"].iloc[0]) if "gap_std" in race_df.columns else float(race_df["value_gap"].std(ddof=0)),
                "top1_top2_diff": diff,
                "horse_count": int(len(race_df)),
            }
        )
    race_df = pd.DataFrame(race_rows)

    top1 = (
        wide_df[wide_df["pair_rank_score"] == 1]
        .groupby("race_id_raw", dropna=False)
        .agg(top1_pair_score_v1=("pair_score_v1", "first"), top1_wide_hit=("wide_hit", "first"))
        .reset_index()
    )
    top3 = (
        wide_df[wide_df["pair_rank_score"] <= 3]
        .groupby("race_id_raw", dropna=False)
        .agg(top3_any_hit=("wide_hit", "max"))
        .reset_index()
    )
    race_df = race_df.merge(top1, on="race_id_raw", how="inner").merge(top3, on="race_id_raw", how="left")
    race_df["top1_wide_hit"] = to_float(race_df["top1_wide_hit"]).fillna(0).astype(int)
    race_df["top3_any_hit"] = to_float(race_df["top3_any_hit"]).fillna(0).astype(int)

    race_df["z_value_score_max"] = zscore(race_df["value_score_max"])
    race_df["z_gap_std"] = zscore(race_df["gap_std"])
    race_df["race_select_score_v1"] = 0.6 * race_df["z_value_score_max"] + 0.4 * race_df["z_gap_std"]
    race_df = race_df.sort_values("race_select_score_v1", ascending=False).reset_index(drop=True)
    race_df["race_select_rank"] = np.arange(1, len(race_df) + 1)

    n_total = len(race_df)
    race_df["selected_top15"] = race_df["race_select_rank"] <= int(math.ceil(n_total * 0.15))
    race_df["selected_top20"] = race_df["race_select_rank"] <= int(math.ceil(n_total * 0.20))
    race_df["selected_top25"] = race_df["race_select_rank"] <= int(math.ceil(n_total * 0.25))
    race_df["selected_recommended"] = race_df["selected_top15"]
    race_df["recommended_rate"] = 0.15
    return race_df


def summarize_roi(joined: pd.DataFrame, pattern: str, stake: int) -> dict:
    if pattern == "top1":
        sub = joined[joined["pair_rank_score"] == 1].copy()
    else:
        sub = joined[joined["pair_rank_score"] <= 3].copy()
    bets = len(sub)
    races = sub["race_id_raw"].nunique() if len(sub) else 0
    stake_total = bets * stake
    ret = float(sub["return_yen"].sum()) if len(sub) else 0.0
    roi = float((ret / stake_total) * 100.0) if stake_total else np.nan
    hit_rate = float(sub["wide_hit_real"].mean()) if len(sub) else np.nan
    return {
        "pattern": pattern,
        "month": "ALL",
        "races": int(races),
        "bets": int(bets),
        "hit_rate": hit_rate,
        "roi_pct": roi,
        "avg_payout_yen": float(sub.loc[sub["wide_hit_real"] == 1, "payout_yen"].mean()) if (len(sub) and (sub["wide_hit_real"] == 1).any()) else np.nan,
        "stake_total": int(stake_total),
        "return_total": ret,
    }


def summarize_monthly(joined: pd.DataFrame, pattern: str, stake: int) -> list[dict]:
    if pattern == "top1":
        sub = joined[joined["pair_rank_score"] == 1].copy()
    else:
        sub = joined[joined["pair_rank_score"] <= 3].copy()
    rows = []
    for month in MONTHS:
        mdf = sub[sub["race_month"] == month].copy()
        bets = len(mdf)
        stake_total = bets * stake
        ret = float(mdf["return_yen"].sum()) if len(mdf) else 0.0
        roi = float((ret / stake_total) * 100.0) if stake_total else np.nan
        rows.append(
            {
                "pattern": pattern,
                "month": month,
                "races": int(mdf["race_id_raw"].nunique()) if len(mdf) else 0,
                "bets": int(bets),
                "hit_rate": float(mdf["wide_hit_real"].mean()) if len(mdf) else np.nan,
                "roi_pct": roi,
                "avg_payout_yen": float(mdf.loc[mdf["wide_hit_real"] == 1, "payout_yen"].mean()) if (len(mdf) and (mdf["wide_hit_real"] == 1).any()) else np.nan,
                "stake_total": int(stake_total),
                "return_total": ret,
            }
        )
    return rows


def main() -> int:
    args = parse_args()

    ability = train_ability_model(Path(args.dataset), args.encoding, args.seed)
    score_v2 = build_score_v2(Path(args.base_score), ability, args.encoding)
    wide = build_wide_candidates(score_v2)
    race = build_race_selection(score_v2, wide)

    selected_ids = set(race.loc[race["selected_top15"] == True, "race_id_raw"].astype(str))  # noqa: E712
    target = wide[wide["race_id_raw"].astype(str).isin(selected_ids)].copy()
    if target.empty:
        raise SystemExit("No target bets after selected_top15.")

    target["race_id"] = target["race_id_raw"].map(raw_to_race_id)
    target["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(target["horse_no_1"], target["horse_no_2"])]

    payout_df = load_payouts(Path(args.payout_root))
    if payout_df.empty:
        raise SystemExit("No payout data found.")

    joined = target.merge(payout_df, on=["race_id", "pair_key"], how="left")
    joined["wide_hit_real"] = joined["payout_yen"].notna().astype(int)
    joined["return_yen"] = joined["payout_yen"].fillna(0.0)
    joined["race_month"] = pd.to_datetime(joined["race_date"], errors="coerce").dt.strftime("%Y-%m")

    overall_rows = [summarize_roi(joined, "top1", args.stake), summarize_roi(joined, "top3", args.stake)]
    monthly_rows = summarize_monthly(joined, "top1", args.stake) + summarize_monthly(joined, "top3", args.stake)
    result = pd.DataFrame(overall_rows + monthly_rows)

    for pattern in ["top1", "top3"]:
        mask = result["pattern"] == pattern
        monthly = result[mask & (result["month"] != "ALL")]
        worst = float(monthly["roi_pct"].min()) if len(monthly) else np.nan
        result.loc[mask & (result["month"] == "ALL"), "worst_month_roi"] = worst

    base_cmp = None
    base_path = Path(args.base_roi)
    if base_path.exists():
        base = pd.read_csv(base_path, encoding=args.encoding, low_memory=False)
        if {"pattern", "roi_pct", "hit_rate"}.issubset(base.columns):
            base_cmp = base[["pattern", "roi_pct", "hit_rate"]].rename(
                columns={"roi_pct": "base_roi_pct", "hit_rate": "base_hit_rate"}
            )

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_wide = Path(args.out_wide)
    out_race = Path(args.out_race)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)
    out_wide.parent.mkdir(parents=True, exist_ok=True)
    out_race.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(out_csv, index=False, encoding=args.encoding)
    wide.to_csv(out_wide, index=False, encoding=args.encoding)
    race.to_csv(out_race, index=False, encoding=args.encoding)

    report_lines: list[str] = []
    report_lines.append("wide roi with ability gap report")
    report_lines.append("")
    report_lines.append(f"input_dataset={args.dataset}")
    report_lines.append(f"input_base_score={args.base_score}")
    report_lines.append(f"output_csv={out_csv}")
    report_lines.append(f"output_wide={out_wide}")
    report_lines.append(f"output_race={out_race}")
    report_lines.append("")
    report_lines.append("ability model")
    report_lines.append("- target: top3")
    report_lines.append("- features: prev_finish_position, avg_finish_last3, same_distance_win_rate")
    report_lines.append("- split: train=2021-2024 valid=2025 test=2026")
    report_lines.append("")
    report_lines.append("integration")
    report_lines.append("- market_prob = 1 / win_odds")
    report_lines.append("- ability_gap = ability_top3_prob - market_prob")
    report_lines.append("- value_score_v2 = 0.7 * value_score_v1 + 0.3 * ability_gap")
    report_lines.append("- existing race-selection / horse-selection / wide generation flow applied")
    report_lines.append("")
    report_lines.append("roi summary")
    report_lines.extend(result[result["month"] == "ALL"].to_string(index=False).splitlines())
    report_lines.append("")
    report_lines.append("monthly roi")
    report_lines.extend(result[result["month"] != "ALL"].to_string(index=False).splitlines())

    if base_cmp is not None:
        merged = result[result["month"] == "ALL"][["pattern", "roi_pct", "hit_rate"]].merge(base_cmp, on="pattern", how="left")
        merged["roi_diff_vs_base"] = merged["roi_pct"] - merged["base_roi_pct"]
        merged["hit_diff_vs_base"] = merged["hit_rate"] - merged["base_hit_rate"]
        report_lines.append("")
        report_lines.append("comparison vs base(v1)")
        report_lines.extend(merged.to_string(index=False).splitlines())

    out_report.write_text("\n".join(report_lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print("\n=== overall ===")
    print(result[result["month"] == "ALL"].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
