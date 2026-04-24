import argparse
import itertools
import math
from pathlib import Path

import numpy as np
import pandas as pd
import lightgbm as lgb

JYO_TO_VENUE = {
    "01": "SAP", "02": "HAK", "03": "FUK", "04": "NII", "05": "TOK",
    "06": "NAK", "07": "CHU", "08": "KYO", "09": "HAN", "10": "KOK",
}

W1_RANGE = np.arange(0.50, 0.80 + 1e-9, 0.05)
W2_RANGE = np.arange(0.15, 0.40 + 1e-9, 0.05)
W3_RANGE = np.arange(0.00, 0.25 + 1e-9, 0.05)
RATES = [0.15, 0.20, 0.25]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Train(2021-2025) / Test(2026) grid-search for wide selection weights.")
    ap.add_argument("--race", default=r"C:\TXT\wide_race_selection_2026_v1.csv")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--roi", default=r"C:\TXT\wide_roi_real_2026_v1.csv")
    ap.add_argument("--dataset", default=r"C:\TXT\dataset_top3_2021_2026_v1_clean.csv")
    ap.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    ap.add_argument("--out-train", default=r"C:\TXT\wide_selection_weight_grid_train_2021_2025.csv")
    ap.add_argument("--out-test", default=r"C:\TXT\wide_selection_weight_grid_test_2026.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_selection_weight_grid_report_final.txt")
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
        if "202" not in str(fp):
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


def build_oos_predictions(dataset_path: Path, encoding: str) -> pd.DataFrame:
    df = pd.read_csv(dataset_path, encoding=encoding, low_memory=False)
    need = [
        "race_date", "race_id_raw", "horse_no", "horse_name", "jockey_name", "top3",
        "win_odds", "pop_rank", "distance", "field_size", "track_condition",
    ]
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise SystemExit(f"dataset missing columns: {miss}")

    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df[df["race_date"].notna()].copy()
    df["year"] = df["race_date"].dt.year

    for c in ["top3", "win_odds", "pop_rank", "distance", "field_size"]:
        df[c] = to_float(df[c])

    df = df[df["win_odds"].notna()].copy()

    features_num = ["log_win_odds", "distance", "field_size", "pop_rank", "pop_rank_rate"]
    features_cat = ["track_condition", "jockey_name"]
    features = features_num + features_cat

    pred_frames = []

    eval_years = [2021, 2022, 2023, 2024, 2025, 2026]
    for y in eval_years:
        eval_df = df[df["year"] == y].copy()
        if eval_df.empty:
            continue

        if y == 2021:
            # 《推測》2021は過去年が無いため同年学習で近似
            train_df = df[df["year"] == 2021].copy()
        else:
            train_df = df[df["year"] < y].copy()

        if train_df.empty:
            continue

        med_pop = float(train_df["pop_rank"].median())
        med_dist = float(train_df["distance"].median())
        med_field = float(train_df["field_size"].median())

        for split in [train_df, eval_df]:
            split["pop_rank"] = split["pop_rank"].fillna(med_pop)
            split["distance"] = split["distance"].fillna(med_dist)
            split["field_size"] = split["field_size"].fillna(med_field)
            split["track_condition"] = split["track_condition"].fillna("UNKNOWN").astype(str)
            split["jockey_name"] = split["jockey_name"].fillna("UNKNOWN").astype(str)
            split["log_win_odds"] = np.log(split["win_odds"].clip(lower=1e-6))
            split["pop_rank_rate"] = split["pop_rank"] / split["field_size"].replace(0, np.nan)

        med_rank_rate = float(train_df["pop_rank_rate"].median())
        for split in [train_df, eval_df]:
            split["pop_rank_rate"] = split["pop_rank_rate"].fillna(med_rank_rate)
            for col in features_cat:
                split[col] = split[col].astype("category")

        model = lgb.LGBMClassifier(
            objective="binary",
            n_estimators=700,
            learning_rate=0.05,
            num_leaves=31,
            subsample=0.9,
            colsample_bytree=0.9,
            random_state=42,
            n_jobs=-1,
        )

        model.fit(
            train_df[features],
            train_df["top3"].astype(int),
            categorical_feature=features_cat,
        )

        eval_df["pred_top3_raw"] = model.predict_proba(eval_df[features])[:, 1]
        pred_frames.append(
            eval_df[
                [
                    "race_date", "year", "race_id_raw", "horse_no", "horse_name", "jockey_name",
                    "top3", "pred_top3_raw", "win_odds",
                ]
            ].copy()
        )

    if not pred_frames:
        raise SystemExit("No prediction frames generated")

    return pd.concat(pred_frames, ignore_index=True)


def build_pairs_from_horses(horse_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    horse_df = horse_df.copy()
    horse_df["market_win_prob"] = np.where(horse_df["win_odds"] > 0, 1.0 / horse_df["win_odds"], np.nan)
    horse_df["value_gap"] = horse_df["pred_top3_raw"] - horse_df["market_win_prob"]

    g = horse_df.groupby("race_id_raw", dropna=False)
    horse_df["value_gap_rank"] = g["value_gap"].rank(method="first", ascending=False).astype(int)
    gap_mean = g["value_gap"].transform("mean")
    gap_std = g["value_gap"].transform(lambda x: x.std(ddof=0)).fillna(0.0)
    horse_df["gap_std"] = gap_std
    horse_df["value_gap_z"] = np.where(gap_std > 0, (horse_df["value_gap"] - gap_mean) / gap_std, 0.0)

    horse_df["pred_rank"] = g["pred_top3_raw"].rank(method="first", ascending=False).astype(int)

    rank_score = pd.Series(0.2, index=horse_df.index, dtype=float)
    rank_score[horse_df["value_gap_rank"] == 1] = 1.0
    rank_score[horse_df["value_gap_rank"] == 2] = 0.8
    rank_score[horse_df["value_gap_rank"] == 3] = 0.6
    rank_score[horse_df["value_gap_rank"] == 4] = 0.4
    horse_df["rank_score"] = rank_score
    horse_df["value_score_v1"] = 0.6 * horse_df["value_gap_z"] + 0.4 * horse_df["rank_score"]
    horse_df["value_score_rank"] = horse_df.groupby("race_id_raw", dropna=False)["value_score_v1"].rank(method="first", ascending=False).astype(int)

    race_feat_rows = []
    pair_rows = []

    for race_id, rdf in horse_df.groupby("race_id_raw", dropna=False):
        rs = rdf.sort_values("value_score_v1", ascending=False)
        top1 = float(rs.iloc[0]["value_score_v1"])
        top2 = float(rs.iloc[1]["value_score_v1"]) if len(rs) >= 2 else float("nan")
        race_feat_rows.append(
            {
                "race_id_raw": str(race_id),
                "race_date": rs.iloc[0]["race_date"],
                "year": int(rs.iloc[0]["year"]),
                "value_score_max": float(rs["value_score_v1"].max()),
                "gap_std": float(rs["gap_std"].iloc[0]),
                "top1_top2_diff": float(top1 - top2) if pd.notna(top2) else np.nan,
            }
        )

        cand = rdf[(rdf["value_score_rank"] <= 3) | (rdf["pred_rank"] <= 3)].copy()
        recs = cand.to_dict("records")
        for i in range(len(recs)):
            for j in range(i + 1, len(recs)):
                a, b = recs[i], recs[j]
                pair_rows.append(
                    {
                        "race_id_raw": str(race_id),
                        "race_date": a["race_date"],
                        "year": int(a["year"]),
                        "horse_no_1": int(a["horse_no"]),
                        "horse_no_2": int(b["horse_no"]),
                        "pair_score_v1": float(a["pred_top3_raw"] * b["pred_top3_raw"]),
                    }
                )

    pair_df = pd.DataFrame(pair_rows)
    if pair_df.empty:
        raise SystemExit("No pairs generated")

    pair_df["pair_rank_score"] = pair_df.groupby("race_id_raw", dropna=False)["pair_score_v1"].rank(method="first", ascending=False).astype(int)

    race_feat = pd.DataFrame(race_feat_rows)
    race_feat["race_month"] = pd.to_datetime(race_feat["race_date"], errors="coerce").dt.strftime("%Y-%m")

    return race_feat, pair_df


def evaluate_for_config(race_df: pd.DataFrame, joined_pairs: pd.DataFrame, score_col: str, rate: float, pattern: str, stake: int) -> dict:
    keep_n = int(math.ceil(len(race_df) * rate))
    selected = race_df.sort_values(score_col, ascending=False).head(keep_n)
    ids = set(selected["race_id_raw"])

    sub = joined_pairs[joined_pairs["race_id_raw"].isin(ids)].copy()
    if pattern == "top1":
        sub = sub[sub["pair_rank_score"] == 1]
    else:
        sub = sub[sub["pair_rank_score"] <= 3]

    bets = len(sub)
    races = int(sub["race_id_raw"].nunique()) if bets else 0
    stake_total = bets * stake
    ret = float(sub["return_yen"].sum()) if bets else 0.0
    roi = float((ret / stake_total) * 100.0) if stake_total else np.nan

    month_group = sub.groupby("race_month", dropna=False)
    month_rois = {}
    for m, ms in month_group:
        ms_stake = len(ms) * stake
        month_rois[str(m)] = float((ms["return_yen"].sum() / ms_stake) * 100.0) if ms_stake else np.nan

    # ensure months keys for 2026 comparability
    for m in ["2026-01", "2026-02", "2026-03", "2026-04"]:
        month_rois.setdefault(m, np.nan)

    arr = np.array([v for v in month_rois.values() if pd.notna(v)], dtype=float)
    worst = float(arr.min()) if len(arr) else np.nan
    std = float(arr.std(ddof=0)) if len(arr) else np.nan

    month_text = "|".join([f"{k}:{month_rois[k]:.4f}" for k in sorted(month_rois.keys()) if pd.notna(month_rois[k])])

    return {
        "races": races,
        "bets": int(bets),
        "roi_pct": roi,
        "hit_rate": float(sub["wide_hit_real"].mean()) if bets else np.nan,
        "worst_month_roi": worst,
        "roi_monthly_std": std,
        "retained_rate": float(keep_n / len(race_df)),
        "month_roi_text": month_text,
    }


def main() -> int:
    args = parse_args()

    # existence check for declared inputs
    _ = pd.read_csv(Path(args.roi), encoding=args.encoding, low_memory=False)

    horse_pred = build_oos_predictions(Path(args.dataset), args.encoding)
    race_feat, pairs = build_pairs_from_horses(horse_pred)

    payout = load_wide_payouts(Path(args.payout_root))
    if payout.empty:
        raise SystemExit("No payout data found")

    pairs["race_id"] = pairs["race_id_raw"].map(raw_to_race_id)
    pairs["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(pairs["horse_no_1"], pairs["horse_no_2"])]
    pairs = pairs.merge(payout, on=["race_id", "pair_key"], how="left")
    pairs["wide_hit_real"] = pairs["payout_yen"].notna().astype(int)
    pairs["return_yen"] = pairs["payout_yen"].fillna(0.0)
    pairs["race_month"] = pd.to_datetime(pairs["race_date"], errors="coerce").dt.strftime("%Y-%m")

    train_race = race_feat[(race_feat["year"] >= 2021) & (race_feat["year"] <= 2025)].copy()
    test_race = race_feat[race_feat["year"] == 2026].copy()

    if train_race.empty or test_race.empty:
        raise SystemExit("Train or test race set is empty")

    for col in ["value_score_max", "gap_std", "top1_top2_diff"]:
        mean = float(train_race[col].mean())
        std = float(train_race[col].std(ddof=0))
        if std == 0 or np.isnan(std):
            train_race[f"z_{col}"] = 0.0
            test_race[f"z_{col}"] = 0.0
        else:
            train_race[f"z_{col}"] = (train_race[col] - mean) / std
            test_race[f"z_{col}"] = (test_race[col] - mean) / std

    train_pairs = pairs[pairs["year"].between(2021, 2025)].copy()
    test_pairs = pairs[pairs["year"] == 2026].copy()

    rows_train = []
    rows_test = []

    # v1 baseline rows
    for rate, pattern in itertools.product(RATES, ["top1", "top3"]):
        train_race["score"] = 0.6 * train_race["z_value_score_max"] + 0.4 * train_race["z_gap_std"]
        test_race["score"] = 0.6 * test_race["z_value_score_max"] + 0.4 * test_race["z_gap_std"]

        tr = evaluate_for_config(train_race, train_pairs, "score", rate, pattern, args.stake)
        te = evaluate_for_config(test_race, test_pairs, "score", rate, pattern, args.stake)

        base = {
            "w1_raw": 0.6, "w2_raw": 0.4, "w3_raw": 0.0,
            "w1": 0.6, "w2": 0.4, "w3": 0.0,
            "rate": rate, "pattern": pattern, "is_v1": True,
        }
        rows_train.append({**base, **tr})
        rows_test.append({**base, **te})

    for w1, w2, w3 in itertools.product(W1_RANGE, W2_RANGE, W3_RANGE):
        s = w1 + w2 + w3
        if s <= 0:
            continue
        nw1, nw2, nw3 = w1 / s, w2 / s, w3 / s

        train_race["score"] = nw1 * train_race["z_value_score_max"] + nw2 * train_race["z_gap_std"] + nw3 * train_race["z_top1_top2_diff"]
        test_race["score"] = nw1 * test_race["z_value_score_max"] + nw2 * test_race["z_gap_std"] + nw3 * test_race["z_top1_top2_diff"]

        for rate, pattern in itertools.product(RATES, ["top1", "top3"]):
            tr = evaluate_for_config(train_race, train_pairs, "score", rate, pattern, args.stake)
            te = evaluate_for_config(test_race, test_pairs, "score", rate, pattern, args.stake)
            row_common = {
                "w1_raw": float(w1), "w2_raw": float(w2), "w3_raw": float(w3),
                "w1": float(nw1), "w2": float(nw2), "w3": float(nw3),
                "rate": rate, "pattern": pattern, "is_v1": False,
            }
            rows_train.append({**row_common, **tr})
            rows_test.append({**row_common, **te})

    train_df = pd.DataFrame(rows_train)
    test_df = pd.DataFrame(rows_test)

    # choose best on train only (pair top1/top3 integrated)
    train_non_v1 = train_df[~train_df["is_v1"]].copy()
    pivot = train_non_v1.pivot_table(
        index=["w1_raw", "w2_raw", "w3_raw", "w1", "w2", "w3", "rate"],
        columns="pattern",
        values=["roi_pct", "worst_month_roi", "roi_monthly_std", "races"],
        aggfunc="first",
    )
    pivot.columns = [f"{a}_{b}" for a, b in pivot.columns]
    pivot = pivot.reset_index()

    # baseline by rate for both patterns
    base = train_df[train_df["is_v1"]].copy()
    base_pivot = base.pivot_table(index=["rate"], columns="pattern", values=["roi_pct", "worst_month_roi", "roi_monthly_std"], aggfunc="first")
    base_pivot.columns = [f"base_{a}_{b}" for a, b in base_pivot.columns]
    base_pivot = base_pivot.reset_index()

    pivot = pivot.merge(base_pivot, on="rate", how="left")

    # score criteria
    pivot["avg_roi"] = (pivot["roi_pct_top1"] + pivot["roi_pct_top3"]) / 2
    pivot["avg_worst"] = (pivot["worst_month_roi_top1"] + pivot["worst_month_roi_top3"]) / 2
    pivot["avg_std"] = (pivot["roi_monthly_std_top1"] + pivot["roi_monthly_std_top3"]) / 2
    pivot["avg_roi_diff_vs_v1"] = (
        (pivot["roi_pct_top1"] - pivot["base_roi_pct_top1"]) + (pivot["roi_pct_top3"] - pivot["base_roi_pct_top3"])
    ) / 2
    pivot["avg_worst_diff_vs_v1"] = (
        (pivot["worst_month_roi_top1"] - pivot["base_worst_month_roi_top1"]) + (pivot["worst_month_roi_top3"] - pivot["base_worst_month_roi_top3"])
    ) / 2

    pivot["passes"] = (
        (pivot["avg_roi_diff_vs_v1"] >= -1e-9)
        & (pivot["avg_worst_diff_vs_v1"] >= -1e-9)
        & (pivot["races_top1"] >= 100)
    )

    if pivot["passes"].any():
        cand = pivot[pivot["passes"]].copy()
        cand = cand.sort_values(["avg_roi_diff_vs_v1", "avg_worst_diff_vs_v1", "avg_std"], ascending=[False, False, True])
        best_cfg = cand.iloc[0]
    else:
        pivot["balance_score"] = pivot["avg_roi_diff_vs_v1"] * 0.6 + pivot["avg_worst_diff_vs_v1"] * 0.3 - pivot["avg_std"] * 0.02
        pivot = pivot.sort_values("balance_score", ascending=False)
        best_cfg = pivot.iloc[0]

    # extract v1 and best rows on test
    def pick_rows(df: pd.DataFrame, is_v1: bool, cfg: pd.Series | None, rate: float | None) -> pd.DataFrame:
        if is_v1:
            return df[(df["is_v1"]) & (df["rate"] == rate)].copy()
        return df[
            (~df["is_v1"])
            & (df["rate"] == float(cfg["rate"]))
            & (np.isclose(df["w1_raw"], float(cfg["w1_raw"])))
            & (np.isclose(df["w2_raw"], float(cfg["w2_raw"])))
            & (np.isclose(df["w3_raw"], float(cfg["w3_raw"])))
        ].copy()

    best_rate = float(best_cfg["rate"])
    test_v1 = pick_rows(test_df, True, None, best_rate)
    test_best = pick_rows(test_df, False, best_cfg, None)

    out_train = Path(args.out_train)
    out_test = Path(args.out_test)
    out_report = Path(args.out_report)
    out_train.parent.mkdir(parents=True, exist_ok=True)
    out_test.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    train_df.to_csv(out_train, index=False, encoding=args.encoding)
    test_df.to_csv(out_test, index=False, encoding=args.encoding)

    report = []
    report.append("wide selection weight grid final report")
    report.append("")
    report.append(f"train_output={out_train}")
    report.append(f"test_output={out_test}")
    report.append("")
    report.append("best config selected on 2021-2025 only")
    report.append(best_cfg.to_string())
    report.append("")

    report.append("2026 comparison (v1 vs best_v2)")
    merge_cols = ["pattern", "roi_pct", "worst_month_roi", "roi_monthly_std", "month_roi_text", "races", "retained_rate", "w1", "w2", "w3"]
    report.append("[v1]")
    report.extend(test_v1[merge_cols].to_string(index=False).splitlines())
    report.append("[best_v2]")
    report.extend(test_best[merge_cols].to_string(index=False).splitlines())

    # final judgement
    by_pattern = test_v1.set_index("pattern").join(test_best.set_index("pattern"), lsuffix="_v1", rsuffix="_v2")
    diff_adopt = False
    lines_judgement = []
    for p in ["top1", "top3"]:
        if p not in by_pattern.index:
            continue
        row = by_pattern.loc[p]
        roi_diff = float(row["roi_pct_v2"] - row["roi_pct_v1"])
        worst_diff = float(row["worst_month_roi_v2"] - row["worst_month_roi_v1"])
        std_diff = float(row["roi_monthly_std_v2"] - row["roi_monthly_std_v1"])
        if roi_diff > 0 and worst_diff >= 0:
            diff_adopt = True
        lines_judgement.append(f"- {p}: ROI diff={roi_diff:+.4f}, worst_month diff={worst_diff:+.4f}, std diff={std_diff:+.4f}")

    report.append("")
    report.append("final summary")
    report.append(f"- best_rate={best_rate}")
    report.extend(lines_judgement)
    report.append(f"- diff採用判断: {'採用候補あり' if diff_adopt else 'v1維持推奨'}")
    report.append("- 《推測》2026は最終評価専用として使い、重み選択には未使用です。")
    report.append("- 《推測》単年最終評価のため、追加年で再検証して本番固定するのが安全です。")

    out_report.write_text("\n".join(report) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"train_csv: {out_train}")
    print(f"test_csv: {out_test}")
    print(f"report: {out_report}")
    print("\n=== best config ===")
    print(best_cfg.to_string())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
