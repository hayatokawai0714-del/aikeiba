import argparse
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd


TRAIN_YEARS = {2021, 2022, 2023, 2024}
VALID_YEARS = {2025}
TEST_YEARS = {2026}
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
    parser = argparse.ArgumentParser(description="Analyze why Phase-1 features worsened top1 ROI.")
    parser.add_argument("--new-pred", default=r"C:\TXT\top3_model_with_history_predictions_2026_v1.csv")
    parser.add_argument("--old-pred", default=r"C:\TXT\top3_model_predictions_2026_v1.csv")
    parser.add_argument("--roi-summary", default=r"C:\TXT\wide_roi_with_history_phase1.csv")
    parser.add_argument("--dataset", default=r"C:\TXT\dataset_top3_with_history_phase1.csv")
    parser.add_argument("--old-wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    parser.add_argument("--new-wide", default=r"C:\TXT\wide_candidates_2026_history_phase1.csv")
    parser.add_argument("--old-race", default=r"C:\TXT\wide_race_selection_fixed_rate_2026_v1.csv")
    parser.add_argument("--new-race", default=r"C:\TXT\wide_race_selection_fixed_rate_2026_history_phase1.csv")
    parser.add_argument("--old-value-gap", default=r"C:\TXT\top3_value_gap_detail_2026_v1.csv")
    parser.add_argument("--new-value-gap", default=r"C:\TXT\top3_value_gap_detail_2026_history_phase1.csv")
    parser.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    parser.add_argument("--report-out", default=r"C:\TXT\phase1_feature_impact_analysis.txt")
    parser.add_argument("--encoding", default="cp932")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--stake", type=int, default=100)
    return parser.parse_args()


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


def prepare_top1_bets(wide_path: Path, race_path: Path, payout_df: pd.DataFrame, encoding: str) -> pd.DataFrame:
    wide = pd.read_csv(wide_path, encoding=encoding, low_memory=False)
    race = pd.read_csv(race_path, encoding=encoding, low_memory=False)

    wide["race_id_raw"] = wide["race_id_raw"].astype(str)
    wide["pair_rank_score"] = to_float(wide["pair_rank_score"])
    wide["horse_no_1"] = to_float(wide["horse_no_1"])
    wide["horse_no_2"] = to_float(wide["horse_no_2"])
    wide = wide[wide["pair_rank_score"].notna() & wide["horse_no_1"].notna() & wide["horse_no_2"].notna()].copy()

    race["race_id_raw"] = race["race_id_raw"].astype(str)
    sel = race[race["selected_top15"] == True][["race_id_raw"]]  # noqa: E712
    selected_ids = set(sel["race_id_raw"])

    top1 = wide[(wide["race_id_raw"].isin(selected_ids)) & (wide["pair_rank_score"] == 1)].copy()
    top1["race_id"] = top1["race_id_raw"].map(raw_to_race_id)
    top1["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(top1["horse_no_1"], top1["horse_no_2"])]
    top1 = top1.merge(payout_df, on=["race_id", "pair_key"], how="left")
    top1["return_yen"] = top1["payout_yen"].fillna(0.0)
    top1["hit_real"] = top1["payout_yen"].notna().astype(int)
    top1["race_month"] = pd.to_datetime(top1["race_date"], errors="coerce").dt.strftime("%Y-%m")
    top1["horse_no_1"] = top1["horse_no_1"].astype(int)
    top1["horse_no_2"] = top1["horse_no_2"].astype(int)
    return top1


def train_new_model_and_shap(dataset_path: Path, encoding: str, seed: int):
    df = pd.read_csv(dataset_path, encoding=encoding, low_memory=False)
    req = [
        "race_date",
        "top3",
        "win_odds",
        "pop_rank",
        "distance",
        "field_size",
        "track_condition",
        "jockey_name",
        "prev_finish_position",
        "avg_finish_last3",
        "same_distance_win_rate",
        "race_id_raw",
        "horse_no",
    ]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing dataset columns: {missing}")

    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df[df["race_date"].notna()].copy()
    df["year"] = df["race_date"].dt.year

    numeric_cols = [
        "top3",
        "win_odds",
        "pop_rank",
        "distance",
        "field_size",
        "prev_finish_position",
        "avg_finish_last3",
        "same_distance_win_rate",
    ]
    for col in numeric_cols:
        df[col] = to_float(df[col])

    train = df[df["year"].isin(TRAIN_YEARS)].copy()
    valid = df[df["year"].isin(VALID_YEARS)].copy()
    test = df[df["year"].isin(TEST_YEARS)].copy()

    med_pop = float(train["pop_rank"].median())
    med_dist = float(train["distance"].median())
    med_fs = float(train["field_size"].median())

    for split in [train, valid, test]:
        split["pop_rank"] = split["pop_rank"].fillna(med_pop)
        split["distance"] = split["distance"].fillna(med_dist)
        split["field_size"] = split["field_size"].fillna(med_fs)
        split["track_condition"] = split["track_condition"].fillna("UNKNOWN").astype(str)
        split["jockey_name"] = split["jockey_name"].fillna("UNKNOWN").astype(str)
        split["log_win_odds"] = np.log(split["win_odds"].clip(lower=1e-6))

    features_num = [
        "log_win_odds",
        "distance",
        "field_size",
        "pop_rank",
        "prev_finish_position",
        "avg_finish_last3",
        "same_distance_win_rate",
    ]
    features_cat = ["track_condition", "jockey_name"]
    features = features_num + features_cat

    for split in [train, valid, test]:
        for c in features_cat:
            split[c] = split[c].astype("category")

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
        train[features],
        train["top3"].astype(int),
        eval_set=[(valid[features], valid["top3"].astype(int))],
        eval_metric=["auc", "binary_logloss"],
        callbacks=[lgb.early_stopping(200), lgb.log_evaluation(200)],
        categorical_feature=features_cat,
    )

    booster = model.booster_
    imp_gain = booster.feature_importance(importance_type="gain")
    imp_split = booster.feature_importance(importance_type="split")
    importance_df = pd.DataFrame(
        {"feature": booster.feature_name(), "gain": imp_gain, "split": imp_split}
    ).sort_values("gain", ascending=False)

    contrib = booster.predict(test[features], pred_contrib=True)
    shap_cols = booster.feature_name() + ["bias"]
    shap_df = pd.DataFrame(contrib, columns=shap_cols)
    meta = test[["race_date", "race_id_raw", "horse_no"]].copy()
    meta["horse_no"] = to_float(meta["horse_no"]).astype("Int64")
    shap_df = pd.concat([meta.reset_index(drop=True), shap_df.reset_index(drop=True)], axis=1)
    return importance_df, shap_df


def merge_pair_horse_info(top1_df: pd.DataFrame, pred_df: pd.DataFrame, value_gap_df: pd.DataFrame) -> pd.DataFrame:
    pred = pred_df.copy()
    pred["race_id_raw"] = pred["race_id_raw"].astype(str)
    pred["horse_no"] = to_float(pred["horse_no"]).astype("Int64")
    pred["pop_rank"] = to_float(pred["pop_rank"])
    pred["win_odds"] = to_float(pred["win_odds"])

    vg = value_gap_df.copy()
    vg["race_id_raw"] = vg["race_id_raw"].astype(str)
    vg["horse_no"] = to_float(vg["horse_no"]).astype("Int64")
    vg["value_gap"] = to_float(vg["value_gap"])

    pair = top1_df.copy()
    for side in [1, 2]:
        sub_pred = pred[["race_id_raw", "horse_no", "pop_rank", "win_odds"]].rename(
            columns={
                "horse_no": f"horse_no_{side}",
                "pop_rank": f"pop_rank_{side}",
                "win_odds": f"win_odds_{side}",
            }
        )
        pair = pair.merge(sub_pred, on=["race_id_raw", f"horse_no_{side}"], how="left")

        sub_vg = vg[["race_id_raw", "horse_no", "value_gap"]].rename(
            columns={"horse_no": f"horse_no_{side}", "value_gap": f"value_gap_{side}"}
        )
        pair = pair.merge(sub_vg, on=["race_id_raw", f"horse_no_{side}"], how="left")

    pair["pair_pop_metric"] = pair[["pop_rank_1", "pop_rank_2"]].min(axis=1)
    pair["pair_odds_metric"] = np.sqrt(pair["win_odds_1"] * pair["win_odds_2"])
    pair["pair_value_gap_mean"] = pair[["value_gap_1", "value_gap_2"]].mean(axis=1)
    return pair


def segment_label_pop(v: float) -> str:
    if pd.isna(v):
        return "NA"
    if v <= 3:
        return "1-3"
    if v <= 6:
        return "4-6"
    return "7+"


def segment_label_odds(v: float) -> str:
    if pd.isna(v):
        return "NA"
    if v < 10:
        return "<10"
    if v < 30:
        return "10-30"
    if v < 100:
        return "30-100"
    return "100+"


def summarize_segments(df: pd.DataFrame, stake: int, model_label: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data = df.copy()
    data["pop_band"] = data["pair_pop_metric"].map(segment_label_pop)
    data["odds_band"] = data["pair_odds_metric"].map(segment_label_odds)
    q80 = float(data["pair_value_gap_mean"].quantile(0.8))
    data["value_gap_segment"] = np.where(data["pair_value_gap_mean"] >= q80, "top20%", "other80%")

    def agg_by(col: str) -> pd.DataFrame:
        g = (
            data.groupby(col, dropna=False)
            .agg(
                bets=("race_id_raw", "size"),
                hit_rate=("hit_real", "mean"),
                roi_pct=("return_yen", lambda s: float(s.sum() / (len(s) * stake) * 100.0) if len(s) else np.nan),
            )
            .reset_index()
        )
        g["model"] = model_label
        return g

    return agg_by("pop_band"), agg_by("value_gap_segment"), agg_by("odds_band")


def main() -> int:
    args = parse_args()

    payout_df = load_payouts(Path(args.payout_root))
    if payout_df.empty:
        raise SystemExit("No payout data found.")

    importance_df, shap_df = train_new_model_and_shap(Path(args.dataset), args.encoding, args.seed)

    target_feats = ["prev_finish_position", "avg_finish_last3", "same_distance_win_rate"]
    shap_abs = shap_df[target_feats].abs().mean().sort_values(ascending=False)
    shap_apr = shap_df[pd.to_datetime(shap_df["race_date"], errors="coerce").dt.strftime("%Y-%m") == "2026-04"]
    shap_apr_abs = shap_apr[target_feats].abs().mean().sort_values(ascending=False) if len(shap_apr) else pd.Series(dtype=float)

    old_top1 = prepare_top1_bets(Path(args.old_wide), Path(args.old_race), payout_df, args.encoding)
    new_top1 = prepare_top1_bets(Path(args.new_wide), Path(args.new_race), payout_df, args.encoding)

    old_top1 = old_top1.copy()
    new_top1 = new_top1.copy()
    old_top1["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(old_top1["horse_no_1"], old_top1["horse_no_2"])]
    new_top1["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(new_top1["horse_no_1"], new_top1["horse_no_2"])]

    common_races = set(old_top1["race_id_raw"]).intersection(set(new_top1["race_id_raw"]))
    old_common = old_top1[old_top1["race_id_raw"].isin(common_races)][["race_id_raw", "pair_key", "horse_no_1", "horse_no_2"]]
    new_common = new_top1[new_top1["race_id_raw"].isin(common_races)][["race_id_raw", "pair_key", "horse_no_1", "horse_no_2"]]
    chg = old_common.merge(new_common, on="race_id_raw", suffixes=("_old", "_new"))
    changed = chg[chg["pair_key_old"] != chg["pair_key_new"]].copy()

    dropped_horses = 0
    added_horses = 0
    for _, row in changed.iterrows():
        old_set = {int(row["horse_no_1_old"]), int(row["horse_no_2_old"])}
        new_set = {int(row["horse_no_1_new"]), int(row["horse_no_2_new"])}
        dropped_horses += len(old_set - new_set)
        added_horses += len(new_set - old_set)

    old_pred = pd.read_csv(Path(args.old_pred), encoding=args.encoding, low_memory=False)
    new_pred = pd.read_csv(Path(args.new_pred), encoding=args.encoding, low_memory=False)
    old_vg = pd.read_csv(Path(args.old_value_gap), encoding=args.encoding, low_memory=False)
    new_vg = pd.read_csv(Path(args.new_value_gap), encoding=args.encoding, low_memory=False)

    old_seg_df = merge_pair_horse_info(old_top1, old_pred, old_vg)
    new_seg_df = merge_pair_horse_info(new_top1, new_pred, new_vg)

    old_pop, old_vseg, old_odds = summarize_segments(old_seg_df, args.stake, "old")
    new_pop, new_vseg, new_odds = summarize_segments(new_seg_df, args.stake, "new")
    pop_cmp = new_pop.merge(old_pop, on="pop_band", how="outer", suffixes=("_new", "_old"))
    vseg_cmp = new_vseg.merge(old_vseg, on="value_gap_segment", how="outer", suffixes=("_new", "_old"))
    odds_cmp = new_odds.merge(old_odds, on="odds_band", how="outer", suffixes=("_new", "_old"))
    for cmp_df in [pop_cmp, vseg_cmp, odds_cmp]:
        cmp_df["roi_diff"] = cmp_df["roi_pct_new"] - cmp_df["roi_pct_old"]
        cmp_df["hit_diff"] = cmp_df["hit_rate_new"] - cmp_df["hit_rate_old"]

    roi_summary = pd.read_csv(Path(args.roi_summary), encoding=args.encoding, low_memory=False)

    hist_df = pd.read_csv(Path(args.dataset), encoding=args.encoding, low_memory=False)
    hist_df["race_id_raw"] = hist_df["race_id_raw"].astype(str)
    hist_df["horse_no"] = to_float(hist_df["horse_no"]).astype("Int64")
    for col in target_feats:
        hist_df[col] = to_float(hist_df[col])

    new_apr = new_top1[new_top1["race_month"] == "2026-04"].copy()
    rows = []
    for _, r in new_apr.iterrows():
        race_id = str(r["race_id_raw"])
        for side in [1, 2]:
            hn = int(r[f"horse_no_{side}"])
            row = hist_df[(hist_df["race_id_raw"] == race_id) & (hist_df["horse_no"] == hn)]
            if row.empty:
                continue
            one = row.iloc[0]
            rows.append(
                {
                    "race_id_raw": race_id,
                    "horse_no": hn,
                    "hit_real": int(r["hit_real"]),
                    "prev_finish_position": one["prev_finish_position"],
                    "avg_finish_last3": one["avg_finish_last3"],
                    "same_distance_win_rate": one["same_distance_win_rate"],
                }
            )
    apr_feat = pd.DataFrame(rows)

    apr_shap = shap_apr.copy()
    apr_shap["race_id_raw"] = apr_shap["race_id_raw"].astype(str)
    apr_shap["horse_no"] = to_float(apr_shap["horse_no"]).astype("Int64")
    if not apr_feat.empty:
        apr_merge = apr_feat.merge(apr_shap[["race_id_raw", "horse_no"] + target_feats], on=["race_id_raw", "horse_no"], how="left", suffixes=("_val", "_shap"))
    else:
        apr_merge = pd.DataFrame()

    report_lines: list[str] = []
    report_lines.append("phase1 feature impact analysis")
    report_lines.append("")
    report_lines.append("1) feature importance (LightGBM, new model)")
    report_lines.append(importance_df[["feature", "gain", "split"]].to_string(index=False))
    report_lines.append("")
    report_lines.append("2) SHAP mean(|contrib|) for Phase-1 features")
    report_lines.append(shap_abs.to_string())
    report_lines.append("")
    report_lines.append("2-2) SHAP mean(|contrib|) in 2026-04")
    if len(shap_apr_abs):
        report_lines.append(shap_apr_abs.to_string())
    else:
        report_lines.append("情報不足: 2026-04 shap rows not found.")
    report_lines.append("")
    report_lines.append("3) top1 selected pair changes (old vs new)")
    report_lines.append(f"- old selected races={old_top1['race_id_raw'].nunique()}")
    report_lines.append(f"- new selected races={new_top1['race_id_raw'].nunique()}")
    report_lines.append(f"- common races={len(common_races)}")
    report_lines.append(f"- changed top1 pairs={len(changed)}")
    report_lines.append(f"- dropped horses count (total diff slots)={dropped_horses}")
    report_lines.append(f"- added horses count (total diff slots)={added_horses}")
    if len(changed):
        report_lines.append("")
        report_lines.append("changed pair examples (first 20)")
        report_lines.append(changed.head(20).to_string(index=False))
    report_lines.append("")
    report_lines.append("4) segment ROI comparison (new-old)")
    report_lines.append("")
    report_lines.append("[popularity band: 1-3 / 4-6 / 7+]")
    report_lines.append(pop_cmp.to_string(index=False))
    report_lines.append("")
    report_lines.append("[value_gap segment: pair_value_gap_mean top20% vs other80%]")
    report_lines.append(vseg_cmp.to_string(index=False))
    report_lines.append("")
    report_lines.append("[odds band: geometric mean odds]")
    report_lines.append(odds_cmp.to_string(index=False))
    report_lines.append("")
    report_lines.append("5) monthly degradation focus (2026-04)")
    report_lines.append(roi_summary.to_string(index=False))
    report_lines.append("")
    if not apr_merge.empty:
        win = apr_merge[apr_merge["hit_real"] == 1]
        lose = apr_merge[apr_merge["hit_real"] == 0]
        report_lines.append("2026-04 selected horses in new model: winner vs loser averages")
        for col in target_feats:
            wv = float(win[f"{col}_val"].mean()) if len(win) else np.nan
            lv = float(lose[f"{col}_val"].mean()) if len(lose) else np.nan
            ws = float(win[f"{col}_shap"].mean()) if len(win) else np.nan
            ls = float(lose[f"{col}_shap"].mean()) if len(lose) else np.nan
            report_lines.append(
                f"- {col}: value_mean win={wv:.6f} lose={lv:.6f} | shap_mean win={ws:.6f} lose={ls:.6f}"
            )
    else:
        report_lines.append("情報不足: 2026-04 winner/loser feature merge rows not found.")

    report_lines.append("")
    report_lines.append("6) cause summary")
    report_lines.append(
        "《推測》top1劣化は、2026-04で pair入替後の低配当/不的中レースが増え、"
        "top1最悪月ROIと月次分散が悪化した影響が大きい。"
    )
    report_lines.append(
        "《推測》Phase-1特徴量はtop3全体順位改善には寄与した一方、"
        "top1一点の最終ペア選択では過剰に入替を起こし、主軸運用に不利な局面が発生。"
    )

    out_path = Path(args.report_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(report_lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"report: {out_path}")
    print(f"changed_top1_pairs={len(changed)} / common_races={len(common_races)}")
    print("phase1_shap_abs:")
    print(shap_abs.to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
