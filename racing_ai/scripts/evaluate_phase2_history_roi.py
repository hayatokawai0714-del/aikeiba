import argparse
import math
from itertools import combinations
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score


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
    parser = argparse.ArgumentParser(description="Evaluate Phase-2 history features impact on ROI.")
    parser.add_argument("--dataset", default=r"C:\TXT\dataset_top3_with_history_phase2.csv")
    parser.add_argument("--base-score", default=r"C:\TXT\top3_value_score_2026_v1.csv")
    parser.add_argument("--phase1-roi", default=r"C:\TXT\wide_roi_with_ability_gap_v1.csv")
    parser.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    parser.add_argument("--out-csv", default=r"C:\TXT\wide_roi_with_history_phase2.csv")
    parser.add_argument("--out-report", default=r"C:\TXT\wide_roi_with_history_phase2_report.txt")
    parser.add_argument("--encoding", default="cp932")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--stake", type=int, default=100)
    return parser.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def safe_auc(y_true: pd.Series, y_prob: np.ndarray) -> float:
    if pd.Series(y_true).nunique(dropna=False) < 2:
        return float("nan")
    return float(roc_auc_score(y_true, y_prob))


def calc_metric(y_true: pd.Series, y_prob: np.ndarray) -> dict:
    y = y_true.astype(int).to_numpy()
    p = np.clip(np.asarray(y_prob, dtype=float), 1e-8, 1 - 1e-8)
    return {
        "auc": safe_auc(y, p),
        "logloss": float(log_loss(y, p, labels=[0, 1])),
        "brier": float(brier_score_loss(y, p)),
    }


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


def split_dataset(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df[df["race_date"].notna()].copy()
    df["year"] = df["race_date"].dt.year
    train = df[df["year"].isin(TRAIN_YEARS)].copy()
    valid = df[df["year"].isin(VALID_YEARS)].copy()
    test = df[df["year"].isin(TEST_YEARS)].copy()
    if train.empty or valid.empty or test.empty:
        raise SystemExit(f"Split empty: train={len(train)} valid={len(valid)} test={len(test)}")
    return train, valid, test


def train_top3_model(train: pd.DataFrame, valid: pd.DataFrame, test: pd.DataFrame, seed: int) -> dict:
    features_num = [
        "log_win_odds",
        "distance",
        "field_size",
        "pop_rank",
        "prev_finish_position",
        "avg_finish_last3",
        "same_distance_win_rate",
        "prev_margin",
        "avg_margin_last3",
        "prev_last3f_rank",
        "last3f_best_count",
    ]
    features_cat = ["track_condition", "jockey_name"]

    medians = {}
    for c in [
        "pop_rank",
        "distance",
        "field_size",
        "prev_finish_position",
        "avg_finish_last3",
        "same_distance_win_rate",
        "prev_margin",
        "avg_margin_last3",
        "prev_last3f_rank",
        "last3f_best_count",
    ]:
        medians[c] = float(train[c].median())

    for split in [train, valid, test]:
        split["pop_rank"] = split["pop_rank"].fillna(medians["pop_rank"])
        split["distance"] = split["distance"].fillna(medians["distance"])
        split["field_size"] = split["field_size"].fillna(medians["field_size"])
        split["track_condition"] = split["track_condition"].fillna("UNKNOWN").astype(str)
        split["jockey_name"] = split["jockey_name"].fillna("UNKNOWN").astype(str)
        split["log_win_odds"] = np.log(split["win_odds"].clip(lower=1e-6))
        for c in [
            "prev_finish_position",
            "avg_finish_last3",
            "same_distance_win_rate",
            "prev_margin",
            "avg_margin_last3",
            "prev_last3f_rank",
            "last3f_best_count",
        ]:
            split[c] = split[c].fillna(medians[c])
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
    features = features_num + features_cat
    model.fit(
        train[features],
        train["top3"].astype(int),
        eval_set=[(valid[features], valid["top3"].astype(int))],
        eval_metric=["auc", "binary_logloss"],
        callbacks=[lgb.early_stopping(200), lgb.log_evaluation(200)],
        categorical_feature=features_cat,
    )
    test_pred = model.predict_proba(test[features])[:, 1]
    return {"test_pred": test_pred, "metrics": calc_metric(test["top3"], test_pred)}


def train_ability_model(train: pd.DataFrame, valid: pd.DataFrame, test: pd.DataFrame, seed: int) -> pd.DataFrame:
    feats = [
        "prev_finish_position",
        "avg_finish_last3",
        "same_distance_win_rate",
        "prev_margin",
        "avg_margin_last3",
        "prev_last3f_rank",
        "last3f_best_count",
    ]
    med = {c: float(train[c].median()) for c in feats}
    for split in [train, valid, test]:
        for c in feats:
            split[c] = split[c].fillna(med[c])

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
    out = test[["race_date", "race_id_raw", "horse_no", "win_odds"]].copy()
    out["ability_top3_prob"] = model.predict_proba(test[feats])[:, 1]
    out["market_prob"] = np.where(out["win_odds"] > 0, 1.0 / out["win_odds"], np.nan)
    out["ability_gap"] = out["ability_top3_prob"] - out["market_prob"]
    out["race_id_raw"] = out["race_id_raw"].map(normalize_race_id_raw)
    out["horse_no"] = to_float(out["horse_no"]).astype("Int64")
    return out


def build_score_v2(base_score_path: Path, ability_df: pd.DataFrame, encoding: str) -> pd.DataFrame:
    base = pd.read_csv(base_score_path, encoding=encoding, low_memory=False)
    base["race_id_raw"] = base["race_id_raw"].map(normalize_race_id_raw)
    base["horse_no"] = to_float(base["horse_no"]).astype("Int64")
    for c in ["value_score_v1", "pred_top3_raw", "top3", "value_gap_z", "gap_std", "value_gap"]:
        if c in base.columns:
            base[c] = to_float(base[c])
    merged = base.merge(
        ability_df[["race_id_raw", "horse_no", "ability_gap"]],
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
    merged["value_gap_z_v2"] = np.where(race_std > 0, (merged["value_score_v2"] - race_mean) / race_std, 0.0)
    merged["gap_std_v2"] = race_std
    return merged


def build_wide_and_race_selection(score_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    score = score_df.copy()
    score["is_candidate"] = (score["value_score_rank"] <= 3) | (score["pred_top3_rank"] <= 3)
    cand = score[score["is_candidate"]].copy()

    pair_rows = []
    for race_id, race_df in cand.groupby("race_id_raw", dropna=False):
        records = race_df.to_dict("records")
        if len(records) < 2:
            continue
        for left, right in combinations(records, 2):
            ordered = sorted([left, right], key=lambda r: (-float(r["pred_top3_raw"]), int(r["horse_no"])))
            f, s = ordered[0], ordered[1]
            p1 = float(f["pred_top3_raw"])
            p2 = float(s["pred_top3_raw"])
            v1 = float(f["value_score_v2"])
            v2 = float(s["value_score_v2"])
            t1 = int(f["top3"])
            t2 = int(s["top3"])
            pair_rows.append(
                {
                    "race_date": f.get("race_date", s.get("race_date")),
                    "race_id_raw": race_id,
                    "horse_no_1": int(f["horse_no"]),
                    "horse_no_2": int(s["horse_no"]),
                    "pred_top3_1": p1,
                    "pred_top3_2": p2,
                    "value_score_1": v1,
                    "value_score_2": v2,
                    "pair_score_v1": p1 * p2,
                    "pair_value_score_v1": v1 + v2,
                    "top3_1": t1,
                    "top3_2": t2,
                    "wide_hit": int(t1 == 1 and t2 == 1),
                }
            )
    if not pair_rows:
        raise SystemExit("No wide pairs generated.")
    wide = pd.DataFrame(pair_rows)
    wide["pair_rank_score"] = (
        wide.groupby("race_id_raw", dropna=False)["pair_score_v1"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    wide["pair_rank_value"] = (
        wide.groupby("race_id_raw", dropna=False)["pair_value_score_v1"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    wide = wide[(wide["pair_rank_score"] <= 3) | (wide["pair_rank_value"] <= 3)].copy()

    race_rows = []
    for race_id, race_df in score.groupby("race_id_raw", dropna=False):
        rs = race_df.sort_values("value_score_rank")
        top2 = rs.head(2)
        diff = float("nan")
        if len(top2) >= 2:
            diff = float(top2.iloc[0]["value_score_v2"] - top2.iloc[1]["value_score_v2"])
        race_rows.append(
            {
                "race_id_raw": race_id,
                "race_date": race_df["race_date"].iloc[0] if "race_date" in race_df.columns else "",
                "value_score_max": float(race_df["value_score_v2"].max()),
                "value_score_top2_mean": float(top2["value_score_v2"].mean()) if len(top2) else np.nan,
                "gap_std": float(race_df["gap_std_v2"].iloc[0]),
                "top1_top2_diff": diff,
                "horse_count": int(len(race_df)),
            }
        )
    race = pd.DataFrame(race_rows)
    top1 = (
        wide[wide["pair_rank_score"] == 1]
        .groupby("race_id_raw", dropna=False)
        .agg(top1_pair_score_v1=("pair_score_v1", "first"), top1_wide_hit=("wide_hit", "first"))
        .reset_index()
    )
    top3 = (
        wide[wide["pair_rank_score"] <= 3]
        .groupby("race_id_raw", dropna=False)
        .agg(top3_any_hit=("wide_hit", "max"))
        .reset_index()
    )
    race = race.merge(top1, on="race_id_raw", how="inner").merge(top3, on="race_id_raw", how="left")
    race["z_value_score_max"] = zscore(race["value_score_max"])
    race["z_gap_std"] = zscore(race["gap_std"])
    race["race_select_score_v1"] = 0.6 * race["z_value_score_max"] + 0.4 * race["z_gap_std"]
    race = race.sort_values("race_select_score_v1", ascending=False).reset_index(drop=True)
    race["race_select_rank"] = np.arange(1, len(race) + 1)
    n = len(race)
    race["selected_top15"] = race["race_select_rank"] <= int(math.ceil(n * 0.15))
    return wide, race


def calc_roi(wide: pd.DataFrame, race: pd.DataFrame, payout: pd.DataFrame, stake: int) -> pd.DataFrame:
    selected_ids = set(race.loc[race["selected_top15"] == True, "race_id_raw"])  # noqa: E712
    target = wide[wide["race_id_raw"].isin(selected_ids)].copy()
    target["race_id"] = target["race_id_raw"].map(raw_to_race_id)
    target["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(target["horse_no_1"], target["horse_no_2"])]
    target = target.merge(payout, on=["race_id", "pair_key"], how="left")
    target["wide_hit_real"] = target["payout_yen"].notna().astype(int)
    target["return_yen"] = target["payout_yen"].fillna(0.0)
    target["race_month"] = pd.to_datetime(target["race_date"], errors="coerce").dt.strftime("%Y-%m")

    rows = []
    for pattern, cond in [("top1", target["pair_rank_score"] == 1), ("top3", target["pair_rank_score"] <= 3)]:
        sub = target[cond].copy()
        bets = len(sub)
        stake_total = bets * stake
        ret = float(sub["return_yen"].sum()) if bets else 0.0
        roi = float((ret / stake_total) * 100.0) if stake_total else np.nan
        rows.append(
            {
                "pattern": pattern,
                "month": "ALL",
                "races": int(sub["race_id_raw"].nunique()) if bets else 0,
                "bets": int(bets),
                "hit_rate": float(sub["wide_hit_real"].mean()) if bets else np.nan,
                "roi_pct": roi,
                "avg_payout_yen": float(sub.loc[sub["wide_hit_real"] == 1, "payout_yen"].mean()) if (bets and (sub["wide_hit_real"] == 1).any()) else np.nan,
                "stake_total": int(stake_total),
                "return_total": ret,
            }
        )
        monthly_roi = []
        for month in MONTHS:
            m = sub[sub["race_month"] == month].copy()
            m_stake = len(m) * stake
            m_ret = float(m["return_yen"].sum()) if len(m) else 0.0
            m_roi = float((m_ret / m_stake) * 100.0) if m_stake else np.nan
            monthly_roi.append(m_roi)
            rows.append(
                {
                    "pattern": pattern,
                    "month": month,
                    "races": int(m["race_id_raw"].nunique()) if len(m) else 0,
                    "bets": int(len(m)),
                    "hit_rate": float(m["wide_hit_real"].mean()) if len(m) else np.nan,
                    "roi_pct": m_roi,
                    "avg_payout_yen": float(m.loc[m["wide_hit_real"] == 1, "payout_yen"].mean()) if (len(m) and (m["wide_hit_real"] == 1).any()) else np.nan,
                    "stake_total": int(m_stake),
                    "return_total": m_ret,
                }
            )
        vals = np.array([x for x in monthly_roi if pd.notna(x)], dtype=float)
        worst = float(vals.min()) if len(vals) else np.nan
        rows[-(len(MONTHS)+1)]["worst_month_roi"] = worst
    return pd.DataFrame(rows)


def main() -> int:
    args = parse_args()
    df = pd.read_csv(Path(args.dataset), encoding=args.encoding, low_memory=False)
    required = {
        "race_date",
        "race_id_raw",
        "horse_no",
        "top3",
        "win_odds",
        "distance",
        "field_size",
        "pop_rank",
        "track_condition",
        "jockey_name",
        "prev_finish_position",
        "avg_finish_last3",
        "same_distance_win_rate",
        "prev_margin",
        "avg_margin_last3",
        "prev_last3f_rank",
        "last3f_best_count",
    }
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Missing dataset columns: {sorted(missing)}")

    for c in [
        "top3",
        "win_odds",
        "distance",
        "field_size",
        "pop_rank",
        "prev_finish_position",
        "avg_finish_last3",
        "same_distance_win_rate",
        "prev_margin",
        "avg_margin_last3",
        "prev_last3f_rank",
        "last3f_best_count",
    ]:
        df[c] = to_float(df[c])

    df = df[df["win_odds"].notna()].copy()
    df["race_id_raw"] = df["race_id_raw"].map(normalize_race_id_raw)
    df["horse_no"] = to_float(df["horse_no"]).astype("Int64")

    train, valid, test = split_dataset(df)
    top3_res = train_top3_model(train.copy(), valid.copy(), test.copy(), args.seed)
    ability_df = train_ability_model(train.copy(), valid.copy(), test.copy(), args.seed)
    score_v2 = build_score_v2(Path(args.base_score), ability_df, args.encoding)
    wide, race = build_wide_and_race_selection(score_v2)
    payout = load_payouts(Path(args.payout_root))
    if payout.empty:
        raise SystemExit("No payout data found.")
    roi_df = calc_roi(wide, race, payout, args.stake)

    phase1_cmp = None
    phase1_path = Path(args.phase1_roi)
    if phase1_path.exists():
        phase1 = pd.read_csv(phase1_path, encoding=args.encoding, low_memory=False)
        phase1 = phase1[phase1["month"] == "ALL"][["pattern", "roi_pct", "hit_rate"]].copy()
        phase1 = phase1.rename(columns={"roi_pct": "phase1_roi_pct", "hit_rate": "phase1_hit_rate"})
        phase2_all = roi_df[roi_df["month"] == "ALL"][["pattern", "roi_pct", "hit_rate"]].copy()
        phase1_cmp = phase2_all.merge(phase1, on="pattern", how="left")
        phase1_cmp["roi_diff_vs_phase1"] = phase1_cmp["roi_pct"] - phase1_cmp["phase1_roi_pct"]
        phase1_cmp["hit_diff_vs_phase1"] = phase1_cmp["hit_rate"] - phase1_cmp["phase1_hit_rate"]

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)
    roi_df.to_csv(out_csv, index=False, encoding=args.encoding)

    lines = []
    lines.append("wide roi with history phase2 report")
    lines.append("")
    lines.append(f"input_dataset={args.dataset}")
    lines.append(f"output_csv={out_csv}")
    lines.append("")
    lines.append("split")
    lines.append(f"- train rows={len(train)} years=2021-2024")
    lines.append(f"- valid rows={len(valid)} years=2025")
    lines.append(f"- test rows={len(test)} years=2026")
    lines.append("")
    lines.append("test model metrics (top3 model, phase1+phase2 features)")
    lines.append(
        f"- AUC={top3_res['metrics']['auc']:.6f} "
        f"logloss={top3_res['metrics']['logloss']:.6f} "
        f"Brier={top3_res['metrics']['brier']:.6f}"
    )
    lines.append("")
    lines.append("ability model")
    lines.append("- features: prev_finish_position, avg_finish_last3, same_distance_win_rate, prev_margin, avg_margin_last3, prev_last3f_rank, last3f_best_count")
    lines.append("- ability_gap recalculated on test(2026)")
    lines.append("")
    lines.append("roi summary")
    lines.extend(roi_df[roi_df["month"] == "ALL"].to_string(index=False).splitlines())
    lines.append("")
    lines.append("monthly roi")
    lines.extend(roi_df[roi_df["month"] != "ALL"].to_string(index=False).splitlines())
    if phase1_cmp is not None:
        lines.append("")
        lines.append("comparison vs phase1")
        lines.extend(phase1_cmp.to_string(index=False).splitlines())

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print("=== metrics ===")
    print(
        f"AUC={top3_res['metrics']['auc']:.6f} "
        f"logloss={top3_res['metrics']['logloss']:.6f} "
        f"Brier={top3_res['metrics']['brier']:.6f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
