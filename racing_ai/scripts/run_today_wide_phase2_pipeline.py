import argparse
from dataclasses import dataclass
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd


TRAIN_YEARS = {2021, 2022, 2023, 2024}
VALID_YEARS = {2025}
FEATURES_BASE_NUM = ["log_win_odds", "distance", "field_size", "pop_rank"]
FEATURES_BASE_CAT = ["track_condition", "jockey_name"]
FEATURES_PHASE1 = ["prev_finish_position", "avg_finish_last3", "same_distance_win_rate"]
FEATURES_PHASE2 = ["prev_margin", "avg_margin_last3", "prev_last3f_rank", "last3f_best_count"]
RANK_SCORE_MAP = {1: 1.0, 2: 0.8, 3: 0.6, 4: 0.4}


@dataclass
class InputPaths:
    entries: Path
    races: Path
    odds: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run same-day wide prediction pipeline with Phase-2 models.")
    parser.add_argument("--history-dataset", default=r"C:\TXT\dataset_top3_with_history_phase2.csv")
    parser.add_argument("--population", default=r"C:\TXT\population_master_2021_2026_v1.csv")
    parser.add_argument("--today-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--odds-cutoff", required=True, help="YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--entries", default="")
    parser.add_argument("--races", default="")
    parser.add_argument("--odds", default="")
    parser.add_argument("--normalized-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    parser.add_argument("--out", default=r"C:\TXT\today_wide_predictions.csv")
    parser.add_argument("--log-out", default=r"C:\TXT\today_wide_predictions_log.txt")
    parser.add_argument("--encoding", default="cp932")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def parse_today_date(today_date_str: str) -> pd.Timestamp:
    ts = pd.to_datetime(today_date_str, format="%Y-%m-%d", errors="coerce")
    if pd.isna(ts):
        raise SystemExit("--today-date must be YYYY-MM-DD")
    return ts.normalize()


def parse_cutoff(cutoff_str: str) -> pd.Timestamp:
    ts = pd.to_datetime(cutoff_str, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if pd.isna(ts):
        raise SystemExit("--odds-cutoff must be YYYY-MM-DD HH:MM:SS")
    if ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Tokyo")
    return ts.tz_convert("UTC")


def find_input_set_for_date(normalized_root: Path, today_date: pd.Timestamp) -> InputPaths:
    date_folder = today_date.strftime("%Y-%m-%d")
    candidates = []
    for d in sorted(normalized_root.rglob(date_folder), key=lambda p: str(p)):
        if not d.is_dir():
            continue
        entries = d / "entries.csv"
        races = d / "races.csv"
        odds = d / "odds.csv"
        if entries.exists() and races.exists() and odds.exists():
            candidates.append((entries, races, odds))
    if not candidates:
        raise SystemExit(f"No input set found for today-date={date_folder} under: {normalized_root}")
    entries, races, odds = candidates[0]
    return InputPaths(entries=entries, races=races, odds=odds)


def resolve_inputs(args: argparse.Namespace, today_date: pd.Timestamp) -> InputPaths:
    if args.entries and args.races and args.odds:
        return InputPaths(entries=Path(args.entries), races=Path(args.races), odds=Path(args.odds))
    return find_input_set_for_date(Path(args.normalized_root), today_date)


def train_top3_model(history_df: pd.DataFrame, seed: int) -> tuple[lgb.LGBMClassifier, dict[str, float]]:
    df = history_df.copy()
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df[df["race_date"].notna()].copy()
    df["year"] = df["race_date"].dt.year

    for c in ["top3", "win_odds", "distance", "field_size", "pop_rank"] + FEATURES_PHASE1 + FEATURES_PHASE2:
        df[c] = to_float(df[c])

    df = df[df["win_odds"].notna()].copy()
    train = df[df["year"].isin(TRAIN_YEARS)].copy()
    valid = df[df["year"].isin(VALID_YEARS)].copy()
    if train.empty or valid.empty:
        raise SystemExit("Train/valid split is empty for top3 model.")

    medians = {}
    for c in ["pop_rank", "distance", "field_size"] + FEATURES_PHASE1 + FEATURES_PHASE2:
        medians[c] = float(train[c].median())

    for split in [train, valid]:
        split["pop_rank"] = split["pop_rank"].fillna(medians["pop_rank"])
        split["distance"] = split["distance"].fillna(medians["distance"])
        split["field_size"] = split["field_size"].fillna(medians["field_size"])
        for c in FEATURES_PHASE1 + FEATURES_PHASE2:
            split[c] = split[c].fillna(medians[c])
        split["track_condition"] = split["track_condition"].fillna("UNKNOWN").astype(str)
        split["jockey_name"] = split["jockey_name"].fillna("UNKNOWN").astype(str)
        split["log_win_odds"] = np.log(split["win_odds"].clip(lower=1e-6))
        for c in FEATURES_BASE_CAT:
            split[c] = split[c].astype("category")

    features = FEATURES_BASE_NUM + FEATURES_BASE_CAT + FEATURES_PHASE1 + FEATURES_PHASE2
    model = lgb.LGBMClassifier(
        objective="binary",
        n_estimators=2200,
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
        callbacks=[lgb.early_stopping(120), lgb.log_evaluation(200)],
        categorical_feature=FEATURES_BASE_CAT,
    )
    return model, medians


def train_ability_model(history_df: pd.DataFrame, seed: int) -> tuple[lgb.LGBMClassifier, dict[str, float]]:
    df = history_df.copy()
    df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
    df = df[df["race_date"].notna()].copy()
    df["year"] = df["race_date"].dt.year
    for c in ["top3"] + FEATURES_PHASE1 + FEATURES_PHASE2:
        df[c] = to_float(df[c])
    train = df[df["year"].isin(TRAIN_YEARS)].copy()
    valid = df[df["year"].isin(VALID_YEARS)].copy()
    if train.empty or valid.empty:
        raise SystemExit("Train/valid split is empty for ability model.")

    feats = FEATURES_PHASE1 + FEATURES_PHASE2
    medians = {c: float(train[c].median()) for c in feats}
    for split in [train, valid]:
        for c in feats:
            split[c] = split[c].fillna(medians[c])

    model = lgb.LGBMClassifier(
        objective="binary",
        n_estimators=1800,
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
        callbacks=[lgb.early_stopping(120), lgb.log_evaluation(200)],
    )
    return model, medians


def build_history_table(pop_path: Path, encoding: str) -> pd.DataFrame:
    usecols = [
        "race_date",
        "race_id_raw",
        "horse_id",
        "horse_no",
        "distance",
        "finish_position",
        "margin_time",
        "last3f_time",
    ]
    pop = pd.read_csv(pop_path, encoding=encoding, low_memory=False, usecols=usecols)
    pop["race_date"] = pd.to_datetime(pop["race_date"], errors="coerce")
    pop = pop[pop["race_date"].notna() & pop["horse_id"].notna()].copy()
    for c in ["horse_no", "distance", "finish_position", "margin_time", "last3f_time"]:
        pop[c] = to_float(pop[c])
    pop["win"] = np.where(pop["finish_position"] == 1, 1.0, 0.0)
    pop["race_id_raw"] = pop["race_id_raw"].astype(str).str.split(".").str[0]

    pop = pop.sort_values(["race_id_raw", "last3f_time", "horse_no"], ascending=[True, True, True], kind="mergesort")
    pop["last3f_rank"] = pop.groupby("race_id_raw", dropna=False)["last3f_time"].rank(method="first", ascending=True)
    pop = pop.sort_values(["horse_id", "race_date", "race_id_raw", "horse_no"], ascending=True, kind="mergesort").reset_index(drop=True)
    return pop


def prepare_today_frame(paths: InputPaths, today_date: pd.Timestamp, cutoff_utc: pd.Timestamp) -> tuple[pd.DataFrame, dict[str, int]]:
    entries = pd.read_csv(paths.entries, encoding="utf-8-sig", low_memory=False)
    races = pd.read_csv(paths.races, encoding="utf-8-sig", low_memory=False)
    odds = pd.read_csv(paths.odds, encoding="utf-8-sig", low_memory=False)

    entries["race_id"] = entries["race_id"].astype(str)
    entries["horse_no"] = to_float(entries["horse_no"]).astype("Int64")
    entries["horse_id"] = to_float(entries["horse_id"]).astype("Int64")
    entries["is_scratched"] = entries["is_scratched"].fillna(False).astype(bool)
    entries = entries[entries["is_scratched"] == False].copy()  # noqa: E712

    races["race_id"] = races["race_id"].astype(str)
    races["race_date"] = pd.to_datetime(races["race_date"], errors="coerce")
    races["distance"] = to_float(races["distance"])
    races["track_condition"] = races["track_condition"].fillna("UNKNOWN").astype(str)
    races = races[races["race_date"].dt.normalize() == today_date].copy()
    if races.empty:
        raise SystemExit(f"No races found for today-date={today_date.date()} in races input: {paths.races}")

    base = entries.merge(
        races[["race_id", "race_date", "distance", "track_condition"]],
        on="race_id",
        how="left",
    )
    base = base[base["race_date"].notna()].copy()
    if base.empty:
        raise SystemExit("No entries matched today-date races after join.")
    field_size = base.groupby("race_id", dropna=False)["horse_no"].transform("count")
    base["field_size"] = field_size

    # win odds snapshot: use latest captured_at per race
    odds["race_id"] = odds["race_id"].astype(str)
    odds["captured_at"] = pd.to_datetime(odds["captured_at"], errors="coerce", utc=True)
    odds["odds_value"] = to_float(odds["odds_value"])
    odds["horse_no"] = to_float(odds["horse_no"]).astype("Int64")
    win_odds = odds[odds["odds_type"].astype(str).str.lower() == "win"].copy()
    win_odds = win_odds[win_odds["horse_no"].notna() & win_odds["odds_value"].notna()].copy()

    total_odds_rows = int(len(win_odds))
    win_odds = win_odds[win_odds["captured_at"].notna()].copy()
    win_odds = win_odds[win_odds["captured_at"] <= cutoff_utc].copy()
    used_odds_rows = int(len(win_odds))
    excluded_odds_rows = total_odds_rows - used_odds_rows

    win_odds = win_odds.sort_values(
        ["race_id", "captured_at", "odds_snapshot_version", "source_version"],
        ascending=[True, False, False, False],
        kind="mergesort",
    )
    win_odds = win_odds.drop_duplicates(subset=["race_id", "horse_no"], keep="first")
    win_odds = win_odds.rename(columns={"odds_value": "win_odds", "captured_at": "odds_captured_at"})

    base = base.merge(win_odds[["race_id", "horse_no", "win_odds", "odds_captured_at"]], on=["race_id", "horse_no"], how="left")
    base["pop_rank"] = (
        base.groupby("race_id", dropna=False)["win_odds"]
        .rank(method="first", ascending=True)
    )
    base["jockey_name"] = base["jockey_id"].astype(str).where(base["jockey_id"].notna(), "UNKNOWN")
    base["race_id_raw"] = base["race_id"].str.replace("-", "", regex=False).str.replace("R", "", regex=False)
    base["race_id_raw"] = base["race_id_raw"].str.pad(width=16, side="right", fillchar="0")
    odds_stats = {
        "used_odds_rows": used_odds_rows,
        "excluded_odds_rows": excluded_odds_rows,
        "total_odds_rows_before_cutoff": total_odds_rows,
    }
    return base, odds_stats


def compute_today_history_features(today: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    grouped = {hid: g for hid, g in hist.groupby("horse_id", dropna=False)}
    rows = []
    for row in today.itertuples(index=False):
        horse_id = getattr(row, "horse_id")
        race_date = getattr(row, "race_date")
        distance = getattr(row, "distance")
        hist_df = grouped.get(horse_id)

        prev_finish = np.nan
        avg_finish3 = np.nan
        same_dist_rate = np.nan
        prev_margin = np.nan
        avg_margin3 = np.nan
        prev_last3f_rank = np.nan
        last3f_best_count = np.nan

        if hist_df is not None and pd.notna(race_date):
            past = hist_df[hist_df["race_date"] < race_date].copy()
            if not past.empty:
                past = past.sort_values(["race_date", "race_id_raw", "horse_no"], ascending=True, kind="mergesort")
                tail1 = past.tail(1)
                tail3 = past.tail(3)
                prev_finish = float(tail1["finish_position"].iloc[0]) if pd.notna(tail1["finish_position"].iloc[0]) else np.nan
                avg_finish3 = float(tail3["finish_position"].mean()) if tail3["finish_position"].notna().any() else np.nan
                prev_margin = float(tail1["margin_time"].iloc[0]) if pd.notna(tail1["margin_time"].iloc[0]) else np.nan
                avg_margin3 = float(tail3["margin_time"].mean()) if tail3["margin_time"].notna().any() else np.nan
                prev_last3f_rank = float(tail1["last3f_rank"].iloc[0]) if pd.notna(tail1["last3f_rank"].iloc[0]) else np.nan
                last3f_best_count = float((tail3["last3f_rank"] == 1).sum()) if tail3["last3f_rank"].notna().any() else np.nan

                if pd.notna(distance):
                    same_dist = past[past["distance"] == distance]
                    if len(same_dist) > 0:
                        same_dist_rate = float(same_dist["win"].mean())

        rows.append(
            {
                "prev_finish_position": prev_finish,
                "avg_finish_last3": avg_finish3,
                "same_distance_win_rate": same_dist_rate,
                "prev_margin": prev_margin,
                "avg_margin_last3": avg_margin3,
                "prev_last3f_rank": prev_last3f_rank,
                "last3f_best_count": last3f_best_count,
            }
        )

    feat = pd.DataFrame(rows, index=today.index)
    return pd.concat([today, feat], axis=1)


def score_today(
    today_df: pd.DataFrame,
    top3_model: lgb.LGBMClassifier,
    top3_medians: dict[str, float],
    ability_model: lgb.LGBMClassifier,
    ability_medians: dict[str, float],
) -> pd.DataFrame:
    df = today_df.copy()
    for c in ["pop_rank", "distance", "field_size"]:
        df[c] = to_float(df[c])
    df["win_odds"] = to_float(df["win_odds"])
    df["track_condition"] = df["track_condition"].fillna("UNKNOWN").astype(str)
    df["jockey_name"] = df["jockey_name"].fillna("UNKNOWN").astype(str)
    df["log_win_odds"] = np.log(df["win_odds"].clip(lower=1e-6))

    for c in ["pop_rank", "distance", "field_size"] + FEATURES_PHASE1 + FEATURES_PHASE2:
        fill = top3_medians.get(c, np.nan)
        df[c] = to_float(df[c]).fillna(fill)

    for c in FEATURES_BASE_CAT:
        df[c] = df[c].astype("category")

    top3_features = FEATURES_BASE_NUM + FEATURES_BASE_CAT + FEATURES_PHASE1 + FEATURES_PHASE2
    df["pred_top3"] = top3_model.predict_proba(df[top3_features])[:, 1]

    for c in FEATURES_PHASE1 + FEATURES_PHASE2:
        df[c] = df[c].fillna(ability_medians[c])
    df["ability_top3_prob"] = ability_model.predict_proba(df[FEATURES_PHASE1 + FEATURES_PHASE2])[:, 1]

    df["market_prob"] = np.where(df["win_odds"] > 0, 1.0 / df["win_odds"], np.nan)
    race_market_med = df.groupby("race_id", dropna=False)["market_prob"].transform("median")
    global_market_med = float(df["market_prob"].median()) if df["market_prob"].notna().any() else 0.0
    df["market_prob"] = df["market_prob"].fillna(race_market_med).fillna(global_market_med)
    df["ability_gap"] = df["ability_top3_prob"] - df["market_prob"]
    df["value_gap"] = df["pred_top3"] - df["market_prob"]

    df["value_gap_rank"] = (
        df.groupby("race_id", dropna=False)["value_gap"]
        .rank(method="first", ascending=False)
        .fillna(9999)
        .astype(int)
    )
    df["rank_score"] = df["value_gap_rank"].map(RANK_SCORE_MAP).fillna(0.2)
    race_mean = df.groupby("race_id", dropna=False)["value_gap"].transform("mean")
    race_std = df.groupby("race_id", dropna=False)["value_gap"].transform(lambda s: s.std(ddof=0)).fillna(0.0)
    df["value_gap_z"] = np.where(race_std > 0, (df["value_gap"] - race_mean) / race_std, 0.0)
    df["value_score_v1"] = 0.6 * df["value_gap_z"] + 0.4 * df["rank_score"]
    df["value_score"] = 0.7 * df["value_score_v1"] + 0.3 * df["ability_gap"]
    df["pred_top3_rank"] = (
        df.groupby("race_id", dropna=False)["pred_top3"]
        .rank(method="first", ascending=False)
        .fillna(9999)
        .astype(int)
    )
    df["value_score_rank"] = (
        df.groupby("race_id", dropna=False)["value_score"]
        .rank(method="first", ascending=False)
        .fillna(9999)
        .astype(int)
    )
    return df


def apply_race_selection(scored: pd.DataFrame) -> pd.DataFrame:
    race_df = (
        scored.groupby("race_id", dropna=False)
        .agg(
            race_date=("race_date", "first"),
            value_score_max=("value_score", "max"),
            gap_std=("value_score", lambda s: float(s.std(ddof=0))),
        )
        .reset_index()
    )
    race_df["z_value_score_max"] = (race_df["value_score_max"] - race_df["value_score_max"].mean()) / race_df["value_score_max"].std(ddof=0)
    race_df["z_gap_std"] = (race_df["gap_std"] - race_df["gap_std"].mean()) / race_df["gap_std"].std(ddof=0)
    race_df["z_value_score_max"] = race_df["z_value_score_max"].replace([np.inf, -np.inf], 0).fillna(0.0)
    race_df["z_gap_std"] = race_df["z_gap_std"].replace([np.inf, -np.inf], 0).fillna(0.0)
    race_df["race_select_score"] = 0.6 * race_df["z_value_score_max"] + 0.4 * race_df["z_gap_std"]
    race_df = race_df.sort_values("race_select_score", ascending=False).reset_index(drop=True)
    race_df["race_select_rank"] = np.arange(1, len(race_df) + 1)
    keep_n = max(1, int(np.ceil(len(race_df) * 0.15)))
    race_df["race_selected_top15"] = race_df["race_select_rank"] <= keep_n
    return scored.merge(race_df[["race_id", "race_selected_top15", "race_select_rank"]], on="race_id", how="left")


def attach_reason(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    reason = []
    for row in out.itertuples(index=False):
        if not bool(getattr(row, "race_selected_top15")):
            reason.append("race_filtered_out")
        elif int(getattr(row, "value_score_rank")) <= 2:
            reason.append("race_selected & top2_value_score")
        elif int(getattr(row, "value_score_rank")) <= 3 or int(getattr(row, "pred_top3_rank")) <= 3:
            reason.append("race_selected & candidate_pool")
        else:
            reason.append("race_selected & non_candidate")
    out["selection_reason"] = reason
    return out


def main() -> int:
    args = parse_args()
    today_date = parse_today_date(args.today_date)
    cutoff_utc = parse_cutoff(args.odds_cutoff)
    input_paths = resolve_inputs(args, today_date)

    history_df = pd.read_csv(Path(args.history_dataset), encoding=args.encoding, low_memory=False)
    for c in ["horse_id", "horse_no"]:
        history_df[c] = to_float(history_df[c]).astype("Int64")
    hist_table = build_history_table(Path(args.population), args.encoding)
    hist_table["horse_id"] = to_float(hist_table["horse_id"]).astype("Int64")

    top3_model, top3_medians = train_top3_model(history_df, args.seed)
    ability_model, ability_medians = train_ability_model(history_df, args.seed)

    today, odds_stats = prepare_today_frame(input_paths, today_date, cutoff_utc)
    today = compute_today_history_features(today, hist_table)
    scored = score_today(today, top3_model, top3_medians, ability_model, ability_medians)
    scored = apply_race_selection(scored)
    scored = attach_reason(scored)
    scored["today_date"] = today_date.strftime("%Y-%m-%d")
    scored["odds_cutoff"] = args.odds_cutoff

    out_cols = [
        "race_id",
        "horse_no",
        "pred_top3",
        "ability_gap",
        "value_score",
        "today_date",
        "odds_cutoff",
        "selection_reason",
        "race_selected_top15",
        "value_score_rank",
        "pred_top3_rank",
        "win_odds",
        "odds_captured_at",
    ]
    out_df = scored[out_cols].copy().sort_values(["race_id", "value_score_rank", "horse_no"], ascending=[True, True, True]).reset_index(drop=True)

    log_lines = [
        "today wide prediction pipeline log",
        f"today_date={today_date.strftime('%Y-%m-%d')}",
        f"odds_cutoff={args.odds_cutoff}",
        "model_version_top3=phase2_top3_v1",
        "model_version_ability=phase2_ability_v1",
        f"input_entries={input_paths.entries}",
        f"input_races={input_paths.races}",
        f"input_odds={input_paths.odds}",
        f"input_history_dataset={args.history_dataset}",
        f"input_population={args.population}",
        f"rows_entries_used={len(today)}",
        f"rows_output={len(out_df)}",
        f"used_odds_rows={odds_stats['used_odds_rows']}",
        f"excluded_odds_rows={odds_stats['excluded_odds_rows']}",
        f"total_odds_rows_before_cutoff={odds_stats['total_odds_rows_before_cutoff']}",
        f"odds_latest_captured_at_max={pd.to_datetime(out_df['odds_captured_at'], errors='coerce').max()}",
        "missing_rules=track_condition->UNKNOWN,jockey_name->UNKNOWN,numeric->train_median",
        "future_info_rule=history rows filtered by race_date < target race_date",
    ]

    out_path = Path(args.out)
    log_path = Path(args.log_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False, encoding=args.encoding)
    log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")

    print("=== done ===")
    print(f"predictions: {out_path}")
    print(f"log: {log_path}")
    print(f"entries_used={len(today)} output_rows={len(out_df)}")
    selected_races = out_df.loc[out_df["race_selected_top15"] == True, "race_id"].nunique()  # noqa: E712
    print(f"selected_races={selected_races} total_races={out_df['race_id'].nunique()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
