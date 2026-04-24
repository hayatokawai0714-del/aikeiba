import argparse
from datetime import datetime
from itertools import combinations
from pathlib import Path
import uuid

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build per-run and cumulative wide bet logs from today predictions.")
    parser.add_argument("--input", default=r"C:\TXT\today_wide_predictions.csv")
    parser.add_argument("--log-input", default=r"C:\TXT\today_wide_predictions_log.txt")
    parser.add_argument("--out-dir", default=r"C:\TXT\bet_logs")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--model-version", default="")
    parser.add_argument("--feature-version", default="")
    parser.add_argument("--encoding", default="cp932")
    return parser.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def parse_meta_from_log(log_path: Path) -> dict[str, str]:
    meta = {}
    if not log_path.exists():
        return meta
    for line in log_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key in {"today_date", "odds_cutoff", "model_version_top3", "model_version_ability"}:
            meta[key] = value
    return meta


def build_pair_candidates(pred_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    use_df = pred_df[
        (pred_df["race_selected_top15"] == True)  # noqa: E712
        & ((pred_df["value_score_rank"] <= 3) | (pred_df["pred_top3_rank"] <= 3))
    ].copy()

    for race_id, race_df in use_df.groupby("race_id", dropna=False):
        records = race_df.to_dict("records")
        if len(records) < 2:
            continue
        pair_rows = []
        for left, right in combinations(records, 2):
            ordered = sorted(
                [left, right],
                key=lambda r: (-float(r.get("pred_top3", np.nan)), float(r.get("horse_no", 999))),
            )
            first, second = ordered[0], ordered[1]
            pair_score = float(first["pred_top3"]) * float(second["pred_top3"])
            pair_rows.append(
                {
                    "race_id": race_id,
                    "horse_no_1": int(first["horse_no"]),
                    "horse_no_2": int(second["horse_no"]),
                    "horse_name_1": "",
                    "horse_name_2": "",
                    "pred_top3_1": float(first["pred_top3"]),
                    "pred_top3_2": float(second["pred_top3"]),
                    "value_score_1": float(first["value_score"]),
                    "value_score_2": float(second["value_score"]),
                    "selection_reason": f"{first.get('selection_reason','')};{second.get('selection_reason','')}",
                    "pair_score": pair_score,
                }
            )
        if not pair_rows:
            continue
        pair_df = pd.DataFrame(pair_rows).sort_values("pair_score", ascending=False).reset_index(drop=True)
        pair_df["pair_rank"] = np.arange(1, len(pair_df) + 1)
        pair_df = pair_df[pair_df["pair_rank"] <= 3].copy()
        rows.append(pair_df)

    if not rows:
        return pd.DataFrame(
            columns=[
                "race_id",
                "horse_no_1",
                "horse_no_2",
                "horse_name_1",
                "horse_name_2",
                "pred_top3_1",
                "pred_top3_2",
                "value_score_1",
                "value_score_2",
                "selection_reason",
                "pair_score",
                "pair_rank",
            ]
        )
    return pd.concat(rows, ignore_index=True)


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    log_input_path = Path(args.log_input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    pred = pd.read_csv(input_path, encoding=args.encoding, low_memory=False)
    req = {"race_id", "horse_no", "pred_top3", "ability_gap", "value_score", "selection_reason", "today_date", "odds_cutoff", "race_selected_top15", "value_score_rank", "pred_top3_rank"}
    missing = req - set(pred.columns)
    if missing:
        raise SystemExit(f"Missing columns in input: {sorted(missing)}")

    pred["horse_no"] = to_float(pred["horse_no"])
    pred["pred_top3"] = to_float(pred["pred_top3"])
    pred["value_score"] = to_float(pred["value_score"])
    pred["value_score_rank"] = to_float(pred["value_score_rank"])
    pred["pred_top3_rank"] = to_float(pred["pred_top3_rank"])
    pred["race_selected_top15"] = pred["race_selected_top15"].astype(bool)

    meta_log = parse_meta_from_log(log_input_path)
    today_date = str(pred["today_date"].dropna().iloc[0]) if pred["today_date"].notna().any() else meta_log.get("today_date", "")
    odds_cutoff = str(pred["odds_cutoff"].dropna().iloc[0]) if pred["odds_cutoff"].notna().any() else meta_log.get("odds_cutoff", "")
    model_version = args.model_version or meta_log.get("model_version_top3", "phase2_top3_v1")
    feature_version = args.feature_version or "history_phase2_v1"

    run_id = args.run_id.strip()
    if not run_id:
        run_id = f"{today_date.replace('-', '')}_{uuid.uuid4().hex[:8]}"

    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pair_df = build_pair_candidates(pred)
    if pair_df.empty:
        raise SystemExit("No bet candidates generated from input predictions.")

    pair_df.insert(0, "run_id", run_id)
    pair_df.insert(1, "today_date", today_date)
    pair_df.insert(2, "odds_cutoff", odds_cutoff)
    pair_df.insert(3, "model_version", model_version)
    pair_df.insert(4, "feature_version", feature_version)
    pair_df.insert(5, "created_at", created_at)

    pair_df["wide_hit"] = np.nan
    pair_df["payout"] = np.nan
    pair_df["profit"] = np.nan
    pair_df["roi"] = np.nan

    run_file = out_dir / f"wide_bet_log_{run_id}.csv"
    all_file = out_dir / "wide_bet_log_all.csv"

    if run_file.exists():
        raise SystemExit(f"Run file already exists (duplicate run_id): {run_file}")

    if all_file.exists():
        all_df = pd.read_csv(all_file, encoding=args.encoding, low_memory=False)
        if "run_id" in all_df.columns and run_id in set(all_df["run_id"].astype(str)):
            raise SystemExit(f"Duplicate run_id detected in cumulative log: {run_id}")
        out_all = pd.concat([all_df, pair_df], ignore_index=True, sort=False)
    else:
        out_all = pair_df.copy()

    pair_df.to_csv(run_file, index=False, encoding=args.encoding)
    out_all.to_csv(all_file, index=False, encoding=args.encoding)

    print("=== done ===")
    print(f"run_id: {run_id}")
    print(f"run_file: {run_file}")
    print(f"all_file: {all_file}")
    print(f"rows_run: {len(pair_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
