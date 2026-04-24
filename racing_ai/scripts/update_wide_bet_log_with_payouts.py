import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update wide bet log with real payouts.")
    parser.add_argument("--log", default=r"C:\TXT\bet_logs\wide_bet_log_all.csv")
    parser.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    parser.add_argument("--out", default=r"C:\TXT\bet_logs\wide_bet_log_all_updated.csv")
    parser.add_argument("--encoding", default="cp932")
    parser.add_argument("--stake", type=float, default=100.0)
    return parser.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def norm_pair(a: int, b: int) -> str:
    x, y = sorted((int(a), int(b)))
    return f"{x:02d}-{y:02d}"


def load_wide_payouts(root: Path) -> pd.DataFrame:
    rows = []
    for fp in root.rglob("payouts.csv"):
        try:
            df = pd.read_csv(fp, encoding="utf-8-sig", low_memory=False)
        except Exception:
            df = pd.read_csv(fp, encoding="cp932", low_memory=False)

        required = {"race_id", "bet_type", "bet_key", "payout"}
        if not required.issubset(df.columns):
            continue

        sub = df[df["bet_type"].astype(str).str.upper() == "WIDE"][["race_id", "bet_key", "payout"]].copy()
        sub["payout"] = to_float(sub["payout"])
        sub = sub[sub["payout"].notna()].copy()

        def parse_pair(v: str) -> str:
            parts = [p for p in str(v).strip().split("-") if p != ""]
            if len(parts) != 2:
                return ""
            try:
                return norm_pair(int(parts[0]), int(parts[1]))
            except Exception:
                return ""

        sub["pair_key"] = sub["bet_key"].map(parse_pair)
        sub = sub[sub["pair_key"] != ""].copy()
        rows.append(sub[["race_id", "pair_key", "payout"]])

    if not rows:
        return pd.DataFrame(columns=["race_id", "pair_key", "payout"])

    out = pd.concat(rows, ignore_index=True)
    out = out.drop_duplicates(subset=["race_id", "pair_key"], keep="last").reset_index(drop=True)
    return out


def main() -> int:
    args = parse_args()
    log_path = Path(args.log)
    out_path = Path(args.out)

    if not log_path.exists():
        raise SystemExit(f"bet log not found: {log_path}")

    log_df = pd.read_csv(log_path, encoding=args.encoding, low_memory=False)
    required_log = {"race_id", "horse_no_1", "horse_no_2", "wide_hit", "payout", "profit", "roi"}
    missing = required_log - set(log_df.columns)
    if missing:
        raise SystemExit(f"bet log missing columns: {sorted(missing)}")

    log_df["horse_no_1"] = to_float(log_df["horse_no_1"])
    log_df["horse_no_2"] = to_float(log_df["horse_no_2"])
    log_df = log_df[log_df["horse_no_1"].notna() & log_df["horse_no_2"].notna()].copy()
    log_df["horse_no_1"] = log_df["horse_no_1"].astype(int)
    log_df["horse_no_2"] = log_df["horse_no_2"].astype(int)
    log_df["pair_key"] = [norm_pair(a, b) for a, b in zip(log_df["horse_no_1"], log_df["horse_no_2"])]

    # update target: rows where any outcome field is missing
    log_df["wide_hit"] = to_float(log_df["wide_hit"])
    log_df["payout"] = to_float(log_df["payout"])
    log_df["profit"] = to_float(log_df["profit"])
    log_df["roi"] = to_float(log_df["roi"])

    updatable_mask = (
        log_df["wide_hit"].isna()
        | log_df["payout"].isna()
        | log_df["profit"].isna()
        | log_df["roi"].isna()
    )

    payouts = load_wide_payouts(Path(args.payout_root))
    if payouts.empty:
        raise SystemExit("no WIDE payouts loaded from payout root")

    key_to_payout = {(str(race_id), str(pair_key)): float(payout) for race_id, pair_key, payout in payouts.itertuples(index=False)}

    match_count = 0
    non_match_count = 0
    update_count = 0

    for idx, row in log_df[updatable_mask].iterrows():
        key = (str(row["race_id"]), str(row["pair_key"]))
        if key in key_to_payout:
            payout = key_to_payout[key]
            log_df.at[idx, "wide_hit"] = 1.0
            log_df.at[idx, "payout"] = payout
            log_df.at[idx, "profit"] = payout - args.stake
            log_df.at[idx, "roi"] = payout / args.stake
            match_count += 1
            update_count += 1
        else:
            log_df.at[idx, "wide_hit"] = 0.0
            log_df.at[idx, "payout"] = 0.0
            log_df.at[idx, "profit"] = -args.stake
            log_df.at[idx, "roi"] = 0.0
            non_match_count += 1
            update_count += 1

    log_df = log_df.drop(columns=["pair_key"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    log_df.to_csv(out_path, index=False, encoding=args.encoding)

    print("=== done ===")
    print(f"output: {out_path}")
    print(f"updated_rows: {update_count}")
    print(f"matched_rows: {match_count}")
    print(f"non_matched_rows: {non_match_count}")
    print(f"already_filled_skipped: {int((~updatable_mask).sum())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
