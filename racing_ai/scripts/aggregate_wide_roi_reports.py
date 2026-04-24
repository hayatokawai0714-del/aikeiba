import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate daily/monthly ROI reports from wide bet logs.")
    parser.add_argument("--input", default=r"C:\TXT\bet_logs\wide_bet_log_all_updated.csv")
    parser.add_argument("--out-daily", default=r"C:\TXT\reports\daily_roi_report.csv")
    parser.add_argument("--out-monthly", default=r"C:\TXT\reports\monthly_roi_report.csv")
    parser.add_argument("--out-text", default=r"C:\TXT\reports\roi_analysis_report.txt")
    parser.add_argument("--encoding", default="cp932")
    parser.add_argument("--stake", type=float, default=100.0)
    return parser.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def derive_bet_date(df: pd.DataFrame) -> pd.Series:
    if "race_id" in df.columns:
        race_id = df["race_id"].astype(str)
        date_str = race_id.str.slice(0, 8)
        date = pd.to_datetime(date_str, format="%Y%m%d", errors="coerce")
        if date.notna().any():
            return date
    if "today_date" in df.columns:
        date = pd.to_datetime(df["today_date"], errors="coerce")
        return date
    return pd.Series(pd.NaT, index=df.index)


def build_daily(df: pd.DataFrame, stake: float) -> pd.DataFrame:
    g = (
        df.groupby("bet_date", dropna=False)
        .agg(
            purchase_count=("race_id", "size"),
            hit_count=("wide_hit", lambda s: int((to_float(s).fillna(0) == 1).sum())),
            return_amount=("payout", lambda s: float(to_float(s).fillna(0).sum())),
            race_count=("race_id", "nunique"),
        )
        .reset_index()
    )
    g["invest_amount"] = g["purchase_count"] * stake
    g["roi"] = np.where(g["invest_amount"] > 0, g["return_amount"] / g["invest_amount"], np.nan)
    g["hit_rate"] = np.where(g["purchase_count"] > 0, g["hit_count"] / g["purchase_count"], np.nan)
    g["bet_date"] = pd.to_datetime(g["bet_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return g.sort_values("bet_date").reset_index(drop=True)


def build_monthly(df: pd.DataFrame, stake: float) -> pd.DataFrame:
    d = df.copy()
    d["bet_month"] = pd.to_datetime(d["bet_date"], errors="coerce").dt.strftime("%Y-%m")
    g = (
        d.groupby("bet_month", dropna=False)
        .agg(
            purchase_count=("race_id", "size"),
            hit_count=("wide_hit", lambda s: int((to_float(s).fillna(0) == 1).sum())),
            return_amount=("payout", lambda s: float(to_float(s).fillna(0).sum())),
            purchase_race_count=("race_id", "nunique"),
        )
        .reset_index()
    )
    g["invest_amount"] = g["purchase_count"] * stake
    g["roi"] = np.where(g["invest_amount"] > 0, g["return_amount"] / g["invest_amount"], np.nan)
    g["hit_rate"] = np.where(g["purchase_count"] > 0, g["hit_count"] / g["purchase_count"], np.nan)
    return g.sort_values("bet_month").reset_index(drop=True)


def max_consecutive_losses(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    d = df.copy()
    d["wide_hit_num"] = to_float(d["wide_hit"]).fillna(0)
    sort_cols = [c for c in ["bet_date", "race_id", "horse_no_1", "horse_no_2"] if c in d.columns]
    d = d.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    max_streak = 0
    streak = 0
    for value in d["wide_hit_num"]:
        if value == 0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return int(max_streak)


def band_analysis(df: pd.DataFrame, column: str, labels: list[str], bins: list[float], stake: float) -> pd.DataFrame:
    d = df.copy()
    d[column] = to_float(d[column])
    d = d[d[column].notna()].copy()
    if d.empty:
        return pd.DataFrame(columns=["band", "purchase_count", "hit_rate", "roi"])

    d["band"] = pd.cut(d[column], bins=bins, labels=labels, right=False, include_lowest=True)
    out = (
        d.groupby("band", dropna=False, observed=False)
        .agg(
            purchase_count=("race_id", "size"),
            hit_count=("wide_hit", lambda s: int((to_float(s).fillna(0) == 1).sum())),
            return_amount=("payout", lambda s: float(to_float(s).fillna(0).sum())),
        )
        .reset_index()
    )
    out["invest_amount"] = out["purchase_count"] * stake
    out["hit_rate"] = np.where(out["purchase_count"] > 0, out["hit_count"] / out["purchase_count"], np.nan)
    out["roi"] = np.where(out["invest_amount"] > 0, out["return_amount"] / out["invest_amount"], np.nan)
    return out[["band", "purchase_count", "hit_rate", "roi"]]


def value_score_band_analysis(df: pd.DataFrame, stake: float) -> pd.DataFrame:
    if not {"value_score_1", "value_score_2"}.issubset(df.columns):
        return pd.DataFrame(columns=["band", "purchase_count", "hit_rate", "roi"])
    d = df.copy()
    d["value_score_pair"] = to_float(d["value_score_1"]) + to_float(d["value_score_2"])
    d = d[d["value_score_pair"].notna()].copy()
    if d.empty:
        return pd.DataFrame(columns=["band", "purchase_count", "hit_rate", "roi"])
    q1 = float(d["value_score_pair"].quantile(0.33))
    q2 = float(d["value_score_pair"].quantile(0.66))
    bins = [-np.inf, q1, q2, np.inf]
    labels = ["low", "mid", "high"]
    return band_analysis(d, "value_score_pair", labels, bins, stake)


def main() -> int:
    args = parse_args()
    in_path = Path(args.input)
    if not in_path.exists():
        raise SystemExit(f"Input log not found: {in_path}")

    df = pd.read_csv(in_path, encoding=args.encoding, low_memory=False)
    req = {"race_id", "wide_hit", "payout"}
    missing = req - set(df.columns)
    if missing:
        raise SystemExit(f"Input log missing columns: {sorted(missing)}")

    df["bet_date"] = derive_bet_date(df)
    if df["bet_date"].isna().all():
        raise SystemExit("Unable to derive bet_date from race_id/today_date.")

    daily = build_daily(df, args.stake)
    monthly = build_monthly(df, args.stake)

    odds_band = band_analysis(
        df,
        "roi",
        labels=["~5", "5-20", "20+"],
        bins=[-np.inf, 5.0, 20.0, np.inf],
        stake=args.stake,
    )
    value_band = value_score_band_analysis(df, args.stake)

    gap_band_available = "gap_std_1" in df.columns or "gap_std" in df.columns
    if "gap_std" in df.columns:
        gap_col = "gap_std"
    elif "gap_std_1" in df.columns and "gap_std_2" in df.columns:
        df["gap_std_pair"] = (to_float(df["gap_std_1"]) + to_float(df["gap_std_2"])) / 2.0
        gap_col = "gap_std_pair"
    elif "gap_std_1" in df.columns:
        gap_col = "gap_std_1"
    else:
        gap_col = ""

    if gap_band_available and gap_col:
        dgap = df.copy()
        dgap[gap_col] = to_float(dgap[gap_col])
        dgap = dgap[dgap[gap_col].notna()].copy()
        if not dgap.empty:
            med = float(dgap[gap_col].median())
            dgap["gap_band"] = np.where(dgap[gap_col] >= med, "high", "low")
            gap_band = (
                dgap.groupby("gap_band", dropna=False)
                .agg(
                    purchase_count=("race_id", "size"),
                    hit_count=("wide_hit", lambda s: int((to_float(s).fillna(0) == 1).sum())),
                    return_amount=("payout", lambda s: float(to_float(s).fillna(0).sum())),
                )
                .reset_index()
            )
            gap_band["invest_amount"] = gap_band["purchase_count"] * args.stake
            gap_band["hit_rate"] = np.where(gap_band["purchase_count"] > 0, gap_band["hit_count"] / gap_band["purchase_count"], np.nan)
            gap_band["roi"] = np.where(gap_band["invest_amount"] > 0, gap_band["return_amount"] / gap_band["invest_amount"], np.nan)
            gap_band = gap_band[["gap_band", "purchase_count", "hit_rate", "roi"]]
        else:
            gap_band = pd.DataFrame(columns=["gap_band", "purchase_count", "hit_rate", "roi"])
    else:
        gap_band = pd.DataFrame(columns=["gap_band", "purchase_count", "hit_rate", "roi"])

    worst_day = daily.loc[daily["roi"].idxmin()] if len(daily) else None
    worst_month = monthly.loc[monthly["roi"].idxmin()] if len(monthly) else None
    max_loss_streak = max_consecutive_losses(df)

    out_daily = Path(args.out_daily)
    out_monthly = Path(args.out_monthly)
    out_text = Path(args.out_text)
    out_daily.parent.mkdir(parents=True, exist_ok=True)
    out_monthly.parent.mkdir(parents=True, exist_ok=True)
    out_text.parent.mkdir(parents=True, exist_ok=True)

    daily.to_csv(out_daily, index=False, encoding=args.encoding)
    monthly.to_csv(out_monthly, index=False, encoding=args.encoding)

    lines = []
    lines.append("roi analysis report")
    lines.append("")
    lines.append(f"input={in_path}")
    lines.append(f"daily_csv={out_daily}")
    lines.append(f"monthly_csv={out_monthly}")
    lines.append("")
    lines.append("daily summary")
    lines.extend(daily.to_string(index=False).splitlines())
    lines.append("")
    lines.append("monthly summary")
    lines.extend(monthly.to_string(index=False).splitlines())
    lines.append("")
    lines.append("condition analysis: odds band (roi as payout multiple)")
    lines.extend(odds_band.to_string(index=False).splitlines())
    lines.append("")
    lines.append("condition analysis: value_score band")
    if value_band.empty:
        lines.append("情報不足: value_score_1/value_score_2 missing or invalid")
    else:
        lines.extend(value_band.to_string(index=False).splitlines())
    lines.append("")
    lines.append("condition analysis: gap_std band")
    if gap_band.empty:
        lines.append("情報不足: gap_std column not found in input log")
    else:
        lines.extend(gap_band.to_string(index=False).splitlines())
    lines.append("")
    lines.append("worst analysis")
    if worst_day is not None:
        lines.append(
            f"- worst_day={worst_day['bet_date']} roi={float(worst_day['roi']):.6f} "
            f"purchase_count={int(worst_day['purchase_count'])} hit_rate={float(worst_day['hit_rate']):.6f}"
        )
    if worst_month is not None:
        lines.append(
            f"- worst_month={worst_month['bet_month']} roi={float(worst_month['roi']):.6f} "
            f"purchase_count={int(worst_month['purchase_count'])} hit_rate={float(worst_month['hit_rate']):.6f}"
        )
    lines.append(f"- max_consecutive_losses={max_loss_streak}")

    out_text.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"daily: {out_daily}")
    print(f"monthly: {out_monthly}")
    print(f"text: {out_text}")
    print(f"rows_input={len(df)} rows_daily={len(daily)} rows_monthly={len(monthly)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
