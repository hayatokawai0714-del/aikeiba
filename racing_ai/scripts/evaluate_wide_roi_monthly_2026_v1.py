import argparse
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
    ap = argparse.ArgumentParser(description="Evaluate monthly ROI stability for 2026 using real wide payouts.")
    ap.add_argument("--input", default=r"C:\TXT\wide_roi_real_2026_v1.csv")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--race", default=r"C:\TXT\wide_race_selection_fixed_rate_2026_v1.csv")
    ap.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_roi_monthly_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_roi_monthly_report_2026_v1.txt")
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


def load_payouts(payout_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for fp in payout_root.rglob("payouts.csv"):
        if "2026" not in str(fp):
            continue
        try:
            df = pd.read_csv(fp, encoding="utf-8-sig", low_memory=False)
        except Exception:
            df = pd.read_csv(fp, encoding="cp932", low_memory=False)

        if not {"race_id", "bet_type", "bet_key", "payout"}.issubset(df.columns):
            continue

        sub = df[df["bet_type"].astype(str).str.upper() == "WIDE"][
            ["race_id", "bet_key", "payout"]
        ].copy()
        sub["file_path"] = str(fp)
        frames.append(sub)

    if not frames:
        return pd.DataFrame(columns=["race_id", "pair_key", "payout_yen"])

    out = pd.concat(frames, ignore_index=True)
    out["payout_yen"] = to_float(out["payout"])
    out = out[out["payout_yen"].notna()].copy()

    def _norm(v: str) -> str:
        parts = [p for p in str(v).strip().split("-") if p]
        if len(parts) != 2:
            return ""
        try:
            return normalize_pair_key(int(parts[0]), int(parts[1]))
        except Exception:
            return ""

    out["pair_key"] = out["bet_key"].map(_norm)
    out = out[out["pair_key"] != ""].copy()

    # 同一 race+pair の重複は最後に1件採用（同額想定）
    out = out.drop_duplicates(subset=["race_id", "pair_key"], keep="last")
    return out[["race_id", "pair_key", "payout_yen"]].reset_index(drop=True)


def summarize_month(sub: pd.DataFrame, stake: int) -> dict:
    races = int(sub["race_id_raw"].nunique()) if len(sub) else 0
    bets = int(len(sub))
    stake_total = int(bets * stake)
    return_total = float(sub["return_yen"].sum()) if len(sub) else 0.0
    roi = float((return_total / stake_total) * 100.0) if stake_total else np.nan
    hit_rate = float(sub["wide_hit_real"].mean()) if len(sub) else np.nan
    avg_payout = float(sub.loc[sub["wide_hit_real"] == 1, "payout_yen"].mean()) if (sub["wide_hit_real"] == 1).any() else np.nan
    return {
        "races": races,
        "bets": bets,
        "hit_rate": hit_rate,
        "roi_pct": roi,
        "avg_payout_yen": avg_payout,
        "stake_total": stake_total,
        "return_total": return_total,
    }


def main() -> int:
    args = parse_args()

    # 入力存在確認（このファイルは月次再計算の参照元として扱う）
    _ = pd.read_csv(Path(args.input), encoding=args.encoding, low_memory=False)

    wide = pd.read_csv(Path(args.wide), encoding=args.encoding, low_memory=False)
    race = pd.read_csv(Path(args.race), encoding=args.encoding, low_memory=False)

    for col in ["horse_no_1", "horse_no_2", "pair_rank_score"]:
        wide[col] = to_float(wide[col])

    selected = race[race["selected_top15"] == True].copy()  # noqa: E712
    selected["race_id_raw"] = selected["race_id_raw"].astype(str)
    selected_ids = set(selected["race_id_raw"])

    wide["race_id_raw"] = wide["race_id_raw"].astype(str)
    wide = wide[wide["race_id_raw"].isin(selected_ids)].copy()
    if wide.empty:
        raise SystemExit("No bets after selected_top15 filter")

    wide = wide[wide["horse_no_1"].notna() & wide["horse_no_2"].notna() & wide["pair_rank_score"].notna()].copy()

    race_date_map = selected[["race_id_raw", "race_date"]].drop_duplicates()
    wide = wide.merge(race_date_map, on="race_id_raw", how="left", suffixes=("", "_sel"))
    if "race_date_sel" in wide.columns and "race_date" in wide.columns:
        wide["race_date"] = wide["race_date"].fillna(wide["race_date_sel"])
        wide = wide.drop(columns=["race_date_sel"])

    wide["race_id"] = wide["race_id_raw"].map(raw_to_race_id)
    wide["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(wide["horse_no_1"], wide["horse_no_2"])]

    payouts = load_payouts(Path(args.payout_root))
    if payouts.empty:
        raise SystemExit("No real wide payout data found")

    joined = wide.merge(payouts, on=["race_id", "pair_key"], how="left")
    joined["wide_hit_real"] = joined["payout_yen"].notna().astype(int)
    joined["return_yen"] = joined["payout_yen"].fillna(0.0)

    joined["race_month"] = pd.to_datetime(joined["race_date"], errors="coerce").dt.strftime("%Y-%m")

    rows: list[dict] = []
    for pattern, cond in [("top1", joined["pair_rank_score"] == 1), ("top3", joined["pair_rank_score"] <= 3)]:
        p_df = joined[cond].copy()
        for month in MONTHS:
            m_df = p_df[p_df["race_month"] == month].copy()
            stats = summarize_month(m_df, args.stake)
            rows.append({"pattern": pattern, "month": month, **stats})

    out = pd.DataFrame(rows)

    # 安定性指標
    stability_rows = []
    for pattern in ["top1", "top3"]:
        p = out[out["pattern"] == pattern].copy()
        roi_vals = p["roi_pct"].dropna()
        if roi_vals.empty:
            continue
        worst = p.loc[p["roi_pct"].idxmin()]
        best = p.loc[p["roi_pct"].idxmax()]
        stability_rows.append(
            {
                "pattern": pattern,
                "roi_mean": float(roi_vals.mean()),
                "roi_std": float(roi_vals.std(ddof=0)),
                "roi_min": float(roi_vals.min()),
                "roi_max": float(roi_vals.max()),
                "negative_months": int((roi_vals < 100.0).sum()),
                "best_month": str(best["month"]),
                "worst_month": str(worst["month"]),
            }
        )

    stability = pd.DataFrame(stability_rows)

    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False, encoding=args.encoding)

    lines: list[str] = []
    lines.append("wide monthly ROI stability report (2026)")
    lines.append("")
    lines.append(f"input={args.input}")
    lines.append(f"output={out_csv}")
    lines.append("- 条件は既存ロジック固定（selected_top15 + top1/top3 + 実払戻JOIN）")
    lines.append("")
    lines.append("月別ROI一覧")
    lines.extend(out.to_string(index=False).splitlines())
    lines.append("")
    lines.append("安定性評価")
    lines.extend(stability.to_string(index=False).splitlines())

    for pattern in ["top1", "top3"]:
        p = stability[stability["pattern"] == pattern]
        if p.empty:
            continue
        row = p.iloc[0]
        lines.append("")
        lines.append(f"[{pattern}] 最良月/最悪月")
        lines.append(f"- 最良月: {row['best_month']} (ROI {row['roi_max']:.2f}%)")
        lines.append(f"- 最悪月: {row['worst_month']} (ROI {row['roi_min']:.2f}%)")
        lines.append(f"- ROI標準偏差: {row['roi_std']:.2f}")
        lines.append(f"- マイナス月(ROI<100%): {int(row['negative_months'])}ヶ月")
        lines.append("- 《推測》標準偏差が大きいほど月次ブレが強く、安定運用には追加フィルタが必要です。")

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print("\n=== monthly ===")
    print(out.to_string(index=False))
    print("\n=== stability ===")
    print(stability.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
