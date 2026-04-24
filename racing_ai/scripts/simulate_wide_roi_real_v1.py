import argparse
from pathlib import Path

import numpy as np
import pandas as pd


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
    ap = argparse.ArgumentParser(description="Recalculate wide ROI using real payout data.")
    ap.add_argument("--wide", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--race", default=r"C:\TXT\wide_race_selection_fixed_rate_2026_v1.csv")
    ap.add_argument("--assumed", default=r"C:\TXT\wide_roi_simulation_2026_v1.csv")
    ap.add_argument("--payout-root", default=r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai\data\normalized")
    ap.add_argument("--payout-csv", default="")
    ap.add_argument("--out-csv", default=r"C:\TXT\wide_roi_real_2026_v1.csv")
    ap.add_argument("--out-report", default=r"C:\TXT\wide_roi_real_report_2026_v1.txt")
    ap.add_argument("--encoding", default="cp932")
    ap.add_argument("--stake", type=int, default=100)
    return ap.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def normalize_pair_key(a: int, b: int) -> str:
    x, y = sorted((int(a), int(b)))
    return f"{x:02d}-{y:02d}"


def raw_to_race_id(raw: str) -> str:
    s = str(raw).strip()
    if "." in s:
        s = s.split(".")[0]
    s = s.zfill(16)
    date = s[:8]
    jyo = s[8:10]
    race_no = int(s[-2:])
    venue = JYO_TO_VENUE.get(jyo, "UNK")
    return f"{date}-{venue}-{race_no:02d}R"


def source_priority(source_version: str, file_path: str) -> int:
    text = f"{source_version} {file_path}".lower()
    if "real_from_jv" in text:
        return 4
    if "_real" in text:
        return 3
    if "hist_from_jv" in text:
        return 2
    return 1


def load_payout_files(root: Path, extra_csv: str) -> list[Path]:
    files = [p for p in root.rglob("payouts.csv") if p.is_file() and "2026" in str(p)]
    if extra_csv:
        p = Path(extra_csv)
        if p.exists() and p.is_file():
            files.append(p)
    unique = list(dict.fromkeys(files))
    return unique


def load_wide_payouts(files: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for fp in files:
        try:
            df = pd.read_csv(fp, encoding="utf-8-sig", low_memory=False)
        except Exception:
            df = pd.read_csv(fp, encoding="cp932", low_memory=False)

        required = {"race_id", "bet_type", "bet_key", "payout"}
        if not required.issubset(df.columns):
            continue

        sub = df[["race_id", "bet_type", "bet_key", "payout"]].copy()
        sub["source_version"] = df["source_version"] if "source_version" in df.columns else ""
        sub["file_path"] = str(fp)
        frames.append(sub)

    if not frames:
        return pd.DataFrame(columns=["race_id", "pair_key", "payout_yen", "source_version", "file_path"])

    all_df = pd.concat(frames, ignore_index=True)
    all_df["bet_type"] = all_df["bet_type"].astype(str).str.upper().str.strip()
    wide = all_df[all_df["bet_type"] == "WIDE"].copy()
    if wide.empty:
        return pd.DataFrame(columns=["race_id", "pair_key", "payout_yen", "source_version", "file_path"])

    wide["race_id"] = wide["race_id"].astype(str).str.strip()
    wide["payout_yen"] = to_float(wide["payout"])

    def _norm_key(v: str) -> str:
        s = str(v).strip().replace(" ", "")
        parts = [p for p in s.split("-") if p != ""]
        if len(parts) != 2:
            return ""
        try:
            return normalize_pair_key(int(parts[0]), int(parts[1]))
        except Exception:
            return ""

    wide["pair_key"] = wide["bet_key"].astype(str).map(_norm_key)
    wide = wide[(wide["pair_key"] != "") & wide["payout_yen"].notna()].copy()

    wide["src_pri"] = [source_priority(str(sv), str(fp)) for sv, fp in zip(wide["source_version"], wide["file_path"])]
    wide = wide.sort_values(["race_id", "pair_key", "src_pri", "payout_yen"], ascending=[True, True, False, False])
    wide = wide.drop_duplicates(subset=["race_id", "pair_key"], keep="first").reset_index(drop=True)

    return wide[["race_id", "pair_key", "payout_yen", "source_version", "file_path"]]


def summarize_pattern(joined: pd.DataFrame, pattern: str, stake: int) -> dict:
    if pattern == "top1":
        sub = joined[joined["pair_rank_score"] == 1].copy()
    elif pattern == "top3":
        sub = joined[joined["pair_rank_score"] <= 3].copy()
    else:
        raise ValueError(pattern)

    if sub.empty:
        return {
            "pattern": pattern,
            "races": 0,
            "bets": 0,
            "stake_total": 0,
            "return_total": 0.0,
            "roi_pct": np.nan,
            "hit_rate": np.nan,
            "race_hit_rate": np.nan,
            "avg_payout_hit": np.nan,
            "matched_bets": 0,
            "matched_hits": 0,
            "unmatched_bets": 0,
            "unmatched_hits": 0,
        }

    sub["wide_hit_real"] = sub["payout_yen"].notna().astype(int)
    sub["return_yen"] = sub["payout_yen"].fillna(0.0)
    bets = int(len(sub))
    races = int(sub["race_id_raw"].nunique())
    stake_total = int(bets * stake)
    return_total = float(sub["return_yen"].sum())

    hit_mask = sub["wide_hit_real"] == 1
    matched_mask = sub["payout_yen"].notna()

    hit_rate = float(hit_mask.mean())
    race_hit_rate = float(sub.groupby("race_id_raw", dropna=False)["wide_hit_real"].max().mean())

    matched_hits = int((hit_mask & matched_mask).sum())
    unmatched_hits = int((hit_mask & ~matched_mask).sum())
    avg_payout_hit = float(sub.loc[hit_mask & matched_mask, "payout_yen"].mean()) if matched_hits else np.nan
    label_hit = (sub["wide_hit"] == 1).astype(int)
    label_real_mismatch = int((label_hit != sub["wide_hit_real"]).sum())

    return {
        "pattern": pattern,
        "races": races,
        "bets": bets,
        "stake_total": stake_total,
        "return_total": return_total,
        "roi_pct": float((return_total / stake_total) * 100.0) if stake_total else np.nan,
        "hit_rate": hit_rate,
        "race_hit_rate": race_hit_rate,
            "avg_payout_hit": avg_payout_hit,
            "matched_bets": int(matched_mask.sum()),
            "matched_hits": matched_hits,
            "unmatched_bets": int((~matched_mask).sum()),
            "unmatched_hits": unmatched_hits,
            "label_real_mismatch": label_real_mismatch,
        }


def main() -> int:
    args = parse_args()

    wide_path = Path(args.wide)
    race_path = Path(args.race)
    payout_root = Path(args.payout_root)
    out_csv = Path(args.out_csv)
    out_report = Path(args.out_report)

    wide_df = pd.read_csv(wide_path, encoding=args.encoding, low_memory=False)
    race_df = pd.read_csv(race_path, encoding=args.encoding, low_memory=False)

    for c in ["horse_no_1", "horse_no_2", "pair_rank_score", "wide_hit"]:
        wide_df[c] = to_float(wide_df[c])

    wide_df = wide_df[
        wide_df["horse_no_1"].notna()
        & wide_df["horse_no_2"].notna()
        & wide_df["pair_rank_score"].notna()
        & wide_df["wide_hit"].notna()
    ].copy()

    selected = race_df[race_df["selected_top15"] == True].copy()  # noqa: E712
    selected_ids = set(selected["race_id_raw"].astype(str))

    wide_df["race_id_raw"] = wide_df["race_id_raw"].astype(str)
    target = wide_df[wide_df["race_id_raw"].isin(selected_ids)].copy()
    if target.empty:
        raise SystemExit("No target bets after applying selected_top15 races")

    target["race_id"] = target["race_id_raw"].map(raw_to_race_id)
    target["pair_key"] = [normalize_pair_key(a, b) for a, b in zip(target["horse_no_1"], target["horse_no_2"])]

    payout_files = load_payout_files(payout_root, args.payout_csv)
    if not payout_files:
        raise SystemExit(f"No payout csv found under: {payout_root}")

    payout_df = load_wide_payouts(payout_files)
    if payout_df.empty:
        raise SystemExit("No WIDE payout rows found in payout csv files")

    joined = target.merge(payout_df, on=["race_id", "pair_key"], how="left")

    summary_rows = [
        summarize_pattern(joined, "top1", args.stake),
        summarize_pattern(joined, "top3", args.stake),
    ]
    summary = pd.DataFrame(summary_rows)

    # Add comparison with assumed simulation if available.
    assumed_path = Path(args.assumed)
    if assumed_path.exists():
        assumed = pd.read_csv(assumed_path, encoding=args.encoding, low_memory=False)
        if {"pattern", "roi_pct", "avg_payout_multiple"}.issubset(assumed.columns):
            assumed_small = assumed[["pattern", "roi_pct", "avg_payout_multiple"]].copy()
            assumed_small = assumed_small.rename(
                columns={
                    "roi_pct": "assumed_roi_pct",
                    "avg_payout_multiple": "assumed_avg_payout_multiple",
                }
            )
            summary = summary.merge(assumed_small, on="pattern", how="left")
            summary["roi_diff_vs_assumed"] = summary["roi_pct"] - summary["assumed_roi_pct"]

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_csv, index=False, encoding=args.encoding)

    # coverage stats
    overall_matched = int(joined["payout_yen"].notna().sum())
    overall_unmatched = int(joined["payout_yen"].isna().sum())

    race_dates = pd.to_datetime(selected["race_date"], errors="coerce")
    days = int(race_dates.dt.date.nunique()) if race_dates.notna().any() else 0
    races = int(len(selected))
    races_per_day = float(races / days) if days else np.nan

    top1 = summary[summary["pattern"] == "top1"].iloc[0]
    top3 = summary[summary["pattern"] == "top3"].iloc[0]

    lines: list[str] = []
    lines.append("wide real payout ROI report (2026, selected_top15)")
    lines.append("")
    lines.append(f"input_wide={wide_path}")
    lines.append(f"input_selection={race_path}")
    lines.append(f"payout_root={payout_root}")
    lines.append(f"payout_files_loaded={len(payout_files)}")
    lines.append(f"output={out_csv}")
    lines.append("")
    lines.append("join coverage")
    lines.append(f"- joined_rows={len(joined)}")
    lines.append(f"- matched_rows={overall_matched}")
    lines.append(f"- unmatched_rows={overall_unmatched}")
    lines.append("- 的中判定は payout一致（wide_hit_real）を使用")
    lines.append("- マッチしない行は不的中として払戻0円")
    lines.append("")
    lines.append("real ROI summary")
    lines.extend(summary.to_string(index=False).splitlines())
    lines.append("")
    lines.append("仮定ROIとの比較")
    if "assumed_roi_pct" in summary.columns:
        lines.append(f"- top1: real={float(top1['roi_pct']):.2f}% vs assumed={float(top1['assumed_roi_pct']):.2f}% (diff {float(top1['roi_diff_vs_assumed']):+.2f}pt)")
        lines.append(f"- top3: real={float(top3['roi_pct']):.2f}% vs assumed={float(top3['assumed_roi_pct']):.2f}% (diff {float(top3['roi_diff_vs_assumed']):+.2f}pt)")
    else:
        lines.append("- assumed ROI file がないため比較不可")
    lines.append("")
    lines.append("主要指標")
    lines.append(f"- top1 ROI={float(top1['roi_pct']):.2f}% / hit_rate={float(top1['hit_rate']):.6f} / avg_payout={float(top1['avg_payout_hit']):.2f}円")
    lines.append(f"- top3 ROI={float(top3['roi_pct']):.2f}% / hit_rate={float(top3['hit_rate']):.6f} / avg_payout={float(top3['avg_payout_hit']):.2f}円")
    lines.append(f"- 《推測》1日あたり購入レース数={races_per_day:.2f} ({races} races / {days} days)")
    lines.append(f"- 1レース期待値(top1)={(float(top1['return_total'])-float(top1['stake_total']))/max(1,int(top1['races'])):.2f}円")
    lines.append(f"- 1レース期待値(top3)={(float(top3['return_total'])-float(top3['stake_total']))/max(1,int(top3['races'])):.2f}円")
    lines.append("")
    lines.append("結論")
    lines.append(f"- top1 ROI>100%: {'YES' if float(top1['roi_pct']) > 100 else 'NO'}")
    lines.append(f"- top3 ROI>100%: {'YES' if float(top3['roi_pct']) > 100 else 'NO'}")

    out_report.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"output_csv: {out_csv}")
    print(f"output_report: {out_report}")
    print(f"payout_files_loaded={len(payout_files)}")
    print("\n=== real summary ===")
    print(summary.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
