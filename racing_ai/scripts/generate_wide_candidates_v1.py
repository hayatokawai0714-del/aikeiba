import argparse
from itertools import combinations
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Generate temporary wide candidates from value score table.")
    ap.add_argument("--input", default=r"C:\TXT\top3_value_score_2026_v1.csv")
    ap.add_argument("--output", default=r"C:\TXT\wide_candidates_2026_v1.csv")
    ap.add_argument("--report", default=r"C:\TXT\wide_candidates_report_2026_v1.txt")
    ap.add_argument("--encoding", default="cp932")
    return ap.parse_args()


def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def build_pairs(cand_df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    if len(cand_df) < 2:
        return rows

    records = cand_df.to_dict("records")
    for left, right in combinations(records, 2):
        ordered = sorted(
            [left, right],
            key=lambda r: (-float(r["pred_top3_raw"]), float(r.get("horse_no_num", 9999))),
        )
        first, second = ordered[0], ordered[1]

        pred1 = float(first["pred_top3_raw"])
        pred2 = float(second["pred_top3_raw"])
        val1 = float(first["value_score_v1"])
        val2 = float(second["value_score_v1"])
        top3_1 = int(first["top3"])
        top3_2 = int(second["top3"])

        rows.append(
            {
                "race_date": first.get("race_date", second.get("race_date")),
                "race_id_raw": first["race_id_raw"],
                "horse_no_1": int(first["horse_no_num"]),
                "horse_no_2": int(second["horse_no_num"]),
                "horse_name_1": first.get("horse_name", ""),
                "horse_name_2": second.get("horse_name", ""),
                "pred_top3_1": pred1,
                "pred_top3_2": pred2,
                "value_score_1": val1,
                "value_score_2": val2,
                "pair_score_v1": pred1 * pred2,
                "pair_value_score_v1": val1 + val2,
                "top3_1": top3_1,
                "top3_2": top3_2,
                "wide_hit": int(top3_1 == 1 and top3_2 == 1),
                "candidate_horse_count": int(len(cand_df)),
            }
        )

    return rows


def main() -> int:
    args = parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    report_path = Path(args.report)

    df = pd.read_csv(in_path, encoding=args.encoding, low_memory=False)

    required_cols = [
        "race_id_raw",
        "horse_no",
        "pred_top3_raw",
        "value_score_v1",
        "value_score_rank",
        "top3",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing required columns: {missing}")

    df["pred_top3_raw"] = to_float(df["pred_top3_raw"])
    df["value_score_v1"] = to_float(df["value_score_v1"])
    df["value_score_rank"] = to_float(df["value_score_rank"])
    df["top3"] = to_float(df["top3"]).fillna(0).astype(int)
    df["horse_no_num"] = to_float(df["horse_no"])

    before = len(df)
    df = df[
        df["pred_top3_raw"].notna()
        & df["value_score_v1"].notna()
        & df["value_score_rank"].notna()
        & df["horse_no_num"].notna()
    ].copy()
    dropped = before - len(df)

    if df.empty:
        raise SystemExit("No rows after filtering required numeric fields")

    df["pred_top3_rank"] = (
        df.groupby("race_id_raw", dropna=False)["pred_top3_raw"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    df["is_candidate"] = (df["value_score_rank"] <= 3) | (df["pred_top3_rank"] <= 3)
    cand_df = df[df["is_candidate"]].copy()

    pair_rows: list[dict] = []
    for _, race_cands in cand_df.groupby("race_id_raw", dropna=False):
        pair_rows.extend(build_pairs(race_cands))

    if not pair_rows:
        raise SystemExit("No pair generated. Candidate horses per race were insufficient.")

    pairs = pd.DataFrame(pair_rows)

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

    # 《推測》上位3抽出は pair_score と pair_value の和集合を採用。
    selected = pairs[(pairs["pair_rank_score"] <= 3) | (pairs["pair_rank_value"] <= 3)].copy()

    selected = selected.sort_values(["race_date", "race_id_raw", "pair_rank_score", "pair_rank_value"]).reset_index(drop=True)

    pair_rank_hit = (
        selected.groupby("pair_rank_score", dropna=False)
        .agg(count=("wide_hit", "size"), hit_rate=("wide_hit", "mean"))
        .reset_index()
        .sort_values("pair_rank_score")
    )

    race_count = int(selected["race_id_raw"].nunique())
    total_pairs = int(len(selected))
    overall_hit_rate = float(selected["wide_hit"].mean())

    top1 = selected[selected["pair_rank_score"] == 1].copy()
    top1_hit_rate = float(top1["wide_hit"].mean()) if len(top1) else float("nan")

    top3_any = (
        selected[selected["pair_rank_score"] <= 3]
        .groupby("race_id_raw", dropna=False)["wide_hit"]
        .max()
    )
    top3_any_hit_rate = float(top3_any.mean()) if len(top3_any) else float("nan")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    selected.to_csv(out_path, index=False, encoding=args.encoding)

    lines: list[str] = []
    lines.append("wide candidate generation report (2026)")
    lines.append("")
    lines.append(f"input={in_path}")
    lines.append(f"output={out_path}")
    lines.append(f"rows_input={before}")
    lines.append(f"rows_used={len(df)}")
    lines.append(f"dropped_rows={dropped}")
    lines.append("")
    lines.append("candidate rule")
    lines.append("- value_score_rank <= 3 OR pred_top3_raw race rank <= 3")
    lines.append("- from candidates, generate all 2-horse pairs per race")
    lines.append("- wide_hit = 1 if both horses top3 == 1")
    lines.append("")
    lines.append("pair features")
    lines.append("- pair_score_v1 = pred_top3_1 * pred_top3_2")
    lines.append("- pair_value_score_v1 = value_score_1 + value_score_2")
    lines.append("")
    lines.append("selection rule")
    lines.append("《推測》raceごとに pair_rank_score<=3 または pair_rank_value<=3 の和集合を採用")
    lines.append("")
    lines.append("overall")
    lines.append(f"- total_pairs={total_pairs}")
    lines.append(f"- races={race_count}")
    lines.append(f"- wide_hit_rate={overall_hit_rate:.6f}")
    lines.append(f"- top1_per_race_hit_rate={top1_hit_rate:.6f}")
    lines.append(f"- top3_per_race_any_hit_rate={top3_any_hit_rate:.6f}")
    lines.append("")
    lines.append("pair_rank別 hit率 (pair_rank_score)")
    lines.extend(pair_rank_hit.to_string(index=False).splitlines())

    report_path.write_text("\n".join(lines) + "\n", encoding=args.encoding)

    print("=== done ===")
    print(f"input: {in_path}")
    print(f"output: {out_path}")
    print(f"report: {report_path}")
    print("")
    print("=== overall ===")
    print(f"total_pairs={total_pairs}")
    print(f"races={race_count}")
    print(f"wide_hit_rate={overall_hit_rate:.6f}")
    print(f"top1_per_race_hit_rate={top1_hit_rate:.6f}")
    print(f"top3_per_race_any_hit_rate={top3_any_hit_rate:.6f}")
    print("")
    print("=== pair_rank hit ===")
    print(pair_rank_hit.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
