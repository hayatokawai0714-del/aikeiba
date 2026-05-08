from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pandas as pd


def _fmt_ratio(n: int, d: int) -> float | None:
    return None if d == 0 else float(n / d)


def build_report(*, in_path: Path, out_md: Path) -> dict:
    now = dt.datetime.now().isoformat(timespec="seconds")
    lines: list[str] = ["# pair_learning_base_report", "", f"- 実行日時: {now}", f"- 入力: {in_path}", ""]

    if not in_path.exists():
        lines.append("## Warning")
        lines.append(f"- 入力ファイルが存在しません: {in_path}")
        out_md.parent.mkdir(parents=True, exist_ok=True)
        out_md.write_text("\n".join(lines), encoding="utf-8")
        return {"rows": 0, "hit_rate": None}

    df = pd.read_parquet(in_path)
    rows = int(len(df))
    hit_count = int(df["actual_wide_hit"].sum()) if rows > 0 and "actual_wide_hit" in df.columns else 0
    hit_rate = _fmt_ratio(hit_count, rows)
    payout_missing = int(df["wide_payout"].isna().sum()) if rows > 0 and "wide_payout" in df.columns else 0
    race_count = int(df["race_id"].nunique()) if rows > 0 and "race_id" in df.columns else 0

    lines.append("## 基本統計")
    lines.append(f"- 行数: {rows}")
    lines.append(f"- actual_wide_hit 件数: {hit_count}")
    lines.append(f"- hit_rate: {hit_rate}")
    lines.append(f"- wide_payout 欠損件数: {payout_missing}")
    lines.append(f"- race_id 数: {race_count}")

    lines.append("")
    lines.append("## model_version別件数")
    if rows == 0 or "model_version" not in df.columns:
        lines.append("- データなし")
    else:
        vc = df["model_version"].fillna("NULL").astype(str).value_counts()
        for k, v in vc.items():
            lines.append(f"- {k}: {int(v)}")

    lines.append("")
    lines.append("## pair_rank_in_race別 hit_rate")
    if rows == 0 or "pair_rank_in_race" not in df.columns:
        lines.append("- データなし")
    else:
        g = df.groupby("pair_rank_in_race", dropna=False).agg(cnt=("actual_wide_hit", "size"), hit=("actual_wide_hit", "sum")).reset_index()
        g["hit_rate"] = g.apply(lambda r: _fmt_ratio(int(r["hit"]), int(r["cnt"])), axis=1)
        for r in g.sort_values("pair_rank_in_race").itertuples(index=False):
            lines.append(f"- rank={r.pair_rank_in_race}: cnt={int(r.cnt)} hit={int(r.hit)} hit_rate={r.hit_rate}")

    lines.append("")
    lines.append("## pair_value_score 分位点")
    if rows == 0 or "pair_value_score" not in df.columns:
        lines.append("- データなし")
    else:
        q = df["pair_value_score"].quantile([0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0])
        for idx, val in q.items():
            lines.append(f"- q{idx:.2f}: {val}")

    lines.append("")
    lines.append("## 主要特徴量 欠損率")
    key_cols = [
        "pair_prob_naive",
        "pair_value_score",
        "pair_ai_market_gap_sum",
        "pair_ai_market_gap_max",
        "pair_ai_market_gap_min",
        "pair_fused_prob_sum",
        "pair_fused_prob_min",
        "pair_rank_in_race",
        "field_size",
        "venue",
        "surface",
        "distance",
        "pair_value_score_rank_pct",
        "pair_value_score_z_in_race",
        "pair_prob_naive_rank_pct",
        "pair_prob_naive_z_in_race",
        "pair_rank_bucket",
        "field_size_bucket",
        "distance_bucket",
    ]
    for c in key_cols:
        if c not in df.columns:
            lines.append(f"- {c}: missing_column")
            continue
        miss = int(df[c].isna().sum())
        rate = _fmt_ratio(miss, rows)
        lines.append(f"- {c}: {miss}/{rows} ({rate})")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")
    return {
        "rows": rows,
        "hit_count": hit_count,
        "hit_rate": hit_rate,
        "wide_payout_missing": payout_missing,
        "race_count": race_count,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-path", default="racing_ai/data/modeling/pair_learning_base.parquet")
    ap.add_argument("--out-md", default="racing_ai/reports/pair_learning_base_report.md")
    args = ap.parse_args()
    s = build_report(in_path=Path(args.in_path), out_md=Path(args.out_md))
    print(s)


if __name__ == "__main__":
    main()
