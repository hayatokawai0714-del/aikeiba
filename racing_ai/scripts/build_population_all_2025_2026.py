from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


COLS = [
    "race_id_norm",
    "race_date",
    "horse_no",
    "馬名",
    "騎手",
    "斤量",
    "頭数",
    "人気",
    "単勝オッズ",
    "target_finish",
    "距離",
    "馬場状態",
]


def summarize(df: pd.DataFrame, label: str) -> None:
    print(f"\n[{label}] rows={len(df)} races={df['race_id_norm'].nunique(dropna=False)}")
    if "race_date" in df.columns:
        ds = pd.to_datetime(df["race_date"], errors="coerce")
        if ds.notna().any():
            print(f"[{label}] date_min={ds.min().date()} date_max={ds.max().date()}")
    miss = df[COLS].isna().mean().mul(100).round(3)
    print(f"[{label}] missing_pct:")
    print(miss.to_string())


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Union TARGET 2025Q1 population_norm with JV 2026Q1 extracted from DuckDB."
    )
    ap.add_argument("--csv-2025", required=True, help=r"e.g. C:\TXT\2025Q1_population_norm.csv")
    ap.add_argument("--duckdb", required=True, help=r"e.g. data\warehouse\aikeiba.duckdb")
    ap.add_argument("--out", required=True, help=r"e.g. C:\TXT\population_all_2025_2026.csv")
    ap.add_argument("--encoding", default="cp932", help="Input/output encoding for 2025 csv and output csv")
    args = ap.parse_args()

    p2025 = Path(args.csv_2025)
    if not p2025.exists():
        raise FileNotFoundError(str(p2025))

    df2025 = pd.read_csv(p2025, encoding=args.encoding, dtype=str)
    for c in COLS:
        if c not in df2025.columns:
            raise RuntimeError(f"2025 csv missing column: {c}")
    df2025 = df2025[COLS].copy()

    con = duckdb.connect(str(Path(args.duckdb)))

    # 2026Q1 extraction: prefer feature_store for race_date/distance/field_size, but take
    # jockey_name_raw / pop_rank from warehouse entries (post-fix) when present.
    df2026 = con.execute(
        """
        with base as (
          select
            fs.race_id as race_id_norm,
            cast(fs.race_date as varchar) as race_date,
            e.horse_no as horse_no,
            e.horse_name as 馬名,
            e.jockey_name_raw as 騎手,
            e.weight_carried as 斤量,
            fs.field_size as 頭数,
            e.pop_rank as 人気,
            x.odds_win_final as 単勝オッズ,
            x.finish_position as target_finish,
            fs.distance as 距離,
            r.track_condition as 馬場状態
          from feature_store fs
          join entries e
            on fs.race_id = e.race_id
           and fs.horse_no = e.horse_no
          left join results x
            on fs.race_id = x.race_id
           and fs.horse_no = x.horse_no
          left join races r
            on fs.race_id = r.race_id
          where fs.race_date between '2026-01-01' and '2026-03-31'
            and fs.feature_snapshot_version = 'fs_v1'
        )
        select * from base
        """
    ).fetchdf()

    # Cast numeric-ish columns (keep pandas nullable types)
    df2026["horse_no"] = pd.to_numeric(df2026["horse_no"], errors="coerce").astype("Int64")
    df2026["斤量"] = pd.to_numeric(df2026["斤量"], errors="coerce")
    df2026["頭数"] = pd.to_numeric(df2026["頭数"], errors="coerce").astype("Int64")
    df2026["人気"] = pd.to_numeric(df2026["人気"], errors="coerce").astype("Int64")
    df2026["単勝オッズ"] = pd.to_numeric(df2026["単勝オッズ"], errors="coerce")
    df2026["target_finish"] = pd.to_numeric(df2026["target_finish"], errors="coerce").astype("Int64")
    df2026["距離"] = pd.to_numeric(df2026["距離"], errors="coerce").astype("Int64")

    df2026 = df2026[COLS].copy()
    out = pd.concat([df2025, df2026], ignore_index=True)

    years = pd.to_datetime(out["race_date"], errors="coerce").dt.year
    year_counts = years.value_counts(dropna=False).sort_index()

    summarize(df2025, "2025")
    summarize(df2026, "2026")
    summarize(out, "all")
    print("\n[all] year_counts:")
    print(year_counts.to_string())

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding=args.encoding)
    print("\n[all] wrote =", str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

