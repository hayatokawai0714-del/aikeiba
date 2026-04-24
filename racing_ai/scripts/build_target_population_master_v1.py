from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


HEADER_52_V1 = [
    "year_yy",  # 1
    "month",  # 2
    "day",  # 3
    "kaiji",  # 4 (回次)
    "venue_name",  # 5 (場所名)
    "nichiji",  # 6 (日次)
    "race_no",  # 7
    "race_name",  # 8
    "class_code",  # 9
    "surface",  # 10 (芝/ダ/障)
    "surface_subcode",  # 11 (unknown, often 0)
    "distance",  # 12
    "track_condition",  # 13 (良/稍重/重/不良)
    "horse_name",  # 14
    "sex",  # 15
    "age",  # 16
    "jockey_name",  # 17
    "weight_carried",  # 18
    "field_size",  # 19
    "horse_no",  # 20 (馬番)
    "finish_position",  # 21 (確定着順)
    "finish_position_pop",  # 22 (人気着順)
    "abnormal_code",  # 23 (0/1/3/4)
    "margin_time",  # 24 (着差タイム)
    "pop_rank",  # 25 (人気)
    "win_odds",  # 26 (単勝オッズ)
    "race_time",  # 27 (走破タイム)
    "time_s",  # 28 (タイムS, may be blank)
    "adjusted_time",  # 29 (補正タイム)
    "corner_pos_1",  # 30
    "corner_pos_2",  # 31
    "corner_pos_3",  # 32
    "last3f_time",  # 33 (上がり3F)
    "body_weight",  # 34
    "trainer_name",  # 35
    "trainer_affiliation",  # 36 (栗/美 etc)
    "prize_money",  # 37
    "horse_id",  # 38 (血統登録番号)
    "jockey_code",  # 39
    "trainer_code",  # 40
    "entry_id_18",  # 41 (馬レースID(新))
    "owner_name",  # 42
    "breeder_name",  # 43
    "sire_name",  # 44
    "dam_name",  # 45
    "damsire_name",  # 46
    "coat_color",  # 47
    "birth_date",  # 48 (YYYYMMDD)
    "pci",  # 49
    "extra_50",  # 50
    "extra_51",  # 51
    "extra_52",  # 52
]


@dataclass(frozen=True)
class Summary:
    file: str
    rows: int
    races: int
    entry_dupes: int


def _read_noheader_52(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, header=None, encoding="cp932", dtype=str, keep_default_na=False)
    if df.shape[1] != 52:
        raise ValueError(f"{path}: expected 52 cols, got {df.shape[1]}")
    df.columns = HEADER_52_V1
    return df


def _add_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    entry = out["entry_id_18"].astype(str).str.strip()
    out["race_id_raw"] = entry.str.slice(0, 16)

    year = pd.to_numeric(out["year_yy"], errors="coerce").astype("Int64")
    # TARGET seems to output 2-digit year like 25 for 2025.
    year_full = (2000 + year).astype("Int64")
    month = pd.to_numeric(out["month"], errors="coerce").astype("Int64")
    day = pd.to_numeric(out["day"], errors="coerce").astype("Int64")
    out["race_date"] = (
        year_full.astype(str).str.zfill(4)
        + "-"
        + month.astype(str).str.zfill(2)
        + "-"
        + day.astype(str).str.zfill(2)
    )
    # Convenience aliases for downstream (keep original columns too).
    out["target_finish"] = out["finish_position"]
    return out


def _summarize(df: pd.DataFrame, file: str) -> Summary:
    entry = df["entry_id_18"].astype(str).str.strip()
    dupes = int(entry.duplicated().sum())
    races = int(df["race_id_raw"].nunique(dropna=False))
    return Summary(file=file, rows=int(len(df)), races=races, entry_dupes=dupes)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build TARGET 2025-01..05 population master v1 with fixed header mapping.")
    ap.add_argument("--out-root", default=r"C:\TXT", help="Output directory (default: C:\\TXT)")
    ap.add_argument("--out-master", default=r"C:\TXT\population_master_2025_01_05_v1.csv")
    ap.add_argument(
        "--filter-by-file-month",
        action="store_true",
        default=True,
        help="Filter each input CSV to the month indicated by its filename (e.g. 2025_01_* => month==1).",
    )
    ap.add_argument(
        "--no-filter-by-file-month",
        dest="filter_by_file_month",
        action="store_false",
        help="Disable filename-based month/year filtering (useful when inputs are cumulative and you will dedupe).",
    )
    ap.add_argument("files", nargs="+")
    args = ap.parse_args()

    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    dfs = []
    sums = []
    for f in args.files:
        p = Path(f)
        df = _read_noheader_52(p)
        df = _add_keys(df)

        if args.filter_by_file_month:
            stem = p.stem
            yyyy = None
            mm = None
            for y in range(2000, 2100):
                if stem.startswith(f"{y}_") or stem.startswith(f"{y}-") or f"{y}_" in stem:
                    yyyy = y
                    break
            for m in range(1, 13):
                token = f"_{m:02d}_"
                if token in stem:
                    mm = m
                    break
            if mm is not None:
                mon = pd.to_numeric(df["month"], errors="coerce").astype("Int64")
                before = len(df)
                df2 = df[mon == mm].copy()
                if yyyy is not None:
                    yy = int(str(yyyy)[-2:])
                    year_yy = pd.to_numeric(df2["year_yy"], errors="coerce").astype("Int64")
                    before2 = len(df2)
                    df2 = df2[year_yy == yy].copy()
                    print(f"[filter] {p.name}: month=={mm} kept {before2}/{before}; year=={yy} kept {len(df2)}/{before2}")
                else:
                    print(f"[filter] {p.name}: month=={mm} kept {len(df2)}/{before}")
                df = df2
            else:
                print(f"[filter] {p.name}: month token not found; no filtering applied")

        sums.append(_summarize(df, str(p)))

        out_with_header = out_root / (p.stem + ".with_header.csv")
        df.to_csv(out_with_header, index=False, encoding="cp932")
        print(f"[with_header] wrote {out_with_header}")

        dfs.append(df)

    master = pd.concat(dfs, ignore_index=True)
    # basic checks
    n_rows = int(len(master))
    n_races = int(master["race_id_raw"].nunique(dropna=False))
    entry = master["entry_id_18"].astype(str).str.strip()
    entry_dupes_all = int(entry.duplicated().sum())
    if entry_dupes_all > 0:
        # Keep first occurrence; later files may include overlapping ranges.
        master = master.loc[~entry.duplicated()].copy()
        print(f"[dedupe] dropped duplicates by entry_id_18: {entry_dupes_all}")
        n_rows = int(len(master))
        n_races = int(master["race_id_raw"].nunique(dropna=False))
        entry_dupes_all = int(master["entry_id_18"].astype(str).str.strip().duplicated().sum())

    # missingness for main cols
    def miss_pct(col: str) -> float:
        s = master[col].astype(str).str.strip()
        return float((s == "").mean() * 100.0)

    miss = {
        "race_date": miss_pct("race_date"),
        "venue_name": miss_pct("venue_name"),
        "race_no": miss_pct("race_no"),
        "horse_no": miss_pct("horse_no"),
        "horse_name": miss_pct("horse_name"),
        "jockey_name": miss_pct("jockey_name"),
        "weight_carried": miss_pct("weight_carried"),
        "field_size": miss_pct("field_size"),
        "finish_position": miss_pct("finish_position"),
        "pop_rank": miss_pct("pop_rank"),
        "win_odds": miss_pct("win_odds"),
        "distance": miss_pct("distance"),
        "track_condition": miss_pct("track_condition"),
        "abnormal_code": miss_pct("abnormal_code"),
        "entry_id_18": miss_pct("entry_id_18"),
    }

    ab_counts = master["abnormal_code"].astype(str).str.strip().value_counts(dropna=False).to_dict()

    out_master = Path(args.out_master)
    out_master.parent.mkdir(parents=True, exist_ok=True)
    master.to_csv(out_master, index=False, encoding="cp932")
    print(f"[master] wrote {out_master}")

    print("\n=== summary per file ===")
    for s in sums:
        print(f"- {s.file}: rows={s.rows} races={s.races} entry_dupes={s.entry_dupes}")

    print("\n=== merged checks ===")
    print(f"rows={n_rows} races={n_races} entry_dupes={entry_dupes_all}")
    print("missing_pct:")
    for k, v in miss.items():
        print(f"  - {k}: {v:.3f}%")
    print("abnormal_code_counts:")
    for k, v in list(ab_counts.items())[:20]:
        print(f"  - {k}: {v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
