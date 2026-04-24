import argparse
import csv
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from build_target_population_master_v1 import HEADER_52_V1, _add_keys, _read_noheader_52


TXT_DIR_DEFAULT = r"C:\TXT"


@dataclass
class FileCheck:
    file: str
    ok_cp932: bool
    ok_52_cols_sample: bool
    sample_rows: int
    year_from_name: int | None
    year_yy_min: int | None
    year_yy_max: int | None
    race_date_min: str | None
    race_date_max: str | None


@dataclass
class YearSummary:
    year: int
    files: int
    rows_raw: int
    rows_year_filtered: int
    rows_deduped: int
    races: int
    entry_id_dupes_raw: int
    entry_id_dupes_after: int
    race_horse_dupes_after: int
    date_min: str | None
    date_max: str | None


def _iter_target_kaisai_csvs(txt_dir: Path) -> list[Path]:
    # Heuristic: include "kaisai" and ".csv", exclude generated outputs.
    # We rely on leading 4-digit year in filename as requested.
    candidates: list[Path] = []
    for p in txt_dir.glob("*.csv"):
        name_lower = p.name.lower()
        if "kaisai" not in name_lower:
            continue
        # Exclude non-source/derived exports.
        if "seiseki" in name_lower:
            continue
        if "enriched" in name_lower:
            continue
        if re.search(r"\bq[1-4]\b", name_lower):
            continue
        if name_lower.endswith(".with_header.csv"):
            continue
        if name_lower.startswith("population_") or name_lower.startswith("populationmaster_") or name_lower.startswith(
            "population_master_"
        ):
            continue
        # Keep only known source naming patterns.
        if ("_all_kaisai" not in name_lower) and (re.search(r"_\d{2}_kaisai\.csv$", name_lower) is None):
            continue
        candidates.append(p)
    return sorted(candidates)


def _year_from_filename(p: Path) -> int | None:
    m = re.match(r"^(20\d{2})", p.name)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _kaisai_no_from_filename(p: Path) -> str | None:
    # Try to extract something like "_01_" (kaisai number). If it doesn't exist, return None.
    m = re.search(r"_(\d{2})_", p.stem)
    return m.group(1) if m else None


def _sample_52_cols_cp932(p: Path, max_rows: int = 50) -> tuple[bool, int, bool]:
    """
    Returns (ok_cp932, sample_rows, ok_52_cols_sample).
    """
    rows_read = 0
    try:
        with p.open("r", encoding="cp932", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                rows_read += 1
                if len(row) != 52:
                    return True, rows_read, False
                if rows_read >= max_rows:
                    break
        return True, rows_read, True
    except UnicodeDecodeError:
        return False, rows_read, False


def _file_check(p: Path, sample_rows: int = 50) -> FileCheck:
    year_from_name = _year_from_filename(p)
    ok_cp932, rows_read, ok_52 = _sample_52_cols_cp932(p, max_rows=sample_rows)

    year_yy_min = None
    year_yy_max = None
    race_date_min = None
    race_date_max = None
    if ok_cp932 and ok_52:
        # Light read for min/max without full load: pandas on two columns still parses all.
        # Keep it simple: read full once in processing stage; here we just sample 5000 rows
        # for sanity of content-year mismatch.
        try:
            df = pd.read_csv(
                p,
                header=None,
                names=HEADER_52_V1,
                dtype=str,
                encoding="cp932",
                nrows=5000,
            )
            df = _add_keys(df)
            year_yy = pd.to_numeric(df["year_yy"], errors="coerce")
            year_yy_min = int(year_yy.min()) if year_yy.notna().any() else None
            year_yy_max = int(year_yy.max()) if year_yy.notna().any() else None
            rd = df["race_date"].astype(str)
            race_date_min = rd.min() if len(rd) else None
            race_date_max = rd.max() if len(rd) else None
        except Exception:
            pass

    return FileCheck(
        file=str(p),
        ok_cp932=ok_cp932,
        ok_52_cols_sample=ok_52,
        sample_rows=rows_read,
        year_from_name=year_from_name,
        year_yy_min=year_yy_min,
        year_yy_max=year_yy_max,
        race_date_min=race_date_min,
        race_date_max=race_date_max,
    )


def _write_csv(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="cp932")


def _process_year(year: int, files: list[Path], out_dir: Path, write_intermediate: bool) -> YearSummary:
    dfs: list[pd.DataFrame] = []
    for p in files:
        df = _read_noheader_52(p)
        df = _add_keys(df)
        df["source_file"] = p.name
        df["source_kaisai_no"] = _kaisai_no_from_filename(p)
        dfs.append(df)

    raw = pd.concat(dfs, ignore_index=True)
    entry = raw["entry_id_18"].astype(str).str.strip()
    entry_dupes_raw = int(entry.duplicated().sum())

    if write_intermediate:
        _write_csv(raw, out_dir / f"population_master_{year}_raw.csv")

    # Year filter is mandatory.
    raw["race_year"] = raw["race_date"].astype(str).str.slice(0, 4)
    yf = raw[raw["race_year"] == str(year)].copy()

    # Dedupe by entry_id_18 (primary key)
    before = len(yf)
    yf = yf.drop_duplicates(subset=["entry_id_18"], keep="first").copy()
    rows_deduped = len(yf)

    entry_dupes_after = int(yf["entry_id_18"].astype(str).str.strip().duplicated().sum())
    races = int(yf["race_id_raw"].nunique(dropna=False))

    # Secondary dedupe check
    race_horse_dupes_after = int(
        yf[["race_id_raw", "horse_no"]].astype(str).agg("|".join, axis=1).duplicated().sum()
    )

    date_min = yf["race_date"].astype(str).min() if len(yf) else None
    date_max = yf["race_date"].astype(str).max() if len(yf) else None

    out_final = out_dir / f"population_master_{year}_v1.csv"
    _write_csv(yf, out_final)

    return YearSummary(
        year=year,
        files=len(files),
        rows_raw=int(len(raw)),
        rows_year_filtered=int(before),
        rows_deduped=int(rows_deduped),
        races=races,
        entry_id_dupes_raw=entry_dupes_raw,
        entry_id_dupes_after=entry_dupes_after,
        race_horse_dupes_after=race_horse_dupes_after,
        date_min=date_min,
        date_max=date_max,
    )


def _summarize_years(all_year_dfs: Iterable[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(all_year_dfs, ignore_index=True)
    entry_dupes = int(df["entry_id_18"].astype(str).str.strip().duplicated().sum())
    race_horse_dupes = int(df[["race_id_raw", "horse_no"]].astype(str).agg("|".join, axis=1).duplicated().sum())
    return pd.DataFrame(
        [
            {
                "rows": int(len(df)),
                "races": int(df["race_id_raw"].nunique(dropna=False)),
                "entry_id_dupes": entry_dupes,
                "race_horse_dupes": race_horse_dupes,
                "date_min": df["race_date"].astype(str).min() if len(df) else None,
                "date_max": df["race_date"].astype(str).max() if len(df) else None,
                "years": ",".join(sorted(set(df["race_date"].astype(str).str.slice(0, 4).unique()))),
            }
        ]
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Build TARGET kaisai-seiseki population masters for 2021-2026.")
    ap.add_argument("--txt-dir", default=TXT_DIR_DEFAULT)
    ap.add_argument("--out-dir", default=TXT_DIR_DEFAULT)
    ap.add_argument("--years", default="2021-2026", help="Year range like 2021-2026")
    ap.add_argument("--write-intermediate", action="store_true", default=True)
    ap.add_argument(
        "--no-write-intermediate",
        dest="write_intermediate",
        action="store_false",
        help="Do not write per-year *_raw.csv intermediates.",
    )
    ap.add_argument("--sample-check-rows", type=int, default=50)
    args = ap.parse_args()

    m = re.match(r"^(20\d{2})-(20\d{2})$", args.years)
    if not m:
        raise SystemExit("--years must be like 2021-2026")
    y0, y1 = int(m.group(1)), int(m.group(2))

    txt_dir = Path(args.txt_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = _iter_target_kaisai_csvs(txt_dir)
    by_year: dict[int, list[Path]] = {}
    for p in files:
        y = _year_from_filename(p)
        if y is None:
            continue
        if y < y0 or y > y1:
            continue
        by_year.setdefault(y, []).append(p)

    print("=== discovered files ===")
    for y in range(y0, y1 + 1):
        fs = by_year.get(y, [])
        kaisai = sorted({k for k in (_kaisai_no_from_filename(p) for p in fs) if k is not None})
        print(f"- {y}: files={len(fs)} kaisai={','.join(kaisai) if kaisai else '(unknown)'}")

    print("\n=== basic checks (sample) ===")
    checks: list[FileCheck] = []
    for y in range(y0, y1 + 1):
        for p in sorted(by_year.get(y, [])):
            ck = _file_check(p, sample_rows=args.sample_check_rows)
            checks.append(ck)
            flag = "OK" if (ck.ok_cp932 and ck.ok_52_cols_sample) else "NG"
            mismatch = ""
            if ck.year_from_name is not None and ck.year_yy_min is not None and ck.year_yy_max is not None:
                yy = int(str(ck.year_from_name)[-2:])
                if ck.year_yy_min != yy or ck.year_yy_max != yy:
                    mismatch = f" MISMATCH(year_yy {ck.year_yy_min}-{ck.year_yy_max} vs name {yy})"
            print(f"[{flag}] {p.name}{mismatch}")

    # Persist checks
    checks_df = pd.DataFrame([asdict(c) for c in checks])
    checks_df.to_csv(out_dir / f"target_kaisai_file_checks_{y0}_{y1}.csv", index=False, encoding="utf-8")

    print("\n=== build per-year masters ===")
    summaries: list[YearSummary] = []
    year_frames: list[pd.DataFrame] = []
    for y in range(y0, y1 + 1):
        fs = by_year.get(y, [])
        if not fs:
            print(f"[skip] {y}: no files found")
            continue
        summ = _process_year(y, fs, out_dir, write_intermediate=args.write_intermediate)
        summaries.append(summ)
        df_y = pd.read_csv(out_dir / f"population_master_{y}_v1.csv", dtype=str, encoding="cp932")
        year_frames.append(df_y)
        print(
            f"[year {y}] raw_rows={summ.rows_raw} year_rows={summ.rows_year_filtered} "
            f"deduped_rows={summ.rows_deduped} races={summ.races} "
            f"entry_dupes_raw={summ.entry_id_dupes_raw} entry_dupes_after={summ.entry_id_dupes_after} "
            f"race_horse_dupes_after={summ.race_horse_dupes_after} "
            f"date={summ.date_min}..{summ.date_max}"
        )

    summ_df = pd.DataFrame([asdict(s) for s in summaries])
    summ_df.to_csv(out_dir / f"target_population_year_summaries_{y0}_{y1}.csv", index=False, encoding="utf-8")

    print("\n=== build final union ===")
    if year_frames:
        all_df = pd.concat(year_frames, ignore_index=True)
        out_all = out_dir / f"population_master_{y0}_{y1}_v1.csv"
        _write_csv(all_df, out_all)
        overall = _summarize_years(year_frames)
        overall.to_csv(out_dir / f"target_population_overall_{y0}_{y1}.csv", index=False, encoding="utf-8")
        print(f"[all] wrote {out_all}")
        print(overall.to_string(index=False))

    print("\n[done]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
