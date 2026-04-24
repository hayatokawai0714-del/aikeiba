import argparse
import csv
import glob
import os
from dataclasses import dataclass
from typing import Iterable, Optional


try:
    import duckdb  # type: ignore
except Exception:  # pragma: no cover
    duckdb = None

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None


TARGET_FIELDS = {
    "jockey": ["jockey", "騎手", "kishu"],
    "weight_carried": ["weight", "斤量", "futan"],
    "pop_rank": ["pop", "人気", "ninki", "popularity"],
    "track_condition": ["track_condition", "馬場", "baba"],
}


@dataclass
class FileAudit:
    path: str
    exists: bool
    n_rows: int
    columns: list[str]
    non_null: dict[str, int]


def _read_csv_header_and_rows(path: str, encoding: str) -> tuple[list[str], int]:
    with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        n = 0
        for _ in reader:
            n += 1
        return header, n


def _load_df(path: str) -> "pd.DataFrame":
    if pd is None:
        raise RuntimeError("pandas is required to run this script")
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str)
        except Exception:
            continue
    return pd.read_csv(path, encoding="utf-8", dtype=str, errors="replace")  # type: ignore[arg-type]


def audit_csv(path: str) -> FileAudit:
    if not os.path.exists(path):
        return FileAudit(path=path, exists=False, n_rows=0, columns=[], non_null={})

    # Cheap first pass: header + row count (works even if encoding is odd).
    header, n_rows = _read_csv_header_and_rows(path, encoding="utf-8-sig")

    non_null: dict[str, int] = {}
    try:
        df = _load_df(path)
        header = list(df.columns)
        for col in header:
            if df[col].notna().any():
                non_null[col] = int(df[col].notna().sum())
    except Exception:
        pass

    return FileAudit(path=path, exists=True, n_rows=n_rows, columns=header, non_null=non_null)


def _find_candidate_columns(columns: Iterable[str], keywords: list[str]) -> list[str]:
    out = []
    for c in columns:
        lc = c.lower()
        if any(k.lower() in lc for k in keywords):
            out.append(c)
    return out


def _print_stage(stage: str, audits: list[FileAudit], fields: dict[str, list[str]]) -> None:
    print(f"\n== {stage} ==")
    for a in audits:
        base = os.path.basename(a.path)
        if not a.exists:
            print(f"- {base}: MISSING")
            continue
        print(f"- {base}: rows={a.n_rows} cols={len(a.columns)}")
        for fname, keys in fields.items():
            cand = _find_candidate_columns(a.columns, keys)
            if not cand:
                print(f"  - {fname}: columns=0")
                continue
            nn = {c: a.non_null.get(c, 0) for c in cand[:6]}
            print(f"  - {fname}: columns={len(cand)} non_null(sample)={nn}")


def _audit_for_date(raw_dir: str, normalized_root: str) -> None:
    date_key = os.path.basename(raw_dir).split("_", 1)[0]
    ymd = f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"

    raw_files = [
        os.path.join(raw_dir, "races.csv"),
        os.path.join(raw_dir, "entries.csv"),
        os.path.join(raw_dir, "results.csv"),
    ]
    norm_dir = os.path.join(normalized_root, os.path.basename(raw_dir), ymd)
    norm_files = [
        os.path.join(norm_dir, "races.csv"),
        os.path.join(norm_dir, "entries.csv"),
        os.path.join(norm_dir, "results.csv"),
    ]

    print(f"\n\n### race_date={ymd} ({raw_dir}) ###")
    _print_stage("RAW", [audit_csv(p) for p in raw_files], TARGET_FIELDS)
    _print_stage("NORMALIZED", [audit_csv(p) for p in norm_files], TARGET_FIELDS)


def _duckdb_audit(db_path: str, start: str, end: str) -> None:
    if duckdb is None:
        print("\n[duckdb] duckdb module not available; skipping DB audit")
        return

    con = duckdb.connect(db_path)
    start_ymd = start.replace("-", "")
    end_ymd = end.replace("-", "")
    print("\n== DUCKDB (non-null counts) ==")
    for table, exprs in [
        ("entries", ["jockey_id", "weight_carried"]),
        ("results", ["pop_rank"]),
        ("races", ["track_condition"]),
    ]:
        cols = []
        for c in exprs:
            cols.append(f"sum(case when {c} is not null and cast({c} as varchar) <> '' then 1 else 0 end) as nn_{c}")
        q = f"""
          select
            '{table}' as table,
            count(*) as rows,
            {", ".join(cols)}
          from {table}
          where substr(cast(race_id as varchar), 1, 8) between '{start_ymd}' and '{end_ymd}'
        """
        try:
            df = con.execute(q).fetchdf()
            print(df.to_string(index=False))
        except Exception as e:
            print(f"{table}: ERROR {e}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-root", default="data/raw")
    ap.add_argument("--normalized-root", default="data/normalized")
    ap.add_argument("--db-path", default="data/warehouse/aikeiba.duckdb")
    ap.add_argument("--dates", nargs="*", default=None, help="YYYYMMDD (e.g. 20260105) or raw dir name")
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default="2026-03-31")
    args = ap.parse_args()

    raw_dirs = []
    if args.dates:
        for d in args.dates:
            key = d.replace("-", "")
            if len(key) == 8 and key.isdigit():
                raw_dirs.append(os.path.join(args.raw_root, f"{key}_hist_from_jv"))
            else:
                raw_dirs.append(os.path.join(args.raw_root, d))
    else:
        raw_dirs = sorted(glob.glob(os.path.join(args.raw_root, "2026*_hist_from_jv")))
        raw_dirs = [d for d in raw_dirs if os.path.isdir(d)]

    for d in raw_dirs:
        _audit_for_date(d, args.normalized_root)

    _duckdb_audit(args.db_path, args.start, args.end)


if __name__ == "__main__":
    main()
