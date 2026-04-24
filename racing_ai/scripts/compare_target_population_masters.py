from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


KEY_COLS = [
    "race_date",
    "race_id_raw",
    "entry_id_18",
    "horse_no",
    "target_finish",
    "pop_rank",
    "win_odds",
    "distance",
    "track_condition",
    "abnormal_code",
]


def summarize(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="cp932", dtype=str)
    rows = len(df)
    races = df.get("race_id_raw", pd.Series([], dtype=str)).nunique(dropna=False) if rows else 0
    entry_dupes = int(df.get("entry_id_18", pd.Series([], dtype=str)).duplicated().sum()) if rows else 0
    miss = {}
    for c in KEY_COLS:
        if c not in df.columns:
            miss[c] = float("nan")
            continue
        s = df[c].astype(str).str.strip()
        miss[c] = float((s == "").mean() * 100.0)
    out = {
        "file": str(path),
        "rows": rows,
        "races": int(races),
        "entry_dupes": entry_dupes,
        **{f"miss_{k}": v for k, v in miss.items()},
    }
    return pd.DataFrame([out])


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare two TARGET population master v1 CSVs (e.g., 2024 vs 2025).")
    ap.add_argument("--a", required=True)
    ap.add_argument("--b", required=True)
    args = ap.parse_args()

    a = summarize(Path(args.a))
    b = summarize(Path(args.b))
    out = pd.concat([a, b], ignore_index=True)
    with pd.option_context("display.max_columns", 200, "display.width", 200):
        print(out.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

