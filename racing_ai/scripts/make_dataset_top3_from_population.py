import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class Audit:
    name: str
    rows: int
    races: int
    top3_rate: float
    win_rate: float


KEEP_COLS = [
    "race_date",
    "race_id_raw",
    "horse_no",
    "horse_id",
    "horse_name",
    "jockey_name",
    "weight_carried",
    "field_size",
    "pop_rank",
    "win_odds",
    "distance",
    "track_condition",
    "finish_position",
]


def _to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _audit(df: pd.DataFrame, name: str) -> Audit:
    races = int(df["race_id_raw"].nunique(dropna=False))
    top3_rate = float(df["top3"].mean()) if len(df) else 0.0
    win_rate = float(df["win"].mean()) if len(df) else 0.0
    return Audit(name=name, rows=int(len(df)), races=races, top3_rate=top3_rate, win_rate=win_rate)


def _missingness(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    miss = {}
    for c in cols:
        if c not in df.columns:
            miss[c] = 1.0
            continue
        miss[c] = float(df[c].isna().mean())
    return pd.Series(miss).sort_values(ascending=False)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build top3/win dataset from TARGET population master.")
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--out-clean", dest="out_clean_path", required=True)
    ap.add_argument("--encoding", default="cp932")
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    out_clean_path = Path(args.out_clean_path)

    df = pd.read_csv(in_path, dtype=str, encoding=args.encoding)

    # Ensure numeric finish_position for labels.
    fp = _to_int(df["finish_position"]) if "finish_position" in df.columns else pd.Series([pd.NA] * len(df))
    df = df.assign(finish_position=fp)
    before = len(df)
    df = df[df["finish_position"].notna()].copy()
    dropped_non_numeric = before - len(df)

    df["top3"] = (df["finish_position"] <= 3).astype("int8")
    df["win"] = (df["finish_position"] == 1).astype("int8")

    # Column subset.
    missing_keep = [c for c in KEEP_COLS if c not in df.columns]
    if missing_keep:
        raise SystemExit(f"Missing required columns in input: {missing_keep}")
    out_df = df[KEEP_COLS + ["top3", "win", "abnormal_code"]].copy()

    # Clean version: abnormal_code == 0 only.
    abn = _to_int(out_df["abnormal_code"]).fillna(pd.NA)
    clean_df = out_df[abn == 0].copy()

    # Write outputs (do not overwrite original).
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_clean_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False, encoding=args.encoding)
    clean_df.to_csv(out_clean_path, index=False, encoding=args.encoding)

    # Report
    print(f"[input] {in_path}")
    print(f"[input] rows={before} dropped_non_numeric_finish_position={dropped_non_numeric} kept={len(df)}")

    a_all = _audit(out_df, "all")
    a_clean = _audit(clean_df, "clean(abnormal_code==0)")
    print("")
    print("=== audit ===")
    for a in (a_all, a_clean):
        print(
            f"- {a.name}: rows={a.rows} races={a.races} "
            f"top3_rate={a.top3_rate:.4f} win_rate={a.win_rate:.4f}"
        )

    print("")
    print("=== abnormal_code counts (all) ===")
    print(out_df["abnormal_code"].fillna("").value_counts().head(20).to_string())

    print("")
    print("=== missingness (all, keep cols) ===")
    miss = _missingness(out_df, KEEP_COLS)
    print((miss * 100).round(3).astype(str).add("%").to_string())

    print("")
    print("=== year stats (all) ===")
    year = out_df["race_date"].astype(str).str.slice(0, 4)
    year_df = pd.DataFrame(
        {
            "rows": year.value_counts().sort_index(),
            "top3_rate": out_df.groupby(year)["top3"].mean().sort_index(),
            "win_rate": out_df.groupby(year)["win"].mean().sort_index(),
            "races": out_df.groupby(year)["race_id_raw"].nunique().sort_index(),
        }
    )
    print(year_df.to_string())

    print("")
    print(f"[output] {out_path}")
    print(f"[output] {out_clean_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

