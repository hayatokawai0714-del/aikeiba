import argparse
from pathlib import Path

import pandas as pd


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build minimal validation dataset from TARGET開催成績 enriched CSV."
    )
    ap.add_argument("--input", required=True, help=r"e.g. C:\TXT\2025Q1_kaisai_seiseki.enriched.csv")
    ap.add_argument("--output", required=True, help=r"e.g. C:\TXT\2025Q1_population_min.csv")
    ap.add_argument("--encoding", default="cp932")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(str(in_path))

    df = pd.read_csv(in_path, encoding=args.encoding, dtype=str)

    required = ["race_date", "race_id", "horse_no", "target_finish"]
    for c in required:
        if c not in df.columns:
            raise RuntimeError(f"Missing required column in input: {c}")

    # Column positions (from earlier detection on this export format)
    # NOTE: input is the 'enriched' file which still contains col_01..col_52.
    col_horse_name = "col_14"
    col_jockey = "col_17"
    col_weight = "col_18"
    col_field_size = "col_19"
    col_popularity = "col_25"
    col_win_odds = "col_24"
    col_surface = "col_10"  # 芝ダコード
    col_distance = "col_12"
    col_track = "col_13"  # 馬場状態
    col_last3f = "col_33"
    col_abnormal = "col_23"  # 異常コード

    for c in [
        col_horse_name,
        col_jockey,
        col_weight,
        col_field_size,
        col_popularity,
        col_win_odds,
        col_surface,
        col_distance,
        col_track,
        col_last3f,
        col_abnormal,
    ]:
        if c not in df.columns:
            raise RuntimeError(f"Expected column not found: {c}")

    # Pre stats
    pre_rows = len(df)
    pre_races = df["race_id"].nunique(dropna=False)
    abn_counts = df[col_abnormal].fillna("").astype(str).str.strip().value_counts(dropna=False)
    odds_num = pd.to_numeric(df[col_win_odds].astype(str).str.strip(), errors="coerce")
    odds_missing = int(odds_num.isna().sum())
    finish_num = pd.to_numeric(df["target_finish"], errors="coerce")
    finish_non_numeric = int(finish_num.isna().sum())

    # Filters
    mask_abn_ok = pd.to_numeric(df[col_abnormal].astype(str).str.strip(), errors="coerce").fillna(-999) == 0
    mask_odds_ok = ~odds_num.isna()
    mask_finish_ok = ~finish_num.isna()
    kept = df[mask_abn_ok & mask_odds_ok & mask_finish_ok].copy()

    post_rows = len(kept)
    post_races = kept["race_id"].nunique()

    excluded = pre_rows - post_rows

    # Minimal columns
    out = pd.DataFrame(
        {
            "race_date": kept["race_date"],
            "race_id": kept["race_id"],
            "horse_no": pd.to_numeric(kept["horse_no"], errors="coerce").astype("Int64"),
            "horse_name": kept[col_horse_name],
            "jockey": kept[col_jockey],
            "weight_carried": pd.to_numeric(kept[col_weight].astype(str).str.strip(), errors="coerce"),
            "field_size": pd.to_numeric(kept[col_field_size].astype(str).str.strip(), errors="coerce").astype(
                "Int64"
            ),
            "popularity": pd.to_numeric(kept[col_popularity].astype(str).str.strip(), errors="coerce").astype(
                "Int64"
            ),
            "win_odds": pd.to_numeric(kept[col_win_odds].astype(str).str.strip(), errors="coerce"),
            "target_finish": pd.to_numeric(kept["target_finish"], errors="coerce").astype("Int64"),
            "surface": kept[col_surface],
            "distance": pd.to_numeric(kept[col_distance].astype(str).str.strip(), errors="coerce").astype("Int64"),
            "track_condition": kept[col_track],
            "last3f": pd.to_numeric(kept[col_last3f].astype(str).str.strip(), errors="coerce"),
        }
    )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding=args.encoding)

    # runners per race distribution (post)
    runners = kept["race_id"].value_counts()
    desc = runners.describe()

    print("[build-validation-pop] input =", str(in_path))
    print("[build-validation-pop] output =", str(out_path))
    print("[build-validation-pop] pre_rows =", pre_rows, "pre_races =", pre_races)
    print("[build-validation-pop] post_rows =", post_rows, "post_races =", post_races, "excluded =", excluded)
    print("[build-validation-pop] odds_missing =", odds_missing)
    print("[build-validation-pop] target_finish_non_numeric =", finish_non_numeric)
    print("[build-validation-pop] abnormal_code_counts:")
    print(abn_counts.to_string())
    print(
        "[build-validation-pop] runners_per_race(post): "
        f"min={int(desc['min'])} p25={int(desc['25%'])} median={int(desc['50%'])} "
        f"p75={int(desc['75%'])} max={int(desc['max'])} mean={round(float(desc['mean']), 4)}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

