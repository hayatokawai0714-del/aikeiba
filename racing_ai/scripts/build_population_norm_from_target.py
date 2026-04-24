import argparse
import re
from pathlib import Path

import pandas as pd


VENUE3_MAP = {
    "東京": "TOK",
    "中山": "NAK",
    "京都": "KYO",
    "阪神": "HAN",
    "中京": "CHU",
    "小倉": "KOK",
    "新潟": "NII",
    "福島": "FUK",
    "札幌": "SAP",
    "函館": "HAK",
}

RACE_ID_NORM_RE = re.compile(r"^\d{8}-[A-Z]{3}-\d{2}R$")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Create race_id_norm (YYYYMMDD-VENUE3-RR R) from TARGET開催成績 enriched CSV."
    )
    ap.add_argument("--input", required=True, help=r"e.g. C:\TXT\2025Q1_kaisai_seiseki.enriched.csv")
    ap.add_argument("--output", required=True, help=r"e.g. C:\TXT\2025Q1_population_norm.csv")
    ap.add_argument("--encoding", default="cp932")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(str(in_path))

    df = pd.read_csv(in_path, encoding=args.encoding, dtype=str)

    for c in ["race_date", "horse_no", "target_finish"]:
        if c not in df.columns:
            raise RuntimeError(f"Missing required column: {c}")

    # Column positions for this export (no large refactor; keep minimal assumptions)
    col_venue_name = "col_05"  # 場所名 (e.g. 中山)
    col_race_no = "col_07"  # レース番号
    col_horse_name = "col_14"  # 馬名
    col_jockey = "col_17"  # 騎手
    col_weight = "col_18"  # 斤量
    col_field_size = "col_19"  # 頭数
    col_popularity = "col_25"  # 人気
    col_win_odds = "col_24"  # 単勝オッズ
    col_distance = "col_12"  # 距離
    col_track = "col_13"  # 馬場状態

    for c in [
        col_venue_name,
        col_race_no,
        col_horse_name,
        col_jockey,
        col_weight,
        col_field_size,
        col_popularity,
        col_win_odds,
        col_distance,
        col_track,
    ]:
        if c not in df.columns:
            raise RuntimeError(f"Expected column not found in input: {c}")

    # Build race_id_norm
    race_date_yyyymmdd = df["race_date"].astype(str).str.replace("-", "", regex=False)
    venue3 = df[col_venue_name].map(VENUE3_MAP)
    rr = pd.to_numeric(df[col_race_no], errors="coerce").astype("Int64").astype(str).str.zfill(2)
    df["race_id_norm"] = race_date_yyyymmdd + "-" + venue3.fillna("") + "-" + rr + "R"

    # Minimal output columns
    out = pd.DataFrame(
        {
            "race_id_norm": df["race_id_norm"],
            "race_date": df["race_date"],
            "horse_no": pd.to_numeric(df["horse_no"], errors="coerce").astype("Int64"),
            "馬名": df[col_horse_name],
            "騎手": df[col_jockey],
            "斤量": pd.to_numeric(df[col_weight].astype(str).str.strip(), errors="coerce"),
            "頭数": pd.to_numeric(df[col_field_size].astype(str).str.strip(), errors="coerce").astype("Int64"),
            "人気": pd.to_numeric(df[col_popularity].astype(str).str.strip(), errors="coerce").astype("Int64"),
            "単勝オッズ": pd.to_numeric(df[col_win_odds].astype(str).str.strip(), errors="coerce"),
            "target_finish": pd.to_numeric(df["target_finish"], errors="coerce").astype("Int64"),
            "距離": pd.to_numeric(df[col_distance].astype(str).str.strip(), errors="coerce").astype("Int64"),
            "馬場状態": df[col_track],
        }
    )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding=args.encoding)

    # Validation summary
    races_unique = out["race_id_norm"].nunique(dropna=False)
    null_any = out.isna().any()
    null_cols = [c for c, v in null_any.items() if bool(v)]

    bad_format = (~out["race_id_norm"].astype(str).str.match(RACE_ID_NORM_RE)).sum()
    unknown_venues = sorted(
        set(df[col_venue_name].dropna().unique().tolist()) - set(VENUE3_MAP.keys())
    )

    print("[population-norm] input =", str(in_path))
    print("[population-norm] output =", str(out_path))
    print("[population-norm] rows =", len(out))
    print("[population-norm] race_id_norm_unique_races =", int(races_unique))
    print("[population-norm] bad_race_id_norm_format_rows =", int(bad_format))
    print("[population-norm] null_cols =", ",".join(null_cols) if null_cols else "(none)")
    print("[population-norm] unknown_venue_names =", ",".join(unknown_venues) if unknown_venues else "(none)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

