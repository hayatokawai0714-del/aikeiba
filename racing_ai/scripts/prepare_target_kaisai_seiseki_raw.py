import argparse
from pathlib import Path

import pandas as pd


def generic_cols(n: int) -> list[str]:
    return [f"col_{i:02d}" for i in range(1, n + 1)]


def detect_entry_id_col(df: pd.DataFrame) -> int | None:
    # Column whose values are 18-digit for (almost) all rows.
    for i in range(df.shape[1]):
        s = df.iloc[:, i].astype(str)
        ok = s.str.fullmatch(r"\d{18}").mean()
        if ok >= 0.999:
            return i
    return None


def detect_horse_no_col(df: pd.DataFrame, entry_col: str) -> str | None:
    entry = df[entry_col].astype(str)
    last2 = pd.to_numeric(entry.str.slice(-2), errors="coerce")
    for c in df.columns:
        if c == entry_col:
            continue
        v = pd.to_numeric(df[c], errors="coerce")
        if v.isna().mean() > 0.2:
            continue
        ok = (v == last2).mean()
        if ok >= 0.95:
            return c
    return None


def detect_finish_col(df: pd.DataFrame, entry_col: str, horse_no_col: str | None) -> str | None:
    # finish is usually integer 1..18; exclude horse_no and obvious ids.
    race_key = df[entry_col].astype(str).str.slice(0, 16)
    for c in df.columns:
        if c in {entry_col, horse_no_col}:
            continue
        raw = df[c].astype(str).str.strip()
        v = pd.to_numeric(raw, errors="coerce")
        if v.isna().mean() > 0.2:
            continue
        # must mostly be in [1, 18]
        in_range = ((v >= 1) & (v <= 18)).mean()
        # month/day columns can also be in-range; require enough variety
        uniq = v.nunique(dropna=True)
        if not (in_range >= 0.9 and uniq >= 10):
            continue

        # Within-race, finish positions should be mostly unique (unlike race_no/month/day).
        tmp = pd.DataFrame({"rk": race_key, "val": v})
        g = tmp.groupby("rk")["val"]
        # Use a small sample for speed
        ratios = (g.nunique() / g.size()).head(200)
        if len(ratios) == 0:
            continue
        if float(ratios.mean()) >= 0.9:
            return c
    return None


def detect_win_odds_col(df: pd.DataFrame, entry_col: str) -> str | None:
    # odds is positive float, with many decimals and rarely huge.
    for c in df.columns:
        if c == entry_col:
            continue
        raw = df[c].astype(str).str.strip()
        v = pd.to_numeric(raw, errors="coerce")
        if v.isna().mean() > 0.8:
            continue
        pos = (v > 0).mean()
        if pos < 0.8:
            continue
        # avoid near-constant columns like year/month by requiring variety
        if v.nunique(dropna=True) < 100:
            continue
        # odds usually has decimals in many rows
        has_dot = raw.str.contains(r"\.", regex=True).mean()
        if has_dot < 0.5:
            continue
        # heuristic: odds typically <= 999.9
        plausible = (v <= 999.9).mean()
        if plausible >= 0.95:
            return c
    return None


def detect_abnormal_code_col(df: pd.DataFrame, entry_col: str, finish_col: str | None) -> str | None:
    # Often 0, sometimes non-zero (including decimals in some exports). Prefer column with many zeros.
    for c in df.columns:
        if c in {entry_col, finish_col}:
            continue
        v = pd.to_numeric(df[c].astype(str).str.strip(), errors="coerce")
        if v.isna().mean() > 0.2:
            continue
        zeros = (v == 0).mean()
        small = (v.between(-1, 99)).mean()
        if zeros >= 0.7 and small >= 0.95:
            return c
    return None


def build_race_date_from_first3(df: pd.DataFrame) -> pd.Series:
    years = pd.to_numeric(df["col_01"], errors="coerce").astype("Int64")
    months = pd.to_numeric(df["col_02"], errors="coerce").astype("Int64")
    days = pd.to_numeric(df["col_03"], errors="coerce").astype("Int64")
    y = years.astype(str).str.zfill(2)
    m = months.astype(str).str.zfill(2)
    d = days.astype(str).str.zfill(2)
    return pd.to_datetime("20" + y + "-" + m + "-" + d, errors="coerce")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Prepare TARGET開催成績CSV(ヘッダ無し/CP932) as 'alternative raw' for validation."
        )
    )
    ap.add_argument(
        "--input",
        required=True,
        help=r"Input CSV path (e.g. C:\TXT\2025Q1_kaisai_seiseki.csv)",
    )
    ap.add_argument(
        "--encoding",
        default="cp932",
        help="Input encoding (default: cp932)",
    )
    ap.add_argument(
        "--out-with-header",
        required=True,
        help="Output path for header-added CSV",
    )
    ap.add_argument(
        "--out-enriched",
        required=True,
        help="Output path for enriched CSV (adds race_date/race_id/horse_no/target_finish)",
    )
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(str(in_path))

    df = pd.read_csv(in_path, header=None, encoding=args.encoding, dtype=str)
    if df.shape[1] != 52:
        raise RuntimeError(f"Expected 52 columns, got {df.shape[1]}")

    # Always use generic col_01..col_52 to avoid relying on GUI column order.
    df_named = df.copy()
    df_named.columns = generic_cols(52)
    named_mode = "generic_cols"
    entry_col_idx = detect_entry_id_col(df_named)
    if entry_col_idx is None:
        raise RuntimeError("Could not detect 18-digit entry id column; cannot enrich safely.")
    entry_col_name = df_named.columns[entry_col_idx]

    horse_no_col = detect_horse_no_col(df_named, entry_col_name)
    finish_col = detect_finish_col(df_named, entry_col_name, horse_no_col)
    odds_col = detect_win_odds_col(df_named, entry_col_name)
    abn_col = detect_abnormal_code_col(df_named, entry_col_name, finish_col)

    # Write "with header" file without changing content.
    out_with_header = Path(args.out_with_header)
    out_with_header.parent.mkdir(parents=True, exist_ok=True)
    df_named.to_csv(out_with_header, index=False, encoding=args.encoding)

    # Enriched fields (minimal)
    enriched = df_named.copy()
    enriched["race_date"] = build_race_date_from_first3(enriched).dt.strftime("%Y-%m-%d")
    enriched["race_id"] = enriched[entry_col_name].astype(str).str.slice(0, 16)
    enriched["horse_no"] = (
        pd.to_numeric(enriched[horse_no_col], errors="coerce").astype("Int64")
        if horse_no_col is not None
        else pd.NA
    )
    enriched["target_finish"] = (
        pd.to_numeric(enriched[finish_col], errors="coerce").astype("Int64")
        if finish_col is not None
        else pd.NA
    )

    out_enriched = Path(args.out_enriched)
    out_enriched.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(out_enriched, index=False, encoding=args.encoding)

    # Checks / summary
    entry_id = enriched[entry_col_name].astype(str)
    race_id = enriched["race_id"].astype(str)

    entry_unique = entry_id.nunique(dropna=False)
    race_unique = race_id.nunique(dropna=False)

    finish = enriched["target_finish"]
    odds = (
        pd.to_numeric(enriched[odds_col].astype(str).str.strip(), errors="coerce")
        if odds_col is not None
        else pd.Series([pd.NA] * len(enriched))
    )
    if abn_col is not None:
        abn = enriched[abn_col].fillna("").astype(str).str.strip()
    else:
        abn = pd.Series([""] * len(enriched))

    finish_missing_pct = float(finish.isna().mean() * 100.0)
    odds_missing_pct = float(odds.isna().mean() * 100.0)

    abn_counts = (
        abn.value_counts()
        .rename_axis("異常コード")
        .reset_index(name="rows")
        .sort_values(["rows", "異常コード"], ascending=[False, True])
    )

    runners_per_race = race_id.value_counts()
    dist = runners_per_race.describe()

    print("[prepare-target-kaisai] input =", str(in_path))
    print("[prepare-target-kaisai] rows =", len(enriched))
    print("[prepare-target-kaisai] columns =", df.shape[1], "(fixed 52)")
    print("[prepare-target-kaisai] encoding =", args.encoding)
    print("[prepare-target-kaisai] naming_mode =", named_mode)
    print("[prepare-target-kaisai] detected_entry_id_col =", entry_col_name)
    print("[prepare-target-kaisai] detected_horse_no_col =", horse_no_col)
    print("[prepare-target-kaisai] detected_finish_col =", finish_col)
    print("[prepare-target-kaisai] detected_win_odds_col =", odds_col)
    print("[prepare-target-kaisai] detected_abnormal_col =", abn_col)
    print("[prepare-target-kaisai] out_with_header =", str(out_with_header))
    print("[prepare-target-kaisai] out_enriched =", str(out_enriched))
    print("[prepare-target-kaisai] entry_id_unique =", entry_unique, "dup =", len(enriched) - entry_unique)
    print("[prepare-target-kaisai] race_id_unique =", race_unique)
    print("[prepare-target-kaisai] target_finish_missing_pct =", round(finish_missing_pct, 4))
    print("[prepare-target-kaisai] win_odds_missing_pct =", round(odds_missing_pct, 4))
    print(
        "[prepare-target-kaisai] runners_per_race: "
        f"min={int(dist['min'])} p25={int(dist['25%'])} median={int(dist['50%'])} "
        f"p75={int(dist['75%'])} max={int(dist['max'])} mean={round(float(dist['mean']), 4)}"
    )
    print("[prepare-target-kaisai] 異常コード top10:")
    print(abn_counts.head(10).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
