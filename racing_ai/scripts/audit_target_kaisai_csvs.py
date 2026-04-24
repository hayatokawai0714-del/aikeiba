from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


COLS_52 = [f"col_{i:02d}" for i in range(1, 53)]
KNOWN_COL_ORDER = [
    "年",
    "月",
    "日",
    "回次",
    "日次",
    "場所",
    "場所名",
    "レース番号",
    "競走名",
    "クラスコード",
    "クラス名",
    "芝ダコード",
    "距離",
    "馬場状態",
    "馬名",
    "性別",
    "年齢",
    "騎手",
    "斤量",
    "頭数",
    "馬番",
    "確定着順",
    "人気着順",
    "異常コード",
    "着差タイム",
    "人気",
    "単勝オッズ",
    "走破タイム",
    "タイムS",
    "補正タイム",
    "通過順1角",
    "通過順2角",
    "通過順3角",
    "通過順4角",
    "上がり3Fタイム",
    "馬体重",
    "調教師",
    "所属",
    "賞金",
    "血統登録番号",
    "騎手コード",
    "調教師コード",
    "馬レースID(新)",
    "馬主名",
    "生産者名",
    "父馬名",
    "母馬名",
    "母の父馬名",
    "毛色",
    "生年月日",
    "レースID1",
    "PCI",
]


@dataclass(frozen=True)
class BasicStats:
    path: str
    rows: int
    cols_mode: int
    cols_bad_rows: int
    cp932_readable: bool
    entry_id_unique: bool
    entry_id_dupes: int
    n_races: int
    miss_finish_pct: float
    miss_odds_pct: float
    miss_pop_pct: float
    miss_distance_pct: float
    miss_track_condition_pct: float
    abnormal_counts: dict[str, int]
    detected_entry_col: str
    detected_finish_col: str
    detected_odds_col: str
    detected_pop_col: str
    detected_distance_col: str
    detected_track_condition_col: str
    detected_abnormal_col: str


def _sniff_col_counts(path: Path, *, encoding: str = "cp932", max_lines: int = 5000) -> tuple[int, int, int]:
    counts: list[int] = []
    bad = 0
    with path.open("r", encoding=encoding, errors="strict", newline="") as f:
        r = csv.reader(f)
        for i, row in enumerate(r):
            if i >= max_lines:
                break
            n = len(row)
            counts.append(n)
            if n != 52:
                bad += 1
    if not counts:
        return 0, 0, 0
    # mode
    mode = max(set(counts), key=counts.count)
    return len(counts), mode, bad


def _read_noheader_52(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, header=None, encoding="cp932", dtype=str, keep_default_na=False)
    if df.shape[1] != 52:
        raise ValueError(f"expected 52 columns, got {df.shape[1]}")
    df.columns = COLS_52
    return df


def _is_missing(s: pd.Series) -> pd.Series:
    return s.isna() | (s.astype(str).str.strip() == "")


def audit_one(path: Path) -> BasicStats:
    # 1) encoding + col count sniff
    cp932_ok = True
    try:
        sniff_rows, cols_mode, cols_bad = _sniff_col_counts(path, encoding="cp932")
    except UnicodeDecodeError:
        cp932_ok = False
        sniff_rows, cols_mode, cols_bad = 0, 0, 0

    df = _read_noheader_52(path)

    # --- Detect columns by data patterns (robust to column order drift) ---
    def digit18_rate(col: str) -> float:
        s = df[col].astype(str).str.strip()
        return float(s.str.fullmatch(r"\d{18}", na=False).mean() * 100.0)

    def finish_like_rate(col: str) -> float:
        x = pd.to_numeric(df[col], errors="coerce")
        ok = x.notna() & (x >= 1) & (x <= 18) & (x.round() == x)
        # prefer columns that actually reach >12 sometimes (month/day won't)
        hi = (x > 12).mean() if x.notna().any() else 0.0
        ones = (x == 1).mean() if x.notna().any() else 0.0
        return float(ok.mean() * 100.0 + hi * 50.0 + ones * 50.0)

    def pop_like_rate(col: str) -> float:
        x = pd.to_numeric(df[col], errors="coerce")
        ok = x.notna() & (x >= 1) & (x <= 18) & (x.round() == x)
        hi = (x > 12).mean() if x.notna().any() else 0.0
        ones = (x == 1).mean() if x.notna().any() else 0.0
        return float(ok.mean() * 100.0 + hi * 50.0 + ones * 30.0)

    def odds_like_rate(col: str) -> float:
        x = pd.to_numeric(df[col], errors="coerce")
        # Odds should be positive and usually < 1000.
        ok = x.notna() & (x > 0) & (x < 1000)
        # prefer columns that are not almost-always integer and not constant
        frac = (x % 1 != 0).mean() if ok.any() else 0.0
        # penalize columns that look like IDs (very large) by requiring ok coverage.
        return float(ok.mean() * 100.0 + frac * 20.0)

    def distance_like_rate(col: str) -> float:
        x = pd.to_numeric(df[col], errors="coerce")
        ok = x.notna() & (x >= 800) & (x <= 4000) & (x.round() == x)
        mult100 = ((x % 100) == 0).mean() if x.notna().any() else 0.0
        return float(ok.mean() * 100.0 + mult100 * 10.0)

    def track_condition_like_rate(col: str) -> float:
        s = df[col].astype(str).str.strip()
        ok = s.isin(["良", "稍重", "重", "不良"])
        return float(ok.mean() * 100.0)

    def abnormal_like_rate(col: str) -> float:
        s = df[col].astype(str).str.strip()
        # expect small integer codes like 0/1/3/4; allow blank
        in_set = s.isin(["0", "1", "3", "4"])
        # prefer columns that are mostly in set (not mostly blank), but allow some blanks
        blank = (s == "").mean()
        pct0 = (s == "0").mean()
        score = pct0 * 120.0 + in_set.mean() * 10.0 - blank * 10.0
        return float(score)

    entry_col = max(COLS_52, key=digit18_rate)
    finish_col = max(COLS_52, key=finish_like_rate)
    pop_col = max(COLS_52, key=pop_like_rate)
    odds_col = max(COLS_52, key=odds_like_rate)
    dist_col = max(COLS_52, key=distance_like_rate)
    track_col = max(COLS_52, key=track_condition_like_rate)
    abnormal_col = max(COLS_52, key=abnormal_like_rate)

    # 2) unique entry id (18 digits)
    entry = df[entry_col].astype(str).str.strip()
    entry_digits = entry.str.fullmatch(r"\d{18}", na=False)
    entry_valid = entry[entry_digits]
    entry_dupes = int(entry_valid.duplicated().sum())
    entry_unique = (entry_valid.shape[0] == df.shape[0]) and (entry_dupes == 0)

    # 3) race_id (first 16 of entry)
    race_id = entry.str.slice(0, 16)
    n_races = int(race_id.nunique(dropna=False))

    # 4) missingness (key cols)
    finish = pd.to_numeric(df[finish_col], errors="coerce")
    odds = pd.to_numeric(df[odds_col], errors="coerce")
    pop = pd.to_numeric(df[pop_col], errors="coerce")
    dist = pd.to_numeric(df[dist_col], errors="coerce")
    baba = df[track_col].astype(str).str.strip()

    miss_finish_pct = float(finish.isna().mean() * 100.0)
    miss_odds_pct = float(odds.isna().mean() * 100.0)
    miss_pop_pct = float(pop.isna().mean() * 100.0)
    miss_distance_pct = float(dist.isna().mean() * 100.0)
    miss_track_pct = float((baba == "").mean() * 100.0)

    # 5) abnormal code distribution
    ab = df[abnormal_col].astype(str).str.strip()
    abnormal_counts = ab.value_counts(dropna=False).head(30).to_dict()  # limit

    return BasicStats(
        path=str(path),
        rows=int(df.shape[0]),
        cols_mode=int(cols_mode),
        cols_bad_rows=int(cols_bad),
        cp932_readable=cp932_ok,
        entry_id_unique=bool(entry_unique),
        entry_id_dupes=int(entry_dupes),
        n_races=n_races,
        miss_finish_pct=miss_finish_pct,
        miss_odds_pct=miss_odds_pct,
        miss_pop_pct=miss_pop_pct,
        miss_distance_pct=miss_distance_pct,
        miss_track_condition_pct=miss_track_pct,
        abnormal_counts={str(k): int(v) for k, v in abnormal_counts.items()},
        detected_entry_col=entry_col,
        detected_odds_col=odds_col,
        detected_distance_col=dist_col,
        detected_abnormal_col=abnormal_col,
        detected_finish_col=finish_col,
        detected_pop_col=pop_col,
        detected_track_condition_col=track_col,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit TARGET 開催成績CSV(52列固定想定) quality for training base.")
    ap.add_argument("files", nargs="+")
    args = ap.parse_args()

    stats = []
    for f in args.files:
        p = Path(f)
        if not p.exists():
            raise FileNotFoundError(str(p))
        stats.append(audit_one(p))

    # Print per-file summary
    for s in stats:
        print("\n=== " + s.path + " ===")
        print(f"rows={s.rows}")
        print(f"cols_mode={s.cols_mode} cols_bad_rows(sampled)={s.cols_bad_rows} cp932_readable={s.cp932_readable}")
        print(f"entry_id_unique={s.entry_id_unique} entry_id_dupes={s.entry_id_dupes}")
        print(
            "detected_cols: "
            + f"entry={s.detected_entry_col} "
            + f"finish={s.detected_finish_col} "
            + f"odds={s.detected_odds_col} "
            + f"pop={s.detected_pop_col} "
            + f"distance={s.detected_distance_col} "
            + f"track={s.detected_track_condition_col} "
            + f"abnormal={s.detected_abnormal_col}"
        )
        print(f"n_races={s.n_races}")
        print(
            "missing_pct: "
            + f"finish={s.miss_finish_pct:.3f}% "
            + f"odds={s.miss_odds_pct:.3f}% "
            + f"pop={s.miss_pop_pct:.3f}% "
            + f"distance={s.miss_distance_pct:.3f}% "
            + f"track_condition={s.miss_track_condition_pct:.3f}%"
        )
        top_ab = list(s.abnormal_counts.items())[:10]
        print("abnormal_code_top10=" + ", ".join([f"{k}:{v}" for k, v in top_ab]))

    # Cross-file consistency checks
    print("\n=== cross-file checks ===")
    all_ok_cols = all(s.cols_mode == 52 and s.cols_bad_rows == 0 for s in stats)
    all_ok_entry = all(s.entry_id_unique for s in stats)
    print(f"all_files_52cols_and_no_shift(sampled)={all_ok_cols}")
    print(f"all_files_entry_id_unique={all_ok_entry}")

    # Final judgement heuristic
    # - must be 52 cols stable
    # - entry id unique
    # - finish missing very low
    # - distance missing very low
    # Odds/pop can be partially missing but should be reasonable.
    ok = True
    reasons: list[str] = []
    if not all_ok_cols:
        ok = False
        reasons.append("column_shift_or_not_52cols_detected")
    if not all_ok_entry:
        ok = False
        reasons.append("entry_id_not_unique_or_not_18digits")
    if any(s.miss_finish_pct > 0.5 for s in stats):
        ok = False
        reasons.append("finish_missing_too_high")
    if any(s.miss_distance_pct > 0.5 for s in stats):
        ok = False
        reasons.append("distance_missing_too_high")

    print("\n=== final ===")
    print("mother_data_ok = " + ("YES" if ok else "NO"))
    if reasons:
        print("reasons=" + ", ".join(reasons))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
