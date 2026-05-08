import json
import re
from pathlib import Path

import duckdb
import pandas as pd

BASE = Path(r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai")
REPORT_DIR = BASE / "reports" / "2024_eval_full_v5"
JOINED_CSV = REPORT_DIR / "pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv"
DB_PATH = BASE / "data" / "warehouse" / "aikeiba.duckdb"

START_DATE = pd.Timestamp("2024-01-06")
END_DATE = pd.Timestamp("2024-12-28")


def extract_race_date_from_id(race_id: str):
    if race_id is None:
        return None
    s = str(race_id)
    m = re.search(r"(20\d{6})", s)
    if not m:
        return None
    d = m.group(1)
    try:
        return pd.Timestamp(f"{d[:4]}-{d[4:6]}-{d[6:8]}")
    except Exception:
        return None


def to_md(df: pd.DataFrame, title: str, note: str = "") -> str:
    def esc(v):
        s = "" if pd.isna(v) else str(v)
        return s.replace("|", "\\|")

    header = "| " + " | ".join(df.columns.astype(str).tolist()) + " |"
    sep = "| " + " | ".join(["---"] * len(df.columns)) + " |"
    rows = ["| " + " | ".join(esc(v) for v in row) + " |" for row in df.itertuples(index=False, name=None)]

    lines = [f"# {title}", ""]
    if note:
        lines += [note, ""]
    lines += [f"rows: {len(df)}", "", header, sep] + rows
    return "\n".join(lines) + "\n"


joined = pd.read_csv(JOINED_CSV)
joined["race_date"] = pd.to_datetime(joined["race_date"], errors="coerce")
joined = joined[(joined["race_date"] >= START_DATE) & (joined["race_date"] <= END_DATE)].copy()
joined_races = joined[["race_date", "race_id"]].drop_duplicates()
joined_race_ids = set(joined_races["race_id"].astype(str))

con = duckdb.connect(str(DB_PATH), read_only=True)
odds = con.execute(
    """
    SELECT race_id, odds_snapshot_version, odds_type, horse_no, horse_no_a, horse_no_b, odds_value
    FROM odds
    """
).fetchdf()
con.close()

odds["race_id"] = odds["race_id"].astype(str)
odds["race_date"] = odds["race_id"].map(extract_race_date_from_id)
odds = odds[(odds["race_date"] >= START_DATE) & (odds["race_date"] <= END_DATE)].copy()

# Step 1
by_date_rows = []
all_dates = sorted(set(joined_races["race_date"].dropna().dt.normalize()))
for d in all_dates:
    d_joined = joined_races[joined_races["race_date"].dt.normalize() == d]
    d_joined_race_ids = set(d_joined["race_id"].astype(str))
    d_odds = odds[odds["race_date"].dt.normalize() == d]

    snap_counts = d_odds.groupby("odds_snapshot_version").size().sort_values(ascending=False).to_dict()

    by_date_rows.append({
        "race_date": d.date().isoformat(),
        "race_count": int(d_joined["race_id"].nunique()),
        "odds_race_count": int(d_odds["race_id"].nunique()),
        "place_rows": int((d_odds["odds_type"] == "place").sum()),
        "place_max_rows": int((d_odds["odds_type"] == "place_max").sum()),
        "win_rows": int((d_odds["odds_type"] == "win").sum()),
        "wide_rows": int((d_odds["odds_type"] == "wide").sum()),
        "wide_max_rows": int((d_odds["odds_type"] == "wide_max").sum()),
        "odds_snapshot_version_counts": json.dumps(snap_counts, ensure_ascii=False),
        "horse_no_ge1_count": int((d_odds["horse_no"].fillna(-1) >= 1).sum()),
        "odds_value_non_null_count": int(d_odds["odds_value"].notna().sum()),
        "joined_race_id_match_count": int(sum(rid in set(d_odds["race_id"].astype(str)) for rid in d_joined_race_ids)),
    })

by_date_df = pd.DataFrame(by_date_rows)
by_date_df.to_csv(REPORT_DIR / "odds_availability_by_date_2024.csv", index=False, encoding="utf-8-sig")
(REPORT_DIR / "odds_availability_by_date_2024.md").write_text(
    to_md(by_date_df, "Odds Availability By Date 2024"), encoding="utf-8"
)

# Step 2
type_rows = []
for odds_type, g in odds.groupby("odds_type"):
    date_min = g["race_date"].min()
    date_max = g["race_date"].max()
    snaps = sorted(g["odds_snapshot_version"].dropna().astype(str).unique().tolist())
    type_rows.append({
        "odds_type": odds_type,
        "row_count": int(len(g)),
        "race_count": int(g["race_id"].nunique()),
        "horse_no_valid_count": int((g["horse_no"].fillna(-1) >= 1).sum()),
        "pair_horse_valid_count": int(((g["horse_no_a"].fillna(-1) >= 1) & (g["horse_no_b"].fillna(-1) >= 1)).sum()),
        "odds_value_non_null_count": int(g["odds_value"].notna().sum()),
        "min_race_date": date_min.date().isoformat() if pd.notna(date_min) else None,
        "max_race_date": date_max.date().isoformat() if pd.notna(date_max) else None,
        "snapshot_version": json.dumps(snaps[:30], ensure_ascii=False),
    })

odds_type_df = pd.DataFrame(type_rows)
if not odds_type_df.empty and "odds_type" in odds_type_df.columns:
    odds_type_df = odds_type_df.sort_values("odds_type")
else:
    odds_type_df = pd.DataFrame(
        columns=[
            "odds_type",
            "row_count",
            "race_count",
            "horse_no_valid_count",
            "pair_horse_valid_count",
            "odds_value_non_null_count",
            "min_race_date",
            "max_race_date",
            "snapshot_version",
        ]
    )
odds_type_df.to_csv(REPORT_DIR / "odds_type_availability_2024.csv", index=False, encoding="utf-8-sig")
(REPORT_DIR / "odds_type_availability_2024.md").write_text(
    to_md(odds_type_df, "Odds Type Availability 2024"), encoding="utf-8"
)

# Step 3
joined_race_set = set(joined_races["race_id"].astype(str))
odds_race_set = set(odds["race_id"].astype(str))
matched = joined_race_set & odds_race_set
unmatched_joined = sorted(joined_race_set - odds_race_set)
unmatched_odds = sorted(odds_race_set - joined_race_set)

def race_id_form(s):
    if re.match(r"^\d{16}$", s):
        return "16digit"
    if re.match(r"^\d{12}$", s):
        return "12digit"
    if re.match(r"^\d{8}", s):
        return "starts_yyyymmdd"
    return "other"

joined_forms = pd.Series([race_id_form(x) for x in joined_race_set]).value_counts().to_dict()
odds_forms = pd.Series([race_id_form(x) for x in odds_race_set]).value_counts().to_dict()
joined_date_ok = sum(extract_race_date_from_id(x) is not None for x in joined_race_set)
odds_date_ok = sum(extract_race_date_from_id(x) is not None for x in odds_race_set)

match_df = pd.DataFrame([{
    "joined_race_count": len(joined_race_set),
    "odds_race_count": len(odds_race_set),
    "matched_race_count": len(matched),
    "unmatched_joined_race_count": len(unmatched_joined),
    "unmatched_odds_race_count": len(unmatched_odds),
    "sample_unmatched_joined_race_id": "|".join(unmatched_joined[:20]),
    "sample_unmatched_odds_race_id": "|".join(unmatched_odds[:20]),
    "race_id_format_diff": json.dumps({"joined": joined_forms, "odds": odds_forms}, ensure_ascii=False),
    "race_date_extract_diff": json.dumps({"joined_extractable": joined_date_ok, "joined_total": len(joined_race_set), "odds_extractable": odds_date_ok, "odds_total": len(odds_race_set)}, ensure_ascii=False),
}])

match_df.to_csv(REPORT_DIR / "joined_pairs_odds_match_audit_2024.csv", index=False, encoding="utf-8-sig")
(REPORT_DIR / "joined_pairs_odds_match_audit_2024.md").write_text(
    to_md(match_df, "Joined Pairs vs Odds Match Audit 2024"), encoding="utf-8"
)

# Step 4
root_rows = []

all_joined_dates = sorted(set(joined_races["race_date"].dropna().dt.normalize()))
for d in all_joined_dates:
    d_joined = joined_races[joined_races["race_date"].dt.normalize() == d]
    d_odds = odds[odds["race_date"].dt.normalize() == d]

    if d_odds.empty:
        cause = "no_odds_for_date"
    elif (d_odds["odds_type"] == "place").sum() == 0 and (d_odds["odds_type"] == "place_max").sum() == 0:
        if d_odds["odds_type"].str.contains("place", case=False, na=False).any():
            cause = "odds_type_name_mismatch"
        else:
            cause = "no_place_type"
    elif len(set(d_joined["race_id"].astype(str)) & set(d_odds["race_id"].astype(str))) == 0:
        cause = "race_id_mismatch"
    elif d_odds["odds_snapshot_version"].nunique() > 1:
        cause = "odds_snapshot_version_mismatch"
    elif ((d_odds["odds_type"].isin(["place", "place_max"])) & (d_odds["horse_no"].fillna(-1) < 1)).all():
        cause = "horse_no_invalid"
    elif ((d_odds["odds_type"].isin(["place", "place_max"])) & (d_odds["odds_value"].isna())).all():
        cause = "odds_value_null"
    else:
        cause = "unknown"

    root_rows.append({
        "race_date": d.date().isoformat(),
        "joined_race_count": int(d_joined["race_id"].nunique()),
        "odds_race_count": int(d_odds["race_id"].nunique()),
        "place_rows": int((d_odds["odds_type"] == "place").sum()),
        "place_max_rows": int((d_odds["odds_type"] == "place_max").sum()),
        "cause": cause,
    })

root_df = pd.DataFrame(root_rows)
root_df.to_csv(REPORT_DIR / "odds_place_zero_root_cause_2024.csv", index=False, encoding="utf-8-sig")
(REPORT_DIR / "odds_place_zero_root_cause_2024.md").write_text(
    to_md(root_df, "Odds Place Zero Root Cause 2024"), encoding="utf-8"
)

# Step 5
summary = []
summary.append("# Odds Market Proxy Recovery Plan 2024")
summary.append("")
summary.append("## Findings Summary")
summary.append("")
summary.append(f"- joined_race_count: {len(joined_race_set)}")
summary.append(f"- odds_race_count: {len(odds_race_set)}")
summary.append(f"- matched_race_count: {len(matched)}")
summary.append(f"- place_rows_total: {int((odds['odds_type'] == 'place').sum())}")
summary.append(f"- place_max_rows_total: {int((odds['odds_type'] == 'place_max').sum())}")
summary.append("")
summary.append("## Recommended Minimal Fix")
summary.append("")
summary.append("1. 2024評価用 market proxy の odds 参照キーを joined pairs race_id と同一形式に正規化（DB値は変更しない）。")
summary.append("2. odds_type 判定は `place` / `place_max` を一次、存在しない場合のみ `win` / `wide` proxy を低信頼 fallback として明示分離。")
summary.append("3. odds_snapshot_version は race_date ごとに利用可能な最新を自動選択し、選択結果を監査ログに出力。")
summary.append("4. それでも place系が0日のみ、JV/TARGET からその日の odds 再取得を別バッチで実施（dry-run確認後）。")
summary.append("")
summary.append("## Re-evaluation Readiness")
summary.append("")
summary.append("- 上記1-3の非破壊修正後に 2024 v5 の再評価は実施可能。")
summary.append("- DB更新なしで shadow再計算のみ先行し、market proxy source 構成比が改善した時点でROI再判定を推奨。")

(REPORT_DIR / "odds_market_proxy_recovery_plan_2024.md").write_text("\n".join(summary) + "\n", encoding="utf-8")

print("done")
