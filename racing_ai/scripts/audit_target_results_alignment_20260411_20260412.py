from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd
import duckdb

VENUE_MAP = {
    "札幌": "SAP",
    "函館": "HAK",
    "福島": "FUK",
    "新潟": "NIG",
    "東京": "TOK",
    "中山": "NAK",
    "中京": "CHU",
    "京都": "KYO",
    "阪神": "HAN",
    "小倉": "KOK",
}

SUSPICIOUS_RACES = [
    "20260411-FUK-10R",
    "20260411-HAN-04R",
    "20260411-NAK-05R",
    "20260412-FUK-04R",
    "20260412-NAK-02R",
]


def _to_int(v):
    try:
        if pd.isna(v):
            return None
        return int(float(v))
    except Exception:
        return None


def _race_id_from_raw_row(r: pd.Series) -> tuple[str, str]:
    yy = _to_int(r.iloc[0])
    mm = _to_int(r.iloc[1])
    dd = _to_int(r.iloc[2])
    venue_jp = str(r.iloc[4]).strip()
    race_no = _to_int(r.iloc[6])
    if None in (yy, mm, dd, race_no):
        raise ValueError("missing key fields")
    year = 2000 + yy if yy < 100 else yy
    race_date = f"{year:04d}-{mm:02d}-{dd:02d}"
    venue_code = VENUE_MAP.get(venue_jp, "UNK")
    race_id = f"{year:04d}{mm:02d}{dd:02d}-{venue_code}-{race_no:02d}R"
    return race_date, race_id


def _normalize_finish(v):
    n = _to_int(v)
    if n is None:
        return None
    if 1 <= n <= 18:
        return n
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-results-csv", type=Path, required=True)
    ap.add_argument("--external-results-csv", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--start-date", default="2026-04-11")
    ap.add_argument("--end-date", default="2026-04-12")
    ap.add_argument("--out-mapping-csv", type=Path, required=True)
    ap.add_argument("--out-mapping-md", type=Path, required=True)
    ap.add_argument("--out-suspicious-csv", type=Path, required=True)
    ap.add_argument("--out-suspicious-md", type=Path, required=True)
    ap.add_argument("--out-anomaly-csv", type=Path, required=True)
    ap.add_argument("--out-anomaly-md", type=Path, required=True)
    ap.add_argument("--out-raceid-csv", type=Path, required=True)
    ap.add_argument("--out-raceid-md", type=Path, required=True)
    args = ap.parse_args()

    raw = pd.read_csv(args.target_results_csv, header=None, encoding="cp932")
    ext = pd.read_csv(args.external_results_csv, encoding="utf-8")
    ext = ext[(ext["race_date"] >= args.start_date) & (ext["race_date"] <= args.end_date)].copy()
    ext["finish_ext"] = pd.to_numeric(ext["finish_position"], errors="coerce")

    con = duckdb.connect(str(args.db_path))
    db = con.execute(
        """
        select r.race_date::VARCHAR as race_date, rs.race_id::VARCHAR as race_id, rs.horse_no as umaban, rs.finish_position
        from results rs join races r on r.race_id=rs.race_id
        where r.race_date::VARCHAR between ? and ?
        """,
        [args.start_date, args.end_date],
    ).fetchdf()
    con.close()
    db["finish_db"] = pd.to_numeric(db["finish_position"], errors="coerce")
    db["umaban"] = pd.to_numeric(db["umaban"], errors="coerce")

    # raw parsed records
    recs = []
    for _, r in raw.iterrows():
        try:
            race_date, race_id = _race_id_from_raw_row(r)
        except Exception:
            continue
        if not (args.start_date <= race_date <= args.end_date):
            continue
        recs.append(
            {
                "race_date": race_date,
                "race_id": race_id,
                "venue": str(r.iloc[4]).strip(),
                "race_no": _to_int(r.iloc[6]),
                "umaban": _to_int(r.iloc[7]),
                "horse_name": str(r.iloc[8]).strip(),
                "target_raw_finish_position": _to_int(r.iloc[9]),
                "target_raw_abnormal_code": _to_int(r.iloc[10]),
                "raw_finish_normalized": _normalize_finish(r.iloc[9]),
            }
        )
    raw_df = pd.DataFrame(recs)

    # 1) column mapping audit
    map_rows = [
        {"index": 0, "field_name": "year_2digit", "sample": str(raw.iloc[0, 0])},
        {"index": 1, "field_name": "month", "sample": str(raw.iloc[0, 1])},
        {"index": 2, "field_name": "day", "sample": str(raw.iloc[0, 2])},
        {"index": 3, "field_name": "kaiji", "sample": str(raw.iloc[0, 3])},
        {"index": 4, "field_name": "venue_jp", "sample": str(raw.iloc[0, 4])},
        {"index": 5, "field_name": "nichiji", "sample": str(raw.iloc[0, 5])},
        {"index": 6, "field_name": "race_no", "sample": str(raw.iloc[0, 6])},
        {"index": 7, "field_name": "umaban", "sample": str(raw.iloc[0, 7])},
        {"index": 8, "field_name": "horse_name", "sample": str(raw.iloc[0, 8])},
        {"index": 9, "field_name": "finish_position_raw", "sample": str(raw.iloc[0, 9])},
        {"index": 10, "field_name": "abnormal_code", "sample": str(raw.iloc[0, 10])},
        {"index": 11, "field_name": "horse_id", "sample": str(raw.iloc[0, 11])},
    ]
    mapping_df = pd.DataFrame(map_rows)
    args.out_mapping_csv.parent.mkdir(parents=True, exist_ok=True)
    mapping_df.to_csv(args.out_mapping_csv, index=False, encoding="utf-8")
    try:
        mapping_tbl = mapping_df.to_markdown(index=False)
    except Exception:
        mapping_tbl = mapping_df.to_string(index=False)
    args.out_mapping_md.write_text("# target_results_column_mapping_audit\n\n" + mapping_tbl, encoding="utf-8")

    # 2) suspicious races side-by-side
    sraw = raw_df[raw_df["race_id"].isin(SUSPICIOUS_RACES)].copy()
    sext = ext[ext["race_id"].isin(SUSPICIOUS_RACES)][["race_id", "race_date", "umaban", "horse_name", "finish_position"]].copy()
    sext = sext.rename(columns={"finish_position": "external_finish_position"})
    sdb = db[db["race_id"].isin(SUSPICIOUS_RACES)][["race_id", "race_date", "umaban", "finish_position"]].copy()
    sdb = sdb.rename(columns={"finish_position": "db_finish_position"})
    s = sraw.merge(sext, on=["race_id", "race_date", "umaban"], how="outer").merge(sdb, on=["race_id", "race_date", "umaban"], how="outer")
    s["mismatch_reason"] = ""
    s.loc[pd.to_numeric(s["target_raw_finish_position"], errors="coerce").isna(), "mismatch_reason"] = "raw_finish_missing_or_non_numeric"
    s.loc[(pd.to_numeric(s["target_raw_finish_position"], errors="coerce").notna()) & (~pd.to_numeric(s["target_raw_finish_position"], errors="coerce").between(1, 18)), "mismatch_reason"] = "raw_finish_out_of_range"
    s.loc[
        (pd.to_numeric(s["external_finish_position"], errors="coerce").notna())
        & (pd.to_numeric(s["db_finish_position"], errors="coerce").notna())
        & (pd.to_numeric(s["external_finish_position"], errors="coerce") != pd.to_numeric(s["db_finish_position"], errors="coerce")),
        "mismatch_reason",
    ] = "db_finish_differs_from_external"
    s.to_csv(args.out_suspicious_csv, index=False, encoding="utf-8")
    try:
        suspicious_tbl = s.head(300).to_markdown(index=False)
    except Exception:
        suspicious_tbl = s.head(300).to_string(index=False)
    args.out_suspicious_md.write_text("# suspicious_results_race_samples\n\n" + suspicious_tbl, encoding="utf-8")

    # 3) anomaly origin
    j = raw_df[["race_id", "race_date", "umaban", "target_raw_finish_position", "raw_finish_normalized"]].merge(
        ext[["race_id", "race_date", "umaban", "finish_position"]].rename(columns={"finish_position": "external_finish_position"}),
        on=["race_id", "race_date", "umaban"],
        how="outer",
    ).merge(
        db[["race_id", "race_date", "umaban", "finish_position"]].rename(columns={"finish_position": "db_finish_position"}),
        on=["race_id", "race_date", "umaban"],
        how="outer",
    )
    j["raw_fin"] = pd.to_numeric(j["target_raw_finish_position"], errors="coerce")
    j["ext_fin"] = pd.to_numeric(j["external_finish_position"], errors="coerce")
    j["db_fin"] = pd.to_numeric(j["db_finish_position"], errors="coerce")

    def origin(row) -> str:
        raw_valid = pd.notna(row["raw_fin"]) and 1 <= row["raw_fin"] <= 18
        ext_valid = pd.notna(row["ext_fin"]) and 1 <= row["ext_fin"] <= 18
        db_valid = pd.notna(row["db_fin"]) and 1 <= row["db_fin"] <= 18
        if not raw_valid:
            return "TARGET生CSV時点で異常"
        if raw_valid and not ext_valid:
            return "変換時に列ズレで異常"
        if ext_valid and not db_valid:
            return "DB既存値が異常/欠損"
        if ext_valid and db_valid and int(row["ext_fin"]) != int(row["db_fin"]):
            return "DB値不一致"
        return "ok"

    j["anomaly_origin"] = j.apply(origin, axis=1)
    an = j.groupby("anomaly_origin", dropna=False).size().reset_index(name="count")
    an.to_csv(args.out_anomaly_csv, index=False, encoding="utf-8")
    try:
        anomaly_tbl = an.to_markdown(index=False)
    except Exception:
        anomaly_tbl = an.to_string(index=False)
    args.out_anomaly_md.write_text("# finish_position_anomaly_origin_audit\n\n" + anomaly_tbl, encoding="utf-8")

    # 4) race_id mapping audit
    raw_races = raw_df[["race_date", "race_id"]].drop_duplicates()
    db_races = db[["race_date", "race_id"]].drop_duplicates()
    m = raw_races.merge(db_races.assign(in_db=1), on=["race_date", "race_id"], how="left")
    m["in_db"] = m["in_db"].fillna(0).astype(int)
    m.to_csv(args.out_raceid_csv, index=False, encoding="utf-8")
    try:
        unmatched_tbl = m[m["in_db"] == 0].head(200).to_markdown(index=False)
    except Exception:
        unmatched_tbl = m[m["in_db"] == 0].head(200).to_string(index=False)
    lines = [
        "# target_race_id_mapping_audit",
        f"- raw_race_count: {len(raw_races)}",
        f"- db_race_count: {len(db_races)}",
        f"- matched_race_count: {int((m['in_db']==1).sum())}",
        f"- unmatched_race_count: {int((m['in_db']==0).sum())}",
        "",
        unmatched_tbl if int((m["in_db"] == 0).sum()) else "all matched",
    ]
    args.out_raceid_md.write_text("\n".join(lines), encoding="utf-8")

    print(str(args.out_mapping_csv))
    print(str(args.out_mapping_md))
    print(str(args.out_suspicious_csv))
    print(str(args.out_suspicious_md))
    print(str(args.out_anomaly_csv))
    print(str(args.out_anomaly_md))
    print(str(args.out_raceid_csv))
    print(str(args.out_raceid_md))


if __name__ == "__main__":
    main()
