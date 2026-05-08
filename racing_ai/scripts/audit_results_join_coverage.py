from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def _load(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit results join coverage for candidate pairs.")
    ap.add_argument("--input-csv", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    df = _load(args.input_csv).copy()
    con = duckdb.connect(str(args.db_path))
    races = con.execute("select race_date::VARCHAR as race_date_db, race_id::VARCHAR as race_id from races").fetchdf()
    res = con.execute("select race_id::VARCHAR as race_id, horse_no, finish_position from results").fetchdf()
    con.close()

    df["race_id"] = df["race_id"].astype(str)
    races["race_id"] = races["race_id"].astype(str)
    m = df.merge(races, on="race_id", how="left")
    if "race_date" in m.columns:
        m["race_date"] = m["race_date"].fillna(m.get("race_date_db"))
    elif "race_date_db" in m.columns:
        m["race_date"] = m["race_date_db"]
    else:
        m["race_date"] = None
    m["race_date"] = m["race_date"].fillna("UNKNOWN")

    out_rows = []
    for d, g in m.groupby("race_date", dropna=False):
        race_ids = set(g["race_id"].astype(str))
        res_sub = res[res["race_id"].astype(str).isin(race_ids)].copy()
        matched_race_ids = set(res_sub["race_id"].astype(str).unique().tolist())
        c_race = len(race_ids)
        r_race = len(set(res_sub["race_id"].astype(str)))
        both = 0
        miss1 = 0
        miss2 = 0
        miss_both = 0
        fmap = {
            (str(r["race_id"]), int(r["horse_no"])): r["finish_position"]
            for _, r in res_sub.iterrows()
            if pd.notna(r["horse_no"])
        }
        for _, row in g.iterrows():
            h1 = row.get("horse1_umaban")
            h2 = row.get("horse2_umaban")
            k1 = (str(row["race_id"]), int(h1)) if pd.notna(h1) else None
            k2 = (str(row["race_id"]), int(h2)) if pd.notna(h2) else None
            f1 = fmap.get(k1) if k1 else None
            f2 = fmap.get(k2) if k2 else None
            ok1 = (f1 is not None) and pd.notna(f1)
            ok2 = (f2 is not None) and pd.notna(f2)
            if ok1 and ok2:
                both += 1
            elif (not ok1) and (not ok2):
                miss_both += 1
            elif not ok1:
                miss1 += 1
            else:
                miss2 += 1
        hit_non_null = both
        out_rows.append(
            {
                "race_date": d,
                "candidate_race_count": c_race,
                "results_race_count": r_race,
                "matched_race_count": len(matched_race_ids),
                "unmatched_candidate_race_count": c_race - len(matched_race_ids),
                "candidate_pair_count": int(len(g)),
                "rows_with_both_horse_results": both,
                "rows_missing_horse1_result": miss1,
                "rows_missing_horse2_result": miss2,
                "rows_missing_both_results": miss_both,
                "actual_wide_hit_non_null_count": hit_non_null,
                "actual_wide_hit_coverage_rate": (hit_non_null / len(g)) if len(g) else None,
            }
        )

    out = pd.DataFrame(out_rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    try:
        tbl = out.to_markdown(index=False)
    except Exception:
        tbl = out.to_string(index=False)
    args.out_md.write_text("# Results Join Coverage Audit\n\n" + tbl, encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
