from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb
import pandas as pd


def norm_race_id(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip().upper()
    if not s:
        return None
    s = re.sub(r"[‐‑‒–—―ーｰ]", "-", s).replace(" ", "")
    return s


def to_int(v):
    try:
        if pd.isna(v):
            return None
        return int(float(v))
    except Exception:
        return None


def read_csv_auto(path: Path) -> pd.DataFrame:
    if path.exists() and path.stat().st_size <= 5:
        return pd.DataFrame()
    errs = []
    for enc in ("utf-8", "utf-8-sig", "cp932", "shift_jis"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            errs.append(f"{enc}:{e}")
    raise RuntimeError(f"failed to read {path}: {' | '.join(errs)}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate external odds CSV before backfill apply.")
    ap.add_argument("--external-odds-csv", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    ap.add_argument("--rejected-csv", type=Path, default=None)
    args = ap.parse_args()

    df = read_csv_auto(args.external_odds_csv)
    req = {
        "race_id",
        "race_date",
        "odds_snapshot_version",
        "odds_type",
        "horse_no",
        "horse_no_a",
        "horse_no_b",
        "odds_value",
    }
    if not req.issubset(df.columns):
        missing = sorted(req - set(df.columns))
        raise SystemExit(f"missing required columns: {missing}")

    work = df.copy()
    work["race_id_norm"] = work["race_id"].apply(norm_race_id)
    work["race_date_norm"] = pd.to_datetime(work["race_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    work["horse_no_i"] = work["horse_no"].apply(to_int)
    work["horse_no_a_i"] = work["horse_no_a"].apply(to_int)
    work["horse_no_b_i"] = work["horse_no_b"].apply(to_int)
    work["odds_value_num"] = pd.to_numeric(work["odds_value"], errors="coerce")

    key_cols = ["race_id_norm", "odds_snapshot_version", "odds_type", "horse_no_i", "horse_no_a_i", "horse_no_b_i"]
    duplicate_key_count = int(work.duplicated(subset=key_cols).sum())

    horse_valid = work["horse_no_i"].between(1, 18) | work["horse_no_i"].eq(-1)
    odds_num = work["odds_value_num"].notna()

    con = duckdb.connect(str(args.db_path), read_only=True)
    ent = con.execute("select distinct race_id::varchar as race_id from entries").fetchdf()
    res = con.execute("select distinct race_id::varchar as race_id from results").fetchdf()
    overlap = con.execute(
        """
        select count(*) from odds o
        join read_csv_auto(?) x
          on cast(o.race_id as varchar)=cast(x.race_id as varchar)
         and cast(o.odds_snapshot_version as varchar)=cast(x.odds_snapshot_version as varchar)
         and cast(o.odds_type as varchar)=cast(x.odds_type as varchar)
         and cast(o.horse_no as bigint)=cast(x.horse_no as bigint)
         and cast(o.horse_no_a as bigint)=cast(x.horse_no_a as bigint)
         and cast(o.horse_no_b as bigint)=cast(x.horse_no_b as bigint)
        """,
        [str(args.external_odds_csv)],
    ).fetchone()[0]
    con.close()

    ent_set = set(ent["race_id"].astype(str))
    res_set = set(res["race_id"].astype(str))

    rows = []
    type_counts = work.groupby("odds_type").size().to_dict()

    rows.append(
        {
            "row_count": int(len(work)),
            "race_count": int(work["race_id_norm"].nunique()),
            "race_date_count": int(work["race_date_norm"].nunique()),
            "odds_type_row_count": str(type_counts),
            "place_rows": int((work["odds_type"] == "place").sum()),
            "place_max_rows": int((work["odds_type"] == "place_max").sum()),
            "horse_no_valid_rate": float(horse_valid.mean()) if len(work) else None,
            "odds_value_numeric_rate": float(odds_num.mean()) if len(work) else None,
            "duplicate_key_count": duplicate_key_count,
            "entries_race_id_match_rate": float(work["race_id_norm"].isin(ent_set).mean()) if len(work) else None,
            "results_race_id_match_rate": float(work["race_id_norm"].isin(res_set).mean()) if len(work) else None,
            "existing_odds_overlap_count": int(overlap),
            "insert_candidate_count": int(max(0, len(work) - overlap - duplicate_key_count)),
            "rejected_reason_counts": "{}",
        }
    )

    if args.rejected_csv is not None and args.rejected_csv.exists():
        rej = read_csv_auto(args.rejected_csv)
        reason_counts = rej["reason"].value_counts().to_dict() if "reason" in rej.columns else {}
        rows[0]["rejected_reason_counts"] = str(reason_counts)

    out = pd.DataFrame(rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8-sig")

    md = ["# validate_external_odds_2024", "", out.to_string(index=False)]
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
