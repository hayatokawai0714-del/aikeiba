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
    errs = []
    for enc in ("utf-8", "utf-8-sig", "cp932", "shift_jis"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            errs.append(f"{enc}:{e}")
    raise RuntimeError(f"failed to read {path}: {' | '.join(errs)}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill external odds into odds table (dry-run default, insert-only).")
    ap.add_argument("--external-odds-csv", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--source-name", required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    df = read_csv_auto(args.external_odds_csv)
    req = ["race_id", "race_date", "odds_snapshot_version", "odds_type", "horse_no", "horse_no_a", "horse_no_b", "odds_value"]
    for c in req:
        if c not in df.columns:
            raise SystemExit(f"missing required column: {c}")

    w = df.copy()
    w["race_id"] = w["race_id"].apply(norm_race_id)
    w["race_date"] = pd.to_datetime(w["race_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    w = w[(w["race_date"] >= args.start_date) & (w["race_date"] <= args.end_date)].copy()
    w["horse_no"] = w["horse_no"].apply(to_int)
    w["horse_no_a"] = w["horse_no_a"].apply(to_int)
    w["horse_no_b"] = w["horse_no_b"].apply(to_int)
    w["odds_value"] = pd.to_numeric(w["odds_value"], errors="coerce")
    w["captured_at"] = pd.to_datetime(w["captured_at"], errors="coerce") if "captured_at" in w.columns else pd.NaT
    if "source_version" not in w.columns:
        w["source_version"] = args.source_name

    invalid_key_mask = (
        w["race_id"].isna()
        | w["odds_snapshot_version"].isna()
        | w["odds_type"].isna()
        | w[["horse_no", "horse_no_a", "horse_no_b"]].isna().any(axis=1)
        | w["odds_value"].isna()
    )
    invalid_key_count = int(invalid_key_mask.sum())

    key_cols = ["race_id", "odds_snapshot_version", "odds_type", "horse_no", "horse_no_a", "horse_no_b"]
    duplicate_input_count = int(w.duplicated(subset=key_cols).sum())

    src = w[~invalid_key_mask].drop_duplicates(subset=key_cols, keep="first").copy()

    con = duckdb.connect(str(args.db_path), read_only=(not args.apply))
    con.register("ext_src", src)

    existing = con.execute(
        """
        select s.*
        from ext_src s
        join odds o
          on cast(o.race_id as varchar)=cast(s.race_id as varchar)
         and cast(o.odds_snapshot_version as varchar)=cast(s.odds_snapshot_version as varchar)
         and cast(o.odds_type as varchar)=cast(s.odds_type as varchar)
         and cast(o.horse_no as bigint)=cast(s.horse_no as bigint)
         and cast(o.horse_no_a as bigint)=cast(s.horse_no_a as bigint)
         and cast(o.horse_no_b as bigint)=cast(s.horse_no_b as bigint)
        """
    ).fetchdf()

    con.register("existing_keys", existing[key_cols] if len(existing) else pd.DataFrame(columns=key_cols))
    insert_candidates = con.execute(
        """
        select s.*
        from ext_src s
        left join existing_keys e
          on s.race_id=e.race_id
         and s.odds_snapshot_version=e.odds_snapshot_version
         and s.odds_type=e.odds_type
         and s.horse_no=e.horse_no
         and s.horse_no_a=e.horse_no_a
         and s.horse_no_b=e.horse_no_b
        where e.race_id is null
        """
    ).fetchdf()

    applied = 0
    if args.apply and len(insert_candidates):
        con.register("ins", insert_candidates)
        con.execute(
            """
            insert into odds (race_id, odds_snapshot_version, captured_at, odds_type, horse_no, horse_no_a, horse_no_b, odds_value, source_version)
            select race_id, odds_snapshot_version, captured_at, odds_type, horse_no, horse_no_a, horse_no_b, odds_value, source_version
            from ins
            """
        )
        applied = int(len(insert_candidates))

    con.close()

    odds_type_counts = insert_candidates.groupby("odds_type").size().to_dict() if len(insert_candidates) else {}
    race_date_counts = insert_candidates.groupby("race_date").size().to_dict() if len(insert_candidates) else {}

    out = pd.DataFrame(
        [
            {
                "mode": "apply" if args.apply else "dry_run",
                "insert_candidate_count": int(len(insert_candidates)),
                "skipped_existing_count": int(len(existing)),
                "duplicate_input_count": duplicate_input_count,
                "invalid_key_count": invalid_key_count,
                "odds_type_insert_candidates": str(odds_type_counts),
                "race_date_insert_candidates": str(race_date_counts),
                "applied_insert_count": applied,
            }
        ]
    )

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8-sig")
    args.out_md.write_text("# backfill_external_odds_2024_dryrun\n\n" + out.to_string(index=False), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
