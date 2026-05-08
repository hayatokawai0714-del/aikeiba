from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import duckdb
import pandas as pd


REQUIRED_COLUMNS = ["race_id", "race_date", "bet_type", "bet_key", "payout", "source_version"]


def _normalize_pair_key(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    parts = s.split("-")
    if len(parts) != 2:
        return None
    try:
        a = int(parts[0])
        b = int(parts[1])
    except Exception:
        return None
    x, y = sorted((a, b))
    return f"{x:02d}-{y:02d}"


def _load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8")


def run(
    *,
    db_path: Path,
    input_csv: Path,
    on_conflict: str,
    out_report_json: Path | None,
    dry_run: bool,
) -> dict:
    if not input_csv.exists():
        raise FileNotFoundError(f"input csv not found: {input_csv}")

    df = _load_csv(input_csv)
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"missing required columns: {missing_cols}")

    work = df.copy()
    work["bet_type"] = work["bet_type"].astype(str).str.strip().str.lower()
    work = work[work["bet_type"] == "wide"].copy()
    work["bet_key"] = work["bet_key"].apply(_normalize_pair_key)
    work["race_id"] = work["race_id"].astype(str).str.strip()
    work["source_version"] = work["source_version"].astype(str).str.strip()
    work["race_date"] = pd.to_datetime(work["race_date"], errors="coerce").dt.date
    work["payout"] = pd.to_numeric(work["payout"], errors="coerce")
    if "popularity" in work.columns:
        work["popularity"] = pd.to_numeric(work["popularity"], errors="coerce").astype("Int64")
    else:
        work["popularity"] = pd.Series([pd.NA] * len(work), dtype="Int64")

    invalid_mask = (
        work["race_id"].eq("")
        | work["bet_key"].isna()
        | work["payout"].isna()
        | work["race_date"].isna()
        | work["source_version"].eq("")
    )
    invalid_rows = int(invalid_mask.sum())
    clean = work.loc[~invalid_mask, ["race_id", "race_date", "bet_type", "bet_key", "payout", "popularity", "source_version"]].copy()

    before_dedup = len(clean)
    clean = clean.sort_values(["race_id", "bet_key"]).drop_duplicates(subset=["race_id", "bet_type", "bet_key"], keep="last")
    input_duplicate_dropped = before_dedup - len(clean)

    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS payouts (
          race_id VARCHAR NOT NULL,
          bet_type VARCHAR NOT NULL,
          bet_key VARCHAR NOT NULL,
          payout DOUBLE,
          popularity INTEGER,
          ingested_at TIMESTAMP DEFAULT now(),
          source_version VARCHAR,
          PRIMARY KEY (race_id, bet_type, bet_key)
        )
        """
    )

    con.register("tmp_wide_payouts", clean)
    conflict_df = con.execute(
        """
        SELECT count(*) AS conflict_count
        FROM tmp_wide_payouts t
        JOIN payouts p
          ON p.race_id=t.race_id
         AND lower(p.bet_type)=lower(t.bet_type)
         AND p.bet_key=t.bet_key
        """
    ).fetchdf()
    conflict_count = int(conflict_df.iloc[0]["conflict_count"])

    inserted = 0
    replaced = 0
    skipped = 0
    skip_insertable_count = int(len(clean) - conflict_count)
    if not dry_run:
        if on_conflict == "replace":
            if conflict_count > 0:
                con.execute(
                    """
                    DELETE FROM payouts p
                    USING tmp_wide_payouts t
                    WHERE p.race_id=t.race_id
                      AND lower(p.bet_type)=lower(t.bet_type)
                      AND p.bet_key=t.bet_key
                    """
                )
                replaced = conflict_count
            con.execute(
                """
                INSERT INTO payouts(race_id, bet_type, bet_key, payout, popularity, source_version)
                SELECT race_id, bet_type, bet_key, payout, popularity, source_version
                FROM tmp_wide_payouts
                """
            )
            inserted = int(len(clean))
        elif on_conflict == "skip":
            con.execute(
                """
                INSERT INTO payouts(race_id, bet_type, bet_key, payout, popularity, source_version)
                SELECT t.race_id, t.bet_type, t.bet_key, t.payout, t.popularity, t.source_version
                FROM tmp_wide_payouts t
                LEFT JOIN payouts p
                  ON p.race_id=t.race_id
                 AND lower(p.bet_type)=lower(t.bet_type)
                 AND p.bet_key=t.bet_key
                WHERE p.race_id IS NULL
                """
            )
            inserted = skip_insertable_count
            skipped = conflict_count
        else:
            raise ValueError(f"unknown on_conflict: {on_conflict}")
    else:
        if on_conflict == "replace":
            inserted = int(len(clean))
            replaced = conflict_count
        elif on_conflict == "skip":
            inserted = int(len(clean) - conflict_count)
            skipped = conflict_count

    post_df = con.execute(
        """
        SELECT
          min(r.race_date) AS min_race_date,
          max(r.race_date) AS max_race_date,
          count(*) AS wide_rows,
          count(distinct p.race_id) AS wide_races,
          count(distinct r.race_date) AS wide_race_dates
        FROM payouts p
        JOIN races r ON r.race_id = p.race_id
        WHERE lower(p.bet_type)='wide'
        """
    ).fetchdf()
    post = post_df.iloc[0].to_dict()
    con.unregister("tmp_wide_payouts")
    con.close()

    result = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "db_path": str(db_path),
        "input_csv": str(input_csv),
        "on_conflict": on_conflict,
        "dry_run": dry_run,
        "input_rows": int(len(df)),
        "wide_rows_after_filter": int(len(work)),
        "invalid_rows": invalid_rows,
        "input_duplicate_dropped": int(input_duplicate_dropped),
        "clean_rows": int(len(clean)),
        "conflict_count": conflict_count,
        "inserted_rows": inserted,
        "replaced_rows": replaced,
        "skipped_conflict_rows": skipped,
        "post_wide_min_race_date": str(post.get("min_race_date")),
        "post_wide_max_race_date": str(post.get("max_race_date")),
        "post_wide_rows": int(post.get("wide_rows") or 0),
        "post_wide_race_count": int(post.get("wide_races") or 0),
        "post_wide_race_date_count": int(post.get("wide_race_dates") or 0),
    }

    if out_report_json is not None:
        out_report_json.parent.mkdir(parents=True, exist_ok=True)
        out_report_json.write_text(pd.Series(result).to_json(force_ascii=False, indent=2), encoding="utf-8")

    return result


def main() -> None:
    ap = argparse.ArgumentParser(description="Import external WIDE payouts CSV into DuckDB payouts table.")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--input-csv", type=Path, required=True)
    ap.add_argument("--on-conflict", choices=["skip", "replace"], default="skip")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--out-report-json", type=Path, default=Path("racing_ai/reports/import_wide_payouts_report.json"))
    args = ap.parse_args()

    res = run(
        db_path=args.db_path,
        input_csv=args.input_csv,
        on_conflict=args.on_conflict,
        out_report_json=args.out_report_json,
        dry_run=args.dry_run,
    )
    print(res)


if __name__ == "__main__":
    main()
