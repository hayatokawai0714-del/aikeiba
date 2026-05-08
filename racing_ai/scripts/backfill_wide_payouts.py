from __future__ import annotations

import argparse
import re
from pathlib import Path
import duckdb
import pandas as pd

HYPHENS = r"[‐‑‒–—―ー−－ｰ]"


def norm_key(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = re.sub(HYPHENS, "-", s).replace(" ", "").replace(",", "-").replace("/", "-").replace("_", "-")
    if "-" not in s and s.isdigit():
        if len(s) == 4:
            s = f"{s[:2]}-{s[2:]}"
        elif len(s) == 3:
            s = f"{s[0]}-{s[1:]}"
    p = s.split("-")
    if len(p) != 2:
        return None
    try:
        a, b = int(p[0]), int(p[1])
    except Exception:
        return None
    x, y = sorted((a, b))
    return f"{x:02d}-{y:02d}"


def _find_input(raw_root: Path, race_date: str) -> Path | None:
    ymd = race_date.replace("-", "")
    cands = [
        raw_root / f"{ymd}_real" / "payouts.csv",
        raw_root / f"{ymd}_real_from_jv" / "payouts.csv",
        raw_root / f"{ymd}_jv_auto" / "payouts.csv",
    ]
    for p in cands:
        if p.exists():
            return p
    return None


def _load_wide_rows(path: Path) -> pd.DataFrame:
    errs = []
    df = None
    for enc in ("utf-8", "utf-8-sig", "cp932", "shift_jis"):
        try:
            df = pd.read_csv(path, encoding=enc)
            break
        except Exception as e:
            errs.append(f"{enc}:{e}")
    if df is None:
        raise RuntimeError(f"{path}: read failed: {' | '.join(errs)}")
    required = {"race_id", "bet_type", "winning_combination", "payout_yen"}
    if not required.issubset(df.columns):
        raise ValueError(f"{path}: missing columns {required - set(df.columns)}")
    df["race_id"] = df["race_id"].astype(str)
    bt = df["bet_type"].astype(str)
    is_wide = bt.str.upper().eq("WIDE") | bt.str.contains("ワイド", na=False)
    meta = bt.isin(["RACE", "KAISAI", "VENUE", "JYO", "KAI"])
    wide = df[is_wide].copy()
    wide["bet_key_norm"] = wide["winning_combination"].apply(norm_key)
    wide["payout_num"] = pd.to_numeric(wide["payout_yen"], errors="coerce")
    wide["is_invalid"] = wide["bet_key_norm"].isna() | wide["payout_num"].isna() | (wide["payout_num"] <= 0)
    return df, wide, int(meta.sum())


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill wide payouts from raw payouts files.")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--raw-root", type=Path, default=Path("racing_ai/data/raw"))
    ap.add_argument("--source-wide-payouts-csv", type=Path, default=None, help="External wide payouts CSV")
    ap.add_argument("--source-name", default="raw_wide_payouts_csv")
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.db_path))
    dates = con.execute(
        """
        select race_date::VARCHAR as race_date
        from races
        where race_date::VARCHAR between ? and ?
        group by 1 order by 1
        """,
        [args.start_date, args.end_date],
    ).fetchdf()["race_date"].astype(str).tolist()

    details: list[dict[str, object]] = []
    inserted_total = 0
    external_df: pd.DataFrame | None = None
    if args.source_wide_payouts_csv is not None:
        errs = []
        ext = None
        for enc in ("utf-8", "utf-8-sig", "cp932", "shift_jis"):
            try:
                ext = pd.read_csv(args.source_wide_payouts_csv, encoding=enc)
                break
            except Exception as e:
                errs.append(f"{enc}:{e}")
        if ext is None:
            raise RuntimeError(f"{args.source_wide_payouts_csv}: read failed: {' | '.join(errs)}")
        required = {"race_id", "bet_type", "bet_key", "payout"}
        if not required.issubset(ext.columns):
            raise SystemExit(f"--source-wide-payouts-csv missing columns: {required - set(ext.columns)}")
        ext["race_id"] = ext["race_id"].astype(str)
        ext["bet_type"] = ext["bet_type"].astype(str)
        external_df = ext
    for d in dates:
        if external_df is not None:
            p = args.source_wide_payouts_csv
            raw = external_df[external_df["race_id"].astype(str).str.startswith(d.replace("-", ""))].copy()
            if raw.empty:
                details.append(
                    {
                        "race_date": d,
                        "source_name": args.source_name,
                        "raw_payouts_csv": str(p),
                        "raw_row_count": 0,
                        "meta_row_count": 0,
                        "wide_row_count": 0,
                        "wide_valid_row_count": 0,
                        "invalid_wide_row_count": 0,
                        "new_insert_candidate_count": 0,
                        "applied_insert_count": 0,
                        "skipped_existing_count": 0,
                        "status": "no_source_rows_for_date",
                    }
                )
                continue
            raw["bet_key_norm"] = raw["bet_key"].apply(norm_key)
            raw["payout_num"] = pd.to_numeric(raw["payout"], errors="coerce")
            bt = raw["bet_type"].astype(str)
            is_wide = bt.str.upper().eq("WIDE") | bt.str.contains("ワイド", na=False)
            meta_count = int(bt.isin(["RACE", "KAISAI", "VENUE", "JYO", "KAI"]).sum())
            wide = raw[is_wide].copy()
            wide["is_invalid"] = wide["bet_key_norm"].isna() | wide["payout_num"].isna() | (wide["payout_num"] <= 0)
        else:
            p = _find_input(args.raw_root, d)
            if p is None:
                details.append(
                    {
                        "race_date": d,
                        "source_name": args.source_name,
                        "raw_payouts_csv": "",
                        "raw_row_count": 0,
                        "meta_row_count": 0,
                        "wide_row_count": 0,
                        "wide_valid_row_count": 0,
                        "invalid_wide_row_count": 0,
                        "new_insert_candidate_count": 0,
                        "applied_insert_count": 0,
                        "skipped_existing_count": 0,
                        "status": "raw_missing",
                    }
                )
                continue
            raw, wide, meta_count = _load_wide_rows(p)

        if p is None:
            details.append(
                {
                    "race_date": d,
                    "source_name": args.source_name,
                    "raw_payouts_csv": "",
                    "raw_row_count": 0,
                    "meta_row_count": 0,
                    "wide_row_count": 0,
                    "wide_valid_row_count": 0,
                    "invalid_wide_row_count": 0,
                    "new_insert_candidate_count": 0,
                    "applied_insert_count": 0,
                    "skipped_existing_count": 0,
                    "status": "raw_missing",
                }
            )
            continue
        wide_valid = wide[~wide["is_invalid"]].copy()
        cur = con.execute(
            """
            select p.race_id::VARCHAR as race_id, upper(cast(p.bet_type as varchar)) as bet_type, p.bet_key
            from payouts p
            join races r on r.race_id=p.race_id
            where r.race_date::VARCHAR=? and (upper(cast(p.bet_type as varchar))='WIDE' or cast(p.bet_type as varchar) like '%ワイド%')
            """,
            [d],
        ).fetchdf()
        cur["bet_key_norm"] = cur["bet_key"].apply(norm_key)
        cur_keys = set(zip(cur["race_id"].astype(str), ["WIDE"] * len(cur), cur["bet_key_norm"].astype(str)))
        wide_valid["key"] = list(zip(wide_valid["race_id"].astype(str), ["WIDE"] * len(wide_valid), wide_valid["bet_key_norm"].astype(str)))
        to_insert = wide_valid[~wide_valid["key"].isin(cur_keys)].copy()
        skipped_existing = int(max(0, len(wide_valid) - len(to_insert)))

        applied = 0
        if args.apply and len(to_insert) > 0:
            ins = to_insert[["race_id", "bet_key_norm", "payout_num"]].copy()
            ins["source_version"] = f"backfill_raw_{d}"
            con.register("src_ins", ins)
            con.execute(
                """
                insert into payouts (race_id, bet_type, bet_key, payout, source_version)
                select race_id, 'WIDE', bet_key_norm, payout_num, source_version
                from src_ins
                """
            )
            applied = int(len(ins))
            inserted_total += applied

        details.append(
            {
                "race_date": d,
                "source_name": args.source_name,
                "raw_payouts_csv": str(p),
                "raw_row_count": int(len(raw)),
                "meta_row_count": int(meta_count),
                "wide_row_count": int(len(wide)),
                "wide_valid_row_count": int(len(wide_valid)),
                "invalid_wide_row_count": int(len(wide) - len(wide_valid)),
                "new_insert_candidate_count": int(len(to_insert)),
                "applied_insert_count": int(applied),
                "skipped_existing_count": skipped_existing,
                "status": "ok",
            }
        )

    con.close()

    out = pd.DataFrame(details)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    mode = "APPLY" if args.apply else "DRY_RUN"
    lines = [
        "# backfill_wide_payouts",
        f"- mode: {mode}",
        f"- dates: {len(dates)}",
        f"- total_new_insert_candidates: {int(out['new_insert_candidate_count'].sum())}",
        f"- total_applied_inserts: {int(inserted_total)}",
        "",
    ]
    try:
        lines.append(out.to_markdown(index=False))
    except Exception:
        lines.append(out.to_string(index=False))
    args.out_md.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
