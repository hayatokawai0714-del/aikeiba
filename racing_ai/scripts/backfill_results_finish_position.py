from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import pandas as pd


def _load_raw_results(path: Path) -> pd.DataFrame:
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
    has_finish = ("finish_pos" in df.columns) or ("finish_position" in df.columns)
    req = {"race_id", "umaban"}
    if (not req.issubset(df.columns)) or (not has_finish):
        raise ValueError(f"{path}: missing columns {req - set(df.columns)} or finish_pos/finish_position")
    finish_col = "finish_pos" if "finish_pos" in df.columns else "finish_position"
    out = df[["race_id", "umaban", finish_col]].copy()
    out = out.rename(columns={finish_col: "finish_pos"})
    out["race_id"] = out["race_id"].astype(str)
    out["horse_no"] = pd.to_numeric(out["umaban"], errors="coerce")
    out["finish_position_raw"] = out["finish_pos"]
    out["finish_position_num"] = pd.to_numeric(out["finish_pos"], errors="coerce")
    out["is_invalid_finish"] = out["finish_position_num"].isna() | (out["finish_position_num"] <= 0) | (out["finish_position_num"] > 18)
    out["finish_position"] = out["finish_position_num"].where(~out["is_invalid_finish"], pd.NA)
    out = out.dropna(subset=["horse_no"])
    out["horse_no"] = out["horse_no"].astype(int)
    return out


def _find_raw_file(raw_root: Path, race_date: str) -> Path | None:
    ymd = race_date.replace("-", "")
    candidates = [
        raw_root / f"{ymd}_real" / "results.csv",
        raw_root / f"{ymd}_real_from_jv" / "results.csv",
        raw_root / f"{ymd}_jv_auto" / "results.csv",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill results.finish_position from raw results files.")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--raw-root", type=Path, default=Path("racing_ai/data/raw"))
    ap.add_argument("--source-results-csv", type=Path, default=None, help="External results CSV with race_id/umaban/finish_pos or finish_position")
    ap.add_argument("--source-name", default="raw_results_csv")
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--update-mismatched-valid", action="store_true", help="also update rows where existing valid finish differs from source valid finish")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.db_path))
    races = con.execute(
        """
        select race_date::VARCHAR as race_date
        from races
        where race_date::VARCHAR between ? and ?
        group by 1 order by 1
        """,
        [args.start_date, args.end_date],
    ).fetchdf()["race_date"].astype(str).tolist()

    detail_rows: list[dict[str, object]] = []
    total_apply = 0
    external_df: pd.DataFrame | None = None
    if args.source_results_csv is not None:
        ext = _load_raw_results(args.source_results_csv)
        ext2 = ext.copy()
        if "finish_position" not in ext2.columns and "finish_pos" in ext2.columns:
            ext2 = ext2.rename(columns={"finish_pos": "finish_position"})
        if "finish_position" in ext2.columns and "finish_pos" in ext2.columns:
            ext2 = ext2.drop(columns=["finish_pos"])
        req = {"race_id", "umaban", "finish_position"}
        if not req.issubset(ext2.columns):
            raise SystemExit(f"--source-results-csv missing columns: {req - set(ext2.columns)}")
        ext2["race_id"] = ext2["race_id"].astype(str)
        ext2["horse_no"] = pd.to_numeric(ext2["umaban"], errors="coerce")
        ext2["finish_position_raw"] = ext2["finish_position"]
        ext2["finish_position_num"] = pd.to_numeric(ext2["finish_position"], errors="coerce")
        ext2["is_invalid_finish"] = ext2["finish_position_num"].isna() | (ext2["finish_position_num"] <= 0) | (ext2["finish_position_num"] > 18)
        ext2["finish_position"] = ext2["finish_position_num"].where(~ext2["is_invalid_finish"], pd.NA)
        ext2 = ext2.dropna(subset=["horse_no"])
        ext2["horse_no"] = ext2["horse_no"].astype(int)
        external_df = ext2
    for d in races:
        if external_df is not None:
            raw_path = args.source_results_csv
            raw = external_df[external_df["race_id"].astype(str).str.startswith(d.replace("-", ""))].copy()
            if raw.empty:
                detail_rows.append(
                    {
                        "race_date": d,
                        "source_name": args.source_name,
                        "raw_results_csv": str(raw_path),
                        "raw_row_count": 0,
                        "raw_invalid_finish_count": 0,
                        "raw_valid_finish_count": 0,
                        "candidate_updates_null_only": 0,
                        "candidate_updates_invalid_only": 0,
                        "applied_updates": 0,
                        "skipped_no_change_count": 0,
                        "status": "no_source_rows_for_date",
                    }
                )
                continue
        else:
            raw_path = _find_raw_file(args.raw_root, d)
            if raw_path is None:
                detail_rows.append(
                    {
                        "race_date": d,
                        "source_name": args.source_name,
                        "raw_results_csv": "",
                        "raw_row_count": 0,
                        "raw_invalid_finish_count": 0,
                        "raw_valid_finish_count": 0,
                        "candidate_updates_null_only": 0,
                        "candidate_updates_invalid_only": 0,
                        "applied_updates": 0,
                        "skipped_no_change_count": 0,
                        "status": "raw_missing",
                    }
                )
                continue
            raw = _load_raw_results(raw_path)

        if raw_path is None:
            detail_rows.append(
                {
                    "race_date": d,
                    "source_name": args.source_name,
                    "raw_results_csv": "",
                    "raw_row_count": 0,
                    "raw_invalid_finish_count": 0,
                    "raw_valid_finish_count": 0,
                    "candidate_updates_null_only": 0,
                    "candidate_updates_invalid_only": 0,
                    "applied_updates": 0,
                    "skipped_no_change_count": 0,
                    "status": "raw_missing",
                }
            )
            continue
        src = raw[["race_id", "horse_no", "finish_position"]].dropna(subset=["finish_position"]).drop_duplicates()
        con.register("src_raw", src)
        null_c = con.execute(
            """
            select count(*) as c
            from results r
            join src_raw s on s.race_id=r.race_id and s.horse_no=r.horse_no
            join races rc on rc.race_id=r.race_id
            where rc.race_date::VARCHAR=? and r.finish_position is null
            """,
            [d],
        ).fetchdf().iloc[0]["c"]
        invalid_c = con.execute(
            """
            select count(*) as c
            from results r
            join src_raw s on s.race_id=r.race_id and s.horse_no=r.horse_no
            join races rc on rc.race_id=r.race_id
            where rc.race_date::VARCHAR=? and (r.finish_position<=0 or r.finish_position>18)
            """,
            [d],
        ).fetchdf().iloc[0]["c"]
        mismatch_c = con.execute(
            """
            select count(*) as c
            from results r
            join src_raw s on s.race_id=r.race_id and s.horse_no=r.horse_no
            join races rc on rc.race_id=r.race_id
            where rc.race_date::VARCHAR=?
              and r.finish_position between 1 and 18
              and cast(r.finish_position as INTEGER) <> cast(s.finish_position as INTEGER)
            """,
            [d],
        ).fetchdf().iloc[0]["c"]

        applied = 0
        if args.apply:
            if args.update_mismatched_valid:
                con.execute(
                    """
                    update results r
                    set finish_position = cast(s.finish_position as INTEGER)
                    from src_raw s, races rc
                    where r.race_id=s.race_id and r.horse_no=s.horse_no
                      and rc.race_id=r.race_id and rc.race_date::VARCHAR=?
                      and (
                        r.finish_position is null
                        or r.finish_position<=0
                        or r.finish_position>18
                        or (r.finish_position between 1 and 18 and cast(r.finish_position as INTEGER) <> cast(s.finish_position as INTEGER))
                      )
                    """,
                    [d],
                )
                applied = int(null_c + invalid_c + mismatch_c)
            else:
                con.execute(
                    """
                    update results r
                    set finish_position = cast(s.finish_position as INTEGER)
                    from src_raw s, races rc
                    where r.race_id=s.race_id and r.horse_no=s.horse_no
                      and rc.race_id=r.race_id and rc.race_date::VARCHAR=?
                      and (r.finish_position is null or r.finish_position<=0 or r.finish_position>18)
                    """,
                    [d],
                )
                applied = int(null_c + invalid_c)
            total_apply += applied

        no_change = int(max(0, len(src) - (null_c + invalid_c + (mismatch_c if args.update_mismatched_valid else 0))))
        detail_rows.append(
            {
                "race_date": d,
                "source_name": args.source_name,
                "raw_results_csv": str(raw_path),
                "raw_row_count": int(len(raw)),
                "raw_invalid_finish_count": int(raw["is_invalid_finish"].sum()),
                "raw_valid_finish_count": int((~raw["is_invalid_finish"]).sum()),
                "candidate_updates_null_only": int(null_c),
                "candidate_updates_invalid_only": int(invalid_c),
                "candidate_updates_mismatched_valid": int(mismatch_c),
                "applied_updates": int(applied),
                "skipped_no_change_count": no_change,
                "status": "ok",
            }
        )

    con.close()

    out = pd.DataFrame(detail_rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    mode = "APPLY" if args.apply else "DRY_RUN"
    summary = {
        "mode": mode,
        "dates": len(races),
        "candidate_updates_null_only_total": int(out["candidate_updates_null_only"].sum()),
        "candidate_updates_invalid_only_total": int(out["candidate_updates_invalid_only"].sum()),
        "candidate_updates_mismatched_valid_total": int(out.get("candidate_updates_mismatched_valid", pd.Series(dtype=int)).sum() if len(out) else 0),
        "applied_updates_total": int(total_apply),
    }
    try:
        tbl = out.to_markdown(index=False)
    except Exception:
        tbl = out.to_string(index=False)
    lines = [
        "# backfill_results_finish_position",
        f"- mode: {summary['mode']}",
        f"- dates: {summary['dates']}",
        f"- candidate_updates_null_only_total: {summary['candidate_updates_null_only_total']}",
        f"- candidate_updates_invalid_only_total: {summary['candidate_updates_invalid_only_total']}",
        f"- candidate_updates_mismatched_valid_total: {summary['candidate_updates_mismatched_valid_total']}",
        f"- applied_updates_total: {summary['applied_updates_total']}",
        "",
        tbl,
    ]
    args.out_md.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
