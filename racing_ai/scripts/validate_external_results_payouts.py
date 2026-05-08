from __future__ import annotations

import argparse
import re
from pathlib import Path
import duckdb
import pandas as pd

HYPHENS = r"[‐‑‒–—―ー−－ｰ]"


def norm_race_id(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip().upper()
    if not s:
        return None
    s = re.sub(HYPHENS, "-", s).replace(" ", "")
    return s


def norm_umaban(v: object) -> int | None:
    try:
        return int(float(v))
    except Exception:
        return None


def norm_pair_key(v: object) -> str | None:
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


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate external results and wide payouts CSVs before backfill.")
    ap.add_argument("--results-csv", type=Path, required=True)
    ap.add_argument("--wide-payouts-csv", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    def read_csv_auto(path: Path) -> pd.DataFrame:
        errs = []
        for enc in ("utf-8", "utf-8-sig", "cp932", "shift_jis"):
            try:
                return pd.read_csv(path, encoding=enc)
            except Exception as e:
                errs.append(f"{enc}:{e}")
        raise RuntimeError(f"failed to read {path}: " + " | ".join(errs))

    res = read_csv_auto(args.results_csv)
    pay = read_csv_auto(args.wide_payouts_csv)

    # TARGET raw exports are often headerless fixed-position rows.
    # If required headers are absent, emit a clear report instead of crashing.
    res_has_named = {"race_id", "umaban"}.issubset(set(res.columns))
    pay_has_named = {"race_id", "bet_type", "bet_key", "payout"}.issubset(set(pay.columns))
    if (not res_has_named) or (not pay_has_named):
        out = pd.DataFrame(
            [
                {
                    "dataset": "results",
                    "row_count": int(len(res)),
                    "required_columns_present": bool(res_has_named),
                    "notes": "Headerless TARGET raw format detected. Convert to normalized schema first.",
                },
                {
                    "dataset": "wide_payouts",
                    "row_count": int(len(pay)),
                    "required_columns_present": bool(pay_has_named),
                    "notes": "Headerless TARGET raw format detected. Convert to normalized schema first.",
                },
            ]
        )
        args.out_csv.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(args.out_csv, index=False, encoding="utf-8")
        try:
            tbl = out.to_markdown(index=False)
        except Exception:
            tbl = out.to_string(index=False)
        args.out_md.write_text(
            "# validate_external_results_payouts\n\n"
            "TARGET生形式（ヘッダなし）を検知したため、正規化前提チェックを中断しました。\n\n"
            + tbl,
            encoding="utf-8",
        )
        print(str(args.out_csv))
        print(str(args.out_md))
        return

    rows: list[dict[str, object]] = []

    # results validation
    req_res = {"race_id", "umaban"}
    has_finish = ("finish_position" in res.columns) or ("finish_pos" in res.columns)
    res_work = res.copy()
    if "finish_position" not in res_work.columns and "finish_pos" in res_work.columns:
        res_work["finish_position"] = res_work["finish_pos"]
    res_work["race_id_norm"] = res_work["race_id"].apply(norm_race_id) if "race_id" in res_work.columns else None
    res_work["umaban_norm"] = res_work["umaban"].apply(norm_umaban) if "umaban" in res_work.columns else None
    res_work["finish_num"] = pd.to_numeric(res_work["finish_position"], errors="coerce") if "finish_position" in res_work.columns else pd.Series(dtype=float)
    res_work["race_date"] = res_work["race_id_norm"].astype(str).str.slice(0, 8).str.replace(r"(\d{4})(\d{2})(\d{2})", r"\1-\2-\3", regex=True)
    res_t = res_work[(res_work["race_date"] >= args.start_date) & (res_work["race_date"] <= args.end_date)].copy()
    if len(res_t):
        res_t["finish_in_natural_range"] = res_t["finish_num"].between(1, 18, inclusive="both")
    else:
        res_t["finish_in_natural_range"] = pd.Series(dtype=bool)

    con = duckdb.connect(str(args.db_path))
    ent = con.execute(
        """
        select e.race_id::VARCHAR as race_id, e.horse_no
        from entries e join races r on r.race_id=e.race_id
        where r.race_date::VARCHAR between ? and ?
        """,
        [args.start_date, args.end_date],
    ).fetchdf()
    db_res = con.execute(
        """
        select rs.race_id::VARCHAR as race_id, rs.horse_no, rs.finish_position
        from results rs join races r on r.race_id=rs.race_id
        where r.race_date::VARCHAR between ? and ?
        """,
        [args.start_date, args.end_date],
    ).fetchdf()
    db_pay = con.execute(
        """
        select p.race_id::VARCHAR as race_id, p.bet_type, p.bet_key, p.payout
        from payouts p join races r on r.race_id=p.race_id
        where r.race_date::VARCHAR between ? and ?
        """,
        [args.start_date, args.end_date],
    ).fetchdf()
    con.close()

    ent["race_id_norm"] = ent["race_id"].apply(norm_race_id)
    ent["horse_no_norm"] = ent["horse_no"].apply(norm_umaban)
    db_res["race_id_norm"] = db_res["race_id"].apply(norm_race_id)
    db_res["horse_no_norm"] = db_res["horse_no"].apply(norm_umaban)
    db_res["finish_num"] = pd.to_numeric(db_res["finish_position"], errors="coerce")

    res_pairs = set(zip(res_t["race_id_norm"], res_t["umaban_norm"]))
    ent_pairs = set(zip(ent["race_id_norm"], ent["horse_no_norm"]))
    common_pairs = res_pairs & ent_pairs
    db_pairs = set(zip(db_res["race_id_norm"], db_res["horse_no_norm"]))
    diff_pairs = res_pairs - db_pairs

    rows.append(
        {
            "dataset": "results",
            "row_count": int(len(res_t)),
            "race_count": int(res_t["race_id_norm"].nunique()),
            "required_columns_present": req_res.issubset(res.columns) and has_finish,
            "race_id_normalizable": float(res_t["race_id_norm"].notna().mean()) if len(res_t) else None,
            "umaban_normalizable": float(res_t["umaban_norm"].notna().mean()) if len(res_t) else None,
            "finish_position_normalizable": float(res_t["finish_num"].notna().mean()) if len(res_t) else None,
            "finish_position_natural_range_rate": float(res_t["finish_in_natural_range"].mean()) if len(res_t) else None,
            "invalid_finish_position_count": int((~res_t["finish_in_natural_range"].fillna(False)).sum()) if len(res_t) else 0,
            "duplicate_race_umaban_count": int(res_t.duplicated(subset=["race_id_norm", "umaban_norm"]).sum()),
            "entries_race_id_match_rate": float(res_t["race_id_norm"].isin(set(ent["race_id_norm"])).mean()) if len(res_t) else None,
            "entries_umaban_pair_match_rate": (len(common_pairs) / len(res_pairs)) if len(res_pairs) else None,
            "existing_db_diff_count": int(len(diff_pairs)),
            "backfill_possible_count": int((res_t["finish_num"].notna()).sum()),
        }
    )

    # payouts validation
    req_pay = {"race_id", "bet_type", "bet_key", "payout"}
    pay_work = pay.copy()
    pay_work["race_id_norm"] = pay_work["race_id"].apply(norm_race_id) if "race_id" in pay_work.columns else None
    pay_work["bet_key_norm"] = pay_work["bet_key"].apply(norm_pair_key) if "bet_key" in pay_work.columns else None
    pay_work["payout_num"] = pd.to_numeric(pay_work["payout"], errors="coerce") if "payout" in pay_work.columns else pd.Series(dtype=float)
    pay_work["is_wide"] = pay_work["bet_type"].astype(str).str.upper().eq("WIDE") | pay_work["bet_type"].astype(str).str.contains("ワイド", na=False)
    pay_work["race_date"] = pay_work["race_id_norm"].astype(str).str.slice(0, 8).str.replace(r"(\d{4})(\d{2})(\d{2})", r"\1-\2-\3", regex=True)
    pay_t = pay_work[(pay_work["race_date"] >= args.start_date) & (pay_work["race_date"] <= args.end_date)].copy()
    pay_t = pay_t[pay_t["is_wide"]].copy()

    db_pay["race_id_norm"] = db_pay["race_id"].apply(norm_race_id)
    db_pay["bet_key_norm"] = db_pay["bet_key"].apply(norm_pair_key)
    db_pay["is_wide"] = db_pay["bet_type"].astype(str).str.upper().eq("WIDE") | db_pay["bet_type"].astype(str).str.contains("ワイド", na=False)
    db_pay_w = db_pay[db_pay["is_wide"]].copy()
    db_keys = set(zip(db_pay_w["race_id_norm"], db_pay_w["bet_key_norm"]))
    src_keys = set(zip(pay_t["race_id_norm"], pay_t["bet_key_norm"]))
    diff_pay = src_keys - db_keys

    rows.append(
        {
            "dataset": "wide_payouts",
            "row_count": int(len(pay_t)),
            "race_count": int(pay_t["race_id_norm"].nunique()),
            "required_columns_present": req_pay.issubset(pay.columns),
            "race_id_normalizable": float(pay_t["race_id_norm"].notna().mean()) if len(pay_t) else None,
            "bet_type_wide_rate": float(pay_t["is_wide"].mean()) if len(pay_t) else None,
            "bet_key_normalizable": float(pay_t["bet_key_norm"].notna().mean()) if len(pay_t) else None,
            "duplicate_race_bet_key_count": int(pay_t.duplicated(subset=["race_id_norm", "bet_key_norm"]).sum()),
            "payout_numeric_coverage": float(pay_t["payout_num"].notna().mean()) if len(pay_t) else None,
            "existing_db_diff_count": int(len(diff_pay)),
            "backfill_possible_count": int((pay_t["bet_key_norm"].notna() & pay_t["payout_num"].notna()).sum()),
        }
    )

    out = pd.DataFrame(rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    try:
        tbl = out.to_markdown(index=False)
    except Exception:
        tbl = out.to_string(index=False)
    anomaly_lines = []
    if len(res_t):
        rr = res_t.groupby("race_id_norm").agg(
            top3_count=("finish_num", lambda s: int(((s >= 1) & (s <= 3)).sum())),
            dup_race_umaban=("umaban_norm", lambda s: int(s.duplicated().sum())),
            invalid_finish=("finish_in_natural_range", lambda s: int((~s.fillna(False)).sum())),
        ).reset_index()
        bad = rr[(rr["top3_count"] != 3) | (rr["dup_race_umaban"] > 0) | (rr["invalid_finish"] > 0)].copy()
        anomaly_lines.append(f"- anomalous_race_count: {len(bad)}")
        if len(bad):
            anomaly_lines.append("- anomalous_race_ids (sample up to 30): " + ", ".join(bad["race_id_norm"].astype(str).head(30).tolist()))
    args.out_md.write_text(
        "# validate_external_results_payouts\n\n"
        + tbl
        + ("\n\n## Results Anomalies\n" + "\n".join(anomaly_lines) if anomaly_lines else ""),
        encoding="utf-8",
    )
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
