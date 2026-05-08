from __future__ import annotations

import argparse
import itertools
import re
from pathlib import Path
import duckdb
import pandas as pd

HYPHENS = r"[‐‑‒–—―ー−－ｰ]"


def normalize_race_id(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = re.sub(HYPHENS, "-", s).replace(" ", "").upper()
    if any(ch.isalpha() for ch in s):
        return s
    if s.isdigit():
        return s.zfill(12)
    return s


def normalize_umaban(v: object) -> int | None:
    try:
        return int(float(v))
    except Exception:
        return None


def normalize_bet_type(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"wide", "ワイド"}:
        return "wide"
    return s


def normalize_pair_key(v1: object, v2: object | None = None) -> str | None:
    if v2 is not None:
        a = normalize_umaban(v1)
        b = normalize_umaban(v2)
        if a is None or b is None:
            return None
        x, y = sorted((a, b))
        return f"{x:02d}-{y:02d}"
    if v1 is None:
        return None
    s = str(v1).strip()
    s = re.sub(HYPHENS, "-", s).replace(" ", "")
    # allow 0307, 3-7, 03-07, 3,7
    s = s.replace(",", "-").replace("/", "-")
    if "-" not in s and s.isdigit() and len(s) in (2, 3, 4):
        if len(s) <= 2:
            return None
        if len(s) == 3:
            a, b = int(s[0]), int(s[1:])
        else:
            a, b = int(s[:2]), int(s[2:])
        x, y = sorted((a, b))
        return f"{x:02d}-{y:02d}"
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


def _load(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def main() -> None:
    ap = argparse.ArgumentParser(description="Join results/payouts to candidate pairs for wide evaluation.")
    ap.add_argument("--input-csv", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    ap.add_argument("--start-date", default=None)
    ap.add_argument("--end-date", default=None)
    ap.add_argument("--external-results-csv", type=Path, default=None)
    ap.add_argument("--results-source-priority", default="db", help="comma-separated priority: external,db")
    ap.add_argument("--out-diff-csv", type=Path, default=None)
    ap.add_argument("--out-diff-md", type=Path, default=None)
    args = ap.parse_args()

    df = _load(args.input_csv).copy()
    if "race_id" not in df.columns:
        raise SystemExit("missing race_id")
    if "pair_norm" not in df.columns:
        if {"horse1_umaban", "horse2_umaban"}.issubset(df.columns):
            df["pair_norm"] = df.apply(lambda r: normalize_pair_key(r["horse1_umaban"], r["horse2_umaban"]), axis=1)
        else:
            raise SystemExit("missing pair_norm and horse umaban columns")

    if "race_date" in df.columns and args.start_date and args.end_date:
        df["race_date"] = df["race_date"].astype(str)
        df = df[(df["race_date"] >= args.start_date) & (df["race_date"] <= args.end_date)].copy()

    df["race_id"] = df["race_id"].apply(normalize_race_id)
    df["pair_norm"] = df["pair_norm"].apply(normalize_pair_key)
    if "horse1_umaban" in df.columns:
        df["horse1_umaban"] = df["horse1_umaban"].apply(normalize_umaban)
    if "horse2_umaban" in df.columns:
        df["horse2_umaban"] = df["horse2_umaban"].apply(normalize_umaban)
    df["bet_key"] = df["pair_norm"]

    race_ids = [x for x in df["race_id"].dropna().astype(str).unique().tolist() if x]
    if not race_ids:
        raise SystemExit("no valid race_id")

    con = duckdb.connect(str(args.db_path))
    qmarks = ",".join(["?"] * len(race_ids))
    res = con.execute(
        f"""
        SELECT r.race_date::VARCHAR AS race_date, r.race_id::VARCHAR AS race_id, rs.horse_no, rs.finish_position
        FROM results rs
        JOIN races r ON r.race_id=rs.race_id
        WHERE r.race_id IN ({qmarks})
        """,
        race_ids,
    ).fetchdf()
    pay = con.execute(
        f"""
        SELECT r.race_date::VARCHAR AS race_date, r.race_id::VARCHAR AS race_id, p.bet_type, p.bet_key, p.payout
        FROM payouts p
        JOIN races r ON r.race_id=p.race_id
        WHERE r.race_id IN ({qmarks})
        """,
        race_ids,
    ).fetchdf()
    con.close()

    res["race_id"] = res["race_id"].apply(normalize_race_id)
    res["horse_no"] = res["horse_no"].apply(normalize_umaban)
    res["finish_position_num"] = pd.to_numeric(res["finish_position"], errors="coerce")
    res["db_status"] = res["finish_position_num"].apply(lambda x: "ok" if pd.notna(x) and 1 <= float(x) <= 18 else "invalid")

    pay["race_id"] = pay["race_id"].apply(normalize_race_id)
    pay["bet_type_norm"] = pay["bet_type"].apply(normalize_bet_type)
    pay = pay[pay["bet_type_norm"] == "wide"].copy()
    pay["bet_key_norm"] = pay["bet_key"].apply(normalize_pair_key)
    pay["wide_payout"] = pd.to_numeric(pay["payout"], errors="coerce")

    # optional external results source
    ext = None
    if args.external_results_csv is not None and args.external_results_csv.exists():
        ext = pd.read_csv(args.external_results_csv)
        if "race_id" not in ext.columns:
            raise SystemExit("external results missing race_id")
        if "umaban" not in ext.columns:
            raise SystemExit("external results missing umaban")
        if "finish_position" not in ext.columns and "finish_pos" in ext.columns:
            ext["finish_position"] = ext["finish_pos"]
        if "finish_position" not in ext.columns:
            raise SystemExit("external results missing finish_position/finish_pos")
        ext["race_id"] = ext["race_id"].apply(normalize_race_id)
        ext["horse_no"] = ext["umaban"].apply(normalize_umaban)
        ext["finish_position_num"] = pd.to_numeric(ext["finish_position"], errors="coerce")
        ext["external_status"] = ext["finish_position_num"].apply(lambda x: "ok" if pd.notna(x) and 1 <= float(x) <= 18 else "invalid")
        if "race_date" in ext.columns and args.start_date and args.end_date:
            ext["race_date"] = ext["race_date"].astype(str)
            ext = ext[(ext["race_date"] >= args.start_date) & (ext["race_date"] <= args.end_date)].copy()
        ext = ext[ext["race_id"].isin(race_ids)].copy()

    # resolve preferred result source row by row
    priority = [x.strip().lower() for x in str(args.results_source_priority).split(",") if x.strip()]
    if not priority:
        priority = ["db"]
    db_map = {
        (str(r["race_id"]), int(r["horse_no"])): (r["finish_position_num"], r.get("db_status", "invalid"))
        for _, r in res.dropna(subset=["horse_no"]).iterrows()
    }
    ext_map = {}
    if ext is not None:
        ext_map = {
            (str(r["race_id"]), int(r["horse_no"])): (r["finish_position_num"], r.get("external_status", "invalid"))
            for _, r in ext.dropna(subset=["horse_no"]).iterrows()
        }

    if args.out_diff_csv is not None or args.out_diff_md is not None:
        keys = sorted(set(db_map.keys()) | set(ext_map.keys()))
        diff_rows = []
        same = mismatch = external_only = db_only = external_invalid = db_invalid = 0
        external_valid = db_valid = external_preferred = db_fallback = 0
        for k in keys:
            rid, um = k
            ev, es = ext_map.get(k, (pd.NA, "missing"))
            dv, ds = db_map.get(k, (pd.NA, "missing"))
            ev_ok = pd.notna(ev) and 1 <= float(ev) <= 18
            dv_ok = pd.notna(dv) and 1 <= float(dv) <= 18
            if ev_ok:
                external_valid += 1
            if dv_ok:
                db_valid += 1
            if ev_ok and dv_ok and int(float(ev)) == int(float(dv)):
                diff_type = "same"; same += 1
            elif ev_ok and dv_ok:
                diff_type = "mismatch"; mismatch += 1
            elif ev_ok and not dv_ok:
                diff_type = "external_only"; external_only += 1
            elif dv_ok and not ev_ok:
                diff_type = "db_only"; db_only += 1
            elif (not ev_ok) and (es != "missing"):
                diff_type = "external_invalid"; external_invalid += 1
            else:
                diff_type = "db_invalid"; db_invalid += 1

            chosen = None
            for src in priority:
                if src == "external" and ev_ok:
                    chosen = "external"; break
                if src == "db" and dv_ok:
                    chosen = "db"; break
            if chosen == "external":
                external_preferred += 1
            elif chosen == "db":
                db_fallback += 1
            diff_rows.append(
                {
                    "race_id": rid,
                    "umaban": um,
                    "horse_name": pd.NA,
                    "external_finish_position": ev,
                    "db_finish_position": dv,
                    "external_status": es,
                    "db_status": ds,
                    "diff_type": diff_type,
                }
            )
        diff_df = pd.DataFrame(diff_rows)
        if args.out_diff_csv is not None:
            args.out_diff_csv.parent.mkdir(parents=True, exist_ok=True)
            diff_df.to_csv(args.out_diff_csv, index=False, encoding="utf-8")
        if args.out_diff_md is not None:
            summary_lines = [
                "# external_vs_db_results_diff",
                f"- same_count: {same}",
                f"- mismatch_count: {mismatch}",
                f"- external_only_count: {external_only}",
                f"- db_only_count: {db_only}",
                f"- external_invalid_count: {external_invalid}",
                f"- db_invalid_count: {db_invalid}",
                f"- external_valid_count: {external_valid}",
                f"- db_valid_count: {db_valid}",
                f"- external_preferred_count: {external_preferred}",
                f"- db_fallback_count: {db_fallback}",
                "",
            ]
            try:
                tbl = diff_df.head(200).to_markdown(index=False)
            except Exception:
                tbl = diff_df.head(200).to_string(index=False)
            args.out_diff_md.write_text("\n".join(summary_lines) + tbl, encoding="utf-8")

    def _choose_finish(race_id: str, horse_no: int):
        ev, es = ext_map.get((race_id, horse_no), (pd.NA, "missing"))
        dv, ds = db_map.get((race_id, horse_no), (pd.NA, "missing"))
        ev_ok = pd.notna(ev) and 1 <= float(ev) <= 18
        dv_ok = pd.notna(dv) and 1 <= float(dv) <= 18
        src = "none"
        chosen = pd.NA
        for s in priority:
            if s == "external" and ev_ok:
                src = "external"; chosen = ev; break
            if s == "db" and dv_ok:
                src = "db"; chosen = dv; break
        conflict = bool(ev_ok and dv_ok and int(float(ev)) != int(float(dv)))
        return chosen, src, conflict, ev, dv, es, ds

    # race-level quality snapshots from selected source
    selected_rows = []
    all_race_horses = sorted(set((str(x[0]), int(x[1])) for x in (set(db_map.keys()) | set(ext_map.keys()))))
    for rid, hn in all_race_horses:
        fin, src_used, conf, ev, dv, es, ds = _choose_finish(rid, hn)
        selected_rows.append(
            {
                "race_id": rid,
                "horse_no": hn,
                "finish_position_num": fin,
                "result_source_used": src_used,
                "result_source_conflict": conf,
                "external_finish_position": ev,
                "db_finish_position": dv,
                "external_status": es,
                "db_status": ds,
            }
        )
    selected_df = pd.DataFrame(selected_rows)
    top3_df = selected_df[selected_df["finish_position_num"].isin([1, 2, 3]) & selected_df["horse_no"].notna()].copy()
    top3_map: dict[str, list[int]] = (
        top3_df.groupby("race_id")["horse_no"].apply(lambda s: sorted(set(int(x) for x in s.dropna().tolist()))).to_dict()
    )

    def _expected_keys(nums: list[int]) -> list[str]:
        if len(nums) != 3:
            return []
        return sorted(normalize_pair_key(a, b) for a, b in itertools.combinations(nums, 2))

    expected_key_map: dict[str, list[str]] = {rid: _expected_keys(nums) for rid, nums in top3_map.items()}
    payout_key_map: dict[str, list[str]] = (
        pay.dropna(subset=["bet_key_norm"]).groupby("race_id")["bet_key_norm"].apply(lambda s: sorted(set(str(x) for x in s.tolist()))).to_dict()
    )
    finish_cov_map: dict[str, float] = (
        selected_df.groupby("race_id")["finish_position_num"].apply(lambda s: float(s.notna().mean()) if len(s) else 0.0).to_dict()
    )

    finish_map: dict[tuple[str, int], float] = {}
    source_map: dict[tuple[str, int], tuple[str, bool, object, object, str, str]] = {}
    for _, r in selected_df.iterrows():
        hn = r["horse_no"]
        if hn is None:
            continue
        k = (str(r["race_id"]), int(hn))
        finish_map[k] = r["finish_position_num"]
        source_map[k] = (
            str(r.get("result_source_used", "none")),
            bool(r.get("result_source_conflict", False)),
            r.get("external_finish_position", pd.NA),
            r.get("db_finish_position", pd.NA),
            str(r.get("external_status", "missing")),
            str(r.get("db_status", "missing")),
        )

    def get_finish(row, idx: int):
        col = f"horse{idx}_umaban"
        if col in row and pd.notna(row[col]):
            hn = normalize_umaban(row[col])
            if hn is None:
                return pd.NA
            return finish_map.get((str(row["race_id"]), hn), pd.NA)
        return pd.NA

    df["horse1_finish_position"] = df.apply(lambda r: get_finish(r, 1), axis=1)
    df["horse2_finish_position"] = df.apply(lambda r: get_finish(r, 2), axis=1)
    def _get_source_meta(row, idx: int):
        col = f"horse{idx}_umaban"
        hn = normalize_umaban(row.get(col))
        if hn is None:
            return ("none", False, pd.NA, pd.NA, "missing", "missing")
        return source_map.get((str(row["race_id"]), hn), ("none", False, pd.NA, pd.NA, "missing", "missing"))
    h1m = df.apply(lambda r: _get_source_meta(r, 1), axis=1)
    h2m = df.apply(lambda r: _get_source_meta(r, 2), axis=1)
    df["result_source_used"] = [
        "external" if ("external" in {a[0], b[0]}) else ("db" if ("db" in {a[0], b[0]}) else "none")
        for a, b in zip(h1m, h2m)
    ]
    df["result_source_conflict"] = [bool(a[1] or b[1]) for a, b in zip(h1m, h2m)]
    df["external_finish_position_h1"] = [a[2] for a in h1m]
    df["external_finish_position_h2"] = [b[2] for b in h2m]
    df["db_finish_position_h1"] = [a[3] for a in h1m]
    df["db_finish_position_h2"] = [b[3] for b in h2m]
    df["external_status_h1"] = [a[4] for a in h1m]
    df["external_status_h2"] = [b[4] for b in h2m]
    df["db_status_h1"] = [a[5] for a in h1m]
    df["db_status_h2"] = [b[5] for b in h2m]
    df["external_finish_position"] = df["external_finish_position_h1"].astype(str) + "|" + df["external_finish_position_h2"].astype(str)
    df["db_finish_position"] = df["db_finish_position_h1"].astype(str) + "|" + df["db_finish_position_h2"].astype(str)
    df["external_status"] = df["external_status_h1"].astype(str) + "|" + df["external_status_h2"].astype(str)
    df["db_status"] = df["db_status_h1"].astype(str) + "|" + df["db_status_h2"].astype(str)

    df["result_top3_count"] = df["race_id"].map(lambda rid: len(top3_map.get(str(rid), [])))
    df["result_expected_wide_keys"] = df["race_id"].map(lambda rid: ",".join(expected_key_map.get(str(rid), [])))
    df["payout_wide_keys"] = df["race_id"].map(lambda rid: ",".join(payout_key_map.get(str(rid), [])))
    df["payout_wide_count"] = df["race_id"].map(lambda rid: len(payout_key_map.get(str(rid), [])))
    df["expected_vs_payout_match"] = df.apply(
        lambda r: set(expected_key_map.get(str(r["race_id"]), [])) == set(payout_key_map.get(str(r["race_id"]), [])),
        axis=1,
    )

    def _quality_status(row) -> str:
        rid = str(row["race_id"])
        if rid not in top3_map:
            return "results_missing"
        if finish_cov_map.get(rid, 0.0) < 1.0:
            return "invalid_finish_position"
        if int(row["result_top3_count"]) != 3:
            return "top3_count_not_3"
        if int(row["payout_wide_count"]) == 0:
            return "payout_wide_missing"
        if int(row["payout_wide_count"]) != 3:
            return "expected_payout_mismatch"
        if not bool(row["expected_vs_payout_match"]):
            return "expected_payout_mismatch"
        return "ok"

    df["result_quality_status"] = df.apply(_quality_status, axis=1)

    def result_status(row) -> str:
        if row["result_quality_status"] != "ok":
            return "result_quality_failed"
        f1 = row["horse1_finish_position"]
        f2 = row["horse2_finish_position"]
        if pd.isna(f1) and pd.isna(f2):
            return "missing_both"
        if pd.isna(f1):
            return "missing_horse1"
        if pd.isna(f2):
            return "missing_horse2"
        if pd.isna(pd.to_numeric([f1], errors="coerce")[0]) or pd.isna(pd.to_numeric([f2], errors="coerce")[0]):
            return "invalid_finish_position"
        return "ok"

    df["result_join_status"] = df.apply(result_status, axis=1)

    def hit_label(row):
        if row["result_quality_status"] != "ok":
            return pd.NA
        keys = expected_key_map.get(str(row["race_id"]), [])
        if not keys:
            return pd.NA
        return 1 if str(row["pair_norm"]) in set(keys) else 0

    df["actual_wide_hit"] = df.apply(hit_label, axis=1)
    # raw hit keeps previous definition to compare filtered coverage
    def raw_hit(row):
        f1 = pd.to_numeric(row["horse1_finish_position"], errors="coerce")
        f2 = pd.to_numeric(row["horse2_finish_position"], errors="coerce")
        if pd.isna(f1) or pd.isna(f2):
            return pd.NA
        return 1 if (int(f1) <= 3 and int(f2) <= 3) else 0
    df["raw_actual_wide_hit"] = df.apply(raw_hit, axis=1)

    pmap = {(str(r["race_id"]), str(r["bet_key_norm"])): r["wide_payout"] for _, r in pay.iterrows() if pd.notna(r["bet_key_norm"])}
    df["wide_payout"] = df.apply(lambda r: pmap.get((str(r["race_id"]), str(r["pair_norm"]))), axis=1)

    def payout_status(row) -> str:
        if pd.isna(row["pair_norm"]):
            return "invalid_bet_key"
        h = row["actual_wide_hit"]
        p = row["wide_payout"]
        if pd.isna(h):
            if pd.notna(p):
                return "payout_but_hit_unknown"
            return "hit_unknown"
        if int(h) == 1 and pd.isna(p):
            return "missing_payout"
        if int(h) == 1 and pd.notna(p):
            return "ok"
        if int(h) == 0:
            return "not_hit"
        return "no_payout_table"

    df["payout_join_status"] = df.apply(payout_status, axis=1)
    df.loc[(pd.to_numeric(df["actual_wide_hit"], errors="coerce") == 0) & (df["wide_payout"].isna()), "wide_payout"] = 0

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out_csv, index=False, encoding="utf-8")

    total = len(df)
    result_ok = int((df["result_join_status"] == "ok").sum())
    payout_ok = int((df["payout_join_status"] == "ok").sum())
    hit_na = int(df["actual_wide_hit"].isna().sum())
    raw_hit_na = int(df["raw_actual_wide_hit"].isna().sum())
    pay_na = int(df["wide_payout"].isna().sum())
    hit_but_pay_missing = int(((pd.to_numeric(df["actual_wide_hit"], errors="coerce") == 1) & df["wide_payout"].isna()).sum())
    pay_but_hit_na = int((df["wide_payout"].notna() & df["actual_wide_hit"].isna()).sum())
    pair_bad = int(df["pair_norm"].isna().sum())
    qok_races = int(df.loc[df["result_quality_status"] == "ok", "race_id"].nunique())
    qng_races = int(df.loc[df["result_quality_status"] != "ok", "race_id"].nunique())
    qok_cands = int((df["result_quality_status"] == "ok").sum())
    qng_cands = int((df["result_quality_status"] != "ok").sum())
    raw_cov = 1.0 - (raw_hit_na / total if total else 0.0)
    q_cov = 1.0 - (hit_na / total if total else 0.0)
    lines = [
        "# JOIN Quality Audit",
        f"- rows: {total}",
        f"- results_join_success_rate: {result_ok/total if total else 0:.4f}",
        f"- payout_join_success_rate(ok/rows): {payout_ok/total if total else 0:.4f}",
        f"- actual_wide_hit_missing_count: {hit_na}",
        f"- wide_payout_missing_count: {pay_na}",
        f"- hit_but_payout_missing_count: {hit_but_pay_missing}",
        f"- payout_present_but_hit_na_count: {pay_but_hit_na}",
        f"- pair_norm_unmatched_or_invalid_count: {pair_bad}",
        f"- raw_actual_wide_hit_coverage: {raw_cov:.6f}",
        f"- quality_filtered_actual_wide_hit_coverage: {q_cov:.6f}",
        f"- quality_ok_race_count: {qok_races}",
        f"- quality_ng_race_count: {qng_races}",
        f"- quality_ok_candidate_count: {qok_cands}",
        f"- quality_ng_candidate_count: {qng_cands}",
    ]
    args.out_md.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
