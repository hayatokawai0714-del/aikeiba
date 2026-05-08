from __future__ import annotations

import argparse
import itertools
from pathlib import Path

import duckdb
import pandas as pd


def norm_pair(a: int, b: int) -> str:
    x, y = sorted((int(a), int(b)))
    return f"{x:02d}-{y:02d}"


def parse_pair(v: object) -> tuple[int, int] | None:
    if v is None or pd.isna(v):
        return None
    s = str(v).strip().replace(",", "-").replace("_", "-").replace(" ", "")
    if "-" not in s and s.isdigit():
        if len(s) == 4:
            s = f"{s[:2]}-{s[2:]}"
        elif len(s) == 3:
            s = f"{s[0]}-{s[1:]}"
    parts = s.split("-")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit consistency among results-based hit keys, payout keys and candidate actual hit keys.")
    ap.add_argument("--pairs-csv", type=Path, required=True)
    ap.add_argument("--external-wide-csv", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--out-race-csv", type=Path, required=True)
    ap.add_argument("--out-race-md", type=Path, required=True)
    ap.add_argument("--out-mismatch-csv", type=Path, required=True)
    ap.add_argument("--out-mismatch-md", type=Path, required=True)
    args = ap.parse_args()

    pairs = pd.read_csv(args.pairs_csv)
    if "race_date" not in pairs.columns:
        pairs["race_date"] = pairs["race_id"].astype(str).str.slice(0, 8).str.replace(r"(\d{4})(\d{2})(\d{2})", r"\1-\2-\3", regex=True)
    pairs = pairs[(pairs["race_date"] >= args.start_date) & (pairs["race_date"] <= args.end_date)].copy()

    ext = pd.read_csv(args.external_wide_csv)
    ext["race_date"] = ext["race_date"].astype(str)
    ext = ext[(ext["race_date"] >= args.start_date) & (ext["race_date"] <= args.end_date)].copy()
    ext["bet_key_norm"] = ext["bet_key"].astype(str).map(lambda s: norm_pair(*parse_pair(s)) if parse_pair(s) else None)
    ext = ext.dropna(subset=["bet_key_norm"]).copy()

    con = duckdb.connect(str(args.db_path))
    res = con.execute(
        """
        select rc.race_date::VARCHAR as race_date, r.race_id::VARCHAR as race_id, r.horse_no, r.finish_position
        from results r join races rc on rc.race_id=r.race_id
        where rc.race_date::VARCHAR between ? and ?
        """,
        [args.start_date, args.end_date],
    ).fetchdf()
    con.close()

    res["finish_position"] = pd.to_numeric(res["finish_position"], errors="coerce")
    res["horse_no"] = pd.to_numeric(res["horse_no"], errors="coerce")
    res_top3 = res[res["finish_position"].isin([1, 2, 3]) & res["horse_no"].notna()].copy()

    race_rows: list[dict[str, object]] = []
    mismatch_rows: list[dict[str, object]] = []

    race_ids = sorted(set(pairs["race_id"].astype(str).tolist()))
    for rid in race_ids:
        p_r = pairs[pairs["race_id"].astype(str) == rid].copy()
        race_date = str(p_r["race_date"].iloc[0]) if len(p_r) else ""
        top3 = sorted(res_top3[res_top3["race_id"].astype(str) == rid]["horse_no"].astype(int).tolist())
        result_expected = sorted({norm_pair(a, b) for a, b in itertools.combinations(top3, 2)}) if len(top3) >= 3 else []
        payout_keys = sorted(ext[ext["race_id"].astype(str) == rid]["bet_key_norm"].dropna().astype(str).unique().tolist())
        hit_rows = p_r[pd.to_numeric(p_r["actual_wide_hit"], errors="coerce") == 1].copy()
        candidate_hit_keys = sorted(hit_rows["pair_norm"].dropna().astype(str).unique().tolist())

        expected_vs_payout_match = set(result_expected) == set(payout_keys) if (len(result_expected) > 0 or len(payout_keys) > 0) else True
        ch_vs_pay = len(set(candidate_hit_keys) & set(payout_keys))
        ch_not_pay = sorted(set(candidate_hit_keys) - set(payout_keys))
        pay_not_ch = sorted(set(payout_keys) - set(candidate_hit_keys))

        status = "ok"
        if not expected_vs_payout_match:
            status = "expected_vs_payout_mismatch"
        if len(ch_not_pay) > 0:
            status = "candidate_hit_mismatch"
        if len(top3) < 3:
            status = "insufficient_top3"

        race_rows.append(
            {
                "race_id": rid,
                "race_date": race_date,
                "result_top3_umaban_list": ",".join(f"{x:02d}" for x in top3),
                "result_expected_wide_keys": ",".join(result_expected),
                "payout_wide_keys": ",".join(payout_keys),
                "candidate_actual_hit_keys": ",".join(candidate_hit_keys),
                "expected_vs_payout_match": bool(expected_vs_payout_match),
                "candidate_hit_vs_payout_match_count": int(ch_vs_pay),
                "candidate_hit_not_in_payout_count": int(len(ch_not_pay)),
                "payout_not_in_candidate_hit_count": int(len(pay_not_ch)),
                "top3_count": int(len(top3)),
                "payout_wide_count": int(len(payout_keys)),
                "candidate_hit_count": int(len(candidate_hit_keys)),
                "status": status,
                "sample_mismatch_keys": ",".join((ch_not_pay + pay_not_ch)[:8]),
            }
        )

        # row-level mismatch audit for candidate actual hit rows
        for _, row in hit_rows.iterrows():
            pair = str(row.get("pair_norm", ""))
            h1 = row.get("horse1_umaban")
            h2 = row.get("horse2_umaban")
            f1 = pd.to_numeric(row.get("horse1_finish_position"), errors="coerce")
            f2 = pd.to_numeric(row.get("horse2_finish_position"), errors="coerce")
            in_result_expected = pair in set(result_expected)
            in_payout = pair in set(payout_keys)
            reason = "ok"
            if pd.isna(f1) or pd.isna(f2):
                reason = "missing_finish_position"
            elif not in_result_expected:
                reason = "hit_but_not_in_result_top3"
            elif not in_payout:
                reason = "hit_but_not_in_payout"
            pp = parse_pair(pair)
            if pp is None:
                reason = "invalid_umaban"
            mismatch_rows.append(
                {
                    "race_id": rid,
                    "race_date": race_date,
                    "pair_norm": pair,
                    "horse1_umaban": h1,
                    "horse2_umaban": h2,
                    "horse1_finish_position": row.get("horse1_finish_position"),
                    "horse2_finish_position": row.get("horse2_finish_position"),
                    "actual_wide_hit": row.get("actual_wide_hit"),
                    "pair_in_result_expected_wide_keys": bool(in_result_expected),
                    "pair_in_payout_wide_keys": bool(in_payout),
                    "mismatch_reason": reason,
                    "payout_join_status": row.get("payout_join_status"),
                    "result_join_status": row.get("result_join_status"),
                    "pair_selected_flag": row.get("pair_selected_flag"),
                    "model_dynamic_selected_flag": row.get("model_dynamic_selected_flag"),
                }
            )

    race_df = pd.DataFrame(race_rows)
    mis_df = pd.DataFrame(mismatch_rows)

    args.out_race_csv.parent.mkdir(parents=True, exist_ok=True)
    race_df.to_csv(args.out_race_csv, index=False, encoding="utf-8")
    mis_df.to_csv(args.out_mismatch_csv, index=False, encoding="utf-8")

    try:
        race_tbl = race_df.head(120).to_markdown(index=False)
    except Exception:
        race_tbl = race_df.head(120).to_string(index=False)
    args.out_race_md.write_text(
        "\n".join(
            [
                f"# actual_wide_hit_consistency_audit_{args.start_date}_{args.end_date}",
                "",
                f"- race_count: {len(race_df)}",
                f"- expected_vs_payout_match_races: {int(race_df['expected_vs_payout_match'].sum()) if len(race_df) else 0}",
                f"- candidate_hit_not_in_payout_total: {int(race_df['candidate_hit_not_in_payout_count'].sum()) if len(race_df) else 0}",
                "",
                race_tbl,
            ]
        ),
        encoding="utf-8",
    )

    try:
        mis_tbl = mis_df.head(200).to_markdown(index=False)
    except Exception:
        mis_tbl = mis_df.head(200).to_string(index=False)
    args.out_mismatch_md.write_text(
        "\n".join(
            [
                f"# actual_wide_hit_mismatch_rows_{args.start_date}_{args.end_date}",
                "",
                f"- rows: {len(mis_df)}",
                "",
                mis_tbl,
            ]
        ),
        encoding="utf-8",
    )

    print(str(args.out_race_csv))
    print(str(args.out_race_md))
    print(str(args.out_mismatch_csv))
    print(str(args.out_mismatch_md))


if __name__ == "__main__":
    main()

