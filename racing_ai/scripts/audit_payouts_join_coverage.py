from __future__ import annotations

import argparse
import re
from pathlib import Path
import duckdb
import pandas as pd

HYPHENS = r"[‐‑‒–—―ー−－ｰ]"


def norm_pair(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    s = re.sub(HYPHENS, "-", s).replace(" ", "").replace(",", "-")
    parts = s.split("-")
    if len(parts) != 2:
        if s.isdigit() and len(s) == 4:
            parts = [s[:2], s[2:]]
        else:
            return None
    try:
        a = int(parts[0]); b = int(parts[1])
    except Exception:
        return None
    x, y = sorted((a, b))
    return f"{x:02d}-{y:02d}"


def _load(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit payouts join coverage for wide candidates.")
    ap.add_argument("--input-csv", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    df = _load(args.input_csv).copy()
    df["race_id"] = df["race_id"].astype(str)
    if "race_date" not in df.columns:
        con = duckdb.connect(str(args.db_path))
        r = con.execute("select race_id::VARCHAR as race_id, race_date::VARCHAR as race_date from races").fetchdf()
        con.close()
        df = df.merge(r, on="race_id", how="left")
    if "pair_norm" not in df.columns:
        raise SystemExit("pair_norm missing")
    df["pair_norm"] = df["pair_norm"].apply(norm_pair)
    hit = pd.to_numeric(df.get("actual_wide_hit"), errors="coerce")

    con = duckdb.connect(str(args.db_path))
    p = con.execute("select r.race_date::VARCHAR as race_date, p.race_id::VARCHAR as race_id, p.bet_type, p.bet_key, p.payout from payouts p join races r on r.race_id=p.race_id").fetchdf()
    con.close()
    p["race_id"] = p["race_id"].astype(str)
    p["bet_type_norm"] = p["bet_type"].astype(str).str.lower()
    p = p[p["bet_type_norm"].isin(["wide", "ワイド"])].copy()
    p["bet_key_norm"] = p["bet_key"].apply(norm_pair)

    rows = []
    for d, g in df.groupby("race_date", dropna=False):
        pday = p[p["race_date"] == d].copy()
        pmap = set(zip(pday["race_id"].astype(str), pday["bet_key_norm"].astype(str)))
        gh = g[pd.to_numeric(g.get("actual_wide_hit"), errors="coerce") == 1].copy()
        gh_keys = list(zip(gh["race_id"].astype(str), gh["pair_norm"].astype(str)))
        matched = sum((k in pmap) for k in gh_keys)
        rows.append(
            {
                "race_date": d,
                "candidate_hit_count": int(len(gh)),
                "payouts_wide_race_count": int(pday["race_id"].nunique()),
                "payouts_wide_row_count": int(len(pday)),
                "matched_payout_count": int(matched),
                "hit_without_payout_count": int(len(gh) - matched),
                "payout_without_hit_count": int(max(0, len(pday) - matched)),
                "payout_join_coverage_rate": (matched / len(gh)) if len(gh) else None,
                "bet_type_values": ",".join(sorted(set(pday["bet_type"].astype(str).unique().tolist()))),
                "sample_unmatched_hit_keys": ",".join([f"{a}:{b}" for (a, b) in gh_keys if (a, b) not in pmap][:5]),
                "sample_payout_keys": ",".join([f"{str(rid)}:{str(key)}" for rid, key in list(pmap)[:5]]),
            }
        )

    out = pd.DataFrame(rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    try:
        tbl = out.to_markdown(index=False)
    except Exception:
        tbl = out.to_string(index=False)
    args.out_md.write_text("# Payouts Join Coverage Audit\n\n" + tbl, encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

