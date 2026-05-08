from __future__ import annotations

import argparse
import re
from pathlib import Path
import duckdb
import pandas as pd

HYPHENS = r"[‐‑‒–—―ー−－ｰ]"


def normalize_pair_key(v: object) -> str | None:
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
    parts = s.split("-")
    if len(parts) != 2:
        return None
    try:
        a, b = int(parts[0]), int(parts[1])
    except Exception:
        return None
    x, y = sorted((a, b))
    return f"{x:02d}-{y:02d}"


def key_pattern(v: object) -> str:
    if v is None:
        return "null"
    s = str(v).strip()
    if re.fullmatch(r"\d{2}-\d{2}", s):
        return "03-07"
    if re.fullmatch(r"\d-\d+", s):
        return "3-7"
    if re.fullmatch(r"\d{4}", s):
        return "0307"
    if "," in s:
        return "3,7"
    if "_" in s:
        return "03_07"
    return "other"


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit wide payout bet_key quality and matching.")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--input-csv", type=Path, required=True, help="joined candidate csv with actual_wide_hit")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    c = pd.read_csv(args.input_csv)
    c["candidate_key"] = c["pair_norm"].apply(normalize_pair_key)
    c["is_hit"] = pd.to_numeric(c["actual_wide_hit"], errors="coerce") == 1
    c["race_date"] = c["race_id"].astype(str).str.slice(0, 8).str.replace(r"(\d{4})(\d{2})(\d{2})", r"\1-\2-\3", regex=True)

    con = duckdb.connect(str(args.db_path))
    p = con.execute(
        """
        select r.race_date::VARCHAR as race_date, p.race_id::VARCHAR as race_id, p.bet_type, p.bet_key, p.payout
        from payouts p
        join races r on r.race_id=p.race_id
        where r.race_date between '2026-04-10' and '2026-04-12'
        """
    ).fetchdf()
    con.close()

    p["bet_type_str"] = p["bet_type"].astype(str)
    p["is_wide"] = p["bet_type_str"].str.upper().eq("WIDE") | p["bet_type_str"].str.contains("ワイド", na=False)
    pw = p[p["is_wide"]].copy()
    pw["norm_key"] = pw["bet_key"].apply(normalize_pair_key)
    pw["pattern"] = pw["bet_key"].apply(key_pattern)

    rows: list[dict[str, object]] = []
    for d, cg in c.groupby("race_date"):
        pg = pw[pw["race_date"] == d]
        hitg = cg[cg["is_hit"]].copy()
        hit_keys = set(zip(hitg["race_id"].astype(str), hitg["candidate_key"].astype(str)))
        pkeys = set(zip(pg["race_id"].astype(str), pg["norm_key"].astype(str)))
        matched = len([k for k in hit_keys if k in pkeys])
        unmatched = len(hit_keys) - matched
        rows.append(
            {
                "race_date": d,
                "candidate_hit_count": int(len(hitg)),
                "payout_wide_race_count": int(pg["race_id"].nunique()),
                "payout_wide_row_count": int(len(pg)),
                "payout_bet_type_values": ",".join(sorted(p[p["race_date"] == d]["bet_type_str"].dropna().unique().tolist())),
                "payout_key_raw_samples": ",".join(pg["bet_key"].astype(str).head(8).tolist()),
                "payout_key_normalized_samples": ",".join(pg["norm_key"].astype(str).head(8).tolist()),
                "candidate_hit_key_samples": ",".join([f"{a}:{b}" for a, b in list(hit_keys)[:8]]),
                "matched_count": int(matched),
                "unmatched_hit_count": int(unmatched),
                "unmatched_hit_key_samples": ",".join([f"{a}:{b}" for a, b in list(hit_keys - pkeys)[:8]]),
                "payout_keys_not_in_hits_samples": ",".join([f"{a}:{b}" for a, b in list(pkeys - hit_keys)[:8]]),
                "key_format_patterns": ",".join(f"{k}:{v}" for k, v in pg["pattern"].value_counts().to_dict().items()),
            }
        )

    out = pd.DataFrame(rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    try:
        tbl = out.to_markdown(index=False)
    except Exception:
        tbl = out.to_string(index=False)
    args.out_md.write_text("# wide_payout_key_audit\n\n" + tbl, encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

