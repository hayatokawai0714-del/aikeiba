from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

VENUE_MAP = {
    "札幌": "SAP",
    "函館": "HAK",
    "福島": "FUK",
    "新潟": "NIG",
    "東京": "TOK",
    "中山": "NAK",
    "中京": "CHU",
    "京都": "KYO",
    "阪神": "HAN",
    "小倉": "KOK",
}

ODDS_TYPE_ALIAS = {
    "place": "place",
    "複勝": "place",
    "複": "place",
    "place_max": "place_max",
    "複勝上限": "place_max",
    "複勝max": "place_max",
    "win": "win",
    "単勝": "win",
    "wide": "wide",
    "ワイド": "wide",
    "wide_max": "wide_max",
    "ワイドmax": "wide_max",
}

HYPHENS = r"[‐‑‒–—―ーｰ]"


def read_csv_auto(path: Path, header: bool) -> pd.DataFrame:
    errs = []
    for enc in ("utf-8", "utf-8-sig", "cp932", "shift_jis"):
        try:
            return pd.read_csv(path, encoding=enc, header=0 if header else None)
        except Exception as e:
            errs.append(f"{enc}:{e}")
    raise RuntimeError(f"failed to read {path}: {' | '.join(errs)}")


def to_int(v) -> int | None:
    try:
        if pd.isna(v):
            return None
        return int(float(v))
    except Exception:
        return None


def to_float(v) -> float | None:
    try:
        if pd.isna(v):
            return None
        s = str(v).strip().replace(",", "")
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def norm_race_id(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip().upper()
    if not s:
        return None
    s = re.sub(HYPHENS, "-", s).replace(" ", "")
    return s


def build_race_id_from_parts(row: pd.Series) -> tuple[str | None, str | None]:
    yy = to_int(row.get("year"))
    mm = to_int(row.get("month"))
    dd = to_int(row.get("day"))
    venue_raw = str(row.get("venue", "")).strip()
    race_no = to_int(row.get("race_no"))
    if None in (yy, mm, dd, race_no):
        return None, None
    year = yy + 2000 if yy < 100 else yy
    venue = VENUE_MAP.get(venue_raw, venue_raw[:3].upper() if venue_raw else "UNK")
    race_date = f"{year:04d}-{mm:02d}-{dd:02d}"
    return race_date, f"{year:04d}{mm:02d}{dd:02d}-{venue}-{race_no:02d}R"


def norm_odds_type(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    key = s.lower()
    if key in ODDS_TYPE_ALIAS:
        return ODDS_TYPE_ALIAS[key]
    return ODDS_TYPE_ALIAS.get(s)


def main() -> None:
    ap = argparse.ArgumentParser(description="Convert TARGET/JV odds CSV to external odds schema.")
    ap.add_argument("--input-csv", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    ap.add_argument("--out-rejected-csv", type=Path, required=True)
    ap.add_argument("--odds-snapshot-version", required=True)
    ap.add_argument("--source-version", default="")
    ap.add_argument("--header", action="store_true", help="Treat input as headered CSV")
    ap.add_argument("--start-date", default="2024-01-06")
    ap.add_argument("--end-date", default="2024-12-28")
    args = ap.parse_args()

    src = read_csv_auto(args.input_csv, header=args.header)
    if not args.header:
        # Headerless mode: TARGET result/payoff style indices to named columns (minimal support)
        # 0:yy,1:mm,2:dd,4:venue,6:race_no,7:horse_no, place:20, place_max:21 (if present)
        c = src.copy()
        renamed = {}
        for idx, name in [(0, "year"), (1, "month"), (2, "day"), (4, "venue"), (6, "race_no"), (7, "horse_no"), (20, "place"), (21, "place_max"), (22, "win")]:
            if idx in c.columns:
                renamed[idx] = name
        c = c.rename(columns=renamed)
        src = c

    rows: list[dict[str, object]] = []
    rejected: list[dict[str, object]] = []

    has_long = {"race_id", "odds_type", "odds_value"}.issubset(set(src.columns))

    for i, r in src.iterrows():
        race_id = norm_race_id(r.get("race_id"))
        race_date = r.get("race_date")
        if pd.notna(race_date):
            race_date = str(race_date)[:10]
        else:
            race_date = None

        if race_id is None:
            rd2, rid2 = build_race_id_from_parts(r)
            race_date = race_date or rd2
            race_id = rid2

        if race_id is None or race_date is None:
            rejected.append({"row_index": i, "reason": "race_id_or_race_date_missing", "raw": str(dict(r))})
            continue

        emitted = 0

        if has_long:
            ot = norm_odds_type(r.get("odds_type"))
            if ot is None:
                rejected.append({"row_index": i, "reason": "invalid_odds_type", "raw": str(dict(r))})
                continue
            h = to_int(r.get("horse_no", -1))
            ha = to_int(r.get("horse_no_a", -1))
            hb = to_int(r.get("horse_no_b", -1))
            val = to_float(r.get("odds_value"))
            if val is None:
                rejected.append({"row_index": i, "reason": "odds_value_not_numeric", "raw": str(dict(r))})
                continue
            if ot in ("place", "place_max", "win"):
                h = h if h is not None else -1
                ha, hb = -1, -1
            elif ot in ("wide", "wide_max"):
                if ha is None or hb is None:
                    rejected.append({"row_index": i, "reason": "wide_pair_missing", "raw": str(dict(r))})
                    continue
                h = -1
            rows.append(
                {
                    "race_id": race_id,
                    "race_date": race_date,
                    "odds_snapshot_version": r.get("odds_snapshot_version") or args.odds_snapshot_version,
                    "odds_type": ot,
                    "horse_no": h,
                    "horse_no_a": ha if ha is not None else -1,
                    "horse_no_b": hb if hb is not None else -1,
                    "odds_value": val,
                    "captured_at": r.get("captured_at") if "captured_at" in src.columns else None,
                    "source_version": r.get("source_version") if "source_version" in src.columns else args.source_version,
                }
            )
            emitted += 1
        else:
            h = to_int(r.get("horse_no"))
            if h is None:
                rejected.append({"row_index": i, "reason": "horse_no_missing", "raw": str(dict(r))})
                continue
            place = to_float(r.get("place"))
            place_max = to_float(r.get("place_max"))
            place_rep = to_float(r.get("place_representative"))
            if place is None and place_rep is not None:
                place = place_rep
            if place_max is None and place is not None:
                place_max = place
            if place is None and place_max is None:
                rejected.append({"row_index": i, "reason": "place_and_place_max_missing", "raw": str(dict(r))})
                continue
            for ot, val in (("place", place), ("place_max", place_max), ("win", to_float(r.get("win")))):
                if val is None:
                    continue
                rows.append(
                    {
                        "race_id": race_id,
                        "race_date": race_date,
                        "odds_snapshot_version": args.odds_snapshot_version,
                        "odds_type": ot,
                        "horse_no": h,
                        "horse_no_a": -1,
                        "horse_no_b": -1,
                        "odds_value": val,
                        "captured_at": r.get("captured_at") if "captured_at" in src.columns else None,
                        "source_version": args.source_version or args.input_csv.name,
                    }
                )
                emitted += 1

        if emitted == 0:
            rejected.append({"row_index": i, "reason": "no_output_generated", "raw": str(dict(r))})

    out = pd.DataFrame(rows)
    if len(out):
        out["race_date"] = pd.to_datetime(out["race_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        out = out[(out["race_date"] >= args.start_date) & (out["race_date"] <= args.end_date)].copy()
        out = out.drop_duplicates(
            subset=["race_id", "odds_snapshot_version", "odds_type", "horse_no", "horse_no_a", "horse_no_b"],
            keep="first",
        )

    rej = pd.DataFrame(rejected)

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8-sig")
    rej.to_csv(args.out_rejected_csv, index=False, encoding="utf-8-sig")

    type_dist = out.groupby("odds_type").size().reset_index(name="row_count") if len(out) else pd.DataFrame(columns=["odds_type", "row_count"])
    date_race = out.groupby("race_date")["race_id"].nunique().reset_index(name="race_count") if len(out) else pd.DataFrame(columns=["race_date", "race_count"])

    lines = [
        "# convert_target_odds_2024",
        f"- input_csv: {args.input_csv}",
        f"- output_rows: {len(out)}",
        f"- rejected_rows: {len(rej)}",
        f"- output_csv: {args.out_csv}",
        f"- rejected_csv: {args.out_rejected_csv}",
        "",
        "## odds_type distribution",
    ]
    lines.append(type_dist.to_string(index=False) if len(type_dist) else "(no rows)")
    lines += ["", "## race_date race_count"]
    lines.append(date_race.to_string(index=False) if len(date_race) else "(no rows)")

    args.out_md.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))
    print(str(args.out_rejected_csv))


if __name__ == "__main__":
    main()
