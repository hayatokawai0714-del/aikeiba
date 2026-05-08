from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


VENUE_MAP = {
    "\u672d\u5e4c": "SAP",  # 札幌
    "\u51fd\u9928": "HAK",  # 函館
    "\u798f\u5cf6": "FUK",  # 福島
    "\u65b0\u6f5f": "NIG",  # 新潟
    "\u6771\u4eac": "TOK",  # 東京
    "\u4e2d\u5c71": "NAK",  # 中山
    "\u4e2d\u4eac": "CHU",  # 中京
    "\u4eac\u90fd": "KYO",  # 京都
    "\u962a\u795e": "HAN",  # 阪神
    "\u5c0f\u5009": "KOK",  # 小倉
}


def _to_int(v) -> int | None:
    try:
        if pd.isna(v):
            return None
        return int(float(v))
    except Exception:
        return None


def _race_id_from_row(row: pd.Series) -> tuple[str, str]:
    yy = _to_int(row.iloc[0])
    mm = _to_int(row.iloc[1])
    dd = _to_int(row.iloc[2])
    venue_jp = str(row.iloc[4]).strip()
    race_no = _to_int(row.iloc[6])
    if None in (yy, mm, dd, race_no):
        raise ValueError("date/race fields missing")
    year = 2000 + yy if yy < 100 else yy
    race_date = f"{year:04d}-{mm:02d}-{dd:02d}"
    venue_code = VENUE_MAP.get(venue_jp, "UNK")
    race_id = f"{year:04d}{mm:02d}{dd:02d}-{venue_code}-{race_no:02d}R"
    return race_date, race_id


def convert_results(raw_path: Path, out_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(raw_path, header=None, encoding="cp932")
    rows: list[dict[str, object]] = []
    for _, r in df.iterrows():
        try:
            race_date, race_id = _race_id_from_row(r)
        except Exception:
            continue
        umaban = _to_int(r.iloc[7])
        finish = _to_int(r.iloc[9])
        horse_name = str(r.iloc[8]).strip()
        horse_id = str(r.iloc[11]).strip() if len(r) > 11 else ""
        status = str(r.iloc[10]).strip() if len(r) > 10 else ""
        # TARGET生出力で 0 や >18 は不正値扱い
        finish_norm = finish if (finish is not None and 1 <= finish <= 18) else None
        rows.append(
            {
                "race_date": race_date,
                "race_id": race_id,
                "umaban": umaban,
                "horse_name": horse_name,
                "finish_position": finish_norm,
                "status": status,
                "source": raw_path.name,
                "horse_id": horse_id,
            }
        )
    out = pd.DataFrame(rows)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False, encoding="utf-8")
    return out


def convert_payouts(raw_path: Path, out_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(raw_path, header=None, encoding="cp932")
    # TARGET payoff A のワイドは固定オフセット(3本):
    # (127,128,129), (131,132,133), (135,136,137)
    wide_triplets = [(127, 128, 129), (131, 132, 133), (135, 136, 137)]
    rows: list[dict[str, object]] = []
    for _, r in df.iterrows():
        try:
            race_date, race_id = _race_id_from_row(r)
        except Exception:
            continue
        for a_idx, b_idx, p_idx in wide_triplets:
            if max(a_idx, b_idx, p_idx) >= len(r):
                continue
            a = _to_int(r.iloc[a_idx])
            b = _to_int(r.iloc[b_idx])
            payout = _to_int(r.iloc[p_idx])
            if a is None or b is None or payout is None or payout <= 0:
                continue
            x, y = sorted((a, b))
            bet_key = f"{x:02d}-{y:02d}"
            rows.append(
                {
                    "race_date": race_date,
                    "race_id": race_id,
                    "bet_type": "WIDE",
                    "bet_key": bet_key,
                    "payout": payout,
                    "source": raw_path.name,
                }
            )
    out = pd.DataFrame(rows).drop_duplicates(subset=["race_id", "bet_type", "bet_key"])
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False, encoding="utf-8")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Convert TARGET raw CSVs (headerless) to normalized external CSVs.")
    ap.add_argument("--target-results-csv", type=Path, required=True)
    ap.add_argument("--target-payoff-csv", type=Path, required=True)
    ap.add_argument("--out-results-csv", type=Path, required=True)
    ap.add_argument("--out-wide-payouts-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    res = convert_results(args.target_results_csv, args.out_results_csv)
    pay = convert_payouts(args.target_payoff_csv, args.out_wide_payouts_csv)

    lines = [
        "# convert_target_raw_to_external_csv",
        f"- input_results: {args.target_results_csv}",
        f"- input_payoff: {args.target_payoff_csv}",
        f"- output_results_rows: {len(res)}",
        f"- output_wide_payout_rows: {len(pay)}",
        f"- output_results_csv: {args.out_results_csv}",
        f"- output_wide_payouts_csv: {args.out_wide_payouts_csv}",
    ]
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out_results_csv))
    print(str(args.out_wide_payouts_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

