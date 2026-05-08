from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pandas as pd


def _detect_encoding(path: Path) -> str:
    # Heuristic: try UTF-8 first, then CP932.
    # TARGET exports are typically CP932 without header.
    for enc in ("utf-8", "cp932"):
        try:
            with path.open("r", encoding=enc, errors="strict") as f:
                f.read(4096)
            return enc
        except Exception:
            continue
    return "unknown"


def _count_rows(path: Path, encoding: str) -> int | None:
    try:
        # Headerless; count lines quickly.
        with path.open("r", encoding=encoding, errors="ignore") as f:
            return sum(1 for _ in f)
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Inventory TARGET seasonal exports (results + payoff-A).")
    ap.add_argument("--input-dir", type=Path, required=True)
    ap.add_argument("--years", default="2026,2025,2024,2023,2022,2021")
    ap.add_argument("--seasons", default="spring,summer,autumn")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    years = [y.strip() for y in str(args.years).split(",") if y.strip()]
    seasons = [s.strip() for s in str(args.seasons).split(",") if s.strip()]

    rows = []
    for y in years:
        for s in seasons:
            res_name = f"target_results_{y}_{s}.csv"
            pay_name = f"target_payoff_a_{y}_{s}.csv"
            res_path = args.input_dir / res_name
            pay_path = args.input_dir / pay_name
            res_exists = res_path.exists()
            pay_exists = pay_path.exists()
            res_size = res_path.stat().st_size if res_exists else None
            pay_size = pay_path.stat().st_size if pay_exists else None
            res_enc = _detect_encoding(res_path) if res_exists else None
            pay_enc = _detect_encoding(pay_path) if pay_exists else None
            res_rows = _count_rows(res_path, res_enc or "utf-8") if res_exists else None
            pay_rows = _count_rows(pay_path, pay_enc or "utf-8") if pay_exists else None

            notes = ""
            if res_exists and res_rows == 0:
                notes += "results_empty;"
            if pay_exists and pay_rows == 0:
                notes += "payoff_empty;"

            pair_complete = bool(res_exists and pay_exists)

            rows.append(
                {
                    "year": y,
                    "season": s,
                    "results_file_exists": bool(res_exists),
                    "payoff_file_exists": bool(pay_exists),
                    "results_file_path": str(res_path),
                    "payoff_file_path": str(pay_path),
                    "results_file_size": res_size,
                    "payoff_file_size": pay_size,
                    "results_row_count": res_rows,
                    "payoff_row_count": pay_rows,
                    "pair_complete": pair_complete,
                    "results_encoding_detected": res_enc,
                    "payoff_encoding_detected": pay_enc,
                    "notes": notes.rstrip(";"),
                }
            )

    out_df = pd.DataFrame(rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.out_csv, index=False, encoding="utf-8")

    complete = out_df[out_df["pair_complete"] == True]  # noqa: E712
    missing = out_df[out_df["pair_complete"] == False]  # noqa: E712

    md_lines = [
        "# TARGET External File Inventory",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- input_dir: {args.input_dir}",
        "",
        "## Summary",
        "",
        f"- total_pairs_expected: {len(out_df)}",
        f"- pair_complete: {len(complete)}",
        f"- missing_any: {len(missing)}",
        "",
        "## Output",
        "",
        f"- csv: {args.out_csv}",
        "",
    ]
    args.out_md.write_text("\n".join(md_lines), encoding="utf-8")

    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

