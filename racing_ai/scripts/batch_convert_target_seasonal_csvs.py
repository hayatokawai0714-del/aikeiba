from __future__ import annotations

import argparse
import datetime as dt
import subprocess
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CONVERTER = ROOT / "scripts" / "convert_target_raw_to_external_csv.py"


def _run(cmd: list[str]) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
        return int(p.returncode), out.strip()
    except Exception as e:
        return 999, f"exception: {e}"


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch convert TARGET seasonal exports into normalized external CSVs.")
    ap.add_argument("--input-dir", type=Path, required=True, help="Directory containing target_results_YYYY_season.csv and target_payoff_a_YYYY_season.csv")
    ap.add_argument("--out-dir", type=Path, required=True, help="Output directory for converted external CSVs")
    ap.add_argument("--years", default="2026,2025,2024,2023,2022,2021")
    ap.add_argument("--seasons", default="spring,summer,autumn")
    ap.add_argument("--python", default=None, help="Optional python executable (default: current interpreter)")
    ap.add_argument("--write-union", action="store_true", help="Also write union CSVs across all converted pairs")
    ap.add_argument("--union-results-name", default="external_results_2021_2026_seasonal.csv")
    ap.add_argument("--union-wide-payouts-name", default="external_wide_payouts_2021_2026_seasonal.csv")
    ap.add_argument("--out-summary-csv", type=Path, required=True)
    ap.add_argument("--out-summary-md", type=Path, required=True)
    args = ap.parse_args()

    years = [y.strip() for y in str(args.years).split(",") if y.strip()]
    seasons = [s.strip() for s in str(args.seasons).split(",") if s.strip()]

    py = args.python or "py -3.11"
    # For subprocess, we need tokenized executable; prefer "py" to match windows launcher.
    # If user passes a full path, it will be treated as a single token.
    if py.startswith("py "):
        exe = py.split()
    else:
        exe = [py]

    args.out_dir.mkdir(parents=True, exist_ok=True)
    args.out_summary_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_summary_md.parent.mkdir(parents=True, exist_ok=True)

    summary_rows = []
    converted_results = []
    converted_payouts = []

    for y in years:
        for s in seasons:
            res_in = args.input_dir / f"target_results_{y}_{s}.csv"
            pay_in = args.input_dir / f"target_payoff_a_{y}_{s}.csv"
            out_results = args.out_dir / f"external_results_{y}_{s}.csv"
            out_payouts = args.out_dir / f"external_wide_payouts_{y}_{s}.csv"
            out_md = args.out_dir / f"convert_target_raw_{y}_{s}.md"
            out_rejected = args.out_dir / f"convert_target_raw_{y}_{s}_rejected.csv"

            if not (res_in.exists() and pay_in.exists()):
                summary_rows.append(
                    {
                        "year": y,
                        "season": s,
                        "status": "skip_missing_inputs",
                        "results_in": str(res_in),
                        "payoff_in": str(pay_in),
                        "out_results": str(out_results),
                        "out_payouts": str(out_payouts),
                        "out_md": str(out_md),
                        "out_rejected": str(out_rejected),
                        "message": "missing target_results or target_payoff_a",
                    }
                )
                continue

            cmd = exe + [
                str(CONVERTER),
                "--target-results-csv",
                str(res_in),
                "--target-payoff-csv",
                str(pay_in),
                "--out-results-csv",
                str(out_results),
                "--out-wide-payouts-csv",
                str(out_payouts),
                "--out-md",
                str(out_md),
            ]
            rc, msg = _run(cmd)
            status = "ok" if rc == 0 else "error"

            # Reject file is produced by the converter (if it does). If not, leave empty note.
            rejected_exists = out_rejected.exists()

            # Count converted rows if available.
            res_rows = None
            pay_rows = None
            try:
                if out_results.exists():
                    res_rows = int(sum(1 for _ in out_results.open("r", encoding="utf-8", errors="ignore")) - 1)
                if out_payouts.exists():
                    pay_rows = int(sum(1 for _ in out_payouts.open("r", encoding="utf-8", errors="ignore")) - 1)
            except Exception:
                pass

            summary_rows.append(
                {
                    "year": y,
                    "season": s,
                    "status": status,
                    "return_code": rc,
                    "results_in": str(res_in),
                    "payoff_in": str(pay_in),
                    "out_results": str(out_results),
                    "out_payouts": str(out_payouts),
                    "out_md": str(out_md),
                    "out_rejected": str(out_rejected),
                    "converted_results_row_count": res_rows,
                    "converted_wide_payouts_row_count": pay_rows,
                    "rejected_file_exists": bool(rejected_exists),
                    "message": msg[:5000],
                }
            )

            if status == "ok" and out_results.exists() and out_payouts.exists():
                converted_results.append(out_results)
                converted_payouts.append(out_payouts)

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(args.out_summary_csv, index=False, encoding="utf-8")

    md_lines = [
        "# Batch Convert TARGET Seasonal CSVs Summary",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- input_dir: {args.input_dir}",
        f"- out_dir: {args.out_dir}",
        f"- years: {','.join(years)}",
        f"- seasons: {','.join(seasons)}",
        "",
        "## Output",
        "",
        f"- summary_csv: {args.out_summary_csv}",
        "",
        "## Counts",
        "",
        f"- total_expected: {len(years)*len(seasons)}",
        f"- converted_ok: {int((summary_df['status']=='ok').sum()) if 'status' in summary_df.columns else 0}",
        f"- skipped_missing: {int((summary_df['status']=='skip_missing_inputs').sum()) if 'status' in summary_df.columns else 0}",
        f"- errors: {int((summary_df['status']=='error').sum()) if 'status' in summary_df.columns else 0}",
        "",
    ]

    union_results_path = args.out_dir / args.union_results_name
    union_payouts_path = args.out_dir / args.union_wide_payouts_name
    if args.write_union and converted_results and converted_payouts:
        # Concatenate by streaming read; keep memory modest.
        # Results union
        res_frames = []
        for p in converted_results:
            res_frames.append(pd.read_csv(p))
        pd.concat(res_frames, ignore_index=True).to_csv(union_results_path, index=False, encoding="utf-8")
        pay_frames = []
        for p in converted_payouts:
            pay_frames.append(pd.read_csv(p))
        pd.concat(pay_frames, ignore_index=True).to_csv(union_payouts_path, index=False, encoding="utf-8")

        md_lines += [
            "## Union Outputs",
            "",
            f"- union_results: {union_results_path}",
            f"- union_wide_payouts: {union_payouts_path}",
            "",
        ]

    args.out_summary_md.write_text("\n".join(md_lines), encoding="utf-8")
    print(str(args.out_summary_csv))
    print(str(args.out_summary_md))


if __name__ == "__main__":
    main()

