from __future__ import annotations

import argparse
import json
from pathlib import Path

from aikeiba.evaluation.wide_label_coverage import build_wide_label_coverage_report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="racing_ai/data/warehouse/aikeiba.duckdb")
    ap.add_argument("--pairs-glob", default="racing_ai/data/bets/wide_pair_candidates_*.parquet")
    ap.add_argument("--pair-base-path", default="racing_ai/data/modeling/pair_learning_base.parquet")
    ap.add_argument("--out-md", default="racing_ai/reports/wide_label_coverage_report.md")
    args = ap.parse_args()
    res = build_wide_label_coverage_report(
        db_path=Path(args.db_path),
        pairs_glob=args.pairs_glob,
        pair_base_path=Path(args.pair_base_path),
        out_md=Path(args.out_md),
    )
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
