from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _safe_rate(num: float, den: float) -> float | None:
    if den == 0:
        return None
    return num / den


def main() -> None:
    ap = argparse.ArgumentParser(description="Build markdown plan for results/payouts backfill.")
    ap.add_argument("--results-audit-csv", type=Path, required=True)
    ap.add_argument("--payout-audit-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    r = pd.read_csv(args.results_audit_csv)
    p = pd.read_csv(args.payout_audit_csv)

    hit_cov = _safe_rate(float(r["actual_wide_hit_non_null_count"].sum()), float(r["candidate_pair_count"].sum()))
    payout_cov = _safe_rate(float(p["matched_payout_count"].sum()), float(p["candidate_hit_count"].sum()))

    lines = [
        "# backfill_results_payouts_plan",
        "",
        "## 現状",
        f"- actual_wide_hit coverage: {hit_cov:.4f}" if hit_cov is not None else "- actual_wide_hit coverage: N/A",
        f"- hit rows payout coverage: {payout_cov:.4f}" if payout_cov is not None else "- hit rows payout coverage: N/A",
        "- 欠損主因: results.finish_position欠損、payoutsのbet_key不一致/メタ行混入",
        "",
        "## 必要な補完",
        "- finish_position 補完",
        "- wide_payout 補完",
        "- bet_key 正規化",
        "",
        "## 補完優先順位",
        "1. results.finish_position",
        "2. payouts wide bet_key / payout",
        "3. 2026-04-10〜12",
        "4. 追加検証日",
        "",
        "## 補完後の再評価コマンド",
        "- py -3.11 racing_ai/scripts/join_wide_results_to_candidate_pairs.py ...",
        "- py -3.11 racing_ai/scripts/evaluate_non_rule_model_candidates.py ...",
        "- py -3.11 racing_ai/scripts/evaluate_rule_vs_non_rule_candidates.py ...",
        "- py -3.11 racing_ai/scripts/evaluate_expanded_dynamic_conditions_with_results.py ...",
        "",
        "## 合格基準",
        "- actual_wide_hit coverage >= 0.8",
        "- hit rows payout coverage >= 0.8",
        "- 3日で評価可能",
        "- その後10日以上へ拡張",
    ]
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(lines), encoding="utf-8")
    print(str(args.out_md))


if __name__ == "__main__":
    main()

