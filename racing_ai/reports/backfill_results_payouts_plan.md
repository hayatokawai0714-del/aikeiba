# backfill_results_payouts_plan

## 現状
- actual_wide_hit coverage: 0.1478
- hit rows payout coverage: 0.0556
- 欠損主因: results.finish_position欠損、payoutsのbet_key不一致/メタ行混入

## 必要な補完
- finish_position 補完
- wide_payout 補完
- bet_key 正規化

## 補完優先順位
1. results.finish_position
2. payouts wide bet_key / payout
3. 2026-04-10〜12
4. 追加検証日

## 補完後の再評価コマンド
- py -3.11 racing_ai/scripts/join_wide_results_to_candidate_pairs.py ...
- py -3.11 racing_ai/scripts/evaluate_non_rule_model_candidates.py ...
- py -3.11 racing_ai/scripts/evaluate_rule_vs_non_rule_candidates.py ...
- py -3.11 racing_ai/scripts/evaluate_expanded_dynamic_conditions_with_results.py ...

## 合格基準
- actual_wide_hit coverage >= 0.8
- hit rows payout coverage >= 0.8
- 3日で評価可能
- その後10日以上へ拡張