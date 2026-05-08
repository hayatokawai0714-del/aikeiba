# Odds Market Proxy Recovery Plan 2024

## Findings Summary

- joined_race_count: 3454
- odds_race_count: 0
- matched_race_count: 0
- place_rows_total: 0
- place_max_rows_total: 0

## Recommended Minimal Fix

1. 2024評価用 market proxy の odds 参照キーを joined pairs race_id と同一形式に正規化（DB値は変更しない）。
2. odds_type 判定は `place` / `place_max` を一次、存在しない場合のみ `win` / `wide` proxy を低信頼 fallback として明示分離。
3. odds_snapshot_version は race_date ごとに利用可能な最新を自動選択し、選択結果を監査ログに出力。
4. それでも place系が0日のみ、JV/TARGET からその日の odds 再取得を別バッチで実施（dry-run確認後）。

## Re-evaluation Readiness

- 上記1-3の非破壊修正後に 2024 v5 の再評価は実施可能。
- DB更新なしで shadow再計算のみ先行し、market proxy source 構成比が改善した時点でROI再判定を推奨。
