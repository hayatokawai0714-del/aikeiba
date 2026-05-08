# External Odds Input Spec (2024)

## Purpose
`odds` テーブルへ **非破壊（insert-only）** で 2024 年 odds を補完するための外部入力仕様。
本仕様は validate/dry-run 前提で、apply は別途明示確認後。

## Output Target Schema
DB `odds` テーブル列:
- race_id
- odds_snapshot_version
- captured_at
- odds_type
- horse_no
- horse_no_a
- horse_no_b
- odds_value
- ingested_at (DB default)
- source_version

## Required Columns (external CSV)
- race_id
- race_date
- odds_snapshot_version
- odds_type
- horse_no
- horse_no_a
- horse_no_b
- odds_value

## Recommended Columns
- captured_at
- source_version

## External CSV Rules
- 文字コード: `UTF-8` 推奨（入力は `CP932/Shift_JIS` も受理）
- `race_id` 形式: `YYYYMMDD-XXX-01R`（例: `20240106-NAK-01R`）
- `race_date` 形式: `YYYY-MM-DD`
- `odds_snapshot_version` 例: `odds_2024_target_batch1`
- `odds_type` 最低限:
  - `place`
  - `place_max`
- `odds_type` 追加推奨:
  - `win`
  - `wide`
  - `wide_max`
- `horse_no`:
  - `place/place_max/win` は `1..18`
  - それ以外は `-1`
- `horse_no_a`,`horse_no_b`:
  - `wide/wide_max` は `1..18`
  - それ以外は `-1`
- `odds_value`: 数値（>0 推奨）

## Key Definition (dedupe/upsert policy)
`race_id + odds_snapshot_version + odds_type + horse_no + horse_no_a + horse_no_b`

## Safety Policy
- 既存正常行の上書きは禁止
- apply 時も insert のみ
- DB破壊的更新（DELETE/UPDATE/TRUNCATE）禁止

## Validation Expectations
- 2024 対象日で `place/place_max` 行が存在
- `horse_no` valid rate / `odds_value` numeric rate が十分高い
- entries/results race_id match rate が高い
- existing_odds_overlap は把握し、dry-run で skip 扱い
