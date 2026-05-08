# wide_payout_backfill_audit

- generated_at: 2026-04-29T19:39:33
- db_path: racing_ai\data\warehouse\aikeiba.duckdb
- pair_base_path: racing_ai\data\modeling\pair_learning_base.parquet
- target_range: 2021-01-01 .. 2024-12-31

## payouts schema

| column_name | column_type | null | key |
|---|---|---|---|
| race_id | VARCHAR | NO | PRI |
| bet_type | VARCHAR | NO | PRI |
| bet_key | VARCHAR | NO | PRI |
| payout | DOUBLE | YES | None |
| popularity | INTEGER | YES | None |
| ingested_at | TIMESTAMP | YES | None |
| source_version | VARCHAR | YES | None |

## payouts現状

- min_race_date: 2025-04-20 00:00:00
- max_race_date: 2026-04-26 00:00:00
- payout_rows: 68530
- wide_rows: 9831

## 2021〜2024 wide欠損集計

- total_races: 13822
- missing_wide_race_count: 13822
- total_race_dates: 427
- missing_wide_race_date_count: 427

## 欠損 race_date 一覧（先頭50件）

| race_date | missing_races |
|---|---:|
| 2021-01-05 00:00:00 | 24 |
| 2021-01-09 00:00:00 | 24 |
| 2021-01-10 00:00:00 | 24 |
| 2021-01-11 00:00:00 | 24 |
| 2021-01-16 00:00:00 | 36 |
| 2021-01-17 00:00:00 | 36 |
| 2021-01-23 00:00:00 | 36 |
| 2021-01-24 00:00:00 | 36 |
| 2021-01-30 00:00:00 | 36 |
| 2021-01-31 00:00:00 | 36 |
| 2021-02-06 00:00:00 | 36 |
| 2021-02-07 00:00:00 | 36 |
| 2021-02-13 00:00:00 | 36 |
| 2021-02-14 00:00:00 | 36 |
| 2021-02-20 00:00:00 | 36 |
| 2021-02-21 00:00:00 | 36 |
| 2021-02-27 00:00:00 | 36 |
| 2021-02-28 00:00:00 | 36 |
| 2021-03-06 00:00:00 | 36 |
| 2021-03-07 00:00:00 | 36 |
| 2021-03-13 00:00:00 | 36 |
| 2021-03-14 00:00:00 | 36 |
| 2021-03-20 00:00:00 | 36 |
| 2021-03-21 00:00:00 | 36 |
| 2021-03-27 00:00:00 | 36 |
| 2021-03-28 00:00:00 | 36 |
| 2021-04-03 00:00:00 | 24 |
| 2021-04-04 00:00:00 | 24 |
| 2021-04-10 00:00:00 | 36 |
| 2021-04-11 00:00:00 | 36 |
| 2021-04-17 00:00:00 | 36 |
| 2021-04-18 00:00:00 | 36 |
| 2021-04-24 00:00:00 | 36 |
| 2021-04-25 00:00:00 | 36 |
| 2021-05-01 00:00:00 | 36 |
| 2021-05-02 00:00:00 | 36 |
| 2021-05-08 00:00:00 | 36 |
| 2021-05-09 00:00:00 | 36 |
| 2021-05-15 00:00:00 | 36 |
| 2021-05-16 00:00:00 | 36 |
| 2021-05-22 00:00:00 | 36 |
| 2021-05-23 00:00:00 | 36 |
| 2021-05-29 00:00:00 | 24 |
| 2021-05-30 00:00:00 | 24 |
| 2021-06-05 00:00:00 | 24 |
| 2021-06-06 00:00:00 | 24 |
| 2021-06-12 00:00:00 | 36 |
| 2021-06-13 00:00:00 | 36 |
| 2021-06-19 00:00:00 | 36 |
| 2021-06-20 00:00:00 | 36 |

## 既存データ内の払戻候補ファイル

- hit_count: 487
- racing_ai\data\normalized\20250420_hist_from_jv\2025-04-20\payouts.csv
- racing_ai\data\normalized\20250421_hist_from_jv\2025-04-21\payouts.csv
- racing_ai\data\normalized\20250422_hist_from_jv\2025-04-22\payouts.csv
- racing_ai\data\normalized\20250425_hist_from_jv\2025-04-25\payouts.csv
- racing_ai\data\normalized\20250426_hist_from_jv\2025-04-26\payouts.csv
- racing_ai\data\normalized\20250427_hist_from_jv\2025-04-27\payouts.csv
- racing_ai\data\normalized\20250502_hist_from_jv\2025-05-02\payouts.csv
- racing_ai\data\normalized\20250503_hist_from_jv\2025-05-03\payouts.csv
- racing_ai\data\normalized\20250504_hist_from_jv\2025-05-04\payouts.csv
- racing_ai\data\normalized\20250505_hist_from_jv\2025-05-05\payouts.csv
- racing_ai\data\normalized\20250506_hist_from_jv\2025-05-06\payouts.csv
- racing_ai\data\normalized\20250509_hist_from_jv\2025-05-09\payouts.csv
- racing_ai\data\normalized\20250510_hist_from_jv\2025-05-10\payouts.csv
- racing_ai\data\normalized\20250511_hist_from_jv\2025-05-11\payouts.csv
- racing_ai\data\normalized\20250512_hist_from_jv\2025-05-12\payouts.csv
- racing_ai\data\normalized\20250513_hist_from_jv\2025-05-13\payouts.csv
- racing_ai\data\normalized\20250516_hist_from_jv\2025-05-16\payouts.csv
- racing_ai\data\normalized\20250517_hist_from_jv\2025-05-17\payouts.csv
- racing_ai\data\normalized\20250518_hist_from_jv\2025-05-18\payouts.csv
- racing_ai\data\normalized\20250523_hist_from_jv\2025-05-23\payouts.csv
- racing_ai\data\normalized\20250524_hist_from_jv\2025-05-24\payouts.csv
- racing_ai\data\normalized\20250525_hist_from_jv\2025-05-25\payouts.csv
- racing_ai\data\normalized\20250530_hist_from_jv\2025-05-30\payouts.csv
- racing_ai\data\normalized\20250531_hist_from_jv\2025-05-31\payouts.csv
- racing_ai\data\normalized\20250601_hist_from_jv\2025-06-01\payouts.csv
- racing_ai\data\normalized\20250606_hist_from_jv\2025-06-06\payouts.csv
- racing_ai\data\normalized\20250607_hist_from_jv\2025-06-07\payouts.csv
- racing_ai\data\normalized\20250608_hist_from_jv\2025-06-08\payouts.csv
- racing_ai\data\normalized\20250613_hist_from_jv\2025-06-13\payouts.csv
- racing_ai\data\normalized\20250614_hist_from_jv\2025-06-14\payouts.csv
- racing_ai\data\normalized\20250615_hist_from_jv\2025-06-15\payouts.csv
- racing_ai\data\normalized\20250616_hist_from_jv\2025-06-16\payouts.csv
- racing_ai\data\normalized\20250617_hist_from_jv\2025-06-17\payouts.csv
- racing_ai\data\normalized\20250620_hist_from_jv\2025-06-20\payouts.csv
- racing_ai\data\normalized\20250621_hist_from_jv\2025-06-21\payouts.csv
- racing_ai\data\normalized\20250622_hist_from_jv\2025-06-22\payouts.csv
- racing_ai\data\normalized\20250623_hist_from_jv\2025-06-23\payouts.csv
- racing_ai\data\normalized\20250624_hist_from_jv\2025-06-24\payouts.csv
- racing_ai\data\normalized\20250627_hist_from_jv\2025-06-27\payouts.csv
- racing_ai\data\normalized\20250628_hist_from_jv\2025-06-28\payouts.csv
- racing_ai\data\normalized\20250629_hist_from_jv\2025-06-29\payouts.csv
- racing_ai\data\normalized\20250704_hist_from_jv\2025-07-04\payouts.csv
- racing_ai\data\normalized\20250705_hist_from_jv\2025-07-05\payouts.csv
- racing_ai\data\normalized\20250706_hist_from_jv\2025-07-06\payouts.csv
- racing_ai\data\normalized\20250707_hist_from_jv\2025-07-07\payouts.csv
- racing_ai\data\normalized\20250708_hist_from_jv\2025-07-08\payouts.csv
- racing_ai\data\normalized\20250711_hist_from_jv\2025-07-11\payouts.csv
- racing_ai\data\normalized\20250712_hist_from_jv\2025-07-12\payouts.csv
- racing_ai\data\normalized\20250713_hist_from_jv\2025-07-13\payouts.csv
- racing_ai\data\normalized\20250718_hist_from_jv\2025-07-18\payouts.csv
- racing_ai\data\normalized\20250719_hist_from_jv\2025-07-19\payouts.csv
- racing_ai\data\normalized\20250720_hist_from_jv\2025-07-20\payouts.csv
- racing_ai\data\normalized\20250725_hist_from_jv\2025-07-25\payouts.csv
- racing_ai\data\normalized\20250726_hist_from_jv\2025-07-26\payouts.csv
- racing_ai\data\normalized\20250727_hist_from_jv\2025-07-27\payouts.csv
- racing_ai\data\normalized\20250802_debug_fix\2025-08-02\payouts.csv
- racing_ai\data\normalized\20250803_hist_from_jv\2025-08-03\payouts.csv
- racing_ai\data\normalized\20250804_hist_from_jv\2025-08-04\payouts.csv
- racing_ai\data\normalized\20250805_hist_from_jv\2025-08-05\payouts.csv
- racing_ai\data\normalized\20250808_hist_from_jv\2025-08-08\payouts.csv
- racing_ai\data\normalized\20250809_hist_from_jv\2025-08-09\payouts.csv
- racing_ai\data\normalized\20250810_hist_from_jv\2025-08-10\payouts.csv
- racing_ai\data\normalized\20250815_hist_from_jv\2025-08-15\payouts.csv
- racing_ai\data\normalized\20250816_hist_from_jv\2025-08-16\payouts.csv
- racing_ai\data\normalized\20250817_hist_from_jv\2025-08-17\payouts.csv
- racing_ai\data\normalized\20250822_hist_from_jv\2025-08-22\payouts.csv
- racing_ai\data\normalized\20250823_hist_from_jv\2025-08-23\payouts.csv
- racing_ai\data\normalized\20250824_hist_from_jv\2025-08-24\payouts.csv
- racing_ai\data\normalized\20250831_hist_from_jv\2025-08-31\payouts.csv
- racing_ai\data\normalized\20250901_hist_from_jv\2025-09-01\payouts.csv
- racing_ai\data\normalized\20250902_hist_from_jv\2025-09-02\payouts.csv
- racing_ai\data\normalized\20250905_hist_from_jv\2025-09-05\payouts.csv
- racing_ai\data\normalized\20250906_hist_from_jv\2025-09-06\payouts.csv
- racing_ai\data\normalized\20250907_hist_from_jv\2025-09-07\payouts.csv
- racing_ai\data\normalized\20250914_hist_from_jv\2025-09-14\payouts.csv
- racing_ai\data\normalized\20250915_hist_from_jv\2025-09-15\payouts.csv
- racing_ai\data\normalized\20250916_hist_from_jv\2025-09-16\payouts.csv
- racing_ai\data\normalized\20250919_hist_from_jv\2025-09-19\payouts.csv
- racing_ai\data\normalized\20250920_hist_from_jv\2025-09-20\payouts.csv
- racing_ai\data\normalized\20250921_hist_from_jv\2025-09-21\payouts.csv
- ... (407 more)

## pair_learning_base への影響（2021〜2024）

- pair_rows_in_range: 69083
- wide_payout_nonnull_in_range: 0
- wide_payout_missing_rate_in_range: 1.0
- pair_race_date_count_in_range: 427
- roi_evaluable_race_dates_in_range: 0

### 外部投入CSVスキーマ案

- race_id: 文字列（例: 20210410-TOK-11R）
- race_date: YYYY-MM-DD
- bet_type: `wide` 固定（大文字小文字は投入時に正規化）
- bet_key: 馬番昇順の `NN-NN`（例: `03-07`）
- payout: 払戻金（円）
- source_version: 取込元識別子（例: `target_text_2021_2024_v1`）

任意列:

- popularity: 人気順（整数）

重複キー:

- `race_id + bet_type + bet_key`

衝突時ポリシー:

- `skip`: 既存を維持して新規衝突行は無視
- `replace`: 既存を削除して新規行で置換
