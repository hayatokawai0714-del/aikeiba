# jvlink_wide_payout_export_report

- generated_at: 2026-04-29T21:20:43
- period: 2025-05-03 .. 2025-05-03
- output_csv: racing_ai\data\external\wide_payouts_test_20250503.csv

## Environment

- dotnet_ok: True
- dotnet_info_head: .NET SDK:
- com_jvdtlab_ok: False
- com_jvlink_ok: False
- com_error_jvdtlab: NG1:Retrieving the COM class factory for component with CLSID {2AB1774D-0C41-11D7-916F-0003479BEB3F} failed due to the following error: 80040154 Class not registered (Exception from HRESULT: 0x80040154 (REGDB_E_CLASSNOTREG)).
- com_error_jvlink: NG2:Retrieving the COM class factory for component with CLSID {00000000-0000-0000-0000-000000000000} failed due to the following error: 80040154 Class not registered (Exception from HRESULT: 0x80040154 (REGDB_E_CLASSNOTREG)).

## Summary

- total_days: 1
- ok_days: 1
- error_days: 0
- total_wide_payout_rows: 108
- error_count: 0
- debug_inspect_days: 1

## race_date別件数

| race_date | wide_rows |
|---|---:|
| 2025-05-03 | 108 |

## 日次実行結果

| race_date | status | wide_rows | payouts_csv_path | message |
|---|---|---:|---|---|
| 2025-05-03 | ok | 108 | racing_ai\data\external\jvlink_raw\20250503_jvlink_export\payouts.csv | ok |

## raw debug inspect

| race_date | raw_payouts_file_size | raw_payouts_row_count | raw_payouts_header | raw_bet_type_counts |
|---|---:|---:|---|---|
| 2025-05-03 | 24711 | 745 | race_id,bet_type,winning_combination,payout_yen | {"FUKU": 105, "JYO": 36, "KAI": 36, "KAISAI": 36, "RACE": 72, "SANRENPUKU": 36, "SANRENTAN": 36, "TAN": 141, "UMAREN": 36, "UMATAN": 36, "VENUE": 36, "WAKU": 31, "WIDE": 108} |

### jvopen_failures

- 2025-05-03:
  - jvopen_failed:RASW:20250504000000:1:rc=-111
  - jvopen_failed:SESW:20250504000000:1:rc=-111
  - jvopen_failed:HRSW:20250504000000:1:rc=-111
  - jvopen_failed:O3SW:20250504000000:1:rc=-111
  - jvopen_failed:O1SW:20250504000000:1:rc=-111
  - jvopen_failed:O2SW:20250504000000:1:rc=-111