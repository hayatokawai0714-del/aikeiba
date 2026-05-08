# JV-Link Official Option Compare

- generated_at: 2026-04-30T07:19:29
- note: option=0 ??????????????? option=1/4 ????

| test_name | dataspec | fromtime | option | return_code | return_code_meaning | raw_payouts_row_count | hr_record_count | wide_rows | record_type_counts | date_min | date_max | message |
|---|---|---|---:|---:|---|---:|---:|---:|---|---|---|---|
| A. ????????? | RACE | 20250503000000-20250503235959 | 1 | 0 | success | 0 | 0 | 0 | {} |  |  | no_ra_records_parsed;no_se_records_for_entries_parsed;no_se_records_for_results_parsed;no_hr_records_parsed;jvlink_stream_missing_ra_se_hr_records;hint: JVOpen did not return RA/SE/HR. Try probe mode: --probe-only --dataspec RACE --probe-options 0,1,2,3,4;no_win_odds_parsed_from_se |
| B. ?????????2025 | RACE | 20250503000000-20250503235959 | 4 | -1 | no_data | 0 | 0 | 0 | {} |  |  | jvopen_no_data:RACE |
| C. ?????????2021 | RACE | 20210105000000-20210105235959 | 4 | -1 | no_data | 0 | 0 | 0 | {} |  |  | jvopen_no_data:RACE |