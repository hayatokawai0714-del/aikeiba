# jvopen_failure_diagnostics

- generated_at: 2026-04-29
- scope: setup-mode を `dataspec=RACE` + `option=3/4` に修正後の確認

## 実装確認

- setup-mode 時は `JVOpen("RACE", fromtime, option)` 固定
- setup-mode 時は `--dataspec` 指定を無視（warning表示）
- setup-mode では `HRSW/RASW/SESW/O*SW` を直接 JVOpen しない
- `JVRead` で先頭2文字が `HR` のレコードのみ抽出
- `end_date` 超過で読み取り停止 (`setup_read_stopped_by_end_date`)
- manifest 追加/確認:
  - `setup_mode`
  - `jvopen_dataspec`
  - `jvopen_fromtime`
  - `jvopen_option`
  - `read_record_type_counts`
  - `hr_record_count`
  - `date_min`
  - `date_max`

## 実行結果

### A. setup-mode 強制確認（2021-01-05, dataspec=HRSW指定）
- 実行: `... --setup-mode --dataspec HRSW --option 3`
- ログ: `setup-mode ignores --dataspec=HRSW; forcing dataspec=RACE ...`
- JVOpen: `dataSpec=RACE`, `fromTime=20210105000000`, `option=3`, `rc=0`
- raw: `read_count=312`
- `read_record_type_counts`: `H1=312`
- `hr_record_count`: `0`

### B. 通常取得維持確認（2025-05-03, option=0）
- rows: `108` (wide抽出)
- raw_manifest row_counts:
  - races=36, entries=473, results=443, payouts=745, odds=11523
- 既存 option=0 の挙動は維持

### C. setup-mode（2021-01-05, option=3）
- JVOpen は成立しても `HR` が取得されず wide行は0
- 現状は 2021 側のローカルDataLab状態/取得範囲不足が主因の可能性が高い

## 結論

- 要件どおり setup-mode を SDK例準拠の `RACE + option=3/4` に修正済み。
- 通常取得 option=0 は破壊されていない。
- 2021〜2024の実データ取得可否は、実装よりもローカルDataLabの過去データ保有状態の影響が残る。
