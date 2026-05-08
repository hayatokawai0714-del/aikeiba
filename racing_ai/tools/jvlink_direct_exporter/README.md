# Aikeiba JV-Link Direct Exporter (Windows)

`Aikeiba.JVLinkDirectExporter` は JV-Link SDK から直接レコードを読み、Aikeiba の raw 受け皿に `races.csv / entries.csv / results.csv / payouts.csv / odds.csv / raw_manifest_check.json` を出力する最小 exporter です。

> 重要: JV-Link の `JVRead/JVGets` が返すレコードは基本的に固定長フォーマットです。この exporter は「key=value 形式などで返ってくる環境」を想定した軽量パーサのみ実装しているため、環境によっては **RA/SE/HR を読めても各CSVが0行** になります。  
> その場合は、いったん **JV-Link のファイル出力(CSV) → `aikeiba build-real-raw-from-jv`** で raw を構築してください（`aikeiba.datalab.jvlink_collect` はその経路を前提に正規化します）。

## 前提

- Windows
- JRA-VAN DataLab. 契約済み
- JV-Link SDK 導入済み（COM ProgID: `JVDTLab.JVLink` または `JVLink.JVLink`）
- JV-Link 認証/初期設定済み
- `.NET 8 SDK`
- JV-Link は 32bit 前提のため、exporter も **x86 (win-x86)** 実行を前提とする
- `dotnet run -r win-x86` を使う場合は、x86 ランタイムが必要（未導入なら実行時に `hostfxr.dll not found`）

## ビルド

```powershell
dotnet build .\tools\jvlink_direct_exporter\Aikeiba.JVLinkDirectExporter.csproj -c Release -r win-x86
```

## 実行

### 2026-03-30 の出力例

```powershell
dotnet run --project .\tools\jvlink_direct_exporter\Aikeiba.JVLinkDirectExporter.csproj -r win-x86 -- `
  --race-date 2026-03-30 `
  --output-dir .\data\raw\20260330_real `
  --overwrite `
  --verbose
```

### 0行になる場合（推奨の現実解）

JV-Link の「ファイル出力(CSV)」で `races.csv / entries.csv / results.csv / payouts.csv` を任意のフォルダへ出力し、Aikeiba の正規化コマンドで raw に変換します:

```powershell
# 例: JV-Link が CSV を export\20260417\ に出したケース
python -m aikeiba.cli build-real-raw-from-jv `
  --source-dir .\export\20260417 `
  --target-date 2026-04-17 `
  --out-raw-dir .\data\raw\20260417_real
```

### オプション

- `--race-date YYYY-MM-DD` (必須)
- `--output-dir <path>` (必須)
- `--odds-snapshot-version <label>` (default: `odds_v1`)
- `--captured-at <ISO8601>` (default: now)
- `--overwrite`
- `--verbose`
- `--fromtime <yyyymmddHHMMSS>`
- `--dataspec <spec>` (default: `RACE`)
- `--option <int>` (default: `1`)
- `--dry-run`
- `--probe-only`
- `--list-com-members`
- `--dump-raw-only`
- `--dump-specs <spec:option,...>` (default: `RASW:0,SESW:0,HRSW:0`)
- `--dump-max-records <int>` (default: `20000`)

### 固定長パース実装前の生レコード採取（推奨）

`RA/SE/HR` 固定長仕様を埋めるために、まず生レコードを採取できます。

```powershell
dotnet run --project .\tools\jvlink_direct_exporter\Aikeiba.JVLinkDirectExporter.csproj -r win-x86 -- `
  --race-date 2026-03-29 `
  --output-dir .\data\raw\_jv_rawdump_20260329 `
  --dump-raw-only `
  --dump-specs RASW:0,SESW:0,HRSW:0 `
  --dump-max-records 30000 `
  --verbose
```

出力:
- `raw_dump_RASW_opt0.txt`
- `raw_dump_SESW_opt0.txt`
- `raw_dump_HRSW_opt0.txt`
- `raw_dump_summary.json`

## 出力ファイル

- `races.csv`
  - `race_id,race_date,venue_code,venue,race_no,race_name,distance,surface,track_condition,field_size,grade`
- `entries.csv`
  - `race_id,horse_id,horse_name,umaban,waku,jockey_name,trainer_name,weight_carried,odds,popularity`
- `results.csv`
  - `race_id,horse_id,finish_pos,time,margin,corner1_pos,corner2_pos,corner3_pos,corner4_pos,last3f,odds,popularity`
- `payouts.csv`
  - `race_id,bet_type,winning_combination,payout_yen`
- `odds.csv`
  - `race_id,odds_snapshot_version,captured_at,odds_type,horse_no,horse_no_a,horse_no_b,odds_value,source_version`
  - `odds_type`:
    - `win`: 単勝オッズ（SE由来）
    - `wide`: ワイド最小オッズ（O3由来）
    - `wide_max`: ワイド最大オッズ（O3由来）
    - `place`: 複勝最低オッズ（O1由来）
    - `place_max`: 複勝最高オッズ（O1由来）
    - `bracket`: 枠連オッズ（O1由来）
    - `umaren`: 馬連オッズ（O2由来）
- `raw_manifest_check.json`
  - `race_date,generated_at,source,has_races,has_entries,has_results,has_payouts,row_counts,missing_columns,warnings`

## レコード利用方針

- `RA` -> `races.csv`
- `SE` -> `entries.csv`, `results.csv`
- `HR` -> `payouts.csv`

> 注意: JV-Link SDK バージョンにより `JVRead/JVGets/JVOpen` のシグネチャが異なる場合があります。`Program.cs` の `TryReadRecord` と `Open` にフォールバックを入れているので、必要に応じて環境に合わせて微調整してください。

## race_id / horse_id 方針

- `race_id`: `YYYYMMDD-<VENUE>-NNR`
  - 例: `20260330-NAK-11R`
- `horse_id`: `horse_id` または `ketto_num`（血統登録番号優先）

## よくある失敗

- `JV-Link COM object was not found`
  - JV-Link SDK 未導入、COM 登録未完了、または x64 実行で 32bit COM を読めていない
- `JVInit failed`
  - JV-Link 認証未完了 / 初期設定不足
- `JVOpen failed`
  - `dataspec/fromtime/option` 不整合、対象データ未取得
  - `rc=-111` は `dataspec` 不正の可能性が高い（`RACE` を指定）
- `JVRead/JVGets returned rc=-1`
  - JV-Link 側に対象データがまだ読める状態でない（未ダウンロード・未準備など）
  - JV-Link 検証ツールで対象日のデータ可視化を確認後に再実行
- 0件出力
  - 対象日のデータ未取得、またはレコードのキー解釈が環境差異で合っていない

## Aikeiba への接続

```powershell
aikeiba inspect-raw-dir --raw-dir .\data\raw\20260330_real

aikeiba run-race-day --raw-dir .\data\raw\20260330_real --race-date 2026-03-30 ...
```

## PowerShell ラッパー（任意）

`run_exporter.ps1` を使うと、`YYYYMMDD_real` を自動生成しつつ実行できます。

## dotnet 未導入時の対応

`dotnet --info` で `dotnet` が見つからない場合、exporter は実行できません。
先に **.NET 8 SDK** をインストールし、PowerShell を再起動してから再実行してください。

確認コマンド:

```powershell
dotnet --info
```
