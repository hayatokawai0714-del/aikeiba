# One Click Prediction Pipeline Design

作成日: 2026-04-28

## 目的

ダッシュボードの「今日の予測を更新」ボタンだけで、当日データ取得から買い目生成までを完了させる。

手動CSV出力は通常運用から外す。必要な場合のみ、最終フォールバックとして扱う。

## 現状の問題

- TARGETの手動CSV出力は `races.csv / entries.csv / odds.csv` を毎回用意する必要があり、運用負荷が高い。
- AHKによるTARGET GUI操作は、画面状態・メニュー位置・ダイアログ挙動に依存して不安定。
- JV-Link direct exporter は既存実装があるが、当日によって `races=0 / entries=0 / odds=0` になるケースがある。
- 現行APIは `AIIKEIBA_SKIP_TARGET_AHK=1` のとき、既存raw/normalizedがない日付では止まる。

## 採用方針

第一優先は JV-Link COM 経由の direct exporter。

理由:

- ユーザーがTARGETでJV一括取得済みなら、手動CSV出力なしで実行できる可能性が最も高い。
- 既存コードに `tools/jvlink_direct_exporter` がある。
- 過去に非0件のraw生成実績がある。

AHKによるTARGET GUI操作は採用しない。

理由:

- 座標・DPI・ウィンドウ状態に依存する。
- エラー時に原因が見えにくい。
- 実運用で「ボタンだけ」の信頼性を下げる。

## 最終フロー

`POST /run-today` は以下を順番に実行する。

1. リクエスト検証
   - `today_date`
   - `odds_cutoff`
   - `mode=prediction`
   - token認証

2. 既存normalized確認
   - `races.csv` と `entries.csv` があれば使用
   - `odds.csv` があれば使用
   - `odds.csv` がなければ `odds_missing` として継続

3. 既存raw確認
   - `data/raw/YYYYMMDD_*` を探す
   - `races.csv` と `entries.csv` が非0件なら normalized 生成
   - 0件rawは失敗扱いではなくスキップ候補にする

4. JV-Link direct exporter 実行
   - 出力先: `data/raw/YYYYMMDD_jv_auto`
   - `--race-date today_date`
   - `--overwrite`
   - `--verbose`
   - 必須検証: `races > 0` and `entries > 0`
   - `odds > 0` は準必須

5. normalized生成
   - `jv-file-pipeline`
   - stop理由が `zero_rows:races` または `zero_rows:entries` の場合は、次の取得候補へ進む

6. prediction実行
   - `run_today_wide_phase2_pipeline.py`
   - `races + entries` だけで実行可能
   - oddsがない場合は `pred_top3` ベースでランキング

7. ダッシュボードJSON更新
   - `data/races_today.json`
   - `data/today_wide_predictions.csv`

## データ要件

### prediction

必須:

- `races.csv`
- `entries.csv`

準必須:

- `odds.csv`

任意:

- `results.csv`
- `payouts.csv`

### backtest

必須:

- `races.csv`
- `entries.csv`
- `odds.csv`
- `results.csv`
- `payouts.csv`

## API設計

### `POST /run-today`

入力:

```json
{
  "today_date": "2026-04-28",
  "odds_cutoff": "2026-04-28 12:30:00",
  "mode": "prediction",
  "target_ready_confirmed": true
}
```

処理モード:

- `prediction`: 買い目生成優先。odds欠損でも止めない。
- `backtest`: 検証優先。必要データ欠損時は止める。

レスポンスに含める項目:

- `status`
- `mode`
- `data_source`
- `normalized_dir`
- `raw_dir_used`
- `required_files`
- `optional_files`
- `missing_required`
- `missing_optional`
- `odds_missing`
- `warnings`
- `race_count`
- `selected_races`

## データ取得戦略

### Strategy 1: existing normalized

既に usable な normalized がある場合は最優先で使う。

条件:

- `races.csv` が存在し、非0件
- `entries.csv` が存在し、非0件

### Strategy 2: existing raw

既存rawがある場合は normalized 生成を試す。

条件:

- `races.csv` と `entries.csv` が存在
- かつ非0件

0件rawはスキップし、ログに `raw_zero_rows_skipped` を出す。

### Strategy 3: JV-Link direct exporter

手動CSV出力なしでraw生成する本命経路。

実行例:

```powershell
dotnet run --project .\tools\jvlink_direct_exporter\Aikeiba.JVLinkDirectExporter.csproj -r win-x86 -- `
  --race-date 2026-04-28 `
  --output-dir .\data\raw\20260428_jv_auto `
  --overwrite `
  --verbose
```

成功条件:

- `raw_manifest_check.json` が存在
- `row_counts.races > 0`
- `row_counts.entries > 0`

警告条件:

- `row_counts.odds == 0`

失敗条件:

- `row_counts.races == 0`
- `row_counts.entries == 0`

### Strategy 4: minimal CSV fallback

direct exporter が失敗した場合のみ使用する。

通常ボタン操作からは呼ばない。運用者向けの最後の逃げ道として残す。

## ログ設計

出力先:

- `data/today_wide_predictions_log.txt`

必須ログ:

- `run_id`
- `mode`
- `today_date`
- `odds_cutoff`
- `data_source`
- `strategy_attempted`
- `strategy_selected`
- `required_files`
- `optional_files`
- `missing_required`
- `missing_optional`
- `raw_dir_used`
- `normalized_dir`
- `raw_row_counts`
- `normalized_row_counts`
- `odds_missing`
- `warnings`

## UI設計

画面のボタンは1つに固定する。

表示状態:

- `待機中`
- `JVデータ確認中`
- `当日raw生成中`
- `normalized生成中`
- `買い目生成中`
- `完了`
- `データ不足`

データ不足時は、次を表示する。

- 対象日
- 不足ファイル
- rawが0件だったか
- 最後に成功した利用可能日

## 実装タスク

1. `dashboard_api.py` に data source resolver を追加する。
2. 0件rawをエラーではなくスキップ可能にする。
3. JV-Link direct exporter を `/run-today` から呼ぶ。
4. exporter結果の `raw_manifest_check.json` を検証する。
5. successful strategy をレスポンスとログに出す。
6. UIに進行状態とデータ不足理由を表示する。
7. AHK/TARGET CSV export 経路をデフォルト無効化する。

## 結論

ボタン1回運用の本命は、TARGET GUI操作ではなく JV-Link direct exporter の自動実行である。

買い目生成は `races.csv + entries.csv` があれば止めずに進める。`odds.csv` がない場合は `value_gap` を使わず、`pred_top3` ベースの暫定ランキングで出力する。

4/26や4/28で買い目が出なかった直接原因は、当日raw/normalizedが存在しない、またはJV direct exporterの出力が0件だったためである。
