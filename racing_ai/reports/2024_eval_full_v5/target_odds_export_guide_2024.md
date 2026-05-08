# TARGET Odds Export Guide (2024)

## 目的
2024 odds バックフィル用に、TARGET/JV 由来の外部 odds CSV を安全に作成する。

## TARGETで出力すべき odds 種別
最低限:
- 複勝下限 (`place`)
- 複勝上限 (`place_max`)

可能なら追加:
- 単勝 (`win`)
- ワイド下限 (`wide`)
- ワイド上限 (`wide_max`)

## 最低限必要な情報
- race_id を構成できる情報（開催日、場コード/場名、R番号）
- 馬番（複勝/単勝）
- 複勝下限/上限（または複勝代表値。代表値のみの場合は `place=place_max=代表値`）

## 推奨ファイル名（TARGET出力）
- `target_odds_2024_spring.csv`
- `target_odds_2024_summer.csv`
- `target_odds_2024_autumn.csv`

## 変換後ファイル名
- `external_odds_2024.csv`

## 変換コマンド例
```powershell
py -3.11 scripts\convert_target_odds_to_external_odds.py `
  --input-csv C:\TXT\target_odds_2024_spring.csv `
  --header `
  --odds-snapshot-version odds_2024_target_batch1 `
  --source-version target_odds_2024_spring.csv `
  --out-csv C:\TXT\external_odds_2024.csv `
  --out-md C:\TXT\convert_target_odds_2024.md `
  --out-rejected-csv C:\TXT\convert_target_odds_2024_rejected.csv
```

## 次の検証コマンド
```powershell
py -3.11 scripts\validate_external_odds.py `
  --external-odds-csv C:\TXT\external_odds_2024.csv `
  --db-path data\warehouse\aikeiba.duckdb `
  --out-csv C:\TXT\validate_external_odds_2024.csv `
  --out-md C:\TXT\validate_external_odds_2024.md `
  --rejected-csv C:\TXT\convert_target_odds_2024_rejected.csv
```

## dry-runコマンド
```powershell
py -3.11 scripts\backfill_external_odds.py `
  --external-odds-csv C:\TXT\external_odds_2024.csv `
  --db-path data\warehouse\aikeiba.duckdb `
  --start-date 2024-01-06 `
  --end-date 2024-12-28 `
  --source-name external_odds_2024 `
  --out-csv C:\TXT\backfill_external_odds_2024_dryrun.csv `
  --out-md C:\TXT\backfill_external_odds_2024_dryrun.md
```

apply は実行しない（要確認後）。
