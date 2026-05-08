# required_results_payouts_input_spec

## Results CSV 必須列
- race_id
- race_date
- umaban (または horse_no)
- finish_position

### 推奨列
- horse_id
- horse_name
- status
- source

### 仕様
- race_id: DB と同形式へ正規化可能な文字列 (例: 20260410-NAK-01R)
- finish_position: 数値化可能な着順
- 取消・除外・中止は status 列で区別し、finish_position は空欄可
- 同一キー重複は race_id + umaban で一意

## Wide payouts CSV 必須列
- race_id
- race_date
- bet_type
- bet_key
- payout

### 仕様
- bet_type: WIDE または ワイド
- bet_key: 馬番2頭を昇順2桁ゼロ埋め (03-07)
- payout: 100円あたり払戻
- 同一キー重複は race_id + bet_key で一意

## 正規化ルール
- race_id: 空白除去・大文字化・ハイフン統一
- umaban: 数値化 (01/1 同一扱い)
- bet_key: 3-7 / 03-07 / 0307 / 3,7 / 03_07 を 03-07 に統一
- 不正値: 数値化不可・範囲外は NA として補完対象外
