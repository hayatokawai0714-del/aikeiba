# wide_strategy_grid_report

- 実行日時: 2026-04-29T12:27:32
- 入力ファイル: racing_ai\reports\wide_grid_enhanced.parquet
- 検証期間: 2026-04-10 .. 2026-04-12
- モデル: top3_stability_plus_pace_v3

## 最良パラメータ
- ai_weight: 0.5
- density_top3_max: 1.35
- gap12_min: 0.003
- roi: 0.65
- bet_race_count: 10
- bet_pair_count: 50
- hit_count: 8
- hit_rate: 0.16
- max_drawdown_method: race_date_race_id_ordered_race_aggregate_pnl

## 運用接続状態
- pair score列を本番候補に同梱: True
- venue/surface正規化: True
- popularity_source_audit: racing_ai\reports\popularity_source_audit.parquet
- grid_search_metadata: racing_ai\reports\grid_search_metadata.json
- grid_param_application: racing_ai\reports\grid_param_application.json
- skip追加条件設定値: {'enable_value_skip': False, 'min_ai_market_gap': None, 'enable_market_overrated_skip': False, 'max_market_overrated_top_count': None, 'enable_pair_ev_skip': False, 'min_pair_value_score': None}

## run-race-day 出力パス
- run_summary: None
- predictions: None
- candidate_pairs: None
- race_flags: None
- skip_log: None
- raw_dir必須ファイルチェック結果(stop_reason): missing_required_raw_files

## meta.json
- path: racing_ai\data\models_compare\top3\top3_stability_plus_pace_v3\meta.json
- exists: True
- train_start_date: 2025-04-20
- train_end_date: 2026-03-31
- calibration_start_date: 2026-04-03
- calibration_end_date: 2026-04-04
- validation_start_date: 2026-04-05
- validation_end_date: 2026-04-10
- meta_warnings: []
- calibration/validation期間分離: True

## ROI上位10条件
- roi=0.6500 ai_weight=0.5 density=1.35 gap12=0.003 bet_race_count=10 bet_pair_count=50 hit_rate=0.16 profit=-1750.0
- roi=0.5647 ai_weight=0.5 density=1.8 gap12=0.003 bet_race_count=34 bet_pair_count=170 hit_rate=0.14705882352941177 profit=-7400.0
- roi=0.5487 ai_weight=0.5 density=1.8 gap12=0.01 bet_race_count=30 bet_pair_count=150 hit_rate=0.16 profit=-6770.0
- roi=0.4673 ai_weight=0.65 density=1.35 gap12=0.003 bet_race_count=11 bet_pair_count=55 hit_rate=0.16363636363636364 profit=-2930.0
- roi=0.4178 ai_weight=0.5 density=1.35 gap12=0.01 bet_race_count=9 bet_pair_count=45 hit_rate=0.15555555555555556 profit=-2620.0
- roi=0.4080 ai_weight=0.65 density=1.35 gap12=0.01 bet_race_count=10 bet_pair_count=50 hit_rate=0.14 profit=-2960.0
- roi=0.3826 ai_weight=0.65 density=1.8 gap12=0.003 bet_race_count=23 bet_pair_count=115 hit_rate=0.1391304347826087 profit=-7100.0
- roi=0.3518 ai_weight=0.65 density=1.8 gap12=0.01 bet_race_count=22 bet_pair_count=110 hit_rate=0.12727272727272726 profit=-7130.0

## 月別ROI
- 2026-04: 0.65

## 人気帯別ROI
- 10人気以下: 0.0
- 1人気: 0.7
- 2-3人気: 0.6214285714285714
- 4-5人気: 1.2454545454545454
- 6-9人気: 0.0

## skip_reason別件数
- SKIP_DENSITY_TOO_HIGH: 98
- BUY_OK: 10

## popularity_source別件数
- odds.estimated_rank: 50

## 場別ROI
- UNKNOWN: 0.65

## 芝/ダート別ROI
- UNKNOWN: 0.545
- ダート: 0.0
- 芝: 1.08

## 頭数帯別ROI
- large: 0.7222222222222222
- medium: 0.0

## ai_market_gap帯別ROI
- neg_large: 0.7230769230769231
- neg_small: 0.0
- pos_large: 0.8058823529411765
- pos_small: 0.0

## UNKNOWN venue/surface の生値
- venue: ['3', '6', '9']
- surface: ['ダ']

## grid metadata
- grid_start_date: 2026-04-10
- grid_end_date: 2026-04-12
- selected_best_params: {'ai_weight': 0.5, 'density_top3_max': 1.35, 'gap12_min': 0.003}
- warning_if_used_for_same_period_prediction: Do not apply best params to predictions evaluated on the same grid period.
- apply_start_date: 2026-04-18
- apply_end_date: 2026-04-19
- is_safe_temporal_application: True

## leakage warnings
- warnings: []

## 過学習サイン
- 顕著な過学習サインは未検出