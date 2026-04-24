# racing_ai

New JRA-only horse racing AI project, built from scratch with JRA-VAN DataLab. as the primary datasource.

## Core principles (non-negotiable)

- No reuse of old code (design lessons can be reused).
- Strict time-series: features must be built using information available *before* the target race.
- Do not mix "ability" (odds-free) with "value" (odds-aware).
- Odds can be used only in the value/decision layer (Phase 2).
- Calibrate probabilities for `p_win` and `p_top3`.
- Reproducibility first: same inputs -> same outputs (idempotent pipelines).
- When something looks wrong, do not bet (stop conditions).

## Data layers (file-based JV-Link first)

- `raw`: JV-Link output files as-is (local files, no transformation).
- `normalized`: schema-normalized CSVs with quality gates.
- `warehouse`: DuckDB normalized tables (same final schema as `ingest-csv`).

The file-based flow is the default in Phase 1 for reproducibility, debugability and auditability.

```text
JV-Link raw files -> normalize-raw-jv -> ingest-normalized -> warehouse
```

Audit data is stored in `pipeline_audit_log`:
- raw fetched time (file mtime)
- source file name/path
- target race date
- snapshot version
- status (ok/warn/stop) and metrics JSON

## Phases

### Phase 1 (MVP)

- Ingest/store: races, entries, results, odds (win/place snapshots).
- Stabilize `horse_id` linkage and past-performances joining.
- Feature store snapshots with audit metadata:
  - `feature_generated_at`
  - `source_race_date_max`
  - `feature_snapshot_version`
  - `dataset_fingerprint`
- Build `p_top3` model + calibration.
- Generate wide candidates rule-based (two-stage: axis vs partner).
- Export JSON/CSV for the static dashboard.

### Phase 2 (Wide odds + EV)

Assumption:
- JRA-VAN provides wide odds snapshots.
- MVP does not require wide odds snapshots.

Rules:
- When wide odds are used, always store `odds_time` (captured_at) and `odds_snapshot_version`.
- Training and production must use the same odds timing for comparisons.
- If wide odds are missing: stop safely OR fallback to "no-EV mode".

## Quick start (schema only)

```powershell
cd racing_ai
python -m pip install -e .
aikeiba init-db --db-path data/warehouse/aikeiba.duckdb
```

## File-based JV-Link pipeline (recommended now)

1. Put JV-Link exported files under one directory (minimum: `races.csv`, `entries.csv`).
2. Run raw -> normalized -> warehouse in one command:

```powershell
aikeiba jv-file-pipeline `
  --db-path data/warehouse/aikeiba.duckdb `
  --raw-dir data/raw/2026-04-14_0900 `
  --normalized-root data/normalized `
  --race-date 2026-04-14 `
  --snapshot-version 20260414_0900
```

Or step-by-step:

```powershell
aikeiba normalize-raw-jv --db-path data/warehouse/aikeiba.duckdb --raw-dir data/raw/2026-04-14_0900 --normalized-root data/normalized --race-date 2026-04-14 --snapshot-version 20260414_0900
aikeiba ingest-normalized --db-path data/warehouse/aikeiba.duckdb --normalized-dir data/normalized/20260414_0900/2026-04-14 --race-date 2026-04-14 --snapshot-version 20260414_0900
```

## Race-day one-shot orchestration

`run-race-day` executes the full flow:
1. `jv-file-pipeline`
2. `build-features`
3. `doctor`
4. `infer-top3`
5. decision (skip + candidates)
6. `export-static`

```powershell
aikeiba run-race-day `
  --db-path data/warehouse/aikeiba.duckdb `
  --raw-dir data/raw/2026-04-14_0900 `
  --normalized-root data/normalized `
  --race-date 2026-04-14 `
  --snapshot-version 20260414_0900 `
  --feature-snapshot-version fs_v1 `
  --model-version top3_v1 `
  --odds-snapshot-version odds_v1 `
  --models-root data/models `
  --export-out-dir ..\data `
  --run-summary-path data/exports/run_summary_20260414.json `
  --allow-no-wide-odds
```

Outputs:
- `run_summary.json` (status / stop_reason / warning_count / step details)
- auto archived `run_summary.v1` (`data/exports/run_summary_v1/<race_date>/run_summary_<run_id>.json`)
- `doctor_result.json` / `doctor_result.csv` (structured check records)
- `race_day_run_log` table row
- `inference_log` compatibility row

Built-in warnings/stops after inference:
- warn when `buy_races=0`
- stop when top3 predictions are all null
- stop/warn when race-level `sum(p_top3)` is extreme/unusual
- stop when overlap guard fails
- ROI系を埋めるには、`results.csv` と `payouts.csv` を raw 入力に含める必要があります
- ROI が null のときは `warnings` を確認（例: `results_missing_for_roi`, `payouts_missing_for_roi`, `roi_unavailable_no_bets`）

Decision gate override (検証用):
- `--decision-gap12-min` (default: `0.003`)
- `--decision-density-top3-max` (default: `1.35`)
- 既定値では `buy_races=0` になりやすい検証データで、比較検証のために gate を緩和したい場合のみ使います

実配当データの後差し（最小手順）:
- `run-race-day` は `--results-csv-path` / `--payouts-csv-path` を受け取れます
- 例（raw配下に未配置でも後差し可能）:
```powershell
aikeiba run-race-day `
  --db-path data/warehouse/aikeiba.duckdb `
  --raw-dir data/raw/20260330_real `
  --results-csv-path D:\incoming\results.csv `
  --payouts-csv-path D:\incoming\payouts.csv `
  --race-date 2026-03-30 `
  --snapshot-version 20260330_real `
  --model-version top3_stability_plus_pace_v2 `
  --feature-snapshot-version fs_v1
```

raw ディレクトリの事前検査:
```powershell
aikeiba inspect-raw-dir --raw-dir data/raw/20260330_real --out-json data/exports/raw_input_status.json
```

- `roi_metrics_possible=true` なら ROI 比較入力として必要ファイルが揃っています
- 最終採用判断（adopt/hold/reject）は必ず実配当データで実施してください

JV-Link 実運用データから raw を作る:
```powershell
aikeiba build-real-raw-from-jv `
  --source-dir C:\JVExport\20260330 `
  --target-date 2026-03-30 `
  --out-raw-dir data/raw/20260330_real
```

- 入力: JV-Link のファイル出力（`races.csv`, `entries.csv`, `results.csv`, `payouts.csv`）
- 出力: Aikeiba raw 受け皿へ同名CSV + `raw_manifest_check.json`
- 列名が日本語でも、主要項目は Aikeiba raw スキーマ向けに正規化されます

## Stop / warn gates

- Stop:
- missing required raw files (`races.csv`, `entries.csv`)
- zero rows in races/entries
- `race_id` missing rate above threshold
- `horse_id` missing rate above threshold
- post-ingest doctor stop (e.g., attach rate too low)
- Warn:
- optional raw file missing (`results.csv`, `odds.csv`, `payouts.csv`)
- entries-per-race out of warning range

## Top3 calibration reports

`aikeiba train-top3` now writes calibration artifacts under the model directory:

- `calibration_summary.json`
- `calibration_bins.csv`
- `model_metrics.json`
- `feature_importance.csv`
- `feature_importance_summary.json`

Optional helper output:
- `race_sum_top3.csv` (race-level `sum(p_top3)` before/after)

## Fixed dataset manifest and comparison

Create fixed population manifest:

```powershell
aikeiba make-dataset-manifest `
  --db-path data/warehouse/aikeiba.duckdb `
  --out-dir data/datasets `
  --dataset-name top3_baseline_202604 `
  --task-name top3 `
  --feature-snapshot-version fs_v1 `
  --train-period 2026-03-01..2026-03-19 `
  --valid-period 2026-03-20..2026-03-30 `
  --test-period 2026-04-01..2026-04-14
```

Compare experiments on the same fixed fingerprint:

```powershell
aikeiba compare-experiments `
  --dataset-manifest data/datasets/top3_baseline_202604/dataset_manifest.json `
  --report-dir data/reports/top3_baseline_202604 `
  --experiment-spec "exp_a|data/models/top3/top3_v1" `
  --experiment-spec "exp_b|data/models/top3/top3_v2"
```

`compare-experiments` 実行時に以下を自動生成します:
- `comparison_report.csv`
- `comparison_report.json`
- `comparison_view.json`（静的サイト接続向けの表示用整形）
  - includes validity counters such as `valid_roi_experiment_count` and `valid_total_return_experiment_count`

さらに既定で、静的サイト用固定パスへ最新コピーを反映します:
- `../data/comparison_view.json`
- `../data/comparison_report.json`

無効化/変更:
- `--no-publish-latest`
- `--latest-out-dir <path>`

`comparison_report.csv/json` には校正比較列を含みます:
- `calibration_method`
- `logloss_before` / `logloss_after` / `logloss_delta`
- `brier_before` / `brier_after` / `brier_delta`
- `ece_before` / `ece_after` / `ece_delta`
- `race_sum_top3_mean_before` / `race_sum_top3_mean_after`
- `race_sum_top3_std_before` / `race_sum_top3_std_after`

delta 定義（改善時は負値）:
- `logloss_delta = logloss_after - logloss_before`
- `brier_delta = brier_after - brier_before`
- `ece_delta = ece_after - ece_before`

ステータス:
- `ok`: 指紋一致、校正入力あり
- `ok_with_missing_calibration`: 指紋一致だが `calibration_summary.json` 欠損
- `mismatch`: `dataset_fingerprint` など比較条件不一致

## Daily one-shot (race-day + compare)

`run-daily-cycle` は以下を一気通しで実行します:
1. `run-race-day`
2. （manifest 指定時）compare 生成・latest 反映
3. `daily_cycle_summary.json` 保存

manifest 未指定時は compare をスキップし、summary に warning を残します。

`run-daily-cycle` の experiment 選択:
- `--experiment-spec` 指定あり: `manual`
- 指定なし: `models_root/<task>/` から自動探索（既定 `task=top3`）

daily cycle の追跡:
- `daily_cycle_summary.json`
- `daily_cycle_run_log`（DB append-only）

## Baseline vs Stability 実験フロー

`train-top3` は `--feature-set baseline|stability` を指定できます。

baseline 例:
```powershell
aikeiba train-top3 --db-path data/warehouse/aikeiba.duckdb --models-root data/models_compare --model-version top3_baseline_v1 --feature-snapshot-version fs_v1 --train-end-date 2026-03-19 --valid-start-date 2026-03-20 --valid-end-date 2026-03-30 --test-period 2026-03-20..2026-03-30 --feature-set baseline
```

stability 例:
```powershell
aikeiba train-top3 --db-path data/warehouse/aikeiba.duckdb --models-root data/models_compare --model-version top3_stability_v1 --feature-snapshot-version fs_v1 --train-end-date 2026-03-19 --valid-start-date 2026-03-20 --valid-end-date 2026-03-30 --test-period 2026-03-20..2026-03-30 --feature-set stability
```

一気通し比較:
```powershell
aikeiba run-baseline-vs-stability --db-path data/warehouse/aikeiba.duckdb --models-root data/models_compare --feature-snapshot-version fs_v1 --train-end-date 2026-03-19 --valid-start-date 2026-03-20 --valid-end-date 2026-03-30 --test-period 2026-03-20..2026-03-30 --dataset-manifest-path data/datasets/top3_cmp_baseline/dataset_manifest.json --report-dir data/reports/baseline_vs_stability
```

出力:
- `comparison_report.json/csv`
- `comparison_view.json`
- `experiment_delta_summary.json`
- `experiment_delta_summary.md`

### run_summary linkage for ROI metrics

`run-baseline-vs-stability` links run summaries to each experiment and fills betting metrics in:
- `comparison_report.json/csv`
- `comparison_view.json`
- `experiment_delta_summary.json/md`

Optional arguments:
- `--baseline-run-summary-path`
- `--stability-run-summary-path`
- `--run-summary-search-dir` (repeatable)

If run summaries are not passed explicitly, the command auto-discovers by `model_version`
from `data/exports` and `../data` by default.

Recommended rerun flow (run_summary.v1):

- explicit linkage (more deterministic):
  - `--baseline-run-summary-path ...`
  - `--stability-run-summary-path ...`
- auto linkage (less manual):
  - omit both paths, optionally add `--run-summary-search-dir ...`

`run-baseline-vs-stability` delta now also includes money metrics:
- `total_return_yen_diff_stability_minus_baseline`
- `total_bet_yen_diff_stability_minus_baseline`
- `hit_bets_diff_stability_minus_baseline`
- flags: `total_return_improved`, `total_bet_increased`, `hit_bets_improved`
- decision: `adoption_decision` (`adopt` / `hold` / `reject`) in both json and md

## 展開系特徴量 (MVP)

今回追加した展開系特徴量（point-in-time: 過去走のみ）:
- `avg_corner4_pos_last5`（近5走の4角平均位置）
- `corner4_pos_std_last5`（近5走の4角位置のばらつき）
- `front_runner_rate_last5`（近5走で4角4番手以内の比率）
- `closer_rate_last5`（近5走で4角10番手以降の比率）
- `avg_last3f_rank_last5`（近5走の上がり順位平均）
- `pace_finish_delta_last5`（4角位置 - 着順 の平均）
- `pace_min_history_flag`（履歴不足フラグ）

新しい feature set:
- `stability_plus_pace`（既存 stability + 展開系）

実行例（比較）:
```powershell
aikeiba run-baseline-vs-stability `
  --db-path data/warehouse/aikeiba.duckdb `
  --models-root data/models_compare `
  --feature-snapshot-version fs_v1 `
  --train-end-date 2026-03-30 `
  --valid-start-date 2026-03-20 `
  --valid-end-date 2026-03-30 `
  --test-period 2026-03-20..2026-03-30 `
  --baseline-model-version top3_baseline_v2 `
  --stability-model-version top3_stability_plus_pace_v1 `
  --baseline-feature-set baseline `
  --stability-feature-set stability_plus_pace `
  --baseline-experiment-name exp_top3_baseline_v2 `
  --stability-experiment-name exp_top3_stability_plus_pace_v1 `
  --dataset-manifest-path data/datasets/top3_cmp_baseline/dataset_manifest.json `
  --report-dir data/reports/baseline_vs_stability_plus_pace
```

注意:
- 未来情報は使わず、`source_race_date_max` 以前の履歴のみ使用
- 脚質分類は説明可能な簡易ルール（閾値）を採用
- 比較整合性は `dataset_fingerprint` 一致前提

## run_summary.json schema (fixed)

`run-race-day` writes `run_summary.json` with fixed schema version:
- `summary_schema_version: "run_summary.v1"`

Required keys (always present):
- `summary_schema_version`
- `created_at`
- `race_date`
- `run_id`
- `experiment_name`
- `model_version`
- `feature_snapshot_version`
- `dataset_fingerprint`
- `status`
- `stop_reason` (nullable)
- `warnings` (always array)
- `roi`
- `hit_rate`
- `buy_races`
- `total_bets`
- `hit_bets`
- `total_return_yen`
- `total_bet_yen`
- `max_losing_streak`
- `calibration_summary_path` (nullable)
- `feature_importance_summary_path` (nullable)

Rules:
- Numeric metrics are `number | null` (`0` and `null` are different).
- Non-finite values (`NaN`, `Infinity`) are normalized to `null`.
- `warnings` is always `[]` or `list[str]`.
- If schema-critical keys are broken, run is marked `stop` with `run_summary_schema_error`.

### run_summary operation rules

- Automatic archive path: `data/exports/run_summary_v1/<race_date>/run_summary_<run_id>.json`
- Comparison accepts only `run_summary.v1` (`summary_schema_version == "run_summary.v1"`).
- Invalid examples (treated as `run_summary_invalid`):
  - missing required keys (for example: `model_version`, `dataset_fingerprint`)
  - type mismatch (for example: `warnings` is not array)
  - schema version mismatch (for example: `run_summary.v0`)
- When placing run_summary manually, keep `model_version` and `dataset_fingerprint` aligned with the experiment/model metrics.
- Schema file location: `src/aikeiba/common/run_summary_schema.json`

Minimal valid example:
```json
{
  "summary_schema_version": "run_summary.v1",
  "created_at": "2026-04-15T12:00:00",
  "race_date": "2026-04-14",
  "run_id": "run-001",
  "experiment_name": "exp_top3_stability_v2",
  "model_version": "top3_stability_v2",
  "feature_snapshot_version": "fs_v1",
  "dataset_fingerprint": "fp-001",
  "status": "ok",
  "stop_reason": null,
  "warnings": [],
  "roi": 1.05,
  "hit_rate": 0.20,
  "buy_races": 10,
  "total_bets": 30,
  "hit_bets": 6,
  "total_return_yen": 31500,
  "total_bet_yen": 30000,
  "max_losing_streak": 4,
  "calibration_summary_path": null,
  "feature_importance_summary_path": null
}
```

Invalid examples:
- `summary_schema_version = "run_summary.v0"`
- `warnings = "warn"` (must be array)
- missing `model_version` or other required key

Comparison intake examples:
- accepted: `run_summary.v1` + required keys/type valid
- rejected safely: invalid summary -> ROI fields become null, and `missing_inputs` includes:
  - `run_summary_invalid:schema_version_mismatch:...`
  - `run_summary_invalid:missing_required_key:...`
  - `run_summary_invalid:warnings_not_array`

## JV-Link SDK direct exporter (Windows, C#)

If you want to fetch raw directly from JV-Link SDK (without pre-exported CSV), use:

```powershell
cd racing_ai
dotnet run --project .\tools\jvlink_direct_exporter\Aikeiba.JVLinkDirectExporter.csproj -- `
  --race-date 2026-03-30 `
  --output-dir .\data\raw\20260330_real `
  --overwrite `
  --verbose
```

Then validate and continue pipeline:

```powershell
aikeiba inspect-raw-dir --raw-dir .\data\raw\20260330_real
aikeiba run-race-day --raw-dir .\data\raw\20260330_real --race-date 2026-03-30 ...
```

Details: `tools/jvlink_direct_exporter/README.md`
