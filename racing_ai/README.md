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
- `race_day_run_log` table row
- `inference_log` compatibility row

Built-in warnings/stops after inference:
- warn when `buy_races=0`
- stop when top3 predictions are all null
- stop/warn when race-level `sum(p_top3)` is extreme/unusual

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
