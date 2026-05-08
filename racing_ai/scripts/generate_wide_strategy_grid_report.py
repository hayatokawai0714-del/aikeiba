from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def _find_detail(meta: dict, detail_json_path: Path | None) -> dict:
    if detail_json_path is not None and detail_json_path.exists():
        return json.loads(detail_json_path.read_text(encoding="utf-8"))
    p = meta.get("detail_json_path")
    if p:
        pp = Path(p)
        if pp.exists():
            return json.loads(pp.read_text(encoding="utf-8"))
    return {"details": []}


def _match_detail(details: list[dict], row: dict) -> dict:
    for d in details:
        if (
            float(d.get("ai_weight")) == float(row.get("ai_weight"))
            and float(d.get("density_top3_max")) == float(row.get("density_top3_max"))
            and float(d.get("gap12_min")) == float(row.get("gap12_min"))
        ):
            return d
    return {}


def _fmt_dict(d: dict) -> str:
    if not d:
        return "- なし"
    return "\n".join([f"- {k}: {v}" for k, v in d.items()])


def build_report(*, grid_parquet: Path, grid_json: Path, out_md: Path, detail_json: Path | None = None) -> str:
    df = pd.read_parquet(grid_parquet) if grid_parquet.exists() else pd.DataFrame()
    meta = json.loads(grid_json.read_text(encoding="utf-8")) if grid_json.exists() else {}
    detail = _find_detail(meta, detail_json)
    details = detail.get("details", [])

    leakage_report_path = grid_json.parent / "leakage_guard_report.json"
    leakage = {}
    if leakage_report_path.exists():
        try:
            leakage = json.loads(leakage_report_path.read_text(encoding="utf-8"))
        except Exception:
            leakage = {"warnings": ["failed_to_parse_leakage_guard_report"]}

    model_version = meta.get("model_version")
    meta_path = Path("racing_ai/data/models_compare") / "top3" / str(model_version) / "meta.json"
    model_meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}

    grid_meta_path = Path(meta.get("grid_search_metadata_path", grid_json.parent / "grid_search_metadata.json"))
    grid_meta = json.loads(grid_meta_path.read_text(encoding="utf-8")) if grid_meta_path.exists() else {}
    grid_app_path = Path(meta.get("grid_param_application_path", grid_json.parent / "grid_param_application.json"))
    grid_app = json.loads(grid_app_path.read_text(encoding="utf-8")) if grid_app_path.exists() else {}
    race_day_summary_path = Path("racing_ai/data/exports/run_summary.json")
    race_day_summary = json.loads(race_day_summary_path.read_text(encoding="utf-8")) if race_day_summary_path.exists() else {}

    now = dt.datetime.now().isoformat(timespec="seconds")

    lines: list[str] = []
    lines.append("# wide_strategy_grid_report")
    lines.append("")
    lines.append(f"- 実行日時: {now}")
    lines.append(f"- 入力ファイル: {grid_parquet}")
    lines.append(f"- 検証期間: {meta.get('start_date')} .. {meta.get('end_date')}")
    lines.append(f"- モデル: {model_version}")
    lines.append("")

    best = meta.get("best", {})
    lines.append("## 最良パラメータ")
    lines.append(f"- ai_weight: {best.get('ai_weight')}")
    lines.append(f"- density_top3_max: {best.get('density_top3_max')}")
    lines.append(f"- gap12_min: {best.get('gap12_min')}")
    lines.append(f"- roi: {best.get('roi')}")
    lines.append(f"- bet_race_count: {best.get('bet_race_count')}")
    lines.append(f"- bet_pair_count: {best.get('bet_pair_count')}")
    lines.append(f"- hit_count: {best.get('hit_count')}")
    lines.append(f"- hit_rate: {best.get('hit_rate')}")
    lines.append(f"- max_drawdown_method: {best.get('max_drawdown_method') or meta.get('max_drawdown_method')}")
    lines.append("")

    lines.append("## 運用接続状態")
    lines.append(f"- pair score列を本番候補に同梱: {meta.get('pair_score_in_production_candidates')}")
    lines.append(f"- venue/surface正規化: {meta.get('venue_surface_normalization', {}).get('enabled')}")
    lines.append(f"- popularity_source_audit: {meta.get('popularity_source_audit_path')}")
    lines.append(f"- grid_search_metadata: {meta.get('grid_search_metadata_path')}")
    lines.append(f"- grid_param_application: {meta.get('grid_param_application_path')}")
    lines.append(f"- skip追加条件設定値: {meta.get('skip_reason_extra_settings')}")
    lines.append("")

    lines.append("## run-race-day 出力パス")
    lines.append(f"- run_summary: {race_day_summary.get('run_summary_path')}")
    lines.append(f"- predictions: {race_day_summary.get('predictions_path')}")
    lines.append(f"- candidate_pairs: {race_day_summary.get('candidate_pairs_path')}")
    lines.append(f"- race_flags: {race_day_summary.get('race_flags_path')}")
    lines.append(f"- skip_log: {race_day_summary.get('skip_log_path')}")
    lines.append(f"- raw_dir必須ファイルチェック結果(stop_reason): {race_day_summary.get('stop_reason')}")
    lines.append("")

    lines.append("## meta.json")
    lines.append(f"- path: {meta_path}")
    lines.append(f"- exists: {meta_path.exists()}")
    if model_meta:
        lines.append(f"- train_start_date: {model_meta.get('train_start_date')}")
        lines.append(f"- train_end_date: {model_meta.get('train_end_date')}")
        lines.append(f"- calibration_start_date: {model_meta.get('calibration_start_date')}")
        lines.append(f"- calibration_end_date: {model_meta.get('calibration_end_date')}")
        lines.append(f"- validation_start_date: {model_meta.get('validation_start_date')}")
        lines.append(f"- validation_end_date: {model_meta.get('validation_end_date')}")
        lines.append(f"- meta_warnings: {model_meta.get('meta_warnings')}")
        separation_ok = False
        try:
            tr_end = pd.to_datetime(model_meta.get("train_end_date"))
            cal_start = pd.to_datetime(model_meta.get("calibration_start_date"))
            cal_end = pd.to_datetime(model_meta.get("calibration_end_date"))
            va_start = pd.to_datetime(model_meta.get("validation_start_date"))
            separation_ok = bool((tr_end < cal_start) and (cal_end < va_start))
        except Exception:
            separation_ok = False
        lines.append(f"- calibration/validation期間分離: {separation_ok}")
    lines.append("")

    lines.append("## ROI上位10条件")
    if len(df) == 0:
        lines.append("- データなし")
    else:
        top10 = df.sort_values(["roi", "bet_race_count"], ascending=[False, False]).head(10)
        for r in top10.itertuples():
            lines.append(
                f"- roi={r.roi:.4f} ai_weight={r.ai_weight} density={r.density_top3_max} gap12={r.gap12_min} "
                f"bet_race_count={r.bet_race_count} bet_pair_count={r.bet_pair_count} hit_rate={r.hit_rate} profit={r.profit}"
            )
    lines.append("")

    best_detail = _match_detail(details, best) if best else {}
    lines.append("## 月別ROI")
    lines.append(_fmt_dict(best_detail.get("monthly_roi", {})))
    lines.append("")

    lines.append("## 人気帯別ROI")
    lines.append(_fmt_dict(best_detail.get("popularity_bucket_roi", {})))
    lines.append("")

    lines.append("## skip_reason別件数")
    lines.append(_fmt_dict(best_detail.get("race_count_by_skip_reason", {})))
    lines.append("")

    lines.append("## popularity_source別件数")
    lines.append(_fmt_dict(best_detail.get("popularity_source_counts", {})))
    lines.append("")

    lines.append("## 場別ROI")
    lines.append(_fmt_dict(best_detail.get("venue_roi", {})))
    lines.append("")

    lines.append("## 芝/ダート別ROI")
    lines.append(_fmt_dict(best_detail.get("surface_roi", {})))
    lines.append("")

    lines.append("## 頭数帯別ROI")
    lines.append(_fmt_dict(best_detail.get("field_size_bucket_roi", {})))
    lines.append("")

    lines.append("## ai_market_gap帯別ROI")
    lines.append(_fmt_dict(best_detail.get("ai_market_gap_bucket_roi", {})))
    lines.append("")

    lines.append("## UNKNOWN venue/surface の生値")
    lines.append(f"- venue: {best_detail.get('unknown_raw_venue_values', [])}")
    lines.append(f"- surface: {best_detail.get('unknown_raw_surface_values', [])}")
    lines.append("")

    lines.append("## grid metadata")
    lines.append(f"- grid_start_date: {grid_meta.get('grid_start_date')}")
    lines.append(f"- grid_end_date: {grid_meta.get('grid_end_date')}")
    lines.append(f"- selected_best_params: {grid_meta.get('selected_best_params')}")
    lines.append(f"- warning_if_used_for_same_period_prediction: {grid_meta.get('warning_if_used_for_same_period_prediction')}")
    lines.append(f"- apply_start_date: {grid_app.get('apply_start_date')}")
    lines.append(f"- apply_end_date: {grid_app.get('apply_end_date')}")
    lines.append(f"- is_safe_temporal_application: {grid_app.get('is_safe_temporal_application')}")
    lines.append("")

    lines.append("## leakage warnings")
    lines.append(_fmt_dict({"warnings": leakage.get("warnings", [])} if leakage else {}))
    lines.append("")

    lines.append("## 過学習サイン")
    if len(df) == 0:
        lines.append("- 判定不可（データなし）")
    else:
        warn_lines: list[str] = []
        if best.get("bet_pair_count") is not None and float(best.get("bet_pair_count")) < 10:
            warn_lines.append("- ROIは高いがbet_pair_countが少なすぎる")
        month_roi = best_detail.get("monthly_roi", {})
        vals = [v for v in month_roi.values() if v is not None] if isinstance(month_roi, dict) else []
        if len(vals) >= 2 and (max(vals) - min(vals)) > 1.0:
            warn_lines.append("- 特定月への依存が大きい")
        venue_roi = best_detail.get("venue_roi", {})
        vals2 = [v for v in venue_roi.values() if v is not None] if isinstance(venue_roi, dict) else []
        if len(vals2) >= 2 and (max(vals2) - min(vals2)) > 1.0:
            warn_lines.append("- 特定競馬場への偏りが大きい")
        if not warn_lines:
            lines.append("- 顕著な過学習サインは未検出")
        else:
            lines.extend(warn_lines)

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")
    return str(out_md)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--grid-parquet", default="reports/wide_grid_enhanced.parquet")
    ap.add_argument("--grid-json", default="reports/wide_grid_enhanced.json")
    ap.add_argument("--detail-json", default="")
    ap.add_argument("--out-md", default="reports/wide_strategy_grid_report.md")
    args = ap.parse_args()
    detail_json = Path(args.detail_json) if str(args.detail_json).strip() else None
    path = build_report(
        grid_parquet=Path(args.grid_parquet),
        grid_json=Path(args.grid_json),
        detail_json=detail_json,
        out_md=Path(args.out_md),
    )
    print(path)


if __name__ == "__main__":
    main()
