from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from aikeiba.db.duckdb import DuckDb
from aikeiba.evaluation.comparison_report import make_comparison_report
from aikeiba.evaluation.comparison_view import build_comparison_view, publish_latest_comparison_files
from aikeiba.evaluation.run_summary_linker import find_latest_run_summary_for_model_version
from aikeiba.modeling.top3 import train_top3_bundle


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _stability_group_usage(summary_path: Path | None) -> dict[str, Any]:
    if summary_path is None or not summary_path.exists():
        return {"group_gain": None, "group_split": None, "top20_count": 0}
    payload = _read_json(summary_path)
    grouped = payload.get("grouped_summary", [])
    group_gain = None
    group_split = None
    for row in grouped:
        if row.get("group_name") == "安定度系":
            group_gain = row.get("total_gain")
            group_split = row.get("total_split")
            break
    top20 = payload.get("top_features_by_gain", [])[:20]
    top20_count = 0
    for row in top20:
        name = str(row.get("feature_name", "")).lower()
        if (
            "std" in name
            or "top3_rate_last" in name
            or "board_rate_last" in name
            or "consecutive_" in name
            or "big_loss" in name
            or "worst_finish" in name
            or "min_history" in name
        ):
            top20_count += 1
    return {"group_gain": group_gain, "group_split": group_split, "top20_count": int(top20_count)}


def _diff(stability_value: Any, baseline_value: Any) -> float | None:
    if stability_value is None or baseline_value is None:
        return None
    return float(stability_value) - float(baseline_value)


def _improved_smaller(stability_value: Any, baseline_value: Any) -> bool | None:
    d = _diff(stability_value, baseline_value)
    return None if d is None else d < 0


def _improved_bigger(stability_value: Any, baseline_value: Any) -> bool | None:
    d = _diff(stability_value, baseline_value)
    return None if d is None else d > 0


def _resolve_run_summaries(
    *,
    baseline_model_version: str,
    stability_model_version: str,
    baseline_run_summary_path: Path | None,
    stability_run_summary_path: Path | None,
    run_summary_search_dirs: list[Path] | None,
) -> tuple[Path | None, Path | None]:
    search_dirs = run_summary_search_dirs or [Path("data/exports"), Path("../data")]
    baseline_path = baseline_run_summary_path
    stability_path = stability_run_summary_path
    if baseline_path is None:
        baseline_path = find_latest_run_summary_for_model_version(
            model_version=baseline_model_version,
            search_dirs=search_dirs,
        )
    if stability_path is None:
        stability_path = find_latest_run_summary_for_model_version(
            model_version=stability_model_version,
            search_dirs=search_dirs,
        )
    return baseline_path, stability_path


def _build_metric_comments(
    *,
    logloss_diff: float | None,
    brier_diff: float | None,
    ece_diff: float | None,
    roi_diff: float | None,
    hit_rate_diff: float | None,
    buy_races_diff: float | None,
    max_losing_streak_diff: float | None,
) -> list[str]:
    comments: list[str] = []
    comments.append("logloss: improved" if logloss_diff is not None and logloss_diff < 0 else ("logloss: worsened" if logloss_diff is not None and logloss_diff > 0 else ("logloss: no_change" if logloss_diff == 0 else "logloss: unavailable")))
    comments.append("brier: improved" if brier_diff is not None and brier_diff < 0 else ("brier: worsened" if brier_diff is not None and brier_diff > 0 else ("brier: no_change" if brier_diff == 0 else "brier: unavailable")))
    comments.append("ece: improved" if ece_diff is not None and ece_diff < 0 else ("ece: worsened" if ece_diff is not None and ece_diff > 0 else ("ece: no_change" if ece_diff == 0 else "ece: unavailable")))
    comments.append("roi: improved" if roi_diff is not None and roi_diff > 0 else ("roi: worsened" if roi_diff is not None and roi_diff < 0 else ("roi: no_change" if roi_diff == 0 else "roi: unavailable")))
    comments.append("hit_rate: improved" if hit_rate_diff is not None and hit_rate_diff > 0 else ("hit_rate: worsened" if hit_rate_diff is not None and hit_rate_diff < 0 else ("hit_rate: no_change" if hit_rate_diff == 0 else "hit_rate: unavailable")))
    if buy_races_diff is not None:
        comments.append(f"buy_races_diff: {buy_races_diff}")
    if max_losing_streak_diff is not None:
        comments.append(f"max_losing_streak_diff: {max_losing_streak_diff}")
    return comments


def _build_summary_comment(
    *,
    logloss_improved: bool | None,
    roi_improved: bool | None,
    run_summary_linked: bool,
) -> str:
    if logloss_improved is True and roi_improved is True:
        return "stability improved both probability quality and ROI"
    if logloss_improved is True and roi_improved is not True:
        return "stability improved probability quality, ROI did not improve" if run_summary_linked else "stability improved probability quality, ROI unavailable"
    if roi_improved is True and logloss_improved is not True:
        return "stability improved ROI, probability quality did not improve"
    return "stability effect is limited or unavailable"


def _build_roi_comment(roi_diff: float | None, hit_rate_diff: float | None) -> str:
    if roi_diff is None:
        return "ROI comparison unavailable (run_summary missing)"
    if roi_diff > 0 and (hit_rate_diff is None or hit_rate_diff >= 0):
        return "stability improved ROI and did not hurt hit_rate"
    if roi_diff > 0:
        return "stability improved ROI but hit_rate decreased"
    if roi_diff < 0:
        return "stability worsened ROI"
    return "ROI unchanged"


def _build_money_comment(
    total_return_diff: float | None,
    total_bet_diff: float | None,
    hit_bets_diff: float | None,
) -> str:
    if total_return_diff is None and total_bet_diff is None and hit_bets_diff is None:
        return "money comparison unavailable (run_summary missing)"
    parts: list[str] = []
    if total_return_diff is not None:
        parts.append("return_up" if total_return_diff > 0 else ("return_down" if total_return_diff < 0 else "return_flat"))
    if total_bet_diff is not None:
        parts.append("bet_up" if total_bet_diff > 0 else ("bet_down" if total_bet_diff < 0 else "bet_flat"))
    if hit_bets_diff is not None:
        parts.append("hits_up" if hit_bets_diff > 0 else ("hits_down" if hit_bets_diff < 0 else "hits_flat"))
    return ", ".join(parts)


def _build_stability_feature_comment(top20_count: int, group_gain_diff: float | None) -> str:
    if top20_count <= 0:
        return "stability features were not visible in top20 importance"
    if group_gain_diff is None:
        return f"stability features appeared in top20 (count={top20_count})"
    if group_gain_diff > 0:
        return f"stability group gain increased (+{group_gain_diff:.4f}), top20 count={top20_count}"
    if group_gain_diff < 0:
        return f"stability group gain decreased ({group_gain_diff:.4f}), top20 count={top20_count}"
    return f"stability group gain stayed flat, top20 count={top20_count}"


def _decide_adoption(
    *,
    roi_improved: bool | None,
    hit_rate_improved: bool | None,
    max_losing_streak_worsened: bool | None,
    buy_races_too_low_warning: bool | None,
) -> str:
    if roi_improved is False and hit_rate_improved is False and max_losing_streak_worsened is True:
        return "reject"
    if roi_improved is True and max_losing_streak_worsened is False and (hit_rate_improved is True or hit_rate_improved is None):
        if buy_races_too_low_warning is True:
            return "hold"
        return "adopt"
    return "hold"


def run_baseline_vs_stability_experiment(
    *,
    db: DuckDb,
    models_root: Path,
    feature_snapshot_version: str,
    train_end_date: str,
    valid_start_date: str,
    valid_end_date: str,
    test_period: str,
    baseline_model_version: str,
    stability_model_version: str,
    baseline_feature_set: str,
    stability_feature_set: str,
    baseline_experiment_name: str,
    stability_experiment_name: str,
    dataset_manifest_path: Path,
    report_dir: Path,
    summary_json_path: Path,
    summary_md_path: Path,
    publish_latest: bool,
    latest_out_dir: Path,
    baseline_run_summary_path: Path | None = None,
    stability_run_summary_path: Path | None = None,
    run_summary_search_dirs: list[Path] | None = None,
) -> dict[str, Any]:
    baseline_train = train_top3_bundle(
        db=db,
        models_root=models_root,
        model_version=baseline_model_version,
        feature_snapshot_version=feature_snapshot_version,
        train_end_date=train_end_date,
        valid_start_date=valid_start_date,
        valid_end_date=valid_end_date,
        test_period=test_period,
        feature_set=baseline_feature_set,
    )
    stability_train = train_top3_bundle(
        db=db,
        models_root=models_root,
        model_version=stability_model_version,
        feature_snapshot_version=feature_snapshot_version,
        train_end_date=train_end_date,
        valid_start_date=valid_start_date,
        valid_end_date=valid_end_date,
        test_period=test_period,
        feature_set=stability_feature_set,
    )

    resolved_baseline_run_summary, resolved_stability_run_summary = _resolve_run_summaries(
        baseline_model_version=baseline_model_version,
        stability_model_version=stability_model_version,
        baseline_run_summary_path=baseline_run_summary_path,
        stability_run_summary_path=stability_run_summary_path,
        run_summary_search_dirs=run_summary_search_dirs,
    )

    compare = make_comparison_report(
        dataset_manifest_path=dataset_manifest_path,
        report_dir=report_dir,
        experiment_names=[baseline_experiment_name, stability_experiment_name],
        experiment_model_dirs=[Path(baseline_train["model_dir"]), Path(stability_train["model_dir"])],
        experiment_run_summary_paths=[resolved_baseline_run_summary, resolved_stability_run_summary],
        strict_mismatch=True,
    )
    view = build_comparison_view(
        comparison_report_json_path=Path(compare["comparison_report_json"]),
        dataset_manifest_path=dataset_manifest_path,
        comparison_report_csv_path=Path(compare["comparison_report_csv"]),
        out_path=report_dir / "comparison_view.json",
    )

    latest_paths = {}
    if publish_latest:
        latest_paths = publish_latest_comparison_files(
            comparison_report_json_path=Path(compare["comparison_report_json"]),
            comparison_view_json_path=Path(view["comparison_view_json"]),
            latest_dir=latest_out_dir,
        )

    comp_json = _read_json(Path(compare["comparison_report_json"]))
    rows = {row.get("experiment_name"): row for row in comp_json.get("compared_experiments", [])}
    baseline_row = rows.get(baseline_experiment_name, {})
    stability_row = rows.get(stability_experiment_name, {})

    baseline_fi_path = Path(baseline_train["report_files"]["feature_importance_summary_json"])
    stability_fi_path = Path(stability_train["report_files"]["feature_importance_summary_json"])
    baseline_usage = _stability_group_usage(baseline_fi_path)
    stability_usage = _stability_group_usage(stability_fi_path)

    buy_races_warning = None
    if baseline_row.get("buy_races") is not None and stability_row.get("buy_races") is not None:
        baseline_buy = float(baseline_row.get("buy_races"))
        stability_buy = float(stability_row.get("buy_races"))
        buy_races_warning = baseline_buy > 0 and stability_buy < (baseline_buy * 0.7)

    max_losing_streak_worsened = None
    if baseline_row.get("max_losing_streak") is not None and stability_row.get("max_losing_streak") is not None:
        max_losing_streak_worsened = float(stability_row.get("max_losing_streak")) > float(baseline_row.get("max_losing_streak"))

    logloss_diff = _diff(stability_row.get("logloss_after"), baseline_row.get("logloss_after"))
    brier_diff = _diff(stability_row.get("brier_after"), baseline_row.get("brier_after"))
    ece_diff = _diff(stability_row.get("ece_after"), baseline_row.get("ece_after"))
    roi_diff = _diff(stability_row.get("roi"), baseline_row.get("roi"))
    hit_rate_diff = _diff(stability_row.get("hit_rate"), baseline_row.get("hit_rate"))
    buy_races_diff = _diff(stability_row.get("buy_races"), baseline_row.get("buy_races"))
    total_bets_diff = _diff(stability_row.get("total_bets"), baseline_row.get("total_bets"))
    max_losing_streak_diff = _diff(stability_row.get("max_losing_streak"), baseline_row.get("max_losing_streak"))
    total_return_diff = _diff(stability_row.get("total_return_yen"), baseline_row.get("total_return_yen"))
    total_bet_yen_diff = _diff(stability_row.get("total_bet_yen"), baseline_row.get("total_bet_yen"))
    hit_bets_diff = _diff(stability_row.get("hit_bets"), baseline_row.get("hit_bets"))

    logloss_improved = _improved_smaller(stability_row.get("logloss_after"), baseline_row.get("logloss_after"))
    brier_improved = _improved_smaller(stability_row.get("brier_after"), baseline_row.get("brier_after"))
    ece_improved = _improved_smaller(stability_row.get("ece_after"), baseline_row.get("ece_after"))
    roi_improved = _improved_bigger(stability_row.get("roi"), baseline_row.get("roi"))
    hit_rate_improved = _improved_bigger(stability_row.get("hit_rate"), baseline_row.get("hit_rate"))
    total_return_improved = _improved_bigger(stability_row.get("total_return_yen"), baseline_row.get("total_return_yen"))
    total_bet_increased = _improved_bigger(stability_row.get("total_bet_yen"), baseline_row.get("total_bet_yen"))
    hit_bets_improved = _improved_bigger(stability_row.get("hit_bets"), baseline_row.get("hit_bets"))

    run_summary_linked = (
        baseline_row.get("roi") is not None
        and stability_row.get("roi") is not None
        and baseline_row.get("hit_rate") is not None
        and stability_row.get("hit_rate") is not None
    )
    summary_comment = _build_summary_comment(
        logloss_improved=logloss_improved,
        roi_improved=roi_improved,
        run_summary_linked=run_summary_linked,
    )
    metric_comments = _build_metric_comments(
        logloss_diff=logloss_diff,
        brier_diff=brier_diff,
        ece_diff=ece_diff,
        roi_diff=roi_diff,
        hit_rate_diff=hit_rate_diff,
        buy_races_diff=buy_races_diff,
        max_losing_streak_diff=max_losing_streak_diff,
    )
    roi_comment = _build_roi_comment(roi_diff=roi_diff, hit_rate_diff=hit_rate_diff)
    money_comment = _build_money_comment(
        total_return_diff=total_return_diff,
        total_bet_diff=total_bet_yen_diff,
        hit_bets_diff=hit_bets_diff,
    )
    stability_feature_comment = _build_stability_feature_comment(
        top20_count=int(stability_usage.get("top20_count") or 0),
        group_gain_diff=_diff(stability_usage.get("group_gain"), baseline_usage.get("group_gain")),
    )
    adoption_decision = _decide_adoption(
        roi_improved=roi_improved,
        hit_rate_improved=hit_rate_improved,
        max_losing_streak_worsened=max_losing_streak_worsened,
        buy_races_too_low_warning=buy_races_warning,
    )

    delta_summary = {
        "created_at": dt.datetime.now().isoformat(timespec="seconds"),
        "experiment_baseline_name": baseline_experiment_name,
        "experiment_stability_name": stability_experiment_name,
        "model_version_baseline": baseline_model_version,
        "model_version_stability": stability_model_version,
        "feature_set_baseline": baseline_row.get("feature_set"),
        "feature_set_stability": stability_row.get("feature_set"),
        "dataset_fingerprint": comp_json.get("dataset_manifest", {}).get("dataset_fingerprint"),
        "logloss_after_baseline": baseline_row.get("logloss_after"),
        "logloss_after_stability": stability_row.get("logloss_after"),
        "logloss_after_diff_stability_minus_baseline": logloss_diff,
        "brier_after_baseline": baseline_row.get("brier_after"),
        "brier_after_stability": stability_row.get("brier_after"),
        "brier_after_diff_stability_minus_baseline": brier_diff,
        "ece_after_baseline": baseline_row.get("ece_after"),
        "ece_after_stability": stability_row.get("ece_after"),
        "ece_after_diff_stability_minus_baseline": ece_diff,
        "roi_baseline": baseline_row.get("roi"),
        "roi_stability": stability_row.get("roi"),
        "roi_diff_stability_minus_baseline": roi_diff,
        "hit_rate_baseline": baseline_row.get("hit_rate"),
        "hit_rate_stability": stability_row.get("hit_rate"),
        "hit_rate_diff_stability_minus_baseline": hit_rate_diff,
        "buy_races_baseline": baseline_row.get("buy_races"),
        "buy_races_stability": stability_row.get("buy_races"),
        "buy_races_diff_stability_minus_baseline": buy_races_diff,
        "total_bets_baseline": baseline_row.get("total_bets"),
        "total_bets_stability": stability_row.get("total_bets"),
        "total_bets_diff_stability_minus_baseline": total_bets_diff,
        "max_losing_streak_baseline": baseline_row.get("max_losing_streak"),
        "max_losing_streak_stability": stability_row.get("max_losing_streak"),
        "max_losing_streak_diff_stability_minus_baseline": max_losing_streak_diff,
        "total_return_yen_baseline": baseline_row.get("total_return_yen"),
        "total_return_yen_stability": stability_row.get("total_return_yen"),
        "total_return_yen_diff_stability_minus_baseline": total_return_diff,
        "total_bet_yen_baseline": baseline_row.get("total_bet_yen"),
        "total_bet_yen_stability": stability_row.get("total_bet_yen"),
        "total_bet_yen_diff_stability_minus_baseline": total_bet_yen_diff,
        "hit_bets_baseline": baseline_row.get("hit_bets"),
        "hit_bets_stability": stability_row.get("hit_bets"),
        "hit_bets_diff_stability_minus_baseline": hit_bets_diff,
        "logloss_improved": logloss_improved,
        "brier_improved": brier_improved,
        "ece_improved": ece_improved,
        "roi_improved": roi_improved,
        "hit_rate_improved": hit_rate_improved,
        "total_return_improved": total_return_improved,
        "total_bet_increased": total_bet_increased,
        "hit_bets_improved": hit_bets_improved,
        "buy_races_too_low_warning": buy_races_warning,
        "max_losing_streak_worsened": max_losing_streak_worsened,
        "stability_group_gain_baseline": baseline_usage.get("group_gain"),
        "stability_group_gain_stability": stability_usage.get("group_gain"),
        "stability_group_split_baseline": baseline_usage.get("group_split"),
        "stability_group_split_stability": stability_usage.get("group_split"),
        "stability_top20_stability_count": stability_usage.get("top20_count"),
        "summary_comment": summary_comment,
        "metric_comments": metric_comments,
        "money_comment": money_comment,
        "roi_comment": roi_comment,
        "stability_feature_comment": stability_feature_comment,
        "adoption_decision": adoption_decision,
        "baseline_run_summary_path": str(resolved_baseline_run_summary) if resolved_baseline_run_summary else None,
        "stability_run_summary_path": str(resolved_stability_run_summary) if resolved_stability_run_summary else None,
    }
    _atomic_write_json(summary_json_path, delta_summary)

    md_lines = [
        "# baseline vs stability",
        "",
        f"- generated_at: {delta_summary['created_at']}",
        f"- baseline: `{baseline_model_version}`",
        f"- stability: `{stability_model_version}`",
        f"- summary_comment: {summary_comment}",
        f"- adoption_decision: {adoption_decision}",
        f"- roi_comment: {roi_comment}",
        f"- money_comment: {money_comment}",
        f"- stability_feature_comment: {stability_feature_comment}",
        "",
        "## metric diffs (stability - baseline)",
        f"- logloss_after_diff: {delta_summary['logloss_after_diff_stability_minus_baseline']}",
        f"- brier_after_diff: {delta_summary['brier_after_diff_stability_minus_baseline']}",
        f"- ece_after_diff: {delta_summary['ece_after_diff_stability_minus_baseline']}",
        f"- roi_diff: {delta_summary['roi_diff_stability_minus_baseline']}",
        f"- hit_rate_diff: {delta_summary['hit_rate_diff_stability_minus_baseline']}",
        f"- buy_races_diff: {delta_summary['buy_races_diff_stability_minus_baseline']}",
        f"- total_bets_diff: {delta_summary['total_bets_diff_stability_minus_baseline']}",
        f"- max_losing_streak_diff: {delta_summary['max_losing_streak_diff_stability_minus_baseline']}",
        f"- total_return_yen_diff: {delta_summary['total_return_yen_diff_stability_minus_baseline']}",
        f"- total_bet_yen_diff: {delta_summary['total_bet_yen_diff_stability_minus_baseline']}",
        f"- hit_bets_diff: {delta_summary['hit_bets_diff_stability_minus_baseline']}",
        "",
        "## comments",
        *[f"- {comment}" for comment in metric_comments],
    ]
    _atomic_write_text(summary_md_path, "\n".join(md_lines))

    return {
        "baseline_model_dir": baseline_train["model_dir"],
        "stability_model_dir": stability_train["model_dir"],
        "comparison_report_json": compare["comparison_report_json"],
        "comparison_view_json": view["comparison_view_json"],
        "experiment_delta_summary_json": str(summary_json_path),
        "experiment_delta_summary_md": str(summary_md_path),
        "baseline_run_summary_path": str(resolved_baseline_run_summary) if resolved_baseline_run_summary else None,
        "stability_run_summary_path": str(resolved_stability_run_summary) if resolved_stability_run_summary else None,
        **latest_paths,
    }
