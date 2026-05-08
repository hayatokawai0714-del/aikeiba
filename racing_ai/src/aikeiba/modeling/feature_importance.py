from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Any

import numpy as np

from aikeiba.modeling.lgbm import LgbmModel


def classify_feature_group(feature_name: str) -> str:
    """
    Rule-based grouping for feature importance summary.
    Assumption: substring-based mapping is sufficient for MVP and can be refined later.
    """
    name = feature_name.lower()

    if "last3f" in name or "agari" in name:
        return "上がり順位系"
    if "margin" in name or "chakusa" in name:
        return "着差系"
    if "distance" in name or "dist_" in name:
        return "距離適性系"
    if "course" in name or "venue" in name:
        return "コース適性系"
    if (
        "std" in name
        or "stability" in name
        or "itb" in name
        or "big_loss" in name
        or "top3_rate_last" in name
        or "board_rate_last" in name
        or "consecutive_" in name
        or "worst_finish" in name
        or "avg_finish_pos" in name
        or "min_history" in name
    ):
        return "安定度系"
    if "pace" in name or "corner" in name or "position" in name:
        return "展開 / ペース系"
    if "class" in name:
        return "クラス慣れ系"
    if "waku" in name or "horse_no" in name or "gate" in name or "frame" in name:
        return "枠順 / 馬番系"
    if "jockey" in name or "trainer" in name or "kishu" in name or "chokyo" in name:
        return "騎手 / 調教師系"
    if "odds" in name or "market" in name or "value" in name or "pop" in name:
        return "市場比較系"
    return "その他"


def _rank_desc_stable(values: list[float], names: list[str]) -> list[int]:
    order = sorted(range(len(values)), key=lambda i: (-values[i], names[i]))
    ranks = [0] * len(values)
    for rank, idx in enumerate(order, start=1):
        ranks[idx] = rank
    return ranks


def _atomic_write_text(path: Path, content: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _atomic_write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    tmp.replace(path)


def build_and_save_feature_importance_reports(
    *,
    out_dir: Path,
    model: LgbmModel,
    model_version: str,
    feature_snapshot_version: str,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    created_at = dt.datetime.now().isoformat(timespec="seconds")

    feature_names = list(model.feature_names)
    gain_values = list(np.asarray(model.booster.feature_importance(importance_type="gain"), dtype=float))
    split_values = list(np.asarray(model.booster.feature_importance(importance_type="split"), dtype=float))

    if not (len(feature_names) == len(gain_values) == len(split_values)):
        raise ValueError("feature importance length mismatch")

    rank_gain = _rank_desc_stable(gain_values, feature_names)
    rank_split = _rank_desc_stable(split_values, feature_names)

    rows: list[dict[str, Any]] = []
    for i, name in enumerate(feature_names):
        rows.append(
            {
                "model_version": model_version,
                "feature_snapshot_version": feature_snapshot_version,
                "feature_name": name,
                "importance_gain": float(gain_values[i]),
                "importance_split": float(split_values[i]),
                "rank_gain": int(rank_gain[i]),
                "rank_split": int(rank_split[i]),
                "created_at": created_at,
            }
        )

    rows_sorted = sorted(rows, key=lambda r: (r["rank_gain"], r["feature_name"]))
    top_by_gain = sorted(rows, key=lambda r: (-r["importance_gain"], r["feature_name"]))[:10]
    top_by_split = sorted(rows, key=lambda r: (-r["importance_split"], r["feature_name"]))[:10]

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        group_name = classify_feature_group(row["feature_name"])
        rec = grouped.setdefault(
            group_name,
            {"group_name": group_name, "feature_count": 0, "total_gain": 0.0, "total_split": 0.0},
        )
        rec["feature_count"] += 1
        rec["total_gain"] += float(row["importance_gain"])
        rec["total_split"] += float(row["importance_split"])

    grouped_summary = []
    for key in sorted(grouped.keys()):
        rec = grouped[key]
        cnt = max(int(rec["feature_count"]), 1)
        grouped_summary.append(
            {
                "group_name": rec["group_name"],
                "feature_count": int(rec["feature_count"]),
                "total_gain": float(rec["total_gain"]),
                "total_split": float(rec["total_split"]),
                "mean_gain": float(rec["total_gain"] / cnt),
                "mean_split": float(rec["total_split"] / cnt),
            }
        )

    summary = {
        "model_version": model_version,
        "feature_snapshot_version": feature_snapshot_version,
        "created_at": created_at,
        "feature_count": len(rows),
        "top_features_by_gain": top_by_gain,
        "top_features_by_split": top_by_split,
        "grouped_summary": grouped_summary,
    }

    csv_path = out_dir / "feature_importance.csv"
    json_path = out_dir / "feature_importance_summary.json"
    _atomic_write_csv(
        csv_path,
        rows_sorted,
        fieldnames=[
            "model_version",
            "feature_snapshot_version",
            "feature_name",
            "importance_gain",
            "importance_split",
            "rank_gain",
            "rank_split",
            "created_at",
        ],
    )
    _atomic_write_text(json_path, json.dumps(summary, ensure_ascii=False, indent=2))

    return {
        "feature_importance_csv": str(csv_path),
        "feature_importance_summary_json": str(json_path),
        "feature_importance_summary": summary,
    }
