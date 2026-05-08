from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score


BASE_FEATURES = [
    "pair_prob_naive",
    "pair_value_score",
    "pair_ai_market_gap_sum",
    "pair_ai_market_gap_max",
    "pair_ai_market_gap_min",
    "pair_fused_prob_sum",
    "pair_fused_prob_min",
    "pair_rank_in_race",
    "pair_value_score_rank_pct",
    "pair_value_score_z_in_race",
    "pair_prob_naive_rank_pct",
    "pair_prob_naive_z_in_race",
    "field_size",
    "distance",
    "venue",
    "surface",
    "pair_rank_bucket",
    "field_size_bucket",
    "distance_bucket",
    "pair_missing_flag",
]


def _safe_auc(y_true: np.ndarray, y_prob: np.ndarray) -> float | None:
    try:
        if len(np.unique(y_true)) < 2:
            return None
        return float(roc_auc_score(y_true, y_prob))
    except Exception:
        return None


def _safe_logloss(y_true: np.ndarray, y_prob: np.ndarray) -> float | None:
    try:
        p = np.clip(y_prob, 1e-6, 1 - 1e-6)
        return float(log_loss(y_true, p))
    except Exception:
        return None


def _topn_hit_compare(dfv: pd.DataFrame, n_list: list[int]) -> list[dict]:
    out = []
    if len(dfv) == 0:
        return out
    for n in n_list:
        m_rule = (
            dfv.sort_values(["race_id", "pair_value_score"], ascending=[True, False])
            .groupby("race_id", as_index=False)
            .head(n)
        )
        m_model = (
            dfv.sort_values(["race_id", "pair_model_score"], ascending=[True, False])
            .groupby("race_id", as_index=False)
            .head(n)
        )
        out.append(
            {
                "top_n_per_race": int(n),
                "rule_rows": int(len(m_rule)),
                "model_rows": int(len(m_model)),
                "rule_hit_rate": float(m_rule["actual_wide_hit"].mean()) if len(m_rule) > 0 else None,
                "model_hit_rate": float(m_model["actual_wide_hit"].mean()) if len(m_model) > 0 else None,
            }
        )
    return out


def _race_date_hit_rate(dfv: pd.DataFrame, score_col: str) -> list[dict]:
    if len(dfv) == 0:
        return []
    out = []
    for d, g in dfv.groupby("race_date"):
        if len(g) == 0:
            continue
        best = g.sort_values(["race_id", score_col], ascending=[True, False]).groupby("race_id", as_index=False).head(1)
        out.append(
            {
                "race_date": str(d),
                "rows": int(len(g)),
                "races": int(g["race_id"].nunique()),
                "top1_hit_rate": float(best["actual_wide_hit"].mean()) if len(best) > 0 else None,
            }
        )
    return sorted(out, key=lambda x: x["race_date"])


def _build_folds(work: pd.DataFrame, min_train_days: int = 1) -> list[tuple[list[str], list[str]]]:
    dates = sorted([str(x) for x in work["race_date"].dropna().astype(str).unique().tolist()])
    folds: list[tuple[list[str], list[str]]] = []
    if len(dates) <= min_train_days:
        return folds
    for i in range(min_train_days, len(dates)):
        train_dates = dates[:i]
        valid_dates = [dates[i]]
        folds.append((train_dates, valid_dates))
    return folds


def train_pair_reranker_timeseries(*, in_path: Path, model_version: str, out_root: Path, exclude_pair_value_score: bool = False) -> dict:
    df = pd.read_parquet(in_path)
    generated_at = dt.datetime.now().isoformat(timespec="seconds")
    warnings: list[str] = []

    if len(df) < 500:
        warnings.append("small_sample_warning:rows_less_than_500")
    if df["race_date"].nunique() < 3:
        warnings.append("time_split_warning:validation_race_dates_less_than_3")

    work = df.copy()
    work["race_date"] = work["race_date"].astype(str)
    work["race_date_ord"] = pd.to_datetime(work["race_date"], errors="coerce")
    work = work.sort_values(["race_date_ord", "race_id", "pair_rank_in_race"], ascending=[True, True, True]).reset_index(drop=True)

    candidate_features = [c for c in BASE_FEATURES if (not exclude_pair_value_score or c != "pair_value_score")]
    feat_cols = [c for c in candidate_features if c in work.columns]
    missing_feats = [c for c in candidate_features if c not in work.columns]
    for c in missing_feats:
        warnings.append(f"feature_missing_warning:{c}")

    folds = _build_folds(work, min_train_days=1)
    if len(folds) == 0:
        warnings.append("fold_warning:not_enough_dates_for_timeseries_fold")

    fold_rows: list[pd.DataFrame] = []
    fold_metrics: list[dict] = []
    fi_rows: list[pd.DataFrame] = []

    for fold_id, (train_dates, valid_dates) in enumerate(folds, start=1):
        train_df = work[work["race_date"].isin(train_dates)].copy()
        valid_df = work[work["race_date"].isin(valid_dates)].copy()
        if len(train_df) == 0 or len(valid_df) == 0:
            continue
        if train_df["actual_wide_hit"].nunique() < 2 or valid_df["actual_wide_hit"].nunique() < 1:
            warnings.append(f"fold_warning:label_variation_insufficient:fold_{fold_id}")
            continue

        X_train = train_df[feat_cols].copy()
        X_valid = valid_df[feat_cols].copy()
        y_train = train_df["actual_wide_hit"].astype(int).values
        y_valid = valid_df["actual_wide_hit"].astype(int).values

        cat_cols = [c for c in X_train.columns if str(X_train[c].dtype) in ("object", "bool", "category")]
        for c in cat_cols:
            X_train[c] = X_train[c].astype("category")
            X_valid[c] = X_valid[c].astype("category")
            X_valid[c] = X_valid[c].cat.set_categories(X_train[c].cat.categories)

        params = {
            "objective": "binary",
            "metric": ["auc", "binary_logloss"],
            "learning_rate": 0.03,
            "num_leaves": 15,
            "max_depth": 4,
            "min_data_in_leaf": 20,
            "lambda_l1": 2.0,
            "lambda_l2": 5.0,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 1,
            "verbose": -1,
            "seed": 42 + fold_id,
        }

        dtrain = lgb.Dataset(X_train, label=y_train, feature_name=list(X_train.columns), categorical_feature=cat_cols, free_raw_data=False)
        dvalid = lgb.Dataset(X_valid, label=y_valid, feature_name=list(X_train.columns), categorical_feature=cat_cols, free_raw_data=False)

        booster = lgb.train(
            params,
            dtrain,
            num_boost_round=800,
            valid_sets=[dvalid],
            callbacks=[lgb.early_stopping(stopping_rounds=80, verbose=False)],
        )

        pred = booster.predict(X_valid)
        valid_pred = valid_df.copy()
        valid_pred["pair_model_score"] = pred
        valid_pred["fold_id"] = fold_id
        fold_rows.append(valid_pred)

        auc = _safe_auc(y_valid, pred)
        ll = _safe_logloss(y_valid, pred)
        topn = _topn_hit_compare(valid_pred, [1, 2, 3])

        metric = {
            "fold_id": fold_id,
            "train_dates": train_dates,
            "valid_dates": valid_dates,
            "train_rows": int(len(train_df)),
            "validation_rows": int(len(valid_df)),
            "auc": auc,
            "logloss": ll,
            "topn": topn,
        }
        fold_metrics.append(metric)

        fi = pd.DataFrame({
            "fold_id": fold_id,
            "feature": booster.feature_name(),
            "importance_gain": booster.feature_importance(importance_type="gain"),
            "importance_split": booster.feature_importance(importance_type="split"),
        })
        fi_rows.append(fi)

    if len(fold_rows) == 0:
        warnings.append("fold_warning:no_valid_folds")
        fold_pred = work.head(0).copy()
        fold_pred["pair_model_score"] = pd.Series(dtype=float)
        fold_pred["fold_id"] = pd.Series(dtype=int)
    else:
        fold_pred = pd.concat(fold_rows, ignore_index=True)

    fold_pred_path = Path("racing_ai/data/modeling/pair_reranker_fold_predictions.parquet")
    fold_pred_path.parent.mkdir(parents=True, exist_ok=True)
    fold_pred.to_parquet(fold_pred_path, index=False)

    # aggregate fold metrics
    fold_summary = []
    model_better_top1 = 0
    model_better_top2 = 0
    model_better_top3 = 0
    for m in fold_metrics:
        t = {x["top_n_per_race"]: x for x in m["topn"]}
        r1, m1 = t.get(1, {}).get("rule_hit_rate"), t.get(1, {}).get("model_hit_rate")
        r2, m2 = t.get(2, {}).get("rule_hit_rate"), t.get(2, {}).get("model_hit_rate")
        r3, m3 = t.get(3, {}).get("rule_hit_rate"), t.get(3, {}).get("model_hit_rate")
        if m1 is not None and r1 is not None and m1 >= r1:
            model_better_top1 += 1
        if m2 is not None and r2 is not None and m2 >= r2:
            model_better_top2 += 1
        if m3 is not None and r3 is not None and m3 >= r3:
            model_better_top3 += 1
        fold_summary.append(
            {
                "fold_id": m["fold_id"],
                "auc": m["auc"],
                "logloss": m["logloss"],
                "top1_rule": r1,
                "top1_model": m1,
                "top2_rule": r2,
                "top2_model": m2,
                "top3_rule": r3,
                "top3_model": m3,
            }
        )

    overall_valid_rows = int(len(fold_pred))
    overall_valid_dates = int(fold_pred["race_date"].nunique()) if overall_valid_rows > 0 else 0

    # pooled topN
    pooled_topn = _topn_hit_compare(fold_pred, [1, 2, 3]) if overall_valid_rows > 0 else []
    pooled_map = {x["top_n_per_race"]: x for x in pooled_topn}
    pooled_auc = _safe_auc(fold_pred["actual_wide_hit"].astype(int).values, fold_pred["pair_model_score"].values) if overall_valid_rows > 0 else None

    safety_gate = {
        "validation_race_dates_ge_3": overall_valid_dates >= 3,
        "validation_rows_ge_300": overall_valid_rows >= 300,
        "model_top1_ge_rule_top1": (pooled_map.get(1, {}).get("model_hit_rate") is not None and pooled_map.get(1, {}).get("rule_hit_rate") is not None and pooled_map.get(1, {}).get("model_hit_rate") >= pooled_map.get(1, {}).get("rule_hit_rate")),
        "model_top2_ge_rule_top2": (pooled_map.get(2, {}).get("model_hit_rate") is not None and pooled_map.get(2, {}).get("rule_hit_rate") is not None and pooled_map.get(2, {}).get("model_hit_rate") >= pooled_map.get(2, {}).get("rule_hit_rate")),
        "auc_ge_0_55": (pooled_auc is not None and pooled_auc >= 0.55),
        "no_small_sample_warning": ("small_sample_warning:rows_less_than_500" not in warnings),
    }
    safety_gate_result = bool(all(safety_gate.values()))

    # feature importance aggregated
    if len(fi_rows) > 0:
        fi_all = pd.concat(fi_rows, ignore_index=True)
        fi_agg = fi_all.groupby("feature", as_index=False).agg(
            importance_gain=("importance_gain", "mean"),
            importance_split=("importance_split", "mean"),
        ).sort_values("importance_gain", ascending=False)
    else:
        fi_agg = pd.DataFrame(columns=["feature", "importance_gain", "importance_split"])

    model_dir = out_root / model_version
    model_dir.mkdir(parents=True, exist_ok=True)

    # train final model on all rows (artifact only; not production-connected)
    final_model_path = model_dir / "model.txt"
    if len(work) > 0 and work["actual_wide_hit"].nunique() >= 2 and len(feat_cols) > 0:
        X_all = work[feat_cols].copy()
        y_all = work["actual_wide_hit"].astype(int).values
        cat_cols_all = [c for c in X_all.columns if str(X_all[c].dtype) in ("object", "bool", "category")]
        for c in cat_cols_all:
            X_all[c] = X_all[c].astype("category")
        d_all = lgb.Dataset(X_all, label=y_all, feature_name=list(X_all.columns), categorical_feature=cat_cols_all, free_raw_data=False)
        booster_final = lgb.train(
            {
                "objective": "binary",
                "metric": ["auc", "binary_logloss"],
                "learning_rate": 0.03,
                "num_leaves": 15,
                "max_depth": 4,
                "min_data_in_leaf": 20,
                "lambda_l1": 2.0,
                "lambda_l2": 5.0,
                "feature_fraction": 0.8,
                "bagging_fraction": 0.8,
                "bagging_freq": 1,
                "verbose": -1,
                "seed": 42,
            },
            d_all,
            num_boost_round=200,
        )
        booster_final.save_model(str(final_model_path))

    meta = {
        "model_version": model_version,
        "target": "actual_wide_hit",
        "features": feat_cols,
        "exclude_pair_value_score": bool(exclude_pair_value_score),
        "row_count_all": int(len(work)),
        "race_date_count_all": int(work["race_date"].nunique()) if len(work) > 0 else 0,
        "fold_count": int(len(fold_metrics)),
        "fold_metrics": fold_summary,
        "pooled_auc": pooled_auc,
        "pooled_topn": pooled_topn,
        "race_date_hit_rate_model_top1": _race_date_hit_rate(fold_pred, "pair_model_score") if overall_valid_rows > 0 else [],
        "race_date_hit_rate_rule_top1": _race_date_hit_rate(fold_pred.rename(columns={"pair_value_score": "_tmp_rule"}), "_tmp_rule") if overall_valid_rows > 0 else [],
        "model_beats_rule_fold_count": {
            "top1": model_better_top1,
            "top2": model_better_top2,
            "top3": model_better_top3,
        },
        "validation_rows": overall_valid_rows,
        "validation_race_dates": overall_valid_dates,
        "safety_gate": safety_gate,
        "safety_gate_result": safety_gate_result,
        "warnings": warnings,
        "generated_at": generated_at,
    }
    (model_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    report_path = Path("racing_ai/reports/pair_reranker_timeseries_report.md")
    lines = [
        "# pair_reranker_timeseries_report",
        "",
        f"- generated_at: {generated_at}",
        f"- model_version: {model_version}",
        f"- total_rows: {len(work)}",
        f"- race_date_count: {work['race_date'].nunique() if len(work)>0 else 0}",
        f"- fold_count: {len(fold_metrics)}",
        f"- exclude_pair_value_score: {bool(exclude_pair_value_score)}",
        f"- validation_rows: {overall_valid_rows}",
        f"- validation_race_dates: {overall_valid_dates}",
        f"- pooled_auc: {pooled_auc}",
        "",
        "## Fold Metrics",
    ]
    if len(fold_summary) == 0:
        lines.append("- none")
    else:
        for r in fold_summary:
            lines.append(
                f"- fold={r['fold_id']} auc={r['auc']} logloss={r['logloss']} "
                f"top1(rule/model)={r['top1_rule']}/{r['top1_model']} "
                f"top2(rule/model)={r['top2_rule']}/{r['top2_model']} "
                f"top3(rule/model)={r['top3_rule']}/{r['top3_model']}"
            )

    lines.extend(["", "## Rule vs Model (pooled)"])
    for r in pooled_topn:
        diff = None
        if r.get("model_hit_rate") is not None and r.get("rule_hit_rate") is not None:
            diff = r["model_hit_rate"] - r["rule_hit_rate"]
        lines.append(
            f"- top{r['top_n_per_race']}: rule={r['rule_hit_rate']} model={r['model_hit_rate']} diff={diff}"
        )

    lines.extend(["", "## Race Date Hit Rate (top1)"])
    for r in meta["race_date_hit_rate_model_top1"]:
        lines.append(f"- {r['race_date']}: model_top1_hit_rate={r['top1_hit_rate']} rows={r['rows']} races={r['races']}")

    lines.extend(["", "## Safety Gate"])
    for k, v in safety_gate.items():
        lines.append(f"- {k}: {v}")
    lines.append(f"- safety_gate_result: {safety_gate_result}")

    lines.extend(["", "## Warnings"])
    if warnings:
        lines.extend([f"- {w}" for w in warnings])
    else:
        lines.append("- none")

    lines.extend(["", "## Feature Importance Top10 (mean gain)"])
    if len(fi_agg) == 0:
        lines.append("- none")
    else:
        for r in fi_agg.head(10).itertuples(index=False):
            lines.append(f"- {r.feature}: gain={r.importance_gain} split={r.importance_split}")

    report_path.write_text("\n".join(lines), encoding="utf-8")

    return {
        "model_dir": str(model_dir),
        "meta_path": str(model_dir / "meta.json"),
        "fold_predictions_path": str(fold_pred_path),
        "report_path": str(report_path),
        "rows": int(len(work)),
        "race_date_count": int(work["race_date"].nunique()) if len(work) > 0 else 0,
        "fold_count": int(len(fold_metrics)),
        "fold_metrics": fold_summary,
        "pooled_topn": pooled_topn,
        "safety_gate_result": safety_gate_result,
        "warnings": warnings,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-path", default="racing_ai/data/modeling/pair_learning_base.parquet")
    ap.add_argument("--model-version", default="pair_reranker_ts_v1")
    ap.add_argument("--out-root", default="racing_ai/data/models_compare/pair_reranker")
    ap.add_argument("--exclude-pair-value-score", action="store_true")
    args = ap.parse_args()

    res = train_pair_reranker_timeseries(
        in_path=Path(args.in_path),
        model_version=str(args.model_version),
        out_root=Path(args.out_root),
        exclude_pair_value_score=bool(args.exclude_pair_value_score),
    )
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
