from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pandas as pd


def _read_text_if_exists(p: Path) -> str:
    try:
        if p.exists():
            return p.read_text(encoding="utf-8")
    except Exception:
        return ""
    return ""


def main() -> None:
    ap = argparse.ArgumentParser(description="Write final markdown summary for 2024 v5 evaluation (evaluation helper path).")
    ap.add_argument("--reports-dir", type=Path, required=True, help="e.g. racing_ai/reports/2024_eval_full_v5")
    ap.add_argument("--v4-dir", type=Path, required=True, help="e.g. racing_ai/reports/2024_eval_full_v4")
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    rdir = args.reports_dir
    v4dir = args.v4_dir

    # Files we expect in v5 dir (may be created by user runs)
    req_cmp_md = rdir / "pair_reranker_required_features_compare_2024_v4_v5.md"
    score_cmp_md = rdir / "pair_model_score_distribution_compare_2024_v4_v5_full.md"
    fallback_md = rdir / "market_proxy_fallback_usage_2024_v5.md"
    post_md = rdir / "model_dynamic_postcompute_audit_2024_v5.md"
    eval_md = rdir / "rule_vs_non_rule_candidate_evaluation_2024_v5_quality_ok.md"
    cond_md = rdir / "expanded_dynamic_candidate_conditions_with_results_2024_v5_quality_ok.md"
    stab_md = rdir / "dynamic_vs_rule_daily_stability_2024_v5.md"
    grid_md = rdir / "model_dynamic_threshold_grid_2024_quality_ok_v5.md"

    # Pull key numbers when available
    key_lines: list[str] = []
    eval_csv = rdir / "rule_vs_non_rule_candidate_evaluation_2024_v5_quality_ok.csv"
    if eval_csv.exists():
        df = pd.read_csv(eval_csv, low_memory=False)
        # Expect groups: rule_selected / model_dynamic_non_overlap
        def _get(group: str, col: str):
            g = df[df["group"] == group]
            if len(g) == 0 or col not in g.columns:
                return None
            v = g.iloc[0][col]
            return v

        rule_roi = _get("rule_selected", "roi_proxy")
        dyn_roi = _get("model_dynamic_non_overlap", "roi_proxy")
        key_lines += [
            f"- rule_selected ROI (quality_ok): {rule_roi}",
            f"- model_dynamic_non_overlap ROI (quality_ok): {dyn_roi}",
        ]
        if rule_roi is not None and dyn_roi is not None:
            try:
                key_lines.append(f"- dynamic_minus_rule_roi: {float(dyn_roi) - float(rule_roi)}")
            except Exception:
                pass

    md = [
        "# 2024 v5 Shadow Evaluation Summary (Final)",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- reports_dir: {rdir}",
        f"- v4_dir: {v4dir}",
        "",
        "## Scope / Constraints",
        "",
        "- Production logic was NOT changed (`race_day.py` untouched).",
        "- `pair_selected_flag` semantics are unchanged (rule-based production selection flag).",
        "- `model_dynamic` remains shadow-evaluation only.",
        "- `actual_wide_hit` / `wide_payout` are evaluation-only signals.",
        "",
        "## Critical Caveat (Market Proxy)",
        "",
        "- **`market_proxy_source` is 100% `predictions_scaled_low_confidence` for 2024 v5.**",
        "- This is NOT odds-derived; interpret ROI and any edge-related analyses cautiously.",
        "",
    ]
    if key_lines:
        md += ["## Headline Numbers (from v5 outputs)", ""] + key_lines + [""]

    md += [
        "## Evidence / Artifacts",
        "",
        "### v4 vs v5 feature recovery",
        _read_text_if_exists(req_cmp_md) or "(missing: run compare script to generate)",
        "",
        "### v4 vs v5 score distribution",
        _read_text_if_exists(score_cmp_md) or "(missing: run compare script to generate)",
        "",
        "### Market proxy fallback usage",
        _read_text_if_exists(fallback_md) or "(missing: run audit_market_proxy_fallback_usage.py)",
        "",
        "### model_dynamic postcompute audit",
        _read_text_if_exists(post_md) or "(missing: run audit_model_dynamic_postcompute_from_joined_pairs.py)",
        "",
        "### Evaluation (quality_ok only)",
        _read_text_if_exists(eval_md) or "(missing: run evaluate_rule_vs_non_rule_candidates.py)",
        "",
        "### Expanded dynamic conditions (quality_ok only)",
        _read_text_if_exists(cond_md) or "(missing: run evaluate_expanded_dynamic_conditions_with_results.py)",
        "",
        "### Daily stability",
        _read_text_if_exists(stab_md) or "(missing: run build_dynamic_vs_rule_daily_stability.py)",
        "",
        "### Threshold grid (quality_ok only)",
        _read_text_if_exists(grid_md) or "(missing: run grid_search script)",
        "",
        "## Interpretation Checklist",
        "",
        "- If v5 score std / gap quantiles increased vs v4: the feature recovery is working.",
        "- If non-overlap count increased and gap gate stops being all-skip: dynamic gating is meaningful again.",
        "- If any ROI improvements are seen, re-check that they are not dominated by low-confidence proxy dates/venues.",
        "",
        "## Next Decision",
        "",
        "- Given 2024 v5 uses 100% low-confidence proxy, prioritize restoring odds-derived market proxy for 2024 before making go/no-go calls for 2023 expansion.",
        "- If odds cannot be restored historically, consider ROI-oriented retraining that does not rely on odds proxy features, or redesign dynamic selection without edge components.",
        "",
    ]

    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_md))


if __name__ == "__main__":
    main()

