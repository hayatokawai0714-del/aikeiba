from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def _extract_date(path: Path) -> str:
    for p in path.parts:
        if len(p) == 10 and p[4] == "-" and p[7] == "-":
            return p
    return "UNKNOWN"


def _top_reason(s: pd.Series) -> tuple[str | None, int]:
    vc = s.astype(str).value_counts(dropna=True)
    if len(vc) == 0:
        return None, 0
    return str(vc.index[0]), int(vc.iloc[0])


def main() -> None:
    ap = argparse.ArgumentParser(description="Aggregate multi-day model_dynamic edge diagnostics.")
    ap.add_argument("--glob", default="racing_ai/reports/*/model_dynamic_edge_diagnostics.csv")
    ap.add_argument("--out-csv", type=Path, default=Path("racing_ai/reports/model_dynamic_edge_diagnostics_multi_day.csv"))
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/model_dynamic_edge_diagnostics_multi_day.md"))
    args = ap.parse_args()

    paths = sorted(Path().glob(args.glob))
    rows: list[dict] = []
    for p in paths:
        race_df = pd.read_csv(p)
        day = _extract_date(p)
        pair_path = p.parent / "model_dynamic_edge_pair_diagnostics.csv"
        if not pair_path.exists():
            continue
        pair_df = pd.read_csv(pair_path)
        edge = pd.to_numeric(pair_df.get("pair_edge"), errors="coerce")
        pass_score = pd.to_numeric(pair_df.get("pass_min_score"), errors="coerce")
        pass_edge = pd.to_numeric(pair_df.get("pass_min_edge"), errors="coerce")
        pass_gap = pd.to_numeric(pair_df.get("pass_min_gap"), errors="coerce")
        pass_all = (pass_score.fillna(0).astype(int) & pass_edge.fillna(0).astype(int) & pass_gap.fillna(0).astype(int)).astype(int)
        selected = pd.to_numeric(pair_df.get("model_dynamic_selected_flag"), errors="coerce").fillna(0).astype(int)
        reason, reason_count = _top_reason(pair_df.get("model_dynamic_skip_reason", pd.Series(dtype=object)))
        rows.append(
            {
                "race_date": day,
                "row_count": int(len(pair_df)),
                "race_count": int(pair_df["race_id"].nunique()) if "race_id" in pair_df.columns else int(len(race_df)),
                "pair_edge_min": float(edge.min()) if edge.notna().any() else None,
                "pair_edge_p05": float(edge.quantile(0.05)) if edge.notna().any() else None,
                "pair_edge_p10": float(edge.quantile(0.10)) if edge.notna().any() else None,
                "pair_edge_p25": float(edge.quantile(0.25)) if edge.notna().any() else None,
                "pair_edge_p50": float(edge.quantile(0.50)) if edge.notna().any() else None,
                "pair_edge_p75": float(edge.quantile(0.75)) if edge.notna().any() else None,
                "pair_edge_p90": float(edge.quantile(0.90)) if edge.notna().any() else None,
                "pair_edge_p95": float(edge.quantile(0.95)) if edge.notna().any() else None,
                "pair_edge_p99": float(edge.quantile(0.99)) if edge.notna().any() else None,
                "pair_edge_max": float(edge.max()) if edge.notna().any() else None,
                "pair_edge_mean": float(edge.mean()) if edge.notna().any() else None,
                "positive_edge_pair_count": int((edge > 0).sum()) if edge.notna().any() else 0,
                "positive_edge_pair_rate": float((edge > 0).mean()) if edge.notna().any() else None,
                "positive_edge_race_count": int((race_df["positive_edge_pair_count"] > 0).sum()) if "positive_edge_pair_count" in race_df.columns else None,
                "positive_edge_race_rate": float((race_df["positive_edge_pair_count"] > 0).mean()) if "positive_edge_pair_count" in race_df.columns and len(race_df) > 0 else None,
                "pass_min_score_rate_current": float(pass_score.fillna(0).mean()) if len(pass_score) > 0 else None,
                "pass_min_edge_rate_current": float(pass_edge.fillna(0).mean()) if len(pass_edge) > 0 else None,
                "pass_min_gap_rate_current": float(pass_gap.fillna(0).mean()) if len(pass_gap) > 0 else None,
                "pass_all_rate_current": float(pass_all.mean()) if len(pass_all) > 0 else None,
                "selected_pair_count_current": int(selected.sum()),
                "current_skip_reason_top": reason,
                "current_skip_reason_top_count": reason_count,
            }
        )

    out = pd.DataFrame(rows).sort_values("race_date")
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    all_negative_days = int((out["pair_edge_max"] < 0).sum()) if len(out) > 0 else 0
    p95_negative_days = int((out["pair_edge_p95"] < 0).sum()) if len(out) > 0 else 0
    try:
        table = out.to_markdown(index=False)
    except Exception:
        table = out.to_string(index=False)
    md = [
        "# model_dynamic_edge_diagnostics_multi_day",
        "",
        f"- input_glob: {args.glob}",
        f"- day_count: {len(out)}",
        f"- pair_edge all-negative days (max<0): {all_negative_days}",
        f"- pair_edge p95<0 days: {p95_negative_days}",
        f"- positive_edge_pair_rate mean: {out['positive_edge_pair_rate'].mean() if len(out)>0 else None}",
        f"- positive_edge_race_rate mean: {out['positive_edge_race_rate'].mean() if len(out)>0 else None}",
        "",
        "## Daily Summary",
        "",
        table,
        "",
        "## pair_edge max by day",
        "",
    ]
    for _, r in out.iterrows():
        md.append(f"- {r['race_date']}: pair_edge_max={r['pair_edge_max']}")
    md.extend(
        [
            "",
            "## Comment",
            "",
            "- `pair_edge_max < 0` が多い場合は、差分edgeのスケール不一致またはmarket proxy過大の可能性があります。",
            "- 一部日だけ全負の場合は、日別データ品質または当日proxy構成の影響を疑ってください。",
        ]
    )
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
