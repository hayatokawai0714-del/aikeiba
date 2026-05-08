from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _to_bool(s: pd.Series) -> pd.Series:
    return _to_num(s).fillna(0).astype(int).astype(bool)


def _roi(df: pd.DataFrame) -> tuple[float | None, int, int, float]:
    if len(df) == 0:
        return None, 0, 0, 0.0
    h = _to_num(df.get("actual_wide_hit", pd.Series([None] * len(df))))
    p = _to_num(df.get("wide_payout", pd.Series([None] * len(df))))
    hit = int(h.fillna(0).sum())
    payout = float((p.fillna(0) * (h.fillna(0) > 0).astype(float)).sum())
    cost = float(len(df) * 100.0)
    roi = payout / cost if cost > 0 else None
    return roi, len(df), hit, payout


def main() -> None:
    ap = argparse.ArgumentParser(description="Build daily stability report for rule vs model_dynamic_non_overlap.")
    ap.add_argument("--input-csv", type=Path, required=True)
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--quality-ok-only", action="store_true")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.input_csv)
    if "race_date" not in df.columns:
        raise SystemExit("missing race_date")
    for c in ["pair_selected_flag", "model_dynamic_selected_flag", "actual_wide_hit", "wide_payout", "result_quality_status"]:
        if c not in df.columns:
            df[c] = pd.NA

    df["race_date"] = df["race_date"].astype(str)
    df = df[(df["race_date"] >= args.start_date) & (df["race_date"] <= args.end_date)].copy()
    if args.quality_ok_only:
        df = df[df["result_quality_status"].astype(str) == "ok"].copy()

    rows: list[dict[str, object]] = []
    for d, g in df.groupby("race_date"):
        rule = g[_to_bool(g["pair_selected_flag"])]
        dyn_non_overlap = g[_to_bool(g["model_dynamic_selected_flag"]) & (~_to_bool(g["pair_selected_flag"]))]
        rule_roi, rule_cnt, rule_hit, rule_pay = _roi(rule)
        dyn_roi, dyn_cnt, dyn_hit, dyn_pay = _roi(dyn_non_overlap)
        qok_races = int(g.loc[g["result_quality_status"].astype(str) == "ok", "race_id"].nunique()) if "race_id" in g.columns else 0
        rows.append(
            {
                "race_date": d,
                "quality_ok_race_count": qok_races,
                "rule_selected_candidate_count": rule_cnt,
                "rule_selected_hit_count": rule_hit,
                "rule_selected_total_payout": rule_pay,
                "rule_selected_roi": rule_roi,
                "model_dynamic_non_overlap_candidate_count": dyn_cnt,
                "model_dynamic_non_overlap_hit_count": dyn_hit,
                "model_dynamic_non_overlap_total_payout": dyn_pay,
                "model_dynamic_non_overlap_roi": dyn_roi,
                "dynamic_minus_rule_roi": (dyn_roi - rule_roi) if dyn_roi is not None and rule_roi is not None else pd.NA,
            }
        )

    out = pd.DataFrame(rows).sort_values("race_date")
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    try:
        tbl = out.to_markdown(index=False)
    except Exception:
        tbl = out.to_string(index=False)
    positive_days = int((pd.to_numeric(out["dynamic_minus_rule_roi"], errors="coerce") > 0).sum()) if len(out) else 0
    md = [
        "# dynamic_vs_rule_daily_stability",
        "",
        f"- date_range: {args.start_date} to {args.end_date}",
        f"- quality_ok_only: {args.quality_ok_only}",
        f"- daily_rows: {len(out)}",
        f"- dynamic_minus_rule_roi_positive_days: {positive_days}",
        "",
        tbl,
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()

