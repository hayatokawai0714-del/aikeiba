from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _roi(df: pd.DataFrame) -> tuple[int | None, float | None, float | None, float | None]:
    if len(df) == 0:
        return 0, None, 0.0, None
    hit = _to_num(df.get("actual_wide_hit", pd.Series([None] * len(df))))
    pay = _to_num(df.get("wide_payout", pd.Series([None] * len(df))))
    if not hit.notna().any():
        return None, None, None, None
    h = int(hit.fillna(0).sum())
    payout = float((pay.fillna(0) * (hit.fillna(0) > 0).astype(float)).sum()) if pay.notna().any() else None
    cost = float(len(df) * 100.0)
    roi = (payout / cost) if (payout is not None and cost > 0) else None
    return h, (h / len(df)) if len(df) > 0 else None, payout, roi


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate non-rule model candidate performance.")
    ap.add_argument("--input-csv", type=Path, required=True, help="non_rule_model_candidates*.csv")
    ap.add_argument("--out-csv", type=Path, default=Path("racing_ai/reports/non_rule_model_candidates_evaluation.csv"))
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/non_rule_model_candidates_evaluation.md"))
    args = ap.parse_args()

    df = pd.read_csv(args.input_csv)
    for c in ["pair_model_score", "pair_value_score", "pair_market_implied_prob", "pair_edge", "pair_edge_ratio", "actual_wide_hit", "wide_payout", "non_rule_model_rank"]:
        if c not in df.columns:
            df[c] = pd.NA

    h, hr, payout, roi = _roi(df)
    hit_cov = float(_to_num(df["actual_wide_hit"]).notna().mean()) if len(df) > 0 else None
    pay_cov = float(_to_num(df["wide_payout"]).notna().mean()) if len(df) > 0 else None
    overall = pd.DataFrame(
        [
            {
                "non_rule_candidate_count": int(len(df)),
                "non_rule_hit_count": h,
                "non_rule_hit_rate": hr,
                "non_rule_total_payout": payout,
                "non_rule_cost": float(len(df) * 100.0),
                "non_rule_roi_proxy": roi,
                "hit_label_coverage_rate": hit_cov,
                "payout_coverage_rate": pay_cov,
                "avg_pair_model_score": float(_to_num(df["pair_model_score"]).mean()) if len(df) > 0 else None,
                "avg_pair_value_score": float(_to_num(df["pair_value_score"]).mean()) if len(df) > 0 else None,
                "avg_pair_market_implied_prob": float(_to_num(df["pair_market_implied_prob"]).mean()) if len(df) > 0 else None,
                "avg_pair_edge": float(_to_num(df["pair_edge"]).mean()) if len(df) > 0 else None,
                "avg_pair_edge_ratio": float(_to_num(df["pair_edge_ratio"]).mean()) if len(df) > 0 else None,
            }
        ]
    )

    day_rows = []
    if "race_date" in df.columns:
        for d, g in df.groupby("race_date", dropna=False):
            gh, ghr, gp, groi = _roi(g)
            day_rows.append(
                {
                    "race_date": d,
                    "non_rule_candidate_count": int(len(g)),
                    "non_rule_hit_count": gh,
                    "non_rule_hit_rate": ghr,
                    "non_rule_total_payout": gp,
                    "non_rule_cost": float(len(g) * 100.0),
                    "non_rule_roi_proxy": groi,
                }
            )
    day_df = pd.DataFrame(day_rows)

    rank_rows = []
    r = _to_num(df["non_rule_model_rank"])
    for rk, g in df.assign(_rank=r).groupby("_rank", dropna=True):
        gh, ghr, gp, groi = _roi(g)
        rank_rows.append(
            {
                "non_rule_model_rank": int(rk),
                "candidate_count": int(len(g)),
                "hit_count": gh,
                "hit_rate": ghr,
                "total_payout": gp,
                "cost": float(len(g) * 100.0),
                "roi_proxy": groi,
            }
        )
    rank_df = pd.DataFrame(rank_rows).sort_values("non_rule_model_rank")

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    # store long format with section label for easy downstream handling
    out = pd.concat(
        [
            overall.assign(section="overall"),
            day_df.assign(section="by_day"),
            rank_df.assign(section="by_rank"),
        ],
        ignore_index=True,
        sort=False,
    )
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    try:
        o_tbl = overall.to_markdown(index=False)
        d_tbl = day_df.to_markdown(index=False) if len(day_df) > 0 else "(no day data)"
        r_tbl = rank_df.to_markdown(index=False) if len(rank_df) > 0 else "(no rank data)"
    except Exception:
        o_tbl = overall.to_string(index=False)
        d_tbl = day_df.to_string(index=False) if len(day_df) > 0 else "(no day data)"
        r_tbl = rank_df.to_string(index=False) if len(rank_df) > 0 else "(no rank data)"

    md = [
        "# non_rule_model_candidates_evaluation",
        "",
        "## Overall",
        o_tbl,
        "",
        "## By Day",
        d_tbl,
        "",
        "## By Rank",
        r_tbl,
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
