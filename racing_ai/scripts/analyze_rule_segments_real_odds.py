from __future__ import annotations

import itertools
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

BASE = Path(r"C:\Users\HND2205\Documents\git\aikeiba\racing_ai")
REPORT = BASE / "reports" / "2024_eval_full_v5"
PAIR_CSV = REPORT / "pair_shadow_pair_comparison_expanded_real_odds_2025_2026_with_results_external_priority.csv"
DB = BASE / "data" / "warehouse" / "aikeiba.duckdb"


def to_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    return pd.to_numeric(s, errors="coerce").fillna(0).astype(int).astype(bool)


def add_buckets(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["distance_bucket"] = pd.cut(
        pd.to_numeric(df["distance"], errors="coerce"),
        bins=[0, 1400, 1800, 2200, 4000],
        labels=["sprint", "mile", "middle", "long"],
        include_lowest=True,
    ).astype(str)
    df["field_size_bucket"] = pd.cut(
        pd.to_numeric(df["field_size"], errors="coerce"),
        bins=[0, 11, 14, 18, 30],
        labels=["small<=11", "mid12-14", "large15-18", "xlarge>=19"],
        include_lowest=True,
    ).astype(str)
    df["odds_proxy_bucket"] = pd.qcut(
        pd.to_numeric(df["pair_market_implied_prob"], errors="coerce").rank(method="first"),
        q=5,
        labels=["vp_low", "low", "mid", "high", "vp_high"],
    ).astype(str)
    # popularity proxy: high implied prob = more popular
    df["popularity_bucket"] = pd.qcut(
        pd.to_numeric(df["pair_market_implied_prob"], errors="coerce").rank(method="first"),
        q=5,
        labels=["longshot", "semi_long", "mid", "semi_pop", "popular"],
    ).astype(str)
    df["pair_edge_bucket"] = pd.cut(
        pd.to_numeric(df["pair_edge"], errors="coerce"),
        bins=[-999, -0.05, 0.0, 0.03, 0.08, 999],
        labels=["neg_large", "neg_small", "pos_small", "pos_mid", "pos_high"],
        include_lowest=True,
    ).astype(str)
    df["pair_value_score_bucket"] = pd.qcut(
        pd.to_numeric(df["pair_value_score"], errors="coerce").rank(method="first"),
        q=5,
        labels=["v1_low", "v2", "v3", "v4", "v5_high"],
    ).astype(str)

    df["pair_value_score_rank"] = (
        df.groupby("race_id")["pair_value_score"].rank(ascending=False, method="first").astype("Int64")
    )
    return df


def metrics(g: pd.DataFrame) -> dict:
    hit = pd.to_numeric(g["actual_wide_hit"], errors="coerce").fillna(0)
    payout = pd.to_numeric(g["wide_payout"], errors="coerce").fillna(0)
    wins = (hit > 0).astype(float)
    total_payout = float((payout * wins).sum())
    cost = float(len(g) * 100)
    profit = total_payout - cost
    roi = total_payout / cost if cost > 0 else np.nan
    hit_rate = float(wins.mean()) if len(g) else np.nan
    return {
        "candidate_count": int(len(g)),
        "hit_count": int(hit.sum()),
        "hit_rate": hit_rate,
        "total_payout": total_payout,
        "cost": cost,
        "profit": profit,
        "roi": roi,
    }


def breakdown(df: pd.DataFrame, axis: str) -> pd.DataFrame:
    rows = []
    for k, g in df.groupby(axis, dropna=False):
        m = metrics(g)
        m["segment_axis"] = axis
        m["segment_value"] = str(k)
        rows.append(m)
    return pd.DataFrame(rows)


def race_level_filter(df: pd.DataFrame, min_race_edge: float, max_per_race: int) -> pd.DataFrame:
    picked = []
    for rid, g in df.groupby("race_id"):
        if pd.to_numeric(g["pair_edge"], errors="coerce").mean() < min_race_edge:
            continue
        gg = g.sort_values("pair_value_score", ascending=False).head(max_per_race)
        picked.append(gg)
    if not picked:
        return df.head(0).copy()
    return pd.concat(picked, ignore_index=True)


def main() -> None:
    df = pd.read_csv(PAIR_CSV)
    rule = df[to_bool(df["pair_selected_flag"])].copy()
    rule = rule[rule["result_quality_status"].astype(str) == "ok"].copy()

    con = duckdb.connect(str(DB), read_only=True)
    race_meta = con.execute(
        """
        select race_id::varchar as race_id, race_date::varchar as race_date, venue, surface, distance
        from races
        where race_date between cast('2025-04-20' as date) and cast('2026-04-26' as date)
        """
    ).fetchdf()
    fs = con.execute(
        """
        select e.race_id::varchar as race_id, count(*) as field_size
        from entries e join races r on r.race_id=e.race_id
        where r.race_date between cast('2025-04-20' as date) and cast('2026-04-26' as date)
        group by 1
        """
    ).fetchdf()
    con.close()

    rule = rule.merge(race_meta, on="race_id", how="left", suffixes=("", "_r"))
    rule = rule.merge(fs, on="race_id", how="left")
    rule = add_buckets(rule)

    axes = [
        "race_date",
        "venue",
        "surface",
        "distance_bucket",
        "field_size_bucket",
        "odds_proxy_bucket",
        "popularity_bucket",
        "pair_value_score_rank",
        "pair_edge_bucket",
        "pair_value_score_bucket",
    ]

    bd = pd.concat([breakdown(rule, a) for a in axes], ignore_index=True)
    bd = bd.sort_values(["segment_axis", "profit"], ascending=[True, False])
    bd.to_csv(REPORT / "rule_profit_breakdown_real_odds_2025_2026.csv", index=False, encoding="utf-8-sig")
    (REPORT / "rule_profit_breakdown_real_odds_2025_2026.md").write_text(
        "# rule_profit_breakdown_real_odds_2025_2026\n\n" + bd.to_string(index=False), encoding="utf-8"
    )

    neg = bd[(bd["candidate_count"] >= 30) & (bd["roi"] < 0.8)].copy().sort_values("profit")
    neg.to_csv(REPORT / "rule_negative_segments_real_odds_2025_2026.csv", index=False, encoding="utf-8-sig")
    (REPORT / "rule_negative_segments_real_odds_2025_2026.md").write_text(
        "# rule_negative_segments_real_odds_2025_2026\n\n" + neg.to_string(index=False), encoding="utf-8"
    )

    pos = bd[(bd["candidate_count"] >= 30) & (bd["roi"] > 1.2)].copy().sort_values("profit", ascending=False)
    pos.to_csv(REPORT / "rule_positive_segments_real_odds_2025_2026.csv", index=False, encoding="utf-8-sig")
    (REPORT / "rule_positive_segments_real_odds_2025_2026.md").write_text(
        "# rule_positive_segments_real_odds_2025_2026\n\n" + pos.to_string(index=False), encoding="utf-8"
    )

    base_m = metrics(rule)

    # shadow grid
    neg_map = {(r.segment_axis, str(r.segment_value)) for r in neg.itertuples(index=False)}

    rows = []
    score_thresholds = [None, rule["pair_value_score"].quantile(0.5), rule["pair_value_score"].quantile(0.7)]
    edge_thresholds = [None, 0.0, 0.02, 0.05]
    race_edge_thresholds = [None, 0.0, 0.01]
    max_per_race_opts = [None, 3, 2, 1]

    for exclude_neg, sv, ev, rev, mpr in itertools.product([False, True], score_thresholds, edge_thresholds, race_edge_thresholds, max_per_race_opts):
        g = rule.copy()
        name = []
        if exclude_neg:
            name.append("exclude_negative_segments")
            # remove rows matching any negative segment
            mask = pd.Series(False, index=g.index)
            for axis in axes:
                axis_vals = {v for a, v in neg_map if a == axis}
                if axis_vals:
                    mask = mask | g[axis].astype(str).isin(axis_vals)
            g = g[~mask].copy()
        if sv is not None:
            name.append(f"value>={sv:.4f}")
            g = g[pd.to_numeric(g["pair_value_score"], errors="coerce") >= sv].copy()
        if ev is not None:
            name.append(f"edge>={ev:.3f}")
            g = g[pd.to_numeric(g["pair_edge"], errors="coerce") >= ev].copy()
        if rev is not None:
            name.append(f"race_edge_mean>={rev:.3f}")
        if mpr is not None:
            name.append(f"max_per_race={mpr}")
        if (rev is not None) or (mpr is not None):
            g = race_level_filter(g, min_race_edge=rev if rev is not None else -999, max_per_race=mpr if mpr is not None else 999)

        m = metrics(g)
        m["scenario"] = " | ".join(name) if name else "baseline"
        m["buy_reduction_rate"] = 1.0 - (m["candidate_count"] / base_m["candidate_count"] if base_m["candidate_count"] else 0.0)
        rows.append(m)

    grid = pd.DataFrame(rows).drop_duplicates(subset=["scenario"]).sort_values("roi", ascending=False)
    grid.to_csv(REPORT / "rule_filter_shadow_grid_real_odds_2025_2026.csv", index=False, encoding="utf-8-sig")
    (REPORT / "rule_filter_shadow_grid_real_odds_2025_2026.md").write_text(
        "# rule_filter_shadow_grid_real_odds_2025_2026\n\n" + grid.to_string(index=False), encoding="utf-8"
    )

    print("done")


if __name__ == "__main__":
    main()
