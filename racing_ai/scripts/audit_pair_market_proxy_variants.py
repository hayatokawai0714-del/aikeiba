from __future__ import annotations

import argparse
from pathlib import Path
import math
import pandas as pd

EPS = 1e-9


def _load(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _q(s: pd.Series, p: float):
    x = pd.to_numeric(s, errors="coerce")
    if x.notna().sum() == 0:
        return None
    return float(x.quantile(p))


def _m(s: pd.Series):
    x = pd.to_numeric(s, errors="coerce")
    if x.notna().sum() == 0:
        return None
    return float(x.mean())


def _recover_p1_p2(sum_s: pd.Series, prod_p: pd.Series) -> tuple[pd.Series, pd.Series]:
    s = pd.to_numeric(sum_s, errors="coerce")
    p = pd.to_numeric(prod_p, errors="coerce")
    disc = s * s - 4.0 * p
    valid = s.notna() & p.notna() & (disc >= 0)
    sqrt_disc = disc.where(valid).map(lambda v: math.sqrt(v) if pd.notna(v) else pd.NA)
    p1 = ((s + sqrt_disc) / 2.0).where(valid)
    p2 = ((s - sqrt_disc) / 2.0).where(valid)
    return p1, p2


def _proxy_stats(df: pd.DataFrame, proxy_col: str, race_col: str = "race_id") -> dict:
    proxy = pd.to_numeric(df.get(proxy_col), errors="coerce")
    model = pd.to_numeric(df.get("pair_model_score"), errors="coerce")
    edge = model - proxy
    pos = edge > 0
    pos_count = int(pos.sum()) if edge.notna().any() else 0
    race_pos = (
        df.assign(_pos=pos)
        .groupby(race_col)["_pos"]
        .any()
        .astype(int)
    ) if race_col in df.columns else pd.Series(dtype=int)
    ratio = model / (proxy + EPS)
    return {
        "non_null_count": int(proxy.notna().sum()),
        "min": _q(proxy, 0.0),
        "p10": _q(proxy, 0.10),
        "p25": _q(proxy, 0.25),
        "p50": _q(proxy, 0.50),
        "p75": _q(proxy, 0.75),
        "p90": _q(proxy, 0.90),
        "p95": _q(proxy, 0.95),
        "max": _q(proxy, 1.0),
        "mean": _m(proxy),
        "pair_model_score_p50": _q(model, 0.50),
        "model_minus_proxy_p50": _q(edge, 0.50),
        "positive_edge_count": pos_count,
        "positive_edge_rate": (float(pos.mean()) if len(pos) > 0 else None),
        "positive_edge_race_count": int(race_pos.sum()) if len(race_pos) > 0 else None,
        "positive_edge_race_rate": (float(race_pos.mean()) if len(race_pos) > 0 else None),
        "model_over_proxy_ratio_p50": _q(ratio, 0.50),
        "model_over_proxy_ratio_p90": _q(ratio, 0.90),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit pair market proxy variants (shadow-only post calc).")
    ap.add_argument("--inputs", required=True, help="Comma-separated pair_shadow_pair_comparison.csv paths")
    ap.add_argument("--candidate-inputs", default="", help="Optional comma-separated candidate_pairs.parquet/csv paths aligned by date")
    ap.add_argument("--out-csv", type=Path, default=Path("racing_ai/reports/pair_market_proxy_variant_audit.csv"))
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/pair_market_proxy_variant_audit.md"))
    args = ap.parse_args()

    pair_paths = [Path(x.strip()) for x in args.inputs.split(",") if x.strip()]
    cand_paths = [Path(x.strip()) for x in args.candidate_inputs.split(",") if x.strip()]
    cand_by_date: dict[str, Path] = {}
    for p in cand_paths:
        for part in p.parts:
            if len(part) == 10 and part[4] == "-" and part[7] == "-":
                cand_by_date[part] = p
                break

    rows: list[dict] = []
    for p in pair_paths:
        d = _load(p)
        day = "UNKNOWN"
        for part in p.parts:
            if len(part) == 10 and part[4] == "-" and part[7] == "-":
                day = part
                break
        d["race_date"] = day
        if "pair_market_implied_prob" not in d.columns and "pair_edge" in d.columns:
            d["pair_market_implied_prob"] = pd.to_numeric(d["pair_model_score"], errors="coerce") - pd.to_numeric(d["pair_edge"], errors="coerce")

        # Optional enrich from candidate_pairs for p1/p2 reconstruction
        if day in cand_by_date and cand_by_date[day].exists():
            c = _load(cand_by_date[day])
            keep = [x for x in ["race_id", "pair_norm", "pair_prob_naive", "pair_fused_prob_sum"] if x in c.columns]
            if keep:
                d = d.merge(c[keep], on=["race_id", "pair_norm"], how="left")
        if "pair_fused_prob_sum" in d.columns and "pair_prob_naive" in d.columns:
            p1, p2 = _recover_p1_p2(d["pair_fused_prob_sum"], d["pair_prob_naive"])
            d["horse1_market_prob_recovered"] = p1
            d["horse2_market_prob_recovered"] = p2
        else:
            d["horse1_market_prob_recovered"] = pd.NA
            d["horse2_market_prob_recovered"] = pd.NA

        d["current"] = pd.to_numeric(d.get("pair_market_implied_prob"), errors="coerce")
        p1 = pd.to_numeric(d["horse1_market_prob_recovered"], errors="coerce")
        p2 = pd.to_numeric(d["horse2_market_prob_recovered"], errors="coerce")
        d["product_proxy"] = p1 * p2
        d["sqrt_product_proxy"] = (p1 * p2).map(lambda v: math.sqrt(v) if pd.notna(v) and v >= 0 else pd.NA)
        d["min_proxy"] = pd.concat([p1, p2], axis=1).min(axis=1)
        d["harmonic_proxy"] = 2.0 / ((1.0 / (p1 + EPS)) + (1.0 / (p2 + EPS)))
        # rank-based proxy from current
        d["rank_proxy"] = (
            d.groupby("race_id")["current"]
            .rank(method="average", ascending=False, pct=True)
        )
        d["normalized_current"] = d["rank_proxy"]
        d["calibrated_current"] = d["current"] / d.groupby("race_id")["current"].transform("mean").replace(0, pd.NA)

        for proxy in [
            "current",
            "product_proxy",
            "sqrt_product_proxy",
            "min_proxy",
            "harmonic_proxy",
            "rank_proxy",
            "normalized_current",
            "calibrated_current",
        ]:
            st = _proxy_stats(d, proxy)
            rows.append({"race_date": day, "proxy_name": proxy, **st})

    out = pd.DataFrame(rows).sort_values(["race_date", "proxy_name"])
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    summary = (
        out.groupby("proxy_name")
        .agg(
            days=("race_date", "nunique"),
            positive_edge_rate_mean=("positive_edge_rate", "mean"),
            positive_edge_race_rate_mean=("positive_edge_race_rate", "mean"),
            model_minus_proxy_p50_mean=("model_minus_proxy_p50", "mean"),
            model_over_proxy_ratio_p50_mean=("model_over_proxy_ratio_p50", "mean"),
        )
        .reset_index()
    )
    try:
        table = out.to_markdown(index=False)
        sum_table = summary.to_markdown(index=False)
    except Exception:
        table = out.to_string(index=False)
        sum_table = summary.to_string(index=False)
    md_lines = [
        "# pair_market_proxy_variant_audit",
        "",
        f"- input_files: {len(pair_paths)}",
        f"- output_csv: {args.out_csv}",
        "",
        "## Proxy Summary",
        "",
        sum_table,
        "",
        "## Daily × Proxy",
        "",
        table,
        "",
        "## Notes",
        "",
        "- `current` は既存 pair_market_implied_prob です。",
        "- `product/sqrt/min/harmonic` は horse-level確率復元ができた行でのみ有効です。",
        "- 復元不能行は NA になります（候補データの情報不足）。",
    ]
    args.out_md.write_text("\n".join(md_lines), encoding="utf-8")
    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
