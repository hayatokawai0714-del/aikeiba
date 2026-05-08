from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd


def _to_bool(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    x = s.astype(str).str.lower()
    return x.isin(["true", "1", "t", "yes", "y"])


def _quantiles(series: pd.Series) -> dict[str, float]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return {}
    qs = [0.0, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0]
    out: dict[str, float] = {}
    for q in qs:
        k = "min" if q == 0.0 else ("max" if q == 1.0 else f"p{int(q*100):02d}")
        out[k] = float(s.quantile(q))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Summarize model_dynamic skip reasons and selection sparsity.")
    ap.add_argument("--pairs-csv", type=Path, required=True)
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    ap.add_argument("--out-by-date-csv", type=Path, required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.pairs_csv)
    df["race_date"] = df.get("race_date", "").astype(str)
    if "model_dynamic_selected_flag" not in df.columns:
        df["model_dynamic_selected_flag"] = False
    if "model_dynamic_skip_reason" not in df.columns:
        df["model_dynamic_skip_reason"] = pd.NA

    sel = _to_bool(df["model_dynamic_selected_flag"])
    selected = df[sel].copy()

    # Selected-only reason counts (most useful)
    reason_counts = selected["model_dynamic_skip_reason"].value_counts(dropna=False).rename_axis("skip_reason").reset_index(name="selected_row_count")
    reason_race_counts = (
        selected.groupby("model_dynamic_skip_reason", dropna=False)["race_id"]
        .nunique()
        .rename("selected_race_count")
        .reset_index()
        .rename(columns={"model_dynamic_skip_reason": "skip_reason"})
    )
    summary = reason_counts.merge(reason_race_counts, on="skip_reason", how="left")
    summary["selected_race_count"] = summary["selected_race_count"].fillna(0).astype(int)

    # Race-level skip reasons (all races, including zero-selected races).
    # We assume skip_reason is constant per race; take first non-null if present.
    race_reason = (
        df.groupby("race_id", dropna=False)["model_dynamic_skip_reason"]
        .apply(lambda s: s.dropna().iloc[0] if s.dropna().size > 0 else pd.NA)
        .reset_index(name="race_skip_reason")
    )
    race_reason_counts = (
        race_reason["race_skip_reason"]
        .value_counts(dropna=False)
        .rename_axis("skip_reason")
        .reset_index(name="race_count")
    )

    # Days with dynamic_selected=0 (race-level)
    per_date = []
    for d, g in df.groupby("race_date"):
        s = _to_bool(g["model_dynamic_selected_flag"])
        per_date.append(
            {
                "race_date": str(d),
                "race_count": int(g["race_id"].nunique()),
                "dynamic_selected_count": int(s.sum()),
                "dynamic_selected_race_count": int(g.loc[s, "race_id"].nunique()),
                "dynamic_zero_selected_race_count": int(g["race_id"].nunique() - g.loc[s, "race_id"].nunique()),
                "top_skip_reason_in_selected": (str(g.loc[s, "model_dynamic_skip_reason"].value_counts(dropna=False).index[0]) if s.any() else "NONE_SELECTED"),
            }
        )
    by_date = pd.DataFrame(per_date).sort_values("race_date")

    # Distributions over all rows (diagnostic)
    dist = {
        "model_dynamic_final_score": _quantiles(df["model_dynamic_final_score"]) if "model_dynamic_final_score" in df.columns else {},
        "pair_edge": _quantiles(df["pair_edge"]) if "pair_edge" in df.columns else {},
        "pair_model_score": _quantiles(df["pair_model_score"]) if "pair_model_score" in df.columns else {},
        "pair_model_score_gap_to_next": _quantiles(df["pair_model_score_gap_to_next"]) if "pair_model_score_gap_to_next" in df.columns else {},
    }

    out_payload = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "input": str(args.pairs_csv),
        "model_dynamic_selected_count": int(sel.sum()),
        "model_dynamic_selected_race_count": int(df.loc[sel, "race_id"].nunique()),
        "model_dynamic_zero_selected_race_count": int(df["race_id"].nunique() - df.loc[sel, "race_id"].nunique()),
        "top_skip_reasons_selected_only": summary.head(10).to_dict("records"),
        "top_skip_reasons_race_level": race_reason_counts.head(10).to_dict("records"),
        "distributions": dist,
    }

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_by_date_csv.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(args.out_csv, index=False, encoding="utf-8")
    by_date.to_csv(args.out_by_date_csv, index=False, encoding="utf-8")

    md = [
        "# Model Dynamic Skip Reason Summary",
        "",
        f"- generated_at: {out_payload['generated_at']}",
        f"- input: {out_payload['input']}",
        "",
        "## Overall",
        "",
        f"- model_dynamic_selected_count: {out_payload['model_dynamic_selected_count']}",
        f"- model_dynamic_selected_race_count: {out_payload['model_dynamic_selected_race_count']}",
        f"- model_dynamic_zero_selected_race_count: {out_payload['model_dynamic_zero_selected_race_count']}",
        "",
        "## Top Skip Reasons (selected rows only)",
        "",
    ]
    if len(summary) > 0:
        md += ["| skip_reason | selected_row_count | selected_race_count |", "|---|---:|---:|"]
        for _, r in summary.head(12).iterrows():
            md.append(f"| {r['skip_reason']} | {int(r['selected_row_count'])} | {int(r['selected_race_count'])} |")
        md.append("")

    md += [
        "## Top Skip Reasons (race-level; includes zero-selected races)",
        "",
    ]
    if len(race_reason_counts) > 0:
        md += ["| skip_reason | race_count |", "|---|---:|"]
        for _, r in race_reason_counts.head(12).iterrows():
            md.append(f"| {r['skip_reason']} | {int(r['race_count'])} |")
        md.append("")

    md += [
        "## Distributions (all rows)",
        "",
        "```json",
        json.dumps(dist, ensure_ascii=False, indent=2),
        "```",
        "",
        "## Per-date breakdown",
        "",
        f"- by_date_csv: {args.out_by_date_csv}",
        "",
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")

    print(str(args.out_csv))
    print(str(args.out_md))
    print(str(args.out_by_date_csv))


if __name__ == "__main__":
    main()
