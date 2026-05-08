from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import duckdb
import pandas as pd


def _infer_market_proxy_source_from_db(df: pd.DataFrame, db_path: Path) -> pd.Series:
    """
    Best-effort inference when `market_proxy_source` was not emitted into the CSV.

    Heuristic:
    - If DuckDB has PLACE-like `odds` rows for (race_id, horse_no) for at least one of horse1/horse2: `odds_place`.
    - Else `predictions_scaled_low_confidence` (the evaluation-helper fallback).

    This is intentionally coarse; the preferred path is to have `market_proxy_source` column
    emitted by the expanded builder.
    """
    if "race_id" not in df.columns:
        return pd.Series(["unknown"] * len(df), index=df.index)

    # Need umaban to check odds presence (odds table stores it as horse_no).
    if "horse1_umaban" not in df.columns or "horse2_umaban" not in df.columns:
        return pd.Series(["unknown"] * len(df), index=df.index)

    races = df[["race_id", "horse1_umaban", "horse2_umaban"]].copy()
    races["race_id"] = races["race_id"].astype(str)
    races["horse1_umaban"] = pd.to_numeric(races["horse1_umaban"], errors="coerce").astype("Int64")
    races["horse2_umaban"] = pd.to_numeric(races["horse2_umaban"], errors="coerce").astype("Int64")

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        # Some environments may not have odds; fail gracefully.
        tables = set(con.execute("SHOW TABLES").fetchdf()["name"].tolist())
        if "odds" not in tables:
            return pd.Series(["unknown"] * len(df), index=df.index)

        # Build a compact race_id+horse_no set to query.
        u1 = races[["race_id", "horse1_umaban"]].rename(columns={"horse1_umaban": "horse_no"})
        u2 = races[["race_id", "horse2_umaban"]].rename(columns={"horse2_umaban": "horse_no"})
        u = pd.concat([u1, u2], axis=0, ignore_index=True).dropna(subset=["horse_no"]).drop_duplicates()
        if len(u) == 0:
            return pd.Series(["unknown"] * len(df), index=df.index)

        con.register("u", u)
        hit = con.execute(
            """
            SELECT DISTINCT
              o.race_id::VARCHAR AS race_id,
              o.horse_no::INTEGER AS horse_no
            FROM odds o
            INNER JOIN u
              ON o.race_id::VARCHAR = u.race_id::VARCHAR
             AND o.horse_no::INTEGER = u.horse_no::INTEGER
            WHERE o.horse_no IS NOT NULL
              AND o.horse_no::INTEGER >= 1
              AND o.odds_value IS NOT NULL
              AND (
                lower(o.odds_type::VARCHAR) LIKE '%place%'
                OR o.odds_type::VARCHAR LIKE '%複勝%'
              )
            """
        ).fetchdf()
        if len(hit) == 0:
            return pd.Series(["predictions_scaled_low_confidence"] * len(df), index=df.index)

        hit_set = set(zip(hit["race_id"].astype(str).tolist(), hit["horse_no"].astype(int).tolist()))
        # Row is odds-based if either horse matches odds presence.
        out = []
        for r in races.itertuples(index=False):
            rid = str(r.race_id)
            u1v = (None if pd.isna(r.horse1_umaban) else int(r.horse1_umaban))
            u2v = (None if pd.isna(r.horse2_umaban) else int(r.horse2_umaban))
            if (u1v is not None and (rid, u1v) in hit_set) or (u2v is not None and (rid, u2v) in hit_set):
                out.append("odds_place")
            else:
                out.append("predictions_scaled_low_confidence")
        return pd.Series(out, index=df.index)
    finally:
        try:
            con.close()
        except Exception:
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit market proxy fallback usage from joined pairs CSV.")
    ap.add_argument("--pairs-csv", type=Path, required=True)
    ap.add_argument("--db-path", type=Path, default=None, help="Optional DuckDB path; used to infer market proxy source if column missing.")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    ap.add_argument("--out-odds-type-csv", type=Path, default=None, help="Optional; defaults next to --out-csv as odds_type_audit.csv")
    ap.add_argument("--out-odds-type-md", type=Path, default=None, help="Optional; defaults next to --out-md as odds_type_audit.md")
    args = ap.parse_args()

    df = pd.read_csv(args.pairs_csv, low_memory=False)
    df["race_date"] = df.get("race_date", "").astype(str)

    src = df.get("market_proxy_source")
    if src is None:
        if args.db_path is not None and args.db_path.exists():
            df["market_proxy_source"] = _infer_market_proxy_source_from_db(df, args.db_path)
        else:
            # If column missing, treat as unknown
            df["market_proxy_source"] = "unknown"
    else:
        df["market_proxy_source"] = df["market_proxy_source"].astype(str)

    total = int(len(df))
    counts = df["market_proxy_source"].value_counts(dropna=False).to_dict()

    by_date = (
        df.groupby(["race_date", "market_proxy_source"], dropna=False)
        .size()
        .rename("pair_rows")
        .reset_index()
    )

    # Simple score distribution split
    def _dist(s: pd.Series) -> dict[str, float]:
        x = pd.to_numeric(s, errors="coerce").dropna()
        if len(x) == 0:
            return {}
        return {
            "n": float(len(x)),
            "p50": float(x.quantile(0.5)),
            "p90": float(x.quantile(0.9)),
            "p99": float(x.quantile(0.99)),
            "std": float(x.std()),
        }

    score_by_src = {}
    for k, g in df.groupby("market_proxy_source", dropna=False):
        score_by_src[str(k)] = _dist(g.get("pair_model_score"))

    # model_dynamic performance split (if present)
    perf_by_src = {}
    if "model_dynamic_selected_flag" in df.columns and "actual_wide_hit" in df.columns and "wide_payout" in df.columns:
        sel = df["model_dynamic_selected_flag"].astype(str).str.lower().isin(["1", "true", "t", "yes", "y"])
        for k, g in df.groupby("market_proxy_source", dropna=False):
            gs = g[sel.loc[g.index]]
            n = int(len(gs))
            if n == 0:
                perf_by_src[str(k)] = {"selected": 0, "roi_proxy": None}
                continue
            hit = pd.to_numeric(gs["actual_wide_hit"], errors="coerce")
            pay = pd.to_numeric(gs["wide_payout"], errors="coerce")
            ok = hit.notna() & pay.notna()
            payout = float((hit[ok].fillna(0.0) * pay[ok].fillna(0.0)).sum())
            roi = float(payout / (n * 100)) if n > 0 else None
            perf_by_src[str(k)] = {"selected": n, "roi_proxy": roi}

    out = pd.DataFrame(
        [
            {
                "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
                "pairs_csv": str(args.pairs_csv),
                "total_pair_rows": total,
                "source_counts_json": json.dumps(counts, ensure_ascii=False),
                "score_distribution_by_source_json": json.dumps(score_by_src, ensure_ascii=False),
                "model_dynamic_perf_by_source_json": json.dumps(perf_by_src, ensure_ascii=False),
            }
        ]
    )
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")

    md = [
        "# Market Proxy Fallback Usage Audit",
        "",
        f"- generated_at: {out.loc[0, 'generated_at']}",
        f"- input: {args.pairs_csv}",
        f"- total_pair_rows: {total}",
        "",
        "## Source counts (pair rows)",
        "",
        "```json",
        json.dumps(counts, ensure_ascii=False, indent=2),
        "```",
        "",
        "## Score distribution by source",
        "",
        "```json",
        json.dumps(score_by_src, ensure_ascii=False, indent=2),
        "```",
        "",
    ]
    if perf_by_src:
        md += [
            "## model_dynamic performance by source (selected rows only; ROI proxy)",
            "",
            "```json",
            json.dumps(perf_by_src, ensure_ascii=False, indent=2),
            "```",
            "",
        ]

    md += [
        "## Notes",
        "",
        "- `predictions_scaled_low_confidence` is NOT odds-derived; interpret ROI comparisons cautiously when it dominates.",
        "",
    ]
    args.out_md.write_text("\n".join(md), encoding="utf-8")

    # Optional: odds_type audit (helps debug which odds_type values exist in DB)
    if args.db_path is not None and args.db_path.exists():
        con = duckdb.connect(str(args.db_path), read_only=True)
        try:
            tables = set(con.execute("SHOW TABLES").fetchdf()["name"].tolist())
            if "odds" in tables:
                odds_df = con.execute(
                    """
                    SELECT
                      odds_type::VARCHAR AS odds_type,
                      COUNT(*)::BIGINT AS row_count,
                      SUM(CASE WHEN odds_value IS NOT NULL THEN 1 ELSE 0 END)::BIGINT AS non_null_odds_value_count,
                      SUM(CASE WHEN horse_no IS NOT NULL AND horse_no::INTEGER >= 1 THEN 1 ELSE 0 END)::BIGINT AS horse_no_non_default_count,
                      SUM(CASE WHEN horse_no_a IS NOT NULL OR horse_no_b IS NOT NULL THEN 1 ELSE 0 END)::BIGINT AS pair_horse_cols_non_default_count
                    FROM odds
                    GROUP BY 1
                    ORDER BY row_count DESC
                    """
                ).fetchdf()
            else:
                odds_df = pd.DataFrame(
                    [
                        {
                            "odds_type": None,
                            "row_count": 0,
                            "non_null_odds_value_count": 0,
                            "horse_no_non_default_count": 0,
                            "pair_horse_cols_non_default_count": 0,
                        }
                    ]
                )
        finally:
            try:
                con.close()
            except Exception:
                pass

        out_odds_csv = args.out_odds_type_csv or (args.out_csv.parent / "odds_type_audit.csv")
        out_odds_md = args.out_odds_type_md or (args.out_md.parent / "odds_type_audit.md")
        out_odds_csv.parent.mkdir(parents=True, exist_ok=True)
        out_odds_md.parent.mkdir(parents=True, exist_ok=True)
        odds_df.to_csv(out_odds_csv, index=False, encoding="utf-8")
        out_odds_md.write_text(
            "\n".join(
                [
                    "# odds_type Audit",
                    "",
                    f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
                    f"- db_path: {args.db_path}",
                    "",
                    "Top rows:",
                    "",
                    "```",
                    odds_df.head(30).to_string(index=False),
                    "```",
                    "",
                    "Notes:",
                    "",
                    "- `horse_no_non_default_count` indicates single-horse odds rows (used for market proxy source inference).",
                    "- `pair_horse_cols_non_default_count` indicates pair odds rows (e.g., wide); not used for this inference.",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        print(str(out_odds_csv))
        print(str(out_odds_md))

    print(str(args.out_csv))
    print(str(args.out_md))


if __name__ == "__main__":
    main()
