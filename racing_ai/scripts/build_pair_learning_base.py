from __future__ import annotations

import argparse
import json
import datetime as dt
from pathlib import Path
import re

import pandas as pd

from aikeiba.db.duckdb import DuckDb


def _normalize_pair(a: int | None, b: int | None) -> str | None:
    if a is None or b is None:
        return None
    x, y = sorted((int(a), int(b)))
    return f"{x:02d}-{y:02d}"


def _normalize_pair_key_text(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    parts = s.split("-")
    if len(parts) != 2:
        return None
    try:
        return _normalize_pair(int(parts[0]), int(parts[1]))
    except Exception:
        return None


def _parse_pair_to_nums(v: object) -> tuple[int | None, int | None]:
    k = _normalize_pair_key_text(v)
    if k is None:
        return None, None
    a, b = k.split("-")
    return int(a), int(b)


def _infer_model_version_from_name(path: Path) -> str | None:
    # wide_pair_candidates_YYYY-MM-DD_<model>.parquet
    m = re.match(r"wide_pair_candidates_\d{4}-\d{2}-\d{2}_(.+)\.parquet$", path.name)
    if m:
        return m.group(1)
    return None


def _bucket_rank(v: object) -> str:
    if v is None or pd.isna(v):
        return "UNKNOWN"
    x = float(v)
    if x <= 3:
        return "TOP3"
    if x <= 6:
        return "TOP6"
    if x <= 10:
        return "TOP10"
    return "LOW"


def _bucket_field_size(v: object) -> str:
    if v is None or pd.isna(v):
        return "UNKNOWN"
    n = int(v)
    if n <= 10:
        return "small"
    if n <= 14:
        return "medium"
    return "large"


def _bucket_distance(v: object) -> str:
    if v is None or pd.isna(v):
        return "UNKNOWN"
    d = int(v)
    if d < 1400:
        return "sprint"
    if d < 2000:
        return "mile"
    if d < 2600:
        return "middle"
    return "long"


def _resolve_pair_files(pairs_glob: str) -> list[Path]:
    base = Path(".")
    if "**" in pairs_glob:
        return sorted(base.glob(pairs_glob))
    direct = sorted(base.glob(pairs_glob))
    if len(direct) > 0:
        return direct
    # fallback recursive search by basename pattern
    pat = Path(pairs_glob).name
    return sorted(base.rglob(pat))


def _write_race_date_distribution_report(df: pd.DataFrame, out_path: Path) -> None:
    lines = ["# pair_learning_base_race_date_distribution", ""]
    if len(df) == 0 or "race_date" not in df.columns:
        lines.append("- データなし")
    else:
        d = (
            df.groupby("race_date", as_index=False)
            .agg(rows=("race_id", "size"), races=("race_id", "nunique"), hit=("actual_wide_hit", "sum"))
            .sort_values("race_date")
        )
        d["hit_rate"] = d["hit"] / d["rows"]
        lines.append("| race_date | rows | races | hit | hit_rate |")
        lines.append("|---|---:|---:|---:|---:|")
        for r in d.itertuples(index=False):
            lines.append(f"| {r.race_date} | {int(r.rows)} | {int(r.races)} | {int(r.hit)} | {r.hit_rate:.6f} |")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")


def build_pair_learning_base(*, db_path: Path, pairs_glob: str, out_path: Path, date_report_path: Path | None = None) -> dict:
    pair_files = _resolve_pair_files(pairs_glob)
    pair_file_warnings: list[str] = []
    if len(pair_files) == 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_parquet(out_path, index=False)
        if date_report_path:
            _write_race_date_distribution_report(pd.DataFrame(), date_report_path)
        return {"rows": 0, "pair_files": 0, "out_path": str(out_path), "actual_wide_hit_sum": 0, "wide_payout_missing": 0}

    pair_frames: list[pd.DataFrame] = []
    for p in pair_files:
        df = pd.read_parquet(p)
        if len(df) == 0:
            continue
        df = df.copy()
        has_h1 = "horse1_umaban" in df.columns
        has_h2 = "horse2_umaban" in df.columns
        has_pair_norm = "pair_norm" in df.columns
        has_pair = "pair" in df.columns
        if not (has_h1 and has_h2 and has_pair_norm):
            pair_file_warnings.append(
                f"missing_required_pair_columns:{p.name}:"
                f"horse1_umaban={has_h1},horse2_umaban={has_h2},pair_norm={has_pair_norm}"
            )

        if "horse1_umaban" in df.columns and "horse2_umaban" in df.columns:
            df["horse1_no"] = df[["horse1_umaban", "horse2_umaban"]].min(axis=1).astype("Int64")
            df["horse2_no"] = df[["horse1_umaban", "horse2_umaban"]].max(axis=1).astype("Int64")
        elif "pair" in df.columns:
            pair_file_warnings.append(f"fallback_pair_parse_used:{p.name}")
            parsed = df["pair"].apply(_parse_pair_to_nums)
            df["horse1_no"] = parsed.apply(lambda x: x[0]).astype("Int64")
            df["horse2_no"] = parsed.apply(lambda x: x[1]).astype("Int64")
        else:
            df["horse1_no"] = pd.NA
            df["horse2_no"] = pd.NA
        if "pair_norm" in df.columns:
            df["pair_norm"] = df["pair_norm"].apply(_normalize_pair_key_text)
        else:
            df["pair_norm"] = [
                _normalize_pair(a if pd.notna(a) else None, b if pd.notna(b) else None)
                for a, b in zip(df["horse1_no"], df["horse2_no"])
            ]
        if "model_version" not in df.columns:
            df["model_version"] = _infer_model_version_from_name(p)
        df["_source_file"] = str(p)
        pair_frames.append(df)

    if len(pair_frames) == 0:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_parquet(out_path, index=False)
        if date_report_path:
            _write_race_date_distribution_report(pd.DataFrame(), date_report_path)
        return {"rows": 0, "pair_files": len(pair_files), "out_path": str(out_path), "actual_wide_hit_sum": 0, "wide_payout_missing": 0}

    pairs = pd.concat(pair_frames, ignore_index=True)

    db = DuckDb.connect(db_path)
    races = db.query_df("SELECT race_id, cast(race_date as VARCHAR) AS race_date, venue, surface, distance, field_size_expected FROM races")
    entries = db.query_df("SELECT race_id, horse_no, horse_id FROM entries")
    results = db.query_df("SELECT race_id, horse_no, finish_position FROM results")
    payouts = db.query_df("SELECT race_id, bet_key, payout FROM payouts WHERE lower(bet_type)='wide'")

    # payout map
    payouts = payouts.copy()
    payouts["pair_norm"] = payouts["bet_key"].apply(_normalize_pair_key_text)
    payouts = payouts.dropna(subset=["pair_norm"]) if len(payouts) > 0 else payouts
    payout_map = payouts[["race_id", "pair_norm", "payout"]].drop_duplicates(subset=["race_id", "pair_norm"], keep="last")

    # result-derived hit map (both top3)
    top3 = results[results["finish_position"].between(1, 3, inclusive="both")].copy()
    top3a = top3.rename(columns={"horse_no": "h1"})[["race_id", "h1"]]
    top3b = top3.rename(columns={"horse_no": "h2"})[["race_id", "h2"]]
    hit_pairs = top3a.merge(top3b, on="race_id", how="inner")
    hit_pairs = hit_pairs[hit_pairs["h1"] < hit_pairs["h2"]]
    hit_pairs["pair_norm"] = hit_pairs.apply(lambda r: _normalize_pair(int(r["h1"]), int(r["h2"])), axis=1)
    hit_pairs = hit_pairs[["race_id", "pair_norm"]].drop_duplicates()
    hit_pairs["actual_wide_hit"] = 1

    if "race_date" in pairs.columns:
        pairs = pairs.rename(columns={"race_date": "pair_source_race_date"})
    base = pairs.merge(races, on="race_id", how="left")
    if "race_date" not in base.columns:
        base["race_date"] = pd.NA
    if "pair_source_race_date" in base.columns:
        base["race_date"] = base["race_date"].fillna(base["pair_source_race_date"])

    e1 = entries.rename(columns={"horse_no": "horse1_no", "horse_id": "horse1_id"})
    e2 = entries.rename(columns={"horse_no": "horse2_no", "horse_id": "horse2_id"})
    base = base.merge(e1[["race_id", "horse1_no", "horse1_id"]], on=["race_id", "horse1_no"], how="left")
    base = base.merge(e2[["race_id", "horse2_no", "horse2_id"]], on=["race_id", "horse2_no"], how="left")

    base = base.merge(hit_pairs, on=["race_id", "pair_norm"], how="left")
    base["actual_wide_hit"] = base["actual_wide_hit"].fillna(0).astype(int)

    base = base.merge(payout_map, on=["race_id", "pair_norm"], how="left")
    base = base.rename(columns={"payout": "wide_payout"})
    # Prefer payout-derived hit when available; fallback to results-derived hit.
    base["actual_wide_hit"] = base.apply(
        lambda r: 1 if pd.notna(r.get("wide_payout")) else int(r.get("actual_wide_hit", 0)),
        axis=1,
    )
    base["actual_wide_hit_source"] = base.apply(
        lambda r: "payout" if pd.notna(r.get("wide_payout")) else ("results" if int(r.get("actual_wide_hit", 0)) == 1 else "none"),
        axis=1,
    )
    base["label_source"] = base["actual_wide_hit_source"]

    if "field_size" not in base.columns:
        base["field_size"] = base["field_size_expected"]
    base["field_size"] = base["field_size"].fillna(base.get("field_size_expected"))

    base["wide_payout_missing_flag"] = base["wide_payout"].isna()
    base["horse1_id_missing_flag"] = base["horse1_id"].isna()
    base["horse2_id_missing_flag"] = base["horse2_id"].isna()
    base["generated_at"] = dt.datetime.now().isoformat(timespec="seconds")

    # pair-level normalized/rank features for future small LightGBM reranker
    if "pair_value_score" in base.columns:
        base["pair_value_score_rank_pct"] = base.groupby("race_id")["pair_value_score"].rank(method="average", pct=True)
        grp = base.groupby("race_id")["pair_value_score"]
        mu = grp.transform("mean")
        sd = grp.transform("std").replace(0, pd.NA)
        base["pair_value_score_z_in_race"] = (base["pair_value_score"] - mu) / sd
    else:
        base["pair_value_score_rank_pct"] = pd.NA
        base["pair_value_score_z_in_race"] = pd.NA

    if "pair_prob_naive" in base.columns:
        base["pair_prob_naive_rank_pct"] = base.groupby("race_id")["pair_prob_naive"].rank(method="average", pct=True)
        grp2 = base.groupby("race_id")["pair_prob_naive"]
        mu2 = grp2.transform("mean")
        sd2 = grp2.transform("std").replace(0, pd.NA)
        base["pair_prob_naive_z_in_race"] = (base["pair_prob_naive"] - mu2) / sd2
    else:
        base["pair_prob_naive_rank_pct"] = pd.NA
        base["pair_prob_naive_z_in_race"] = pd.NA

    base["pair_rank_bucket"] = base["pair_rank_in_race"].apply(_bucket_rank) if "pair_rank_in_race" in base.columns else "UNKNOWN"
    base["field_size_bucket"] = base["field_size"].apply(_bucket_field_size) if "field_size" in base.columns else "UNKNOWN"
    base["distance_bucket"] = base["distance"].apply(_bucket_distance) if "distance" in base.columns else "UNKNOWN"

    wanted_cols = [
        "race_id",
        "race_date",
        "horse1_no",
        "horse2_no",
        "horse1_id",
        "horse2_id",
        "actual_wide_hit",
        "wide_payout",
        "pair_prob_naive",
        "pair_value_score",
        "pair_ai_market_gap_sum",
        "pair_ai_market_gap_max",
        "pair_ai_market_gap_min",
        "pair_fused_prob_sum",
        "pair_fused_prob_min",
        "pair_rank_in_race",
        "pair_rank_bucket",
        "field_size",
        "field_size_bucket",
        "venue",
        "surface",
        "distance",
        "distance_bucket",
        "pair_value_score_rank_pct",
        "pair_value_score_z_in_race",
        "pair_prob_naive_rank_pct",
        "pair_prob_naive_z_in_race",
        "model_version",
        "generated_at",
        "wide_payout_missing_flag",
        "horse1_id_missing_flag",
        "horse2_id_missing_flag",
        "pair_missing_flag",
        "pair_norm",
        "_source_file",
        "actual_wide_hit_source",
        "label_source",
    ]
    for c in wanted_cols:
        if c not in base.columns:
            base[c] = pd.NA

    out = base[wanted_cols].copy()
    out = out.drop_duplicates(subset=["race_id", "horse1_no", "horse2_no", "model_version", "_source_file"], keep="last")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    if date_report_path:
        _write_race_date_distribution_report(out, date_report_path)

    return {
        "rows": int(len(out)),
        "pair_files": len(pair_files),
        "out_path": str(out_path),
        "actual_wide_hit_sum": int(out["actual_wide_hit"].sum()) if len(out) > 0 else 0,
        "wide_payout_missing": int(out["wide_payout"].isna().sum()) if len(out) > 0 else 0,
        "race_date_count": int(out["race_date"].nunique()) if len(out) > 0 else 0,
        "race_date_min": str(out["race_date"].min()) if len(out) > 0 else None,
        "race_date_max": str(out["race_date"].max()) if len(out) > 0 else None,
        "race_date_report_path": str(date_report_path) if date_report_path else None,
        "pair_files_list_path": None,
        "warnings": pair_file_warnings,
        "label_source_counts_by_race_date": (
            out.groupby(["race_date", "label_source"]).size().reset_index(name="count").to_dict("records")
            if len(out) > 0
            else []
        ),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="racing_ai/data/warehouse/aikeiba.duckdb")
    ap.add_argument("--pairs-glob", default="racing_ai/data/bets/wide_pair_candidates_*.parquet")
    ap.add_argument("--out-path", default="racing_ai/data/modeling/pair_learning_base.parquet")
    ap.add_argument("--date-report-path", default="racing_ai/reports/pair_learning_base_date_distribution.md")
    args = ap.parse_args()
    res = build_pair_learning_base(
        db_path=Path(args.db_path),
        pairs_glob=args.pairs_glob,
        out_path=Path(args.out_path),
        date_report_path=Path(args.date_report_path) if str(args.date_report_path).strip() else None,
    )
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
