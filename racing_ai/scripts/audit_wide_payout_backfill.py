from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import duckdb
import pandas as pd


def _find_candidate_sources(search_roots: list[Path]) -> list[Path]:
    hits: list[Path] = []
    for root in search_roots:
        if not root.exists():
            continue
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            n = p.name.lower()
            if "payout" in n or "払戻" in n or "haraimodoshi" in n:
                hits.append(p)
    return sorted(hits)


def _schema_text() -> str:
    return "\n".join(
        [
            "### 外部投入CSVスキーマ案",
            "",
            "- race_id: 文字列（例: 20210410-TOK-11R）",
            "- race_date: YYYY-MM-DD",
            "- bet_type: `wide` 固定（大文字小文字は投入時に正規化）",
            "- bet_key: 馬番昇順の `NN-NN`（例: `03-07`）",
            "- payout: 払戻金（円）",
            "- source_version: 取込元識別子（例: `target_text_2021_2024_v1`）",
            "",
            "任意列:",
            "",
            "- popularity: 人気順（整数）",
            "",
            "重複キー:",
            "",
            "- `race_id + bet_type + bet_key`",
            "",
            "衝突時ポリシー:",
            "",
            "- `skip`: 既存を維持して新規衝突行は無視",
            "- `replace`: 既存を削除して新規行で置換",
        ]
    )


def run(
    *,
    db_path: Path,
    pair_base_path: Path,
    out_md: Path,
    source_roots: list[Path],
    start_date: str,
    end_date: str,
) -> dict:
    con = duckdb.connect(str(db_path))
    payouts_schema = con.execute("DESCRIBE payouts").fetchdf()

    payouts_range = con.execute(
        """
        SELECT
          min(r.race_date) AS min_race_date,
          max(r.race_date) AS max_race_date,
          count(*) AS payout_rows,
          sum(CASE WHEN lower(p.bet_type)='wide' THEN 1 ELSE 0 END) AS wide_rows
        FROM payouts p
        JOIN races r ON r.race_id = p.race_id
        """
    ).fetchdf()

    missing_summary = con.execute(
        f"""
        WITH base AS (
          SELECT race_id, race_date
          FROM races
          WHERE race_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        ),
        wide AS (
          SELECT DISTINCT race_id
          FROM payouts
          WHERE lower(bet_type)='wide'
        )
        SELECT
          count(*) AS total_races,
          sum(CASE WHEN w.race_id IS NULL THEN 1 ELSE 0 END) AS missing_wide_race_count,
          count(DISTINCT b.race_date) AS total_race_dates,
          count(DISTINCT CASE WHEN w.race_id IS NULL THEN b.race_date END) AS missing_wide_race_date_count
        FROM base b
        LEFT JOIN wide w ON b.race_id=w.race_id
        """
    ).fetchdf()

    missing_dates = con.execute(
        f"""
        WITH base AS (
          SELECT race_id, race_date
          FROM races
          WHERE race_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        ),
        wide AS (
          SELECT DISTINCT race_id
          FROM payouts
          WHERE lower(bet_type)='wide'
        )
        SELECT b.race_date, count(*) AS missing_races
        FROM base b
        LEFT JOIN wide w ON b.race_id=w.race_id
        WHERE w.race_id IS NULL
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchdf()

    source_hits = _find_candidate_sources(source_roots)

    pair_metrics = {}
    if pair_base_path.exists():
        base = pd.read_parquet(pair_base_path, columns=["race_date", "wide_payout"])
        base = base[base["race_date"].notna()].copy()
        base["race_date"] = pd.to_datetime(base["race_date"], errors="coerce").dt.date
        base = base[base["race_date"].notna()].copy()
        in_range = base[
            (base["race_date"] >= pd.to_datetime(start_date).date())
            & (base["race_date"] <= pd.to_datetime(end_date).date())
        ]
        rows = int(len(in_range))
        nonnull = int(in_range["wide_payout"].notna().sum()) if rows > 0 else 0
        days_all = int(in_range["race_date"].nunique()) if rows > 0 else 0
        days_roi = int(in_range.loc[in_range["wide_payout"].notna(), "race_date"].nunique()) if rows > 0 else 0
        pair_metrics = {
            "pair_rows_in_range": rows,
            "wide_payout_nonnull_in_range": nonnull,
            "wide_payout_missing_rate_in_range": float(1.0 - (nonnull / rows)) if rows > 0 else None,
            "pair_race_date_count_in_range": days_all,
            "roi_evaluable_race_dates_in_range": days_roi,
        }
    else:
        pair_metrics = {
            "pair_rows_in_range": 0,
            "wide_payout_nonnull_in_range": 0,
            "wide_payout_missing_rate_in_range": None,
            "pair_race_date_count_in_range": 0,
            "roi_evaluable_race_dates_in_range": 0,
        }

    lines = [
        "# wide_payout_backfill_audit",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- db_path: {db_path}",
        f"- pair_base_path: {pair_base_path}",
        f"- target_range: {start_date} .. {end_date}",
        "",
        "## payouts schema",
        "",
        "| column_name | column_type | null | key |",
        "|---|---|---|---|",
    ]
    for r in payouts_schema.itertuples(index=False):
        lines.append(f"| {r.column_name} | {r.column_type} | {r.null} | {r.key} |")

    row = payouts_range.iloc[0]
    lines += [
        "",
        "## payouts現状",
        "",
        f"- min_race_date: {row['min_race_date']}",
        f"- max_race_date: {row['max_race_date']}",
        f"- payout_rows: {int(row['payout_rows'])}",
        f"- wide_rows: {int(row['wide_rows']) if pd.notna(row['wide_rows']) else 0}",
    ]

    m = missing_summary.iloc[0]
    lines += [
        "",
        "## 2021〜2024 wide欠損集計",
        "",
        f"- total_races: {int(m['total_races'])}",
        f"- missing_wide_race_count: {int(m['missing_wide_race_count'])}",
        f"- total_race_dates: {int(m['total_race_dates'])}",
        f"- missing_wide_race_date_count: {int(m['missing_wide_race_date_count'])}",
    ]

    lines += ["", "## 欠損 race_date 一覧（先頭50件）", "", "| race_date | missing_races |", "|---|---:|"]
    if len(missing_dates) == 0:
        lines.append("| (none) | 0 |")
    else:
        for r in missing_dates.head(50).itertuples(index=False):
            lines.append(f"| {r.race_date} | {int(r.missing_races)} |")

    lines += ["", "## 既存データ内の払戻候補ファイル", "", f"- hit_count: {len(source_hits)}"]
    for p in source_hits[:80]:
        lines.append(f"- {p}")
    if len(source_hits) > 80:
        lines.append(f"- ... ({len(source_hits)-80} more)")

    lines += [
        "",
        "## pair_learning_base への影響（2021〜2024）",
        "",
        f"- pair_rows_in_range: {pair_metrics['pair_rows_in_range']}",
        f"- wide_payout_nonnull_in_range: {pair_metrics['wide_payout_nonnull_in_range']}",
        f"- wide_payout_missing_rate_in_range: {pair_metrics['wide_payout_missing_rate_in_range']}",
        f"- pair_race_date_count_in_range: {pair_metrics['pair_race_date_count_in_range']}",
        f"- roi_evaluable_race_dates_in_range: {pair_metrics['roi_evaluable_race_dates_in_range']}",
        "",
        _schema_text(),
        "",
    ]

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")
    con.close()
    return {
        "out_md": str(out_md),
        "source_hit_count": len(source_hits),
        "missing_wide_race_count": int(m["missing_wide_race_count"]),
        "missing_wide_race_date_count": int(m["missing_wide_race_date_count"]),
        **pair_metrics,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit wide payout missing coverage and backfill design for 2021-2024.")
    ap.add_argument("--db-path", type=Path, required=True)
    ap.add_argument("--pair-base-path", type=Path, default=Path("racing_ai/data/modeling/pair_learning_base.parquet"))
    ap.add_argument("--out-md", type=Path, default=Path("racing_ai/reports/wide_payout_backfill_audit.md"))
    ap.add_argument("--start-date", default="2021-01-01")
    ap.add_argument("--end-date", default="2024-12-31")
    ap.add_argument(
        "--source-roots",
        default="C:/TXT,racing_ai/data/raw,racing_ai/data/normalized",
        help="Comma-separated roots to scan for payout source files.",
    )
    args = ap.parse_args()

    roots = [Path(s.strip()) for s in str(args.source_roots).split(",") if s.strip()]
    res = run(
        db_path=args.db_path,
        pair_base_path=args.pair_base_path,
        out_md=args.out_md,
        source_roots=roots,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(res)


if __name__ == "__main__":
    main()

