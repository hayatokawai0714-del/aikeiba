from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


def _date_range_list(start: dt.date, end: dt.date) -> list[str]:
    out: list[str] = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += dt.timedelta(days=1)
    return out


def _to_iso_date(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return pd.to_datetime(s).date().isoformat()
    except Exception:
        return None


def _count_csv_data_rows(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
            line_count = sum(1 for _ in f)
        return max(0, line_count - 1)
    except Exception:
        return None


def _table_stats(con: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, Any]:
    if table_name == "entries":
        df = con.execute(
            """
            SELECT CAST(r.race_date AS VARCHAR) AS race_date, COUNT(*) AS rows
            FROM entries e
            JOIN races r ON e.race_id = r.race_id
            GROUP BY 1
            ORDER BY 1
            """
        ).df()
        total_rows = int(con.execute("SELECT COUNT(*) FROM entries").fetchone()[0])
    elif table_name == "results":
        df = con.execute(
            """
            SELECT CAST(r.race_date AS VARCHAR) AS race_date, COUNT(*) AS rows
            FROM results t
            JOIN races r ON t.race_id = r.race_id
            GROUP BY 1
            ORDER BY 1
            """
        ).df()
        total_rows = int(con.execute("SELECT COUNT(*) FROM results").fetchone()[0])
    elif table_name == "payouts":
        df = con.execute(
            """
            SELECT CAST(r.race_date AS VARCHAR) AS race_date, COUNT(*) AS rows
            FROM payouts t
            JOIN races r ON t.race_id = r.race_id
            GROUP BY 1
            ORDER BY 1
            """
        ).df()
        total_rows = int(con.execute("SELECT COUNT(*) FROM payouts").fetchone()[0])
    else:  # races
        df = con.execute(
            """
            SELECT CAST(race_date AS VARCHAR) AS race_date, COUNT(*) AS rows
            FROM races
            GROUP BY 1
            ORDER BY 1
            """
        ).df()
        total_rows = int(con.execute("SELECT COUNT(*) FROM races").fetchone()[0])

    if len(df) == 0:
        return {
            "table_name": table_name,
            "min_race_date": None,
            "max_race_date": None,
            "race_date_count": 0,
            "row_count": total_rows,
            "race_dates": [],
            "race_date_count_since_2021": 0,
        }

    race_dates = sorted([str(x) for x in df["race_date"].tolist()])
    race_dates_since_2021 = [d for d in race_dates if d >= "2021-01-01"]
    return {
        "table_name": table_name,
        "min_race_date": race_dates[0],
        "max_race_date": race_dates[-1],
        "race_date_count": len(race_dates),
        "row_count": total_rows,
        "race_dates": race_dates,
        "race_date_count_since_2021": len(race_dates_since_2021),
    }


def _raw_real_date_dirs(raw_root: Path) -> list[tuple[str, Path]]:
    out: list[tuple[str, Path]] = []
    for p in raw_root.iterdir():
        if not p.is_dir():
            continue
        m = re.match(r"^(\d{8})_real$", p.name)
        if not m:
            continue
        d8 = m.group(1)
        out.append((f"{d8[:4]}-{d8[4:6]}-{d8[6:8]}", p))
    out.sort(key=lambda x: x[0])
    return out


def _pair_candidates_dates(bets_root: Path, model_version: str) -> list[str]:
    dates: set[str] = set()
    pattern = f"wide_pair_candidates_*_{model_version}.parquet"
    for p in bets_root.glob(pattern):
        m = re.match(r"wide_pair_candidates_(\d{4}-\d{2}-\d{2})_", p.name)
        if m:
            dates.add(m.group(1))
    return sorted(dates)


def _pair_learning_dates(pair_base_path: Path) -> list[str]:
    if not pair_base_path.exists():
        return []
    df = pd.read_parquet(pair_base_path, columns=["race_date"])
    dates = sorted({_to_iso_date(v) for v in df["race_date"].tolist() if _to_iso_date(v) is not None})
    return dates


def _json_list(items: list[Any], max_items: int = 200) -> str:
    if len(items) <= max_items:
        return json.dumps(items, ensure_ascii=False)
    head = items[:max_items]
    return json.dumps(head + [f"... (+{len(items) - max_items} more)"], ensure_ascii=False)


def run_audit(
    *,
    db_path: Path,
    raw_root: Path,
    bets_root: Path,
    pair_base_path: Path,
    model_version: str,
    out_md: Path,
) -> dict[str, Any]:
    con = duckdb.connect(str(db_path))
    table_names = ["races", "entries", "results", "payouts"]
    table_stats = [_table_stats(con, t) for t in table_names]

    db_race_dates = sorted(
        con.execute("SELECT DISTINCT CAST(race_date AS VARCHAR) AS d FROM races ORDER BY 1").df()["d"].tolist()
    )
    db_race_dates_since_2021 = [d for d in db_race_dates if d >= "2021-01-01"]

    raw_dirs = _raw_real_date_dirs(raw_root)
    raw_dates = sorted([d for d, _ in raw_dirs])
    raw_dates_since_2021 = [d for d in raw_dates if d >= "2021-01-01"]

    raw_rows_by_date: dict[str, dict[str, Any]] = {}
    zero_raw_dates: list[str] = []
    for d, p in raw_dirs:
        races_rows = _count_csv_data_rows(p / "races.csv")
        entries_rows = _count_csv_data_rows(p / "entries.csv")
        exists_races = (p / "races.csv").exists()
        exists_entries = (p / "entries.csv").exists()
        is_zero = (
            exists_races
            and exists_entries
            and races_rows is not None
            and entries_rows is not None
            and races_rows == 0
            and entries_rows == 0
        )
        if is_zero:
            zero_raw_dates.append(d)
        raw_rows_by_date[d] = {
            "raw_dir": str(p),
            "races_csv_exists": exists_races,
            "entries_csv_exists": exists_entries,
            "races_data_rows": races_rows,
            "entries_data_rows": entries_rows,
            "raw_files_empty": bool(is_zero),
        }

    pair_candidate_dates = _pair_candidates_dates(bets_root, model_version)
    pair_learning_dates = _pair_learning_dates(pair_base_path)

    if len(db_race_dates_since_2021) > 0:
        s = dt.date.fromisoformat(db_race_dates_since_2021[0])
        e = dt.date.fromisoformat(db_race_dates_since_2021[-1])
        full = _date_range_list(s, e)
        missing_dates = [d for d in full if d not in db_race_dates_since_2021]
    else:
        missing_dates = []

    blocked_by_raw_required = []
    for d in db_race_dates_since_2021:
        rr = raw_rows_by_date.get(d)
        if rr is None:
            blocked_by_raw_required.append(
                {"race_date": d, "reason": "raw_dir_missing"}
            )
            continue
        if not rr["races_csv_exists"] or not rr["entries_csv_exists"]:
            blocked_by_raw_required.append(
                {"race_date": d, "reason": "missing_required_raw_files"}
            )
            continue
        if (rr["races_data_rows"] or 0) == 0 or (rr["entries_data_rows"] or 0) == 0:
            blocked_by_raw_required.append(
                {"race_date": d, "reason": "raw_files_empty"}
            )

    usable_for_pair_learning_dates = sorted(
        set(pair_candidate_dates).intersection(set(db_race_dates_since_2021))
    )

    lines: list[str] = [
        "# data_availability_audit",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- db_path: {db_path}",
        f"- raw_root: {raw_root}",
        f"- model_version: {model_version}",
        "",
        "## DuckDB Table Ranges",
        "| table_name | min_race_date | max_race_date | race_date_count | row_count | race_date_count_since_2021 |",
        "|---|---|---|---:|---:|---:|",
    ]
    for s in table_stats:
        lines.append(
            f"| {s['table_name']} | {s['min_race_date'] or 'NA'} | {s['max_race_date'] or 'NA'} | {s['race_date_count']} | {s['row_count']} | {s['race_date_count_since_2021']} |"
        )

    lines += [
        "",
        "## Raw Dir Date Range (_real)",
        f"- min_raw_date: {raw_dates[0] if raw_dates else 'NA'}",
        f"- max_raw_date: {raw_dates[-1] if raw_dates else 'NA'}",
        f"- raw_date_count: {len(raw_dates)}",
        f"- raw_date_count_since_2021: {len(raw_dates_since_2021)}",
        "",
        "## raw CSV 0-row dates (races/entries both empty)",
        _json_list(zero_raw_dates),
        "",
        "## wide_pair_candidates generated date range",
        f"- min: {pair_candidate_dates[0] if pair_candidate_dates else 'NA'}",
        f"- max: {pair_candidate_dates[-1] if pair_candidate_dates else 'NA'}",
        f"- count: {len(pair_candidate_dates)}",
        "",
        "## pair_learning_base date range",
        f"- min: {pair_learning_dates[0] if pair_learning_dates else 'NA'}",
        f"- max: {pair_learning_dates[-1] if pair_learning_dates else 'NA'}",
        f"- count: {len(pair_learning_dates)}",
        "",
        "## run-race-day raw_dir required block risk (DB has race_date but raw is unusable)",
        f"- blocked_dates_count: {len(blocked_by_raw_required)}",
        _json_list(blocked_by_raw_required),
        "",
        "## Summary Table (requested format)",
        "| table_name | min_race_date | max_race_date | race_date_count | row_count | missing_dates | zero_raw_dates | usable_for_pair_learning_dates |",
        "|---|---|---|---:|---:|---|---|---|",
    ]
    for s in table_stats:
        lines.append(
            "| "
            + f"{s['table_name']} | "
            + f"{s['min_race_date'] or 'NA'} | "
            + f"{s['max_race_date'] or 'NA'} | "
            + f"{s['race_date_count']} | "
            + f"{s['row_count']} | "
            + f"{_json_list(missing_dates)} | "
            + f"{_json_list(zero_raw_dates)} | "
            + f"{_json_list(usable_for_pair_learning_dates)} |"
        )

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")

    return {
        "report_path": str(out_md),
        "table_stats": table_stats,
        "raw_date_count": len(raw_dates),
        "zero_raw_dates_count": len(zero_raw_dates),
        "pair_candidates_date_count": len(pair_candidate_dates),
        "pair_learning_date_count": len(pair_learning_dates),
        "blocked_by_raw_required_count": len(blocked_by_raw_required),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="racing_ai/data/warehouse/aikeiba.duckdb")
    ap.add_argument("--raw-root", default="racing_ai/data/raw")
    ap.add_argument("--bets-root", default="racing_ai/data/bets")
    ap.add_argument("--pair-base-path", default="racing_ai/data/modeling/pair_learning_base.parquet")
    ap.add_argument("--model-version", default="top3_stability_plus_pace_v3")
    ap.add_argument("--out-md", default="racing_ai/reports/data_availability_audit.md")
    args = ap.parse_args()

    res = run_audit(
        db_path=Path(args.db_path),
        raw_root=Path(args.raw_root),
        bets_root=Path(args.bets_root),
        pair_base_path=Path(args.pair_base_path),
        model_version=str(args.model_version),
        out_md=Path(args.out_md),
    )
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
