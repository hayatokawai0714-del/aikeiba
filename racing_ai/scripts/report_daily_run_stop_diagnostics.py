from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any

import pandas as pd

from aikeiba.db.duckdb import DuckDb


def _count_data_rows(csv_path: Path) -> int | None:
    if not csv_path.exists():
        return None
    try:
        with csv_path.open("r", encoding="utf-8-sig", errors="ignore") as f:
            line_count = sum(1 for _ in f)
        return max(0, line_count - 1)
    except Exception:
        return None


def _split_stop_reasons(v: Any) -> list[str]:
    if v is None:
        return []
    s = str(v).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(";") if x.strip()]


def _classify_issue(*, missing_raw: list[str], raw_races_rows: int | None, raw_entries_rows: int | None, db_races: int, db_entries: int) -> str:
    if missing_raw:
        return "raw不足"
    if (raw_races_rows is not None and raw_entries_rows is not None) and (raw_races_rows == 0 or raw_entries_rows == 0):
        return "raw不足"
    if db_races == 0 or db_entries == 0:
        if (raw_races_rows or 0) > 0 and (raw_entries_rows or 0) > 0:
            return "DB未投入"
        return "raw不足"
    return "その他"


def build_report(
    *,
    db_path: Path,
    race_day_root: Path,
    raw_root: Path,
    model_version: str,
    out_md: Path,
) -> dict[str, Any]:
    db = DuckDb.connect(db_path)
    date_dirs = sorted([p for p in race_day_root.iterdir() if p.is_dir() and p.name[:4].isdigit()])
    records: list[dict[str, Any]] = []

    for d in date_dirs:
        race_date = d.name
        summary_path = d / model_version / "run_summary.json"
        if not summary_path.exists():
            continue
        summary = json.loads(summary_path.read_text(encoding="utf-8"))

        raw_dir = raw_root / f"{race_date.replace('-', '')}_real"
        races_csv = raw_dir / "races.csv"
        entries_csv = raw_dir / "entries.csv"
        raw_races_rows = _count_data_rows(races_csv)
        raw_entries_rows = _count_data_rows(entries_csv)
        missing_raw = []
        if not races_csv.exists():
            missing_raw.append("races.csv")
        if not entries_csv.exists():
            missing_raw.append("entries.csv")

        db_count = db.query_df(
            """
            SELECT
              (SELECT COUNT(*) FROM races WHERE cast(race_date as VARCHAR)=?) AS races_cnt,
              (SELECT COUNT(*) FROM entries e JOIN races r ON e.race_id=r.race_id WHERE cast(r.race_date as VARCHAR)=?) AS entries_cnt
            """,
            (race_date, race_date),
        ).iloc[0]
        db_races = int(db_count["races_cnt"])
        db_entries = int(db_count["entries_cnt"])

        stop_reason = summary.get("stop_reason")
        stop_reasons = _split_stop_reasons(stop_reason)
        status = str(summary.get("status") or "")
        classification = _classify_issue(
            missing_raw=missing_raw,
            raw_races_rows=raw_races_rows,
            raw_entries_rows=raw_entries_rows,
            db_races=db_races,
            db_entries=db_entries,
        )
        if status != "stop":
            classification = "non_stop"

        records.append(
            {
                "race_date": race_date,
                "status": status,
                "stop_reason": stop_reason,
                "stop_reasons": stop_reasons,
                "raw_dir": str(raw_dir),
                "raw_files_exist": len(missing_raw) == 0,
                "missing_raw_files": missing_raw,
                "raw_races_rows": raw_races_rows,
                "raw_entries_rows": raw_entries_rows,
                "db_races_rows": db_races,
                "db_entries_rows": db_entries,
                "classification": classification,
            }
        )

    df = pd.DataFrame(records).sort_values("race_date")
    stop_df = df[df["status"] == "stop"].copy()

    stop_reason_counts: dict[str, int] = {}
    for reasons in stop_df["stop_reasons"].tolist():
        for r in reasons:
            stop_reason_counts[r] = stop_reason_counts.get(r, 0) + 1

    class_counts = stop_df["classification"].value_counts(dropna=False).to_dict()

    lines = [
        "# daily_run_stop_diagnostics",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- model_version: {model_version}",
        f"- total_days: {len(df)}",
        f"- stop_days: {len(stop_df)}",
        "",
        "## Stop Reason Counts",
    ]
    if stop_reason_counts:
        for k, v in sorted(stop_reason_counts.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- (none)")

    lines += ["", "## Stop Classification Counts"]
    if class_counts:
        for k, v in class_counts.items():
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- (none)")

    lines += [
        "",
        "## Stop Days Detail",
        "| race_date | stop_reason | raw_exists | raw_races_rows | raw_entries_rows | db_races_rows | db_entries_rows | classification |",
        "|---|---|---|---:|---:|---:|---:|---|",
    ]
    for r in stop_df.itertuples(index=False):
        lines.append(
            f"| {r.race_date} | {r.stop_reason} | {r.raw_files_exist} | {r.raw_races_rows if r.raw_races_rows is not None else 'NA'} | {r.raw_entries_rows if r.raw_entries_rows is not None else 'NA'} | {r.db_races_rows} | {r.db_entries_rows} | {r.classification} |"
        )

    lines += ["", "## All Days Raw/DB Coverage", "| race_date | status | raw_races_rows | raw_entries_rows | db_races_rows | db_entries_rows |", "|---|---|---:|---:|---:|---:|"]
    for r in df.itertuples(index=False):
        lines.append(
            f"| {r.race_date} | {r.status} | {r.raw_races_rows if r.raw_races_rows is not None else 'NA'} | {r.raw_entries_rows if r.raw_entries_rows is not None else 'NA'} | {r.db_races_rows} | {r.db_entries_rows} |"
        )

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")
    return {
        "report_path": str(out_md),
        "total_days": int(len(df)),
        "stop_days": int(len(stop_df)),
        "stop_reason_counts": stop_reason_counts,
        "classification_counts": class_counts,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="racing_ai/data/warehouse/aikeiba.duckdb")
    ap.add_argument("--race-day-root", default="racing_ai/data/race_day")
    ap.add_argument("--raw-root", default="racing_ai/data/raw")
    ap.add_argument("--model-version", default="top3_stability_plus_pace_v3")
    ap.add_argument("--out-md", default="racing_ai/reports/daily_run_stop_diagnostics.md")
    args = ap.parse_args()
    res = build_report(
        db_path=Path(args.db_path),
        race_day_root=Path(args.race_day_root),
        raw_root=Path(args.raw_root),
        model_version=str(args.model_version),
        out_md=Path(args.out_md),
    )
    print(res)


if __name__ == "__main__":
    main()
