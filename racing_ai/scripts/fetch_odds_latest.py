import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


REQUIRED_COLS = [
    "race_id",
    "odds_snapshot_version",
    "captured_at",
    "odds_type",
    "horse_no",
    "horse_no_a",
    "horse_no_b",
    "odds_value",
    "source_version",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build normalized/odds_latest.csv from latest available odds snapshots.")
    parser.add_argument("--normalized-root", required=True)
    parser.add_argument("--today-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--output", required=True)
    parser.add_argument("--report-out", default="")
    return parser.parse_args()


def folder_to_date(folder_name: str) -> str | None:
    try:
        dt = datetime.strptime(folder_name, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    if len(folder_name) >= 8 and folder_name[:8].isdigit():
        y, m, d = folder_name[:4], folder_name[4:6], folder_name[6:8]
        try:
            dt = datetime.strptime(f"{y}-{m}-{d}", "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None


def collect_odds_files(normalized_root: Path, today_date: str) -> list[Path]:
    odds_files: list[Path] = []
    for folder in normalized_root.rglob("*"):
        if not folder.is_dir():
            continue
        fdate = folder_to_date(folder.name)
        if fdate != today_date:
            continue
        odds = folder / "odds.csv"
        if odds.exists():
            odds_files.append(odds)
    return sorted(odds_files, key=lambda p: str(p))


def normalize_odds(df: pd.DataFrame) -> pd.DataFrame:
    missing = [col for col in REQUIRED_COLS if col not in df.columns]
    if missing:
        raise ValueError(f"odds.csv missing columns: {missing}")
    use = df[REQUIRED_COLS].copy()
    use["race_id"] = use["race_id"].astype(str)
    use["odds_type"] = use["odds_type"].astype(str).str.lower()
    use["captured_at"] = pd.to_datetime(use["captured_at"], errors="coerce", utc=True)
    use["horse_no"] = pd.to_numeric(use["horse_no"], errors="coerce").astype("Int64")
    use["odds_value"] = pd.to_numeric(use["odds_value"], errors="coerce")
    use = use[use["captured_at"].notna() & use["horse_no"].notna() & use["odds_value"].notna()].copy()
    return use


def _to_iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.isoformat()


def main() -> int:
    args = parse_args()
    normalized_root = Path(args.normalized_root)
    output = Path(args.output)
    report_out = Path(args.report_out) if args.report_out else None
    today_date = args.today_date
    fetched_at = datetime.now().isoformat(timespec="seconds")

    odds_files = collect_odds_files(normalized_root, today_date)
    if not odds_files:
        raise SystemExit(f"No odds.csv files found for today_date={today_date} under {normalized_root}")

    frames: list[pd.DataFrame] = []
    file_stats: list[dict[str, Any]] = []
    for path in odds_files:
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
        norm = normalize_odds(df)
        norm["source_file"] = str(path)
        frames.append(norm)
        file_stats.append(
            {
                "path": str(path),
                "rows": int(len(norm)),
                "mtime": datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
                "captured_at_max": _to_iso_or_none(norm["captured_at"].max() if len(norm) > 0 else None),
            }
        )

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values(
        ["race_id", "horse_no", "odds_type", "captured_at", "odds_snapshot_version", "source_version"],
        ascending=[True, True, True, False, False, False],
        kind="mergesort",
    )
    merged = merged.drop_duplicates(subset=["race_id", "horse_no", "odds_type"], keep="first")
    merged["fetched_at"] = fetched_at

    output.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output, index=False, encoding="utf-8")

    file_mtime_max = max(datetime.fromtimestamp(Path(s["path"]).stat().st_mtime) for s in file_stats)
    captured_max = pd.to_datetime(merged["captured_at"], errors="coerce").max() if len(merged) > 0 else pd.NaT

    report = {
        "today_date": today_date,
        "fetched_at": fetched_at,
        "normalized_odds_file_count": int(len(odds_files)),
        "normalized_odds_files": file_stats,
        "odds_latest_path": str(output),
        "odds_latest_rows": int(len(merged)),
        "odds_latest_captured_at_max": _to_iso_or_none(captured_max),
        "odds_latest_file_mtime": datetime.fromtimestamp(output.stat().st_mtime).isoformat(timespec="seconds"),
        "normalized_odds_file_mtime_max": file_mtime_max.isoformat(timespec="seconds"),
        "warnings": [],
    }

    if report_out:
        report_out.parent.mkdir(parents=True, exist_ok=True)
        report_out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== odds latest built ===")
    print(f"today_date={today_date}")
    print(f"rows={len(merged)}")
    print(f"files={len(odds_files)}")
    print(f"fetched_at={fetched_at}")
    print(f"output={output}")
    print("REPORT_JSON=" + json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
