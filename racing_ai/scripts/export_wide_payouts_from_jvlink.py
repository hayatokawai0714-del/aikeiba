from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import subprocess
from collections import Counter
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DayRunResult:
    race_date: str
    status: str
    message: str
    output_dir: str
    payouts_csv_path: str | None
    wide_rows: int
    errors: int


@dataclass
class RawInspect:
    raw_payouts_header: str
    raw_payouts_row_count: int
    raw_payouts_file_size: int
    raw_bet_type_counts: dict[str, int]
    jvopen_failures: list[str]


def daterange(start_date: dt.date, end_date: dt.date):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += dt.timedelta(days=1)


def normalize_bet_key(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    parts = s.split("-")
    if len(parts) != 2:
        return None
    try:
        a = int(parts[0])
        b = int(parts[1])
    except Exception:
        return None
    x, y = sorted((a, b))
    return f"{x:02d}-{y:02d}"


def is_wide_type(v: object) -> bool:
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in {"wide", "ワイド"}


def to_date_str(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    try:
        d = dt.datetime.fromisoformat(s)
        return d.strftime("%Y-%m-%d")
    except Exception:
        pass
    for fmt in ("%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            d = dt.datetime.strptime(s, fmt)
            return d.strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


def detect_jvlink_environment() -> dict:
    info = {
        "dotnet_ok": False,
        "dotnet_info_head": "",
        "com_jvdtlab_ok": False,
        "com_jvlink_ok": False,
        "com_error_jvdtlab": "",
        "com_error_jvlink": "",
        "notes": [],
    }
    try:
        proc = subprocess.run(
            ["dotnet", "--info"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        info["dotnet_ok"] = proc.returncode == 0
        info["dotnet_info_head"] = (proc.stdout or proc.stderr).splitlines()[0] if (proc.stdout or proc.stderr) else ""
    except Exception as e:
        info["notes"].append(f"dotnet_check_failed:{e}")

    ps_script = (
        "try { New-Object -ComObject JVDTLab.JVLink | Out-Null; 'OK1' } catch { 'NG1:' + $_.Exception.Message }; "
        "try { New-Object -ComObject JVLink.JVLink | Out-Null; 'OK2' } catch { 'NG2:' + $_.Exception.Message }"
    )
    try:
        proc = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        out = (proc.stdout or "").splitlines()
        for line in out:
            if line.startswith("OK1"):
                info["com_jvdtlab_ok"] = True
            elif line.startswith("NG1:"):
                info["com_error_jvdtlab"] = line
            elif line.startswith("OK2"):
                info["com_jvlink_ok"] = True
            elif line.startswith("NG2:"):
                info["com_error_jvlink"] = line
    except Exception as e:
        info["notes"].append(f"com_check_failed:{e}")
    return info


def find_exporter_exe() -> Path | None:
    candidates = [
        Path("racing_ai/tools/jvlink_direct_exporter/bin/Release/net8.0-windows/win-x86/Aikeiba.JVLinkDirectExporter.exe"),
        Path("racing_ai/tools/jvlink_direct_exporter/bin/Debug/net8.0-windows/win-x86/Aikeiba.JVLinkDirectExporter.exe"),
        Path("racing_ai/tools/jvlink_direct_exporter/bin/Debug/net8.0-windows/Aikeiba.JVLinkDirectExporter.exe"),
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def run_exporter_for_day(
    *,
    exporter_exe: Path,
    race_date: dt.date,
    day_output_dir: Path,
    overwrite: bool,
    verbose: bool,
    debug_jvopen: bool,
    setup_mode: bool,
    option: int | None,
    read_retry_count: int,
    read_retry_sleep_sec: int,
    skip_read_errors: bool,
) -> subprocess.CompletedProcess:
    effective_option = option
    if effective_option is None:
        effective_option = 4 if setup_mode else 1
    args = [
        str(exporter_exe),
        "--race-date",
        race_date.strftime("%Y-%m-%d"),
        "--output-dir",
        str(day_output_dir),
        "--overwrite" if overwrite else "",
        "--verbose" if verbose else "",
        "--debug-jvopen" if debug_jvopen else "",
        "--setup-mode" if setup_mode else "",
        "--option",
        str(effective_option),
        "--read-retry-count",
        str(read_retry_count),
        "--read-retry-sleep-sec",
        str(read_retry_sleep_sec),
        "--skip-read-errors" if skip_read_errors else "",
    ]
    args = [a for a in args if a != ""]
    return subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)


def read_payouts_rows(path: Path) -> list[dict[str, str]]:
    last_err: Exception | None = None
    for enc in ["utf-8", "utf-8-sig", "cp932"]:
        try:
            with path.open("r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except Exception as e:
            last_err = e
            continue
    if last_err is not None:
        raise last_err
    return []


def inspect_raw_payouts(path: Path) -> RawInspect:
    if not path.exists():
        return RawInspect("", 0, 0, {}, [])
    file_size = path.stat().st_size
    header = ""
    rows: list[dict[str, str]] = []
    last_err: Exception | None = None
    for enc in ["utf-8", "utf-8-sig", "cp932"]:
        try:
            with path.open("r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f)
                header = ",".join(reader.fieldnames or [])
                rows = list(reader)
            break
        except Exception as e:
            last_err = e
            continue
    if not rows and not header and last_err is not None:
        header = f"read_failed:{last_err}"
    counts = Counter(str(r.get("bet_type", "")).strip() for r in rows if str(r.get("bet_type", "")).strip())
    # Also read manifest jvopen_failures if present.
    failures: list[str] = []
    manifest = path.parent / "raw_manifest_check.json"
    if manifest.exists():
        try:
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            jf = payload.get("jvopen_failures")
            if isinstance(jf, list):
                failures = [str(x) for x in jf]
            else:
                warns = payload.get("warnings")
                if isinstance(warns, list):
                    failures = [str(w) for w in warns if str(w).startswith("jvopen_failed:")]
        except Exception:
            failures = []
    return RawInspect(
        raw_payouts_header=header,
        raw_payouts_row_count=len(rows),
        raw_payouts_file_size=file_size,
        raw_bet_type_counts=dict(sorted(counts.items(), key=lambda x: x[0])),
        jvopen_failures=failures,
    )


def try_float(v: object) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def extract_wide_rows(rows: list[dict[str, str]], fallback_race_date: dt.date, source_version: str) -> list[dict[str, str]]:
    if not rows:
        return []
    out_map: dict[tuple[str, str, str], dict[str, str]] = {}
    fallback = fallback_race_date.strftime("%Y-%m-%d")
    for r in rows:
        race_id = str(r.get("race_id", "")).strip()
        bet_type_val = r.get("bet_type")
        if not is_wide_type(bet_type_val):
            continue

        combo = r.get("winning_combination")
        if combo is None:
            combo = r.get("bet_key")
        bet_key = normalize_bet_key(combo)
        if bet_key is None:
            continue

        payout_val = r.get("payout_yen")
        if payout_val is None:
            payout_val = r.get("payout")
        payout = try_float(payout_val)
        if payout is None or payout <= 0:
            continue

        race_date = to_date_str(r.get("race_date"))
        if race_date is None:
            race_date = to_date_str(race_id[:8]) if len(race_id) >= 8 else None
        race_date = race_date or fallback

        key = (race_id, "wide", bet_key)
        out_map[key] = {
            "race_id": race_id,
            "race_date": race_date,
            "bet_type": "wide",
            "bet_key": bet_key,
            "payout": f"{payout:.0f}" if payout.is_integer() else f"{payout}",
            "source_version": source_version,
        }
    out_rows = list(out_map.values())
    out_rows.sort(key=lambda x: (x["race_date"], x["race_id"], x["bet_key"]))
    return out_rows


def write_export_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["race_id", "race_date", "bet_type", "bet_key", "payout", "source_version"]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})


def write_report(
    *,
    report_path: Path,
    start_date: dt.date,
    end_date: dt.date,
    out_csv: Path,
    environment: dict,
    day_results: list[DayRunResult],
    exported_rows: list[dict[str, str]],
    raw_debug: dict[str, RawInspect] | None,
) -> None:
    total_days = len(day_results)
    ok_days = sum(1 for r in day_results if r.status == "ok")
    err_days = sum(1 for r in day_results if r.status == "error")
    total_errors = sum(r.errors for r in day_results)
    total_wide = len(exported_rows)

    by_date = Counter(r["race_date"] for r in exported_rows if r.get("race_date"))
    lines: list[str] = []
    lines.append("# jvlink_wide_payout_export_report")
    lines.append("")
    lines.append(f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"- period: {start_date} .. {end_date}")
    lines.append(f"- output_csv: {out_csv}")
    lines.append("")
    lines.append("## Environment")
    lines.append("")
    lines.append(f"- dotnet_ok: {environment.get('dotnet_ok')}")
    lines.append(f"- dotnet_info_head: {environment.get('dotnet_info_head')}")
    lines.append(f"- com_jvdtlab_ok: {environment.get('com_jvdtlab_ok')}")
    lines.append(f"- com_jvlink_ok: {environment.get('com_jvlink_ok')}")
    if environment.get("com_error_jvdtlab"):
        lines.append(f"- com_error_jvdtlab: {environment.get('com_error_jvdtlab')}")
    if environment.get("com_error_jvlink"):
        lines.append(f"- com_error_jvlink: {environment.get('com_error_jvlink')}")
    if environment.get("notes"):
        for n in environment["notes"]:
            lines.append(f"- note: {n}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- total_days: {total_days}")
    lines.append(f"- ok_days: {ok_days}")
    lines.append(f"- error_days: {err_days}")
    lines.append(f"- total_wide_payout_rows: {total_wide}")
    lines.append(f"- error_count: {total_errors}")
    if raw_debug is not None and len(raw_debug) > 0:
        lines.append(f"- debug_inspect_days: {len(raw_debug)}")
    lines.append("")
    lines.append("## race_date別件数")
    lines.append("")
    lines.append("| race_date | wide_rows |")
    lines.append("|---|---:|")
    if not by_date:
        lines.append("| (none) | 0 |")
    else:
        for d in sorted(by_date.keys()):
            lines.append(f"| {d} | {by_date[d]} |")
    lines.append("")
    lines.append("## 日次実行結果")
    lines.append("")
    lines.append("| race_date | status | wide_rows | payouts_csv_path | message |")
    lines.append("|---|---|---:|---|---|")
    for r in day_results:
        lines.append(
            f"| {r.race_date} | {r.status} | {r.wide_rows} | {r.payouts_csv_path or ''} | {r.message.replace('|','/')} |"
        )
    if raw_debug is not None and len(raw_debug) > 0:
        lines.append("")
        lines.append("## raw debug inspect")
        lines.append("")
        lines.append("| race_date | raw_payouts_file_size | raw_payouts_row_count | raw_payouts_header | raw_bet_type_counts |")
        lines.append("|---|---:|---:|---|---|")
        for race_date in sorted(raw_debug.keys()):
            d = raw_debug[race_date]
            lines.append(
                f"| {race_date} | {d.raw_payouts_file_size} | {d.raw_payouts_row_count} | {d.raw_payouts_header.replace('|','/')} | {json.dumps(d.raw_bet_type_counts, ensure_ascii=False)} |"
            )
        lines.append("")
        lines.append("### jvopen_failures")
        lines.append("")
        for race_date in sorted(raw_debug.keys()):
            d = raw_debug[race_date]
            if not d.jvopen_failures:
                continue
            lines.append(f"- {race_date}:")
            for f in d.jvopen_failures:
                lines.append(f"  - {f}")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Export WIDE payouts from JV-Link (via local JV-Link direct exporter).")
    ap.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out-csv", type=Path, required=True)
    ap.add_argument("--source-version", required=True)
    ap.add_argument("--encoding", default="utf-8")
    ap.add_argument("--tmp-raw-root", type=Path, default=Path("racing_ai/data/external/jvlink_raw"))
    ap.add_argument("--exporter-exe", type=Path, default=None)
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--debug-inspect-raw", action="store_true", help="Inspect raw payouts.csv and include debug fields in report.")
    ap.add_argument("--debug-jvopen", action="store_true", help="Pass --debug-jvopen to JV-Link direct exporter.")
    ap.add_argument("--setup-mode", action="store_true", help="Use setup retrieval mode in JV-Link exporter.")
    ap.add_argument("--option", type=int, default=None, help="JVOpen option to pass through. In setup mode, use 3 or 4.")
    ap.add_argument("--read-retry-count", type=int, default=2, help="Retry count for JVRead/JVGets read errors.")
    ap.add_argument("--read-retry-sleep-sec", type=int, default=1, help="Sleep seconds between read retries.")
    ap.add_argument("--skip-read-errors", action="store_true", help="Skip unresolved JVRead/JVGets errors.")
    ap.add_argument("--report-md", type=Path, default=Path("racing_ai/reports/jvlink_wide_payout_export_report.md"))
    args = ap.parse_args()

    if args.encoding.lower() != "utf-8":
        raise ValueError("This script currently supports --encoding utf-8 only.")

    start_date = dt.date.fromisoformat(args.start_date)
    end_date = dt.date.fromisoformat(args.end_date)
    if start_date > end_date:
        raise ValueError("start-date must be <= end-date")

    env = detect_jvlink_environment()
    exe = args.exporter_exe if args.exporter_exe else find_exporter_exe()
    if exe is None:
        raise FileNotFoundError("JV-Link exporter exe not found. Build tools/jvlink_direct_exporter first.")

    all_rows: list[dict[str, str]] = []
    day_results: list[DayRunResult] = []
    error_count = 0
    raw_debug: dict[str, RawInspect] = {}

    for d in daterange(start_date, end_date):
        day_label = d.strftime("%Y%m%d")
        out_dir = args.tmp_raw_root / f"{day_label}_jvlink_export"
        out_dir.mkdir(parents=True, exist_ok=True)

        proc = run_exporter_for_day(
            exporter_exe=exe,
            race_date=d,
            day_output_dir=out_dir,
            overwrite=args.overwrite or True,
            verbose=args.verbose,
            debug_jvopen=args.debug_jvopen,
            setup_mode=args.setup_mode,
            option=args.option,
            read_retry_count=args.read_retry_count,
            read_retry_sleep_sec=args.read_retry_sleep_sec,
            skip_read_errors=args.skip_read_errors,
        )

        payouts_path = out_dir / "payouts.csv"
        if proc.returncode != 0:
            error_count += 1
            msg = (proc.stderr or proc.stdout or "").strip().replace("\n", " ")[:300]
            day_results.append(
                DayRunResult(
                    race_date=d.isoformat(),
                    status="error",
                    message=f"exporter_failed:{msg}",
                    output_dir=str(out_dir),
                    payouts_csv_path=str(payouts_path) if payouts_path.exists() else None,
                    wide_rows=0,
                    errors=1,
                )
            )
            continue

        if not payouts_path.exists():
            error_count += 1
            day_results.append(
                DayRunResult(
                    race_date=d.isoformat(),
                    status="error",
                    message="payouts_csv_missing_after_export",
                    output_dir=str(out_dir),
                    payouts_csv_path=None,
                    wide_rows=0,
                    errors=1,
                )
            )
            continue
        if args.debug_inspect_raw:
            raw_debug[d.isoformat()] = inspect_raw_payouts(payouts_path)

        try:
            raw_rows = read_payouts_rows(payouts_path)
            wide_rows = extract_wide_rows(raw_rows, d, args.source_version)
            if wide_rows:
                all_rows.extend(wide_rows)
            day_results.append(
                DayRunResult(
                    race_date=d.isoformat(),
                    status="ok",
                    message="ok",
                    output_dir=str(out_dir),
                    payouts_csv_path=str(payouts_path),
                    wide_rows=len(wide_rows),
                    errors=0,
                )
            )
        except Exception as e:
            error_count += 1
            day_results.append(
                DayRunResult(
                    race_date=d.isoformat(),
                    status="error",
                    message=f"parse_failed:{e}",
                    output_dir=str(out_dir),
                    payouts_csv_path=str(payouts_path),
                    wide_rows=0,
                    errors=1,
                )
            )

    # dedupe by key
    dedup: dict[tuple[str, str, str], dict[str, str]] = {}
    for r in all_rows:
        dedup[(r["race_id"], r["bet_type"], r["bet_key"])] = r
    final_rows = sorted(dedup.values(), key=lambda x: (x["race_date"], x["race_id"], x["bet_key"]))

    write_export_csv(args.out_csv, final_rows)
    write_report(
        report_path=args.report_md,
        start_date=start_date,
        end_date=end_date,
        out_csv=args.out_csv,
        environment=env,
        day_results=day_results,
        exported_rows=final_rows,
        raw_debug=raw_debug if args.debug_inspect_raw else None,
    )

    summary = {
        "start_date": args.start_date,
        "end_date": args.end_date,
        "out_csv": str(args.out_csv),
        "rows": len(final_rows),
        "error_count": error_count,
        "report_md": str(args.report_md),
        "exporter_exe": str(exe),
        "debug_inspect_raw": bool(args.debug_inspect_raw),
        "debug_jvopen": bool(args.debug_jvopen),
        "setup_mode": bool(args.setup_mode),
        "option": args.option,
        "read_retry_count": args.read_retry_count,
        "read_retry_sleep_sec": args.read_retry_sleep_sec,
        "skip_read_errors": bool(args.skip_read_errors),
        "raw_debug": {k: v.__dict__ for k, v in raw_debug.items()} if args.debug_inspect_raw else {},
        "environment": env,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
