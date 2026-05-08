from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd

from aikeiba.db.duckdb import DuckDb


def _safe_json_list(s: object) -> list[str]:
    if s is None:
        return []
    txt = str(s).strip()
    if txt == "" or txt.lower() == "none":
        return []
    try:
        v = json.loads(txt)
        if isinstance(v, list):
            return [str(x) for x in v]
        if isinstance(v, dict):
            # fallback for legacy wrapped forms
            for key in ("missing_files", "required_files"):
                if isinstance(v.get(key), list):
                    return [str(x) for x in v.get(key)]
    except Exception:
        pass
    return []


def build_report(*, db_path: Path, limit: int, out_md: Path) -> str:
    now = dt.datetime.now().isoformat(timespec="seconds")
    lines: list[str] = ["# daily_cycle_run_log_report", "", f"- 実行日時: {now}", f"- DB: {db_path}", f"- limit: {limit}", ""]

    if not db_path.exists():
        lines.append("## Warning")
        lines.append(f"- DBが存在しません: {db_path}")
        out_md.parent.mkdir(parents=True, exist_ok=True)
        out_md.write_text("\n".join(lines), encoding="utf-8")
        return str(out_md)

    try:
        db = DuckDb.connect(db_path)
        tables = db.query_df("SHOW TABLES")
    except Exception as exc:
        lines.append("## Warning")
        lines.append(f"- DB接続に失敗: {exc}")
        out_md.parent.mkdir(parents=True, exist_ok=True)
        out_md.write_text("\n".join(lines), encoding="utf-8")
        return str(out_md)

    table_names = set(tables.iloc[:, 0].astype(str).tolist()) if len(tables) > 0 else set()
    if "daily_cycle_run_log" not in table_names:
        lines.append("## Warning")
        lines.append("- daily_cycle_run_log テーブルが存在しません")
        out_md.parent.mkdir(parents=True, exist_ok=True)
        out_md.write_text("\n".join(lines), encoding="utf-8")
        return str(out_md)

    df = db.query_df(
        """
        SELECT
          cast(created_at as varchar) as created_at,
          run_id,
          command_name,
          cast(race_date as varchar) as race_date,
          model_version,
          raw_dir,
          status,
          stop_reason,
          required_files,
          missing_files,
          raw_precheck_log_path,
          run_summary_path,
          daily_cycle_summary_path,
          cast(generated_at as varchar) as generated_at
        FROM daily_cycle_run_log
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (int(limit),),
    )

    lines.append("## 直近N件一覧")
    if len(df) == 0:
        lines.append("- データなし")
    else:
        lines.append("| created_at | command_name | race_date | model_version | status | stop_reason | missing_files | raw_precheck_log_path |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for r in df.itertuples(index=False):
            missing = ", ".join(_safe_json_list(getattr(r, "missing_files", None)))
            lines.append(
                f"| {r.created_at} | {r.command_name} | {r.race_date} | {r.model_version} | {r.status} | {r.stop_reason} | {missing} | {r.raw_precheck_log_path} |"
            )

    def _count_block(title: str, col: str) -> None:
        lines.append("")
        lines.append(f"## {title}")
        if len(df) == 0:
            lines.append("- データなし")
            return
        c = df[col].fillna("NULL").astype(str).value_counts()
        for k, v in c.items():
            lines.append(f"- {k}: {int(v)}")

    _count_block("status別件数", "status")
    _count_block("stop_reason別件数", "stop_reason")
    _count_block("command_name別件数", "command_name")

    lines.append("")
    lines.append("## missing_files 頻出")
    if len(df) == 0:
        lines.append("- データなし")
    else:
        mf_counts: dict[str, int] = {}
        for s in df["missing_files"].tolist():
            for f in _safe_json_list(s):
                mf_counts[f] = mf_counts.get(f, 0) + 1
        if not mf_counts:
            lines.append("- なし")
        else:
            for k, v in sorted(mf_counts.items(), key=lambda x: (-x[1], x[0])):
                lines.append(f"- {k}: {v}")

    lines.append("")
    lines.append("## raw_precheck_log_path 参照")
    if len(df) == 0:
        lines.append("- データなし")
    else:
        uniq = [x for x in df["raw_precheck_log_path"].dropna().astype(str).unique().tolist() if x.strip()]
        if not uniq:
            lines.append("- なし")
        else:
            for p in uniq:
                lines.append(f"- {p}")

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")
    return str(out_md)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="racing_ai/data/warehouse/aikeiba.duckdb")
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--out-md", default="racing_ai/reports/daily_cycle_run_log_report.md")
    args = ap.parse_args()
    p = build_report(db_path=Path(args.db_path), limit=int(args.limit), out_md=Path(args.out_md))
    print(p)


if __name__ == "__main__":
    main()
