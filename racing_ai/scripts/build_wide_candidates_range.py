from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd

from aikeiba.db.duckdb import DuckDb
from aikeiba.orchestration.race_day import _decision_preview
from aikeiba.decision.skip_reasoning import SkipReasonConfig


def build_range(
    *,
    db_path: Path,
    start_date: str,
    end_date: str,
    model_version: str,
    feature_set_version: str,
    decision_ai_weight: float,
    out_root: Path,
    skip_existing: bool,
) -> dict:
    db = DuckDb.connect(db_path)
    dates_df = db.query_df(
        """
        SELECT distinct cast(race_date as VARCHAR) AS race_date
        FROM races
        WHERE race_date BETWEEN cast(? as DATE) AND cast(? as DATE)
        ORDER BY race_date
        """,
        (start_date, end_date),
    )
    dates = dates_df["race_date"].astype(str).tolist() if len(dates_df) > 0 else []

    out_root.mkdir(parents=True, exist_ok=True)
    report_rows = []
    total_rows = 0

    for d in dates:
        out_path = out_root / f"wide_pair_candidates_{d}_{model_version}.parquet"
        if skip_existing and out_path.exists():
            report_rows.append({"race_date": d, "status": "skipped_existing", "rows": None, "path": str(out_path), "message": "skip_existing"})
            continue

        try:
            decision = _decision_preview(
                db=db,
                race_date=d,
                model_version=model_version,
                density_top3_max=9.9,
                gap12_min=0.0,
                ai_weight=decision_ai_weight,
                skip_reason_config=SkipReasonConfig(density_top3_max=9.9, gap12_min=0.0),
            )
            pairs = decision.get("candidate_pairs", []) if isinstance(decision, dict) else []
            pdf = pd.DataFrame(pairs)
            if len(pdf) == 0:
                pdf = pd.DataFrame(columns=["race_id", "pair"])
            pdf["race_date"] = d
            pdf["model_version"] = model_version
            pdf["feature_set_version"] = feature_set_version
            pdf["decision_ai_weight"] = decision_ai_weight
            pdf["generated_at"] = dt.datetime.now().isoformat(timespec="seconds")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            pdf.to_parquet(out_path, index=False)
            n = int(len(pdf))
            total_rows += n
            report_rows.append({"race_date": d, "status": "ok", "rows": n, "path": str(out_path), "message": None})
        except Exception as exc:
            report_rows.append({"race_date": d, "status": "warning", "rows": None, "path": str(out_path), "message": str(exc)})

    rep_path = Path("racing_ai/reports/wide_candidates_range_report.md")
    lines = [
        "# wide_candidates_range_report",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- db_path: {db_path}",
        f"- range: {start_date}..{end_date}",
        f"- model_version: {model_version}",
        f"- feature_set_version: {feature_set_version}",
        f"- decision_ai_weight: {decision_ai_weight}",
        f"- out_root: {out_root}",
        "",
        "| race_date | status | rows | path | message |",
        "|---|---|---:|---|---|",
    ]
    for r in report_rows:
        lines.append(f"| {r['race_date']} | {r['status']} | {'' if r['rows'] is None else r['rows']} | {r['path']} | {'' if r['message'] is None else r['message']} |")
    rep_path.parent.mkdir(parents=True, exist_ok=True)
    rep_path.write_text("\n".join(lines), encoding="utf-8")

    return {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "date_count": len(dates),
        "ok_count": int(sum(1 for r in report_rows if r["status"] == "ok")),
        "warning_count": int(sum(1 for r in report_rows if r["status"] == "warning")),
        "skipped_count": int(sum(1 for r in report_rows if r["status"] == "skipped_existing")),
        "total_rows": total_rows,
        "report_path": str(rep_path),
        "rows": report_rows,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--models-root", default="racing_ai/data/models_compare")
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--model-version", required=True)
    ap.add_argument("--feature-set-version", default="fs_v1")
    ap.add_argument("--decision-ai-weight", type=float, default=0.65)
    ap.add_argument("--out-root", default="racing_ai/data/bets")
    ap.add_argument("--skip-existing", action="store_true")
    args = ap.parse_args()

    res = build_range(
        db_path=Path(args.db_path),
        start_date=args.start_date,
        end_date=args.end_date,
        model_version=args.model_version,
        feature_set_version=args.feature_set_version,
        decision_ai_weight=float(args.decision_ai_weight),
        out_root=Path(args.out_root),
        skip_existing=bool(args.skip_existing),
    )
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
