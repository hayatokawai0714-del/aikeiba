from __future__ import annotations

import datetime as dt
from pathlib import Path

from aikeiba.db.duckdb import DuckDb


def validate_race_metadata(db: DuckDb, race_date: str) -> list[dict]:
    rows = db.query_df(
        """
        SELECT race_id, venue, surface, distance, field_size_expected
        FROM races
        WHERE race_date = cast(? as DATE)
        ORDER BY race_id
        """,
        (race_date,),
    ).to_dict("records")
    out: list[dict] = []
    for r in rows:
        reasons: list[str] = []
        if r.get("field_size_expected") is None or int(r.get("field_size_expected") or 0) <= 0:
            reasons.append("field_size_expected_le_0")
        if r.get("distance") is None:
            reasons.append("distance_null")
        if r.get("surface") is None:
            reasons.append("surface_null")
        if r.get("venue") is None:
            reasons.append("venue_null")
        if reasons:
            out.append(
                {
                    "race_id": str(r.get("race_id")),
                    "reasons": reasons,
                    "field_size_expected": r.get("field_size_expected"),
                    "distance": r.get("distance"),
                    "surface": r.get("surface"),
                    "venue": r.get("venue"),
                }
            )
    return out


def write_race_metadata_validation_report(
    *,
    race_date: str,
    invalid_rows: list[dict],
    policy: str,
    out_path: Path,
) -> str:
    lines = [
        "# race_metadata_validation_report",
        "",
        f"- generated_at: {dt.datetime.now().isoformat(timespec='seconds')}",
        f"- race_date: {race_date}",
        f"- race_meta_policy: {policy}",
        f"- invalid_race_count: {len(invalid_rows)}",
        "",
        "## Invalid Races",
    ]
    if len(invalid_rows) == 0:
        lines.append("- none")
    else:
        lines.append("| race_id | reasons | field_size_expected | distance | surface | venue |")
        lines.append("|---|---|---:|---:|---|---|")
        for r in invalid_rows:
            lines.append(
                f"| {r['race_id']} | {','.join(r['reasons'])} | {r.get('field_size_expected')} | {r.get('distance')} | {r.get('surface')} | {r.get('venue')} |"
            )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return str(out_path)
