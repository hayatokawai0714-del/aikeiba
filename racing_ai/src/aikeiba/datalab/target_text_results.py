from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from aikeiba.domain.ids import build_race_id_from_parts


RE_DATE_VENUE = re.compile(r"(?P<y>\d{4})年\s*(?P<m>\d{1,2})月\s*(?P<d>\d{1,2})日.*?(?P<venue>中山|東京|京都|阪神|中京|福島|新潟|小倉|札幌|函館)")
RE_RACE_NO = re.compile(r"^\s*(?P<no>[0-9０-９]{1,2})Ｒ")


_FW_TO_ASCII = str.maketrans("０１２３４５６７８９", "0123456789")


def _read_text_auto(path: Path) -> str:
    last_exc: Exception | None = None
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return path.read_text(encoding=enc)
        except Exception as exc:  # noqa: PERF203
            last_exc = exc
    raise ValueError(f"failed to read text: {path} ({last_exc})")


def _to_yyyymmdd(y: str, m: str, d: str) -> str:
    return f"{int(y):04d}{int(m):02d}{int(d):02d}"


def _parse_int_token(tok: str) -> int | None:
    s = str(tok).strip().translate(_FW_TO_ASCII)
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _parse_float_token(tok: str) -> float | None:
    s = str(tok).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


@dataclass(frozen=True)
class ResultRowRaw:
    race_id: str
    horse_no: int
    finish_position: int | None
    last3f_time: float | None
    pop_rank: int | None


def build_results_csv_from_target_result_text(*, input_txt: Path, races_csv: Path, output_csv: Path) -> dict:
    """
    Convert TARGET '成績（整形テキスト）' export to Aikeiba raw results.csv.

    Assumptions:
    - Horse line is whitespace tokenized: fin, waku, umaban, horse_name, sex, age, jockey, weight, time, last3f, pop, ...
    - Corner positions / margins are not present -> emitted as empty.
    """
    text = _read_text_auto(input_txt)

    races_df = pd.read_csv(races_csv, encoding="utf-8-sig")
    races_df.columns = [str(c).strip().lower() for c in races_df.columns]
    if not {"race_id", "venue", "race_no"}.issubset(set(races_df.columns)):
        raise ValueError(f"races_csv missing required columns: {races_csv}")

    race_map: dict[tuple[str, int], str] = {}
    for _, r in races_df.iterrows():
        venue = str(r.get("venue", "")).strip()
        race_no = int(r.get("race_no", 0) or 0)
        race_id = str(r.get("race_id", "")).strip()
        if venue and race_no > 0 and race_id:
            race_map[(venue, race_no)] = race_id

    warnings: list[str] = []
    rows: list[ResultRowRaw] = []

    current_date_yyyymmdd: str | None = None
    current_venue: str | None = None
    current_race_no: int | None = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip("\r\n")

        m_dv = RE_DATE_VENUE.search(line)
        if m_dv:
            current_date_yyyymmdd = _to_yyyymmdd(m_dv.group("y"), m_dv.group("m"), m_dv.group("d"))
            current_venue = m_dv.group("venue")
            continue

        m_rn = RE_RACE_NO.match(line)
        if m_rn:
            current_race_no = _parse_int_token(m_rn.group("no"))
            continue

        if current_date_yyyymmdd is None or current_venue is None or current_race_no is None:
            continue

        s = line.strip()
        if not s:
            continue
        if s.startswith("着枠") or s.startswith("LAP") or s.startswith("通過") or s.startswith("単勝") or s.startswith("複勝") or s.startswith("枠連") or s.startswith("馬連") or s.startswith("ワイド") or s.startswith("馬単") or s.startswith("３連") or s.startswith("-"):
            continue

        # Horse result line
        toks = s.split()
        if len(toks) < 11:
            continue

        fin = _parse_int_token(toks[0])
        umaban = _parse_int_token(toks[2])
        if umaban is None:
            continue

        horse_name = str(toks[3]).strip().replace("　", " ")
        last3f = _parse_float_token(toks[9])
        pop = _parse_int_token(toks[10])

        race_id = race_map.get((current_venue, int(current_race_no)))
        if not race_id:
            # Fallback: build deterministically from parts
            race_id = build_race_id_from_parts(current_date_yyyymmdd, current_venue, int(current_race_no))
            warnings.append(f"race_id_not_found_in_races_csv: venue={current_venue} race_no={current_race_no} -> {race_id}")

        rows.append(
            ResultRowRaw(
                race_id=race_id,
                horse_no=int(umaban),
                finish_position=fin,
                last3f_time=last3f,
                pop_rank=pop,
            )
        )

    df = pd.DataFrame([r.__dict__ for r in rows])
    if len(df) > 0:
        df = df.sort_values(["race_id", "horse_no"]).reset_index(drop=True)

    # Ensure columns expected by raw_pipeline normalizer are present (even if empty)
    for col in [
        "margin",
        "last3f_rank",
        "corner_pos_1",
        "corner_pos_2",
        "corner_pos_3",
        "corner_pos_4",
        "odds_win_final",
    ]:
        if col not in df.columns:
            df[col] = None

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    return {
        "created_at": dt.datetime.now().isoformat(timespec="seconds"),
        "input_txt": str(input_txt),
        "races_csv": str(races_csv),
        "output_csv": str(output_csv),
        "rows": int(len(df)),
        "warnings": sorted(set(warnings)),
    }

