from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from aikeiba.domain.ids import build_race_id_from_parts


VENUE_JP_TO_CODE = {
    "中山": "NAK",
    "東京": "TOK",
    "京都": "KYO",
    "阪神": "HAN",
    "中京": "CHU",
    "福島": "FUK",
    "新潟": "NII",
    "小倉": "KOK",
    "札幌": "SAP",
    "函館": "HAK",
}


_FW_TO_ASCII = str.maketrans("０１２３４５６７８９", "0123456789")


@dataclass(frozen=True)
class RaceRow:
    race_id: str
    race_date: str  # YYYY-MM-DD
    venue_code: str
    venue: str
    race_no: int
    race_name: str
    distance: int | None
    surface: str
    field_size: int | None
    grade: str


@dataclass(frozen=True)
class PayoutRow:
    race_id: str
    bet_type: str
    winning_combination: str
    payout_yen: int


RE_DATE = re.compile(r"(?P<y>\d{4})年\s*(?P<m>\d{1,2})月\s*(?P<d>\d{1,2})日")
RE_VENUE = re.compile(r"(?P<venue>中山|東京|京都|阪神|中京|福島|新潟|小倉|札幌|函館)")
RE_RACE_NO = re.compile(r"^\s*(?P<no>[0-9０-９]{1,2})Ｒ")
RE_SURFACE_DIST_FIELD = re.compile(r"(?P<surface>芝|ダート|障害)\s*(?P<dist>\d{3,4})m.*?(?P<field>\d{1,2})頭")
RE_PAYOUT_ITEM = re.compile(r"(?P<combo>[0-9]{1,2}(?:-[0-9]{1,2}){0,2})\s*\\(?P<yen>[0-9,]+)")


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


def _to_dash_date(y: str, m: str, d: str) -> str:
    return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"


def _parse_int_token(tok: str) -> int | None:
    s = str(tok).strip().translate(_FW_TO_ASCII)
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _normalize_combo(combo: str) -> str:
    s = str(combo).strip().replace("－", "-").replace("―", "-").replace("−", "-")
    parts = [p.strip() for p in s.split("-") if p.strip()]
    if len(parts) == 0:
        return ""
    try:
        nums = [int(p) for p in parts]
    except Exception:
        return s
    return "-".join(f"{n:02d}" for n in nums)


def parse_target_payout_text(*, text: str) -> list[PayoutRow]:
    rows: list[PayoutRow] = []

    current_date: str | None = None
    current_venue_jp: str | None = None
    current_venue_code: str | None = None
    current_race_no: int | None = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip("\r\n")

        m_date = RE_DATE.search(line)
        if m_date:
            current_date = _to_yyyymmdd(m_date.group("y"), m_date.group("m"), m_date.group("d"))
            m_venue = RE_VENUE.search(line)
            if m_venue:
                current_venue_jp = m_venue.group("venue")
                current_venue_code = VENUE_JP_TO_CODE.get(current_venue_jp)
            continue

        m_race = RE_RACE_NO.match(line)
        if m_race:
            current_race_no = _parse_int_token(m_race.group("no"))
            continue

        if current_date is None or current_venue_code is None or current_race_no is None:
            continue

        s = line.strip()
        if not s:
            continue

        for bet_prefix in ("単勝", "複勝", "枠連", "馬連", "ワイド", "馬単", "３連複", "３連単"):
            if not s.startswith(bet_prefix):
                continue
            race_id = build_race_id_from_parts(current_date, current_venue_code, int(current_race_no))
            for item in RE_PAYOUT_ITEM.finditer(s):
                combo = _normalize_combo(item.group("combo"))
                yen = int(item.group("yen").replace(",", ""))
                rows.append(PayoutRow(race_id=race_id, bet_type=bet_prefix, winning_combination=combo, payout_yen=yen))
            break

    return rows


def parse_target_races_text(*, text: str) -> list[RaceRow]:
    rows: list[RaceRow] = []

    current_date_yyyymmdd: str | None = None
    current_date_dash: str | None = None
    current_venue_jp: str | None = None
    current_venue_code: str | None = None
    current_race_no: int | None = None
    current_race_name: str = ""
    current_surface: str = ""
    current_distance: int | None = None
    current_field: int | None = None

    def flush() -> None:
        nonlocal current_race_no, current_race_name, current_surface, current_distance, current_field
        if current_date_yyyymmdd is None or current_date_dash is None or current_venue_code is None or current_venue_jp is None or current_race_no is None:
            return
        race_id = build_race_id_from_parts(current_date_yyyymmdd, current_venue_code, int(current_race_no))
        rows.append(
            RaceRow(
                race_id=race_id,
                race_date=current_date_dash,
                venue_code=current_venue_code,
                venue=current_venue_jp,
                race_no=int(current_race_no),
                race_name=current_race_name.strip(),
                distance=current_distance,
                surface=current_surface,
                field_size=current_field,
                grade="",
            )
        )
        current_race_name = ""
        current_surface = ""
        current_distance = None
        current_field = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip("\r\n")

        m_date = RE_DATE.search(line)
        if m_date:
            # Important: flush the previous race before switching context.
            flush()
            current_date_yyyymmdd = _to_yyyymmdd(m_date.group("y"), m_date.group("m"), m_date.group("d"))
            current_date_dash = _to_dash_date(m_date.group("y"), m_date.group("m"), m_date.group("d"))
            m_venue = RE_VENUE.search(line)
            if m_venue:
                current_venue_jp = m_venue.group("venue")
                current_venue_code = VENUE_JP_TO_CODE.get(current_venue_jp)
            continue

        m_race = RE_RACE_NO.match(line)
        if m_race:
            flush()
            current_race_no = _parse_int_token(m_race.group("no"))
            continue

        if current_date_yyyymmdd is None or current_venue_code is None or current_race_no is None:
            continue

        m_sdf = RE_SURFACE_DIST_FIELD.search(line)
        if m_sdf:
            current_surface = m_sdf.group("surface")
            current_distance = int(m_sdf.group("dist"))
            current_field = int(m_sdf.group("field"))
            current_race_name = line.strip()
            continue

    flush()
    return rows


def build_payouts_csv_from_target_text(*, input_txt: Path, output_csv: Path) -> dict:
    text = _read_text_auto(input_txt)
    rows = parse_target_payout_text(text=text)
    df = pd.DataFrame([r.__dict__ for r in rows], columns=["race_id", "bet_type", "winning_combination", "payout_yen"])
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    return {"rows": int(len(df)), "input_txt": str(input_txt), "output_csv": str(output_csv)}


def build_races_csv_from_target_text(*, input_txt: Path, output_csv: Path) -> dict:
    text = _read_text_auto(input_txt)
    rows = parse_target_races_text(text=text)
    df = pd.DataFrame([r.__dict__ for r in rows])
    if "race_id" in df.columns:
        df = df.drop_duplicates(subset=["race_id"]).reset_index(drop=True)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    return {"rows": int(len(df)), "input_txt": str(input_txt), "output_csv": str(output_csv)}


# Legacy API (kept for compatibility with existing CLI; no longer recommended)
def build_entries_results_from_target_text(  # pragma: no cover
    *,
    input_txt: Path,
    output_entries_csv: Path,
    output_results_csv: Path,
) -> dict:
    raise NotImplementedError("use build-entries-from-target-image + build-results-from-target-text instead")
