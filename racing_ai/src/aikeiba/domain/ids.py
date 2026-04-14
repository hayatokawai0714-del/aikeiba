from __future__ import annotations

from dataclasses import dataclass

VENUE_CODE_MAP = {
    "中山": "NAK",
    "阪神": "HAN",
    "福島": "FUK",
    "東京": "TOK",
    "京都": "KYO",
    "中京": "CHU",
    "新潟": "NII",
    "小倉": "KOK",
    "札幌": "SAP",
    "函館": "HAK",
}


@dataclass(frozen=True)
class RaceKey:
    race_date_yyyymmdd: str  # "YYYYMMDD"
    venue_code: str          # e.g. "NAK"
    race_no: int             # 1-12

    def to_race_id(self) -> str:
        # Stable, readable, join-friendly.
        return f"{self.race_date_yyyymmdd}-{self.venue_code}-{self.race_no:02d}R"


def parse_race_id(race_id: str) -> RaceKey:
    # Expected: YYYYMMDD-XXX-11R
    date, venue, r = race_id.split("-")
    race_no = int(r.replace("R", ""))
    return RaceKey(race_date_yyyymmdd=date, venue_code=venue, race_no=race_no)


def normalize_pair(a: int, b: int) -> str:
    x, y = sorted((int(a), int(b)))
    return f"{x}-{y}"


def normalize_venue_code(venue: str) -> str:
    v = str(venue).strip()
    if v in VENUE_CODE_MAP:
        return VENUE_CODE_MAP[v]
    return v.upper()


def build_race_id_from_parts(race_date_yyyymmdd: str, venue: str, race_no: int) -> str:
    return RaceKey(race_date_yyyymmdd=race_date_yyyymmdd, venue_code=normalize_venue_code(venue), race_no=int(race_no)).to_race_id()
