from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class DuckDb:
    con: duckdb.DuckDBPyConnection

    @staticmethod
    def connect(db_path: Path) -> "DuckDb":
        con = duckdb.connect(str(db_path))
        # Determinism / reproducibility knobs (keep minimal).
        con.execute("PRAGMA threads=1")
        return DuckDb(con=con)

    def execute(self, sql: str, params: tuple | None = None):
        if params is None:
            return self.con.execute(sql)
        return self.con.execute(sql, params)

    def query_df(self, sql: str, params: tuple | None = None):
        if params is None:
            return self.con.execute(sql).df()
        return self.con.execute(sql, params).df()
