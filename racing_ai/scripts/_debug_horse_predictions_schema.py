from __future__ import annotations

import duckdb


def main() -> None:
    con = duckdb.connect("racing_ai/data/warehouse/aikeiba.duckdb", read_only=True)
    cols = con.execute("PRAGMA table_info('horse_predictions')").fetchdf()
    print(cols[["name", "type"]].to_string(index=False))
    sample = con.execute("select * from horse_predictions limit 3").fetchdf()
    print("sample col count", len(sample.columns))
    print(sample.to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()

