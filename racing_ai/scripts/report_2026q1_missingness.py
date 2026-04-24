from __future__ import annotations

import argparse
from dataclasses import dataclass

import duckdb
import pandas as pd


@dataclass(frozen=True)
class Metric:
    name: str
    sql: str


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="data/warehouse/aikeiba.duckdb")
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default="2026-03-31")
    args = ap.parse_args()

    start_ymd = args.start.replace("-", "")
    end_ymd = args.end.replace("-", "")

    con = duckdb.connect(args.db_path)

    metrics: list[Metric] = [
        Metric(
            "entries_jockey_name_raw",
            f"""
            select
              count(*) as n_rows,
              sum(case when jockey_name_raw is not null and cast(jockey_name_raw as varchar)<>'' then 1 else 0 end) as nn,
              round(100.0 * (1 - (sum(case when jockey_name_raw is not null and cast(jockey_name_raw as varchar)<>'' then 1 else 0 end) / nullif(count(*),0))), 4) as miss_pct
            from entries
            where substr(cast(race_id as varchar),1,8) between '{start_ymd}' and '{end_ymd}'
            """,
        ),
        Metric(
            "entries_pop_rank",
            f"""
            select
              count(*) as n_rows,
              sum(case when pop_rank is not null then 1 else 0 end) as nn,
              round(100.0 * (1 - (sum(case when pop_rank is not null then 1 else 0 end) / nullif(count(*),0))), 4) as miss_pct
            from entries
            where substr(cast(race_id as varchar),1,8) between '{start_ymd}' and '{end_ymd}'
            """,
        ),
        Metric(
            "entries_weight_carried",
            f"""
            select
              count(*) as n_rows,
              sum(case when weight_carried is not null then 1 else 0 end) as nn,
              round(100.0 * (1 - (sum(case when weight_carried is not null then 1 else 0 end) / nullif(count(*),0))), 4) as miss_pct
            from entries
            where substr(cast(race_id as varchar),1,8) between '{start_ymd}' and '{end_ymd}'
            """,
        ),
        Metric(
            "races_track_condition",
            f"""
            select
              count(*) as n_rows,
              sum(case when track_condition is not null and cast(track_condition as varchar)<>'' then 1 else 0 end) as nn,
              round(100.0 * (1 - (sum(case when track_condition is not null and cast(track_condition as varchar)<>'' then 1 else 0 end) / nullif(count(*),0))), 4) as miss_pct
            from races
            where substr(cast(race_id as varchar),1,8) between '{start_ymd}' and '{end_ymd}'
            """,
        ),
        Metric(
            "results_target_finish",
            f"""
            select
              count(*) as n_rows,
              sum(case when finish_position is not null then 1 else 0 end) as nn,
              round(100.0 * (1 - (sum(case when finish_position is not null then 1 else 0 end) / nullif(count(*),0))), 4) as miss_pct
            from results
            where substr(cast(race_id as varchar),1,8) between '{start_ymd}' and '{end_ymd}'
            """,
        ),
        Metric(
            "results_pop_rank",
            f"""
            select
              count(*) as n_rows,
              sum(case when pop_rank is not null then 1 else 0 end) as nn,
              round(100.0 * (1 - (sum(case when pop_rank is not null then 1 else 0 end) / nullif(count(*),0))), 4) as miss_pct
            from results
            where substr(cast(race_id as varchar),1,8) between '{start_ymd}' and '{end_ymd}'
            """,
        ),
        Metric(
            "odds_win",
            f"""
            select
              count(*) as n_rows,
              sum(case when odds_value is not null then 1 else 0 end) as nn,
              round(100.0 * (1 - (sum(case when odds_value is not null then 1 else 0 end) / nullif(count(*),0))), 4) as miss_pct
            from odds
            where odds_type='win'
              and substr(cast(race_id as varchar),1,8) between '{start_ymd}' and '{end_ymd}'
            """,
        ),
    ]

    rows = []
    for m in metrics:
        df = con.execute(m.sql).fetchdf()
        r = df.iloc[0].to_dict()
        r["metric"] = m.name
        rows.append(r)

    out = pd.DataFrame(rows)[["metric", "n_rows", "nn", "miss_pct"]]
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()

