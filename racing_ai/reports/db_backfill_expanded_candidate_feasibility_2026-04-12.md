# DB Backfill Expanded Candidate Feasibility

- generated_at: 2026-05-01T12:48:24
- race_date: 2026-04-12
- model_version: top3_stability_plus_pace_v3

## Table Coverage
| source | row_count |
|---|---:|
| races | 36 |
| entries | 526 |
| horse_predictions | 6140 |
| odds(place/place_max) | 2096 |
| results | 494 |
| payouts(wide) | 110 |

## Feasibility
- expanded_pool_from_db: YES
- market_proxy_from_db: YES
- actual_wide_hit_join: YES
- wide_payout_join: YES

## Missing / Risk
- Core backfill path is feasible for expanded shadow evaluation.