# odds_type Audit

- generated_at: 2026-05-06T21:17:53
- db_path: racing_ai\data\warehouse\aikeiba.duckdb

Top rows:

```
odds_type  row_count  non_null_odds_value_count  horse_no_non_default_count  pair_horse_cols_non_default_count
 wide_max     347113                     347113                           0                             347113
     wide     347113                     347113                           0                             347113
   umaren     347113                     347113                           0                             347113
  bracket     119328                     119328                           0                             119328
      win      53179                      50694                       53179                              53179
place_max      51759                      51759                       51759                              51759
    place      51759                      51759                       51759                              51759
```

Notes:

- `horse_no_non_default_count` indicates single-horse odds rows (used for market proxy source inference).
- `pair_horse_cols_non_default_count` indicates pair odds rows (e.g., wide); not used for this inference.
