# Market Proxy Fallback Usage Audit

- generated_at: 2026-05-06T21:17:53
- input: racing_ai\reports\2024_eval_full_v5\pair_shadow_pair_comparison_expanded_20240106_20241228_with_results_external_priority.csv
- total_pair_rows: 149424

## Source counts (pair rows)

```json
{
  "predictions_scaled_low_confidence": 149424
}
```

## Score distribution by source

```json
{
  "predictions_scaled_low_confidence": {
    "n": 149424.0,
    "p50": 0.0463464604322446,
    "p90": 0.0463464604322446,
    "p99": 0.0520082457883097,
    "std": 0.005176043181412673
  }
}
```

## model_dynamic performance by source (selected rows only; ROI proxy)

```json
{
  "predictions_scaled_low_confidence": {
    "selected": 136,
    "roi_proxy": 0.7544117647058823
  }
}
```

## Notes

- `predictions_scaled_low_confidence` is NOT odds-derived; interpret ROI comparisons cautiously when it dominates.
