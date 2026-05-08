# top3_calibration_diagnostics

- generated_at: 2026-04-29T13:26:44
- model_version: top3_stability_plus_pace_v3
- feature_snapshot_version: fs_v1
- date_range: 2026-04-12 .. 2026-04-12
- horse_rows: 526
- race_rows: 36
- sum_top3_extreme_races: 2
- race_meta_invalid_races: 9

## Raw vs Calibrated
- mean(p_top3_raw): 0.209992
- mean(p_top3_calibrated): 0.071470
- mean(raw-calibrated): 0.138522
- mean(sum_raw by race): 3.068217
- mean(sum_calibrated by race): 1.044251
- mean(sum_shrunk by race): 1.044251
- mean(sum_fused by race): 1.724642

## Extreme Race IDs
- 20260412-HAN-09R (2026-04-12): sum_shrunk=0.377346, field_size=0
- 20260412-NAK-09R (2026-04-12): sum_shrunk=0.372395, field_size=0

## Extreme vs Non-Extreme (Race-level means)
| metric | extreme_mean | non_extreme_mean |
|---|---:|---:|
| field_size | 0.000000 | 15.029412 |
| distance | nan | 1655.555556 |
| sum_p_top3_raw | 1.483827 | 3.161417 |
| sum_p_top3_calibrated | 0.374870 | 1.083627 |
| sum_p_top3_shrunk | 0.374870 | 1.083627 |
| sum_p_top3_fused | 1.277941 | 1.750919 |
| density_top3 | 0.929354 | 0.739659 |
| max_p_top3_fused | 0.418164 | 0.303560 |

## Race Meta Invalid vs Valid (Race-level means)
| metric | invalid_mean | valid_mean |
|---|---:|---:|
| field_size | 11.111111 | 15.222222 |
| distance | nan | 1655.555556 |
| sum_p_top3_raw | 2.509894 | 3.254325 |
| sum_p_top3_calibrated | 0.795173 | 1.127277 |
| sum_p_top3_shrunk | 0.795173 | 1.127277 |
| sum_p_top3_fused | 1.563368 | 1.778400 |
| density_top3 | 0.737167 | 0.754541 |
| max_p_top3_fused | 0.296368 | 0.314447 |

## Race Meta Invalid IDs
- 20260412-FUK-09R: distance_null
- 20260412-FUK-10R: field_size_expected_le_0,distance_null
- 20260412-FUK-11R: field_size_expected_le_0,distance_null
- 20260412-HAN-09R: field_size_expected_le_0,distance_null
- 20260412-HAN-10R: field_size_expected_le_0,distance_null
- 20260412-HAN-11R: field_size_expected_le_0,distance_null
- 20260412-NAK-09R: field_size_expected_le_0,distance_null
- 20260412-NAK-10R: field_size_expected_le_0,distance_null
- 20260412-NAK-11R: distance_null

## Calibration Curves (sample rows)
| group_type | group_value | bin | count | prob_mean | actual_rate |
|---|---|---:|---:|---:|---:|
| distance_bucket | UNKNOWN | 1 | 26 | 0.004950 | 0.000000 |
| distance_bucket | UNKNOWN | 3 | 19 | 0.004950 | 0.000000 |
| distance_bucket | UNKNOWN | 4 | 6 | 0.004950 | 0.000000 |
| distance_bucket | UNKNOWN | 5 | 21 | 0.049854 | 0.142857 |
| distance_bucket | UNKNOWN | 6 | 34 | 0.142857 | 0.117647 |
| distance_bucket | UNKNOWN | 7 | 7 | 0.142857 | 0.142857 |
| distance_bucket | long | 0 | 5 | 0.004950 | 0.000000 |
| distance_bucket | long | 1 | 1 | 0.004950 | 0.000000 |
| distance_bucket | long | 2 | 6 | 0.004950 | 0.000000 |
| distance_bucket | long | 5 | 2 | 0.142857 | 0.000000 |
| distance_bucket | long | 6 | 1 | 0.142857 | 1.000000 |
| distance_bucket | long | 7 | 7 | 0.163265 | 0.142857 |
| distance_bucket | long | 8 | 7 | 0.163265 | 0.285714 |
| distance_bucket | middle | 0 | 11 | 0.004950 | 0.000000 |
| distance_bucket | middle | 2 | 7 | 0.004950 | 0.000000 |
| distance_bucket | middle | 3 | 6 | 0.004950 | 0.000000 |
| distance_bucket | middle | 4 | 4 | 0.004950 | 0.000000 |
| distance_bucket | middle | 5 | 2 | 0.066879 | 0.000000 |
| distance_bucket | middle | 6 | 2 | 0.142857 | 0.500000 |
| distance_bucket | middle | 7 | 5 | 0.163265 | 0.400000 |
| distance_bucket | middle | 8 | 4 | 0.163265 | 0.250000 |
| distance_bucket | middle | 9 | 9 | 0.163265 | 0.111111 |
| distance_bucket | mile | 0 | 23 | 0.004950 | 0.000000 |
| distance_bucket | mile | 1 | 10 | 0.004950 | 0.000000 |
| distance_bucket | mile | 2 | 29 | 0.004950 | 0.000000 |
| distance_bucket | mile | 3 | 13 | 0.004950 | 0.000000 |
| distance_bucket | mile | 4 | 36 | 0.004950 | 0.000000 |
| distance_bucket | mile | 5 | 11 | 0.073786 | 0.090909 |
| distance_bucket | mile | 6 | 8 | 0.142857 | 0.250000 |
| distance_bucket | mile | 7 | 10 | 0.163265 | 0.100000 |
| distance_bucket | mile | 8 | 21 | 0.163265 | 0.238095 |
| distance_bucket | mile | 9 | 30 | 0.163265 | 0.133333 |
| distance_bucket | sprint | 0 | 14 | 0.004950 | 0.000000 |
| distance_bucket | sprint | 1 | 16 | 0.004950 | 0.000000 |
| distance_bucket | sprint | 2 | 10 | 0.004950 | 0.000000 |
| distance_bucket | sprint | 3 | 15 | 0.004950 | 0.000000 |
| distance_bucket | sprint | 4 | 6 | 0.004950 | 0.000000 |
| distance_bucket | sprint | 5 | 17 | 0.054787 | 0.000000 |
| distance_bucket | sprint | 6 | 7 | 0.142857 | 0.000000 |
| distance_bucket | sprint | 7 | 24 | 0.163265 | 0.250000 |
| distance_bucket | sprint | 8 | 20 | 0.163265 | 0.100000 |
| distance_bucket | sprint | 9 | 14 | 0.163265 | 0.071429 |
| field_size_bucket | large | 0 | 53 | 0.004950 | 0.000000 |
| field_size_bucket | large | 1 | 37 | 0.004950 | 0.000000 |
| field_size_bucket | large | 2 | 39 | 0.004950 | 0.000000 |
| field_size_bucket | large | 3 | 42 | 0.004950 | 0.000000 |
| field_size_bucket | large | 4 | 25 | 0.004950 | 0.000000 |
| field_size_bucket | large | 5 | 35 | 0.069689 | 0.057143 |
| field_size_bucket | large | 6 | 22 | 0.142857 | 0.090909 |
| field_size_bucket | large | 7 | 46 | 0.163265 | 0.217391 |
| field_size_bucket | large | 8 | 41 | 0.163265 | 0.195122 |
| field_size_bucket | large | 9 | 41 | 0.163265 | 0.146341 |
| field_size_bucket | medium | 2 | 13 | 0.004950 | 0.000000 |
| field_size_bucket | medium | 4 | 21 | 0.004950 | 0.000000 |
| field_size_bucket | medium | 5 | 2 | 0.066879 | 0.000000 |
| field_size_bucket | medium | 6 | 4 | 0.142857 | 0.500000 |
| field_size_bucket | medium | 8 | 11 | 0.163265 | 0.181818 |
| field_size_bucket | medium | 9 | 12 | 0.163265 | 0.000000 |
| field_size_bucket | small | 1 | 16 | 0.004950 | 0.000000 |
| field_size_bucket | small | 3 | 11 | 0.004950 | 0.000000 |
| field_size_bucket | small | 4 | 6 | 0.004950 | 0.000000 |
| field_size_bucket | small | 5 | 16 | 0.039785 | 0.125000 |
| field_size_bucket | small | 6 | 26 | 0.142857 | 0.153846 |
| field_size_bucket | small | 7 | 7 | 0.142857 | 0.142857 |
| popularity_bucket | UNKNOWN | 0 | 53 | 0.004950 | 0.000000 |
| popularity_bucket | UNKNOWN | 1 | 53 | 0.004950 | 0.000000 |
| popularity_bucket | UNKNOWN | 2 | 52 | 0.004950 | 0.000000 |
| popularity_bucket | UNKNOWN | 3 | 53 | 0.004950 | 0.000000 |
| popularity_bucket | UNKNOWN | 4 | 52 | 0.004950 | 0.000000 |
| popularity_bucket | UNKNOWN | 5 | 53 | 0.060555 | 0.075472 |
| popularity_bucket | UNKNOWN | 6 | 52 | 0.142857 | 0.153846 |
| popularity_bucket | UNKNOWN | 7 | 53 | 0.160570 | 0.207547 |
| popularity_bucket | UNKNOWN | 8 | 52 | 0.163265 | 0.192308 |
| popularity_bucket | UNKNOWN | 9 | 53 | 0.163265 | 0.113208 |
| venue | 3 | 0 | 53 | 0.004950 | 0.000000 |
| venue | 3 | 1 | 53 | 0.004950 | 0.000000 |
| venue | 3 | 2 | 1 | 0.004950 | 0.000000 |
| venue | 3 | 5 | 20 | 0.093471 | 0.150000 |
| venue | 3 | 6 | 15 | 0.142857 | 0.133333 |
| venue | 3 | 7 | 46 | 0.163265 | 0.217391 |

## Warnings
- pop_rank_all_missing