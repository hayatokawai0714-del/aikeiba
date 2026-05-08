# top3_calibration_method_compare

- generated_at: 2026-04-29T13:30:33
- feature_snapshot_version: fs_v1
- train_end_date: 2026-03-31
- valid_start_date: 2026-04-05
- valid_end_date: 2026-04-10
- race_meta_policy: skip (invalid races excluded in evaluation)
- invalid_race_count: 12

## Summary
| method | logloss | brier_score | mean(sum_p_top3) | gate_stop | gate_warn | eval_races |
|---|---:|---:|---:|---:|---:|---:|
| isotonic | 0.231143 | 0.062153 | 1.048857 | 1 | 35 | 48 |
| none | 0.320017 | 0.081911 | 2.850830 | 0 | 0 | 48 |
| sigmoid | 0.251256 | 0.064376 | 1.035541 | 0 | 45 | 48 |

## Calibration Curve (Validation)
| method | bin | count | prob_mean | actual_rate |
|---|---:|---:|---:|---:|
| isotonic | 0 | 71 | 0.004950 | 0.000000 |
| isotonic | 1 | 71 | 0.004950 | 0.000000 |
| isotonic | 2 | 71 | 0.035479 | 0.042254 |
| isotonic | 3 | 70 | 0.066879 | 0.114286 |
| isotonic | 4 | 71 | 0.066879 | 0.070423 |
| isotonic | 5 | 71 | 0.066879 | 0.084507 |
| isotonic | 6 | 70 | 0.066879 | 0.071429 |
| isotonic | 7 | 71 | 0.077580 | 0.014085 |
| isotonic | 8 | 71 | 0.157229 | 0.183099 |
| isotonic | 9 | 71 | 0.163265 | 0.112676 |
| none | 0 | 71 | 0.192170 | 0.000000 |
| none | 1 | 71 | 0.192379 | 0.000000 |
| none | 2 | 71 | 0.194437 | 0.042254 |
| none | 3 | 70 | 0.199002 | 0.100000 |
| none | 4 | 71 | 0.199424 | 0.070423 |
| none | 5 | 71 | 0.200120 | 0.112676 |
| none | 6 | 70 | 0.200347 | 0.028571 |
| none | 7 | 71 | 0.205703 | 0.028169 |
| none | 8 | 71 | 0.235176 | 0.225352 |
| none | 9 | 71 | 0.243910 | 0.084507 |
| sigmoid | 0 | 71 | 0.069814 | 0.000000 |
| sigmoid | 1 | 71 | 0.069820 | 0.000000 |
| sigmoid | 2 | 71 | 0.069877 | 0.042254 |
| sigmoid | 3 | 70 | 0.070003 | 0.100000 |
| sigmoid | 4 | 71 | 0.070014 | 0.070423 |
| sigmoid | 5 | 71 | 0.070034 | 0.112676 |
| sigmoid | 6 | 70 | 0.070040 | 0.028571 |
| sigmoid | 7 | 71 | 0.070189 | 0.028169 |
| sigmoid | 8 | 71 | 0.071011 | 0.225352 |
| sigmoid | 9 | 71 | 0.071255 | 0.084507 |

## Invalid Race IDs
- 20260405-HAN-10R
- 20260405-NAK-10R
- 20260405-NAK-11R
- 20260410-FUK-09R
- 20260410-FUK-10R
- 20260410-FUK-11R
- 20260410-HAN-09R
- 20260410-HAN-10R
- 20260410-HAN-11R
- 20260410-NAK-09R
- 20260410-NAK-10R
- 20260410-NAK-11R