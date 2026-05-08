# top3_post_correction_compare

- generated_at: 2026-04-29T13:33:28
- base_calibration_method: isotonic
- feature_snapshot_version: fs_v1
- valid_period: 2026-04-05..2026-04-10
- race_meta_policy: skip
- invalid_race_count: 12
- clip_max: 0.95

## Summary
| method | logloss | brier_score | mean(sum_p_top3) | gate_stop | gate_warn | eval_races |
|---|---:|---:|---:|---:|---:|---:|
| current_shrink_only | 0.231143 | 0.062153 | 1.048857 | 1 | 35 | 48 |
| none | 0.231143 | 0.062153 | 1.048857 | 1 | 35 | 48 |
| scale_to_3 | 0.316579 | 0.093124 | 2.986704 | 0 | 0 | 48 |
| scale_to_expected_top3_clip | 0.316579 | 0.093124 | 2.986704 | 0 | 0 | 48 |

## Hit Rate by Decile
| method | decile | count | prob_mean | hit_rate |
|---|---:|---:|---:|---:|
| current_shrink_only | 0 | 71 | 0.004950 | 0.000000 |
| current_shrink_only | 1 | 71 | 0.004950 | 0.000000 |
| current_shrink_only | 2 | 71 | 0.035479 | 0.042254 |
| current_shrink_only | 3 | 70 | 0.066879 | 0.114286 |
| current_shrink_only | 4 | 71 | 0.066879 | 0.070423 |
| current_shrink_only | 5 | 71 | 0.066879 | 0.084507 |
| current_shrink_only | 6 | 70 | 0.066879 | 0.071429 |
| current_shrink_only | 7 | 71 | 0.077580 | 0.014085 |
| current_shrink_only | 8 | 71 | 0.157229 | 0.183099 |
| current_shrink_only | 9 | 71 | 0.163265 | 0.112676 |
| none | 0 | 71 | 0.004950 | 0.000000 |
| none | 1 | 71 | 0.004950 | 0.000000 |
| none | 2 | 71 | 0.035479 | 0.042254 |
| none | 3 | 70 | 0.066879 | 0.114286 |
| none | 4 | 71 | 0.066879 | 0.070423 |
| none | 5 | 71 | 0.066879 | 0.084507 |
| none | 6 | 70 | 0.066879 | 0.071429 |
| none | 7 | 71 | 0.077580 | 0.014085 |
| none | 8 | 71 | 0.157229 | 0.183099 |
| none | 9 | 71 | 0.163265 | 0.112676 |
| scale_to_3 | 0 | 71 | 0.011629 | 0.000000 |
| scale_to_3 | 1 | 71 | 0.017983 | 0.014085 |
| scale_to_3 | 2 | 71 | 0.089992 | 0.042254 |
| scale_to_3 | 3 | 70 | 0.167793 | 0.042857 |
| scale_to_3 | 4 | 71 | 0.184114 | 0.084507 |
| scale_to_3 | 5 | 71 | 0.187500 | 0.084507 |
| scale_to_3 | 6 | 70 | 0.194855 | 0.057143 |
| scale_to_3 | 7 | 71 | 0.233287 | 0.070423 |
| scale_to_3 | 8 | 71 | 0.361244 | 0.140845 |
| scale_to_3 | 9 | 71 | 0.575891 | 0.154930 |
| scale_to_expected_top3_clip | 0 | 71 | 0.011629 | 0.000000 |
| scale_to_expected_top3_clip | 1 | 71 | 0.017983 | 0.014085 |
| scale_to_expected_top3_clip | 2 | 71 | 0.089992 | 0.042254 |
| scale_to_expected_top3_clip | 3 | 70 | 0.167793 | 0.042857 |
| scale_to_expected_top3_clip | 4 | 71 | 0.184114 | 0.084507 |
| scale_to_expected_top3_clip | 5 | 71 | 0.187500 | 0.084507 |
| scale_to_expected_top3_clip | 6 | 70 | 0.194855 | 0.057143 |
| scale_to_expected_top3_clip | 7 | 71 | 0.233287 | 0.070423 |
| scale_to_expected_top3_clip | 8 | 71 | 0.361244 | 0.140845 |
| scale_to_expected_top3_clip | 9 | 71 | 0.575891 | 0.154930 |