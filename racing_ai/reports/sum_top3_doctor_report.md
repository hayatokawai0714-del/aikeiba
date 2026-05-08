# sum_top3_doctor_report

- generated_at: 2026-04-29T13:16:23
- race_date: 2026-04-12
- model_version: top3_stability_plus_pace_v3
- feature_snapshot_version: fs_v1
- ai_weight: 0.65
- race_count: 36
- stop_count: 2
- warn_count: 22

## Abnormal Race IDs
- 20260412-FUK-01R: warn (sum_top3_unusual) sum_shrunk=0.549202
- 20260412-FUK-02R: warn (sum_top3_unusual) sum_shrunk=0.932710
- 20260412-FUK-03R: warn (sum_top3_unusual) sum_shrunk=0.678009
- 20260412-FUK-05R: warn (sum_top3_unusual) sum_shrunk=0.870782
- 20260412-FUK-07R: warn (sum_top3_unusual) sum_shrunk=0.845423
- 20260412-FUK-08R: warn (sum_top3_unusual) sum_shrunk=1.146595
- 20260412-FUK-09R: warn (sum_top3_unusual) sum_shrunk=0.678713
- 20260412-FUK-10R: warn (sum_top3_unusual) sum_shrunk=1.106483
- 20260412-FUK-11R: warn (sum_top3_unusual) sum_shrunk=0.811669
- 20260412-HAN-03R: warn (sum_top3_unusual) sum_shrunk=1.035357
- 20260412-HAN-05R: warn (sum_top3_unusual) sum_shrunk=0.534350
- 20260412-HAN-07R: warn (sum_top3_unusual) sum_shrunk=0.846030
- 20260412-HAN-09R: stop (sum_top3_extreme) sum_shrunk=0.377346
- 20260412-HAN-10R: warn (sum_top3_unusual) sum_shrunk=1.015653
- 20260412-HAN-11R: warn (sum_top3_unusual) sum_shrunk=1.014851
- 20260412-NAK-01R: warn (sum_top3_unusual) sum_shrunk=0.795508
- 20260412-NAK-02R: warn (sum_top3_unusual) sum_shrunk=1.167003
- 20260412-NAK-03R: warn (sum_top3_unusual) sum_shrunk=0.897451
- 20260412-NAK-05R: warn (sum_top3_unusual) sum_shrunk=0.595673
- 20260412-NAK-06R: warn (sum_top3_unusual) sum_shrunk=0.784297
- 20260412-NAK-08R: warn (sum_top3_unusual) sum_shrunk=1.091025
- 20260412-NAK-09R: stop (sum_top3_extreme) sum_shrunk=0.372395
- 20260412-NAK-10R: warn (sum_top3_unusual) sum_shrunk=0.815818
- 20260412-NAK-11R: warn (sum_top3_unusual) sum_shrunk=0.963626

## Shrink Improvement Summary
- calibrated_sum_mean: 1.044251
- shrunk_sum_mean: 1.044251
- mean_delta(shrunk-calibrated): 0.000000

## Race Table
| race_id | sum_p_top3_raw | sum_p_top3_calibrated | sum_p_top3_shrunk | sum_p_top3_fused | field_size | max_p_top3_fused | density_top3 | doctor_status | doctor_reason |
|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| 20260412-FUK-01R | 3.034450 | 0.549202 | 0.549202 | 1.406981 | 15 | 0.318199 | 0.708682 | warn | sum_top3_unusual |
| 20260412-FUK-02R | 3.336274 | 0.932710 | 0.932710 | 1.656262 | 16 | 0.241687 | 0.673808 | warn | sum_top3_unusual |
| 20260412-FUK-03R | 3.240952 | 0.678009 | 0.678009 | 1.490706 | 16 | 0.294979 | 0.646700 | warn | sum_top3_unusual |
| 20260412-FUK-04R | 3.575402 | 1.765805 | 1.765805 | 2.197773 | 16 | 0.326041 | 0.813415 | pass | ok |
| 20260412-FUK-05R | 3.332566 | 0.870782 | 0.870782 | 1.582832 | 16 | 0.353218 | 0.769791 | warn | sum_top3_unusual |
| 20260412-FUK-06R | 3.309251 | 1.458274 | 1.458274 | 1.997878 | 15 | 0.306589 | 0.816151 | pass | ok |
| 20260412-FUK-07R | 3.117174 | 0.845423 | 0.845423 | 1.599525 | 15 | 0.272598 | 0.731418 | warn | sum_top3_unusual |
| 20260412-FUK-08R | 3.384063 | 1.146595 | 1.146595 | 1.795287 | 16 | 0.279245 | 0.732255 | warn | sum_top3_unusual |
| 20260412-FUK-09R | 3.148416 | 0.678713 | 0.678713 | 1.491164 | 16 | 0.209639 | 0.573400 | warn | sum_top3_unusual |
| 20260412-FUK-10R | 3.212772 | 1.106483 | 1.106483 | 1.769214 | 16 | 0.254940 | 0.627592 | warn | sum_top3_unusual |
| 20260412-FUK-11R | 2.970128 | 0.811669 | 0.811669 | 1.577585 | 15 | 0.211072 | 0.586216 | warn | sum_top3_unusual |
| 20260412-FUK-12R | 3.397220 | 1.208524 | 1.208524 | 1.835540 | 16 | 0.247733 | 0.613882 | pass | ok |
| 20260412-HAN-01R | 3.467505 | 1.387247 | 1.387247 | 1.951710 | 16 | 0.331351 | 0.825220 | pass | ok |
| 20260412-HAN-02R | 3.517659 | 1.483633 | 1.483633 | 2.014361 | 16 | 0.277324 | 0.756251 | pass | ok |
| 20260412-HAN-03R | 2.769656 | 1.035357 | 1.035357 | 1.653276 | 13 | 0.456122 | 0.924498 | warn | sum_top3_unusual |
| 20260412-HAN-04R | 3.611570 | 1.841783 | 1.841783 | 2.248892 | 16 | 0.348489 | 0.875205 | pass | ok |
| 20260412-HAN-05R | 2.457314 | 0.534350 | 0.534350 | 1.397328 | 12 | 0.334137 | 0.640457 | warn | sum_top3_unusual |
| 20260412-HAN-06R | 3.215655 | 1.265502 | 1.265502 | 1.872576 | 15 | 0.249122 | 0.689949 | pass | ok |
| 20260412-HAN-07R | 2.370208 | 0.846030 | 0.846030 | 1.599919 | 11 | 0.287711 | 0.808429 | warn | sum_top3_unusual |
| 20260412-HAN-08R | 3.549552 | 1.724285 | 1.724285 | 2.170785 | 16 | 0.397094 | 0.847246 | pass | ok |
| 20260412-HAN-09R | 1.579600 | 0.377346 | 0.377346 | 1.268611 | 8 | 0.393471 | 0.905486 | stop | sum_top3_extreme |
| 20260412-HAN-10R | 2.621949 | 1.015653 | 1.015653 | 1.710175 | 13 | 0.303286 | 0.728634 | warn | sum_top3_unusual |
| 20260412-HAN-11R | 2.066730 | 1.014851 | 1.014851 | 1.709653 | 10 | 0.288176 | 0.803046 | warn | sum_top3_unusual |
| 20260412-HAN-12R | 3.524943 | 1.504041 | 1.504041 | 2.027627 | 16 | 0.279641 | 0.785958 | pass | ok |
| 20260412-NAK-01R | 3.240810 | 0.795508 | 0.795508 | 1.567080 | 16 | 0.325114 | 0.697445 | warn | sum_top3_unusual |
| 20260412-NAK-02R | 3.414423 | 1.167003 | 1.167003 | 1.808552 | 16 | 0.356435 | 0.846119 | warn | sum_top3_unusual |
| 20260412-NAK-03R | 2.747002 | 0.897451 | 0.897451 | 1.633343 | 13 | 0.413891 | 0.794795 | warn | sum_top3_unusual |
| 20260412-NAK-04R | 3.084281 | 1.315417 | 1.315417 | 1.905021 | 14 | 0.307483 | 0.874058 | pass | ok |
| 20260412-NAK-05R | 3.216470 | 0.595673 | 0.595673 | 1.438920 | 16 | 0.239651 | 0.633907 | warn | sum_top3_unusual |
| 20260412-NAK-06R | 3.670035 | 0.784297 | 0.784297 | 1.559793 | 18 | 0.294444 | 0.734973 | warn | sum_top3_unusual |
| 20260412-NAK-07R | 3.498793 | 1.463225 | 1.463225 | 1.983603 | 16 | 0.456122 | 0.869791 | pass | ok |
| 20260412-NAK-08R | 3.379066 | 1.091025 | 1.091025 | 1.759166 | 16 | 0.213862 | 0.595164 | warn | sum_top3_unusual |
| 20260412-NAK-09R | 1.388055 | 0.372395 | 0.372395 | 1.287272 | 7 | 0.442857 | 0.953221 | stop | sum_top3_extreme |
| 20260412-NAK-10R | 2.599602 | 0.815818 | 0.815818 | 1.580282 | 13 | 0.286086 | 0.713229 | warn | sum_top3_unusual |
| 20260412-NAK-11R | 3.001792 | 0.963626 | 0.963626 | 1.676357 | 15 | 0.277780 | 0.743677 | warn | sum_top3_unusual |
| 20260412-NAK-12R | 3.404481 | 1.249340 | 1.249340 | 1.862071 | 16 | 0.281794 | 0.667050 | pass | ok |