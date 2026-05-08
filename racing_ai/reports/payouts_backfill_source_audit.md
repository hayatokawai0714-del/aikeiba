# payouts_backfill_source_audit

               source_name  exists  row_count  wide_row_count        available_dates                          key_columns payout_amount_column  can_backfill_wide_payout                                                       notes
                db.payouts    True      68530          9939.0 2025-04-20..2026-04-26             race_id,bet_type,bet_key               payout                      True        Primary source. Contains mixed meta/system rows too.
           raw.payouts.csv    True        274             NaN 2025-04-20..2026-04-28 race_id,bet_type,winning_combination           payout_yen                      True Needs per-file validation (some exports include meta rows).
external.csv(wide/payout*)    True        131             NaN                                                      varies               varies                      True   Potential backfill source; inspect schema before loading.