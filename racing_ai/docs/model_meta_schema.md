# model meta schema

`data/models*/top3/<model_version>/meta.json` に以下を保存します。

- model_version
- feature_set_version
- train_start_date
- train_end_date
- calibration_start_date
- calibration_end_date
- validation_start_date
- validation_end_date
- model_created_at
- target
- objective
- calibration_method
- source_table
- row_count_train
- row_count_calibration
- row_count_validation

不明値は `null` を許容し、リークガード側で warning として扱います。

