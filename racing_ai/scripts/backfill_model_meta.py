from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path


def backfill(models_root: Path) -> dict:
    changed = 0
    scanned = 0
    for p in models_root.glob("top3/*/meta.json"):
        scanned += 1
        meta = json.loads(p.read_text(encoding="utf-8"))
        changed_flag = False
        defaults = {
            "model_version": meta.get("model_version"),
            "feature_set_version": meta.get("feature_snapshot_version"),
            "train_start_date": None,
            "train_end_date": meta.get("train_end_date"),
            "calibration_start_date": meta.get("valid_start_date"),
            "calibration_end_date": meta.get("valid_end_date"),
            "validation_start_date": meta.get("valid_start_date"),
            "validation_end_date": meta.get("valid_end_date"),
            "model_created_at": meta.get("created_at") or dt.datetime.now().isoformat(timespec="seconds"),
            "target": "is_top3",
            "objective": "binary",
            "calibration_method": None,
            "source_table": "feature_store + results",
            "row_count_train": None,
            "row_count_calibration": None,
            "row_count_validation": None,
        }
        for k, v in defaults.items():
            if k not in meta:
                meta[k] = v
                changed_flag = True
        if changed_flag:
            p.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            changed += 1
    return {"scanned": scanned, "changed": changed}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--models-root", default="racing_ai/data/models_compare")
    args = ap.parse_args()
    out = backfill(Path(args.models_root))
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

