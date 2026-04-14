from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SnapshotMeta:
    feature_generated_at: str
    source_race_date_max: str
    feature_snapshot_version: str
    dataset_fingerprint: str
    odds_snapshot_version: str | None
