from __future__ import annotations

from pathlib import Path


def discover_experiments(
    *,
    models_root: Path,
    task: str = "top3",
) -> list[dict[str, Path | str]]:
    """
    Discover comparable experiments under models_root/<task>/.
    Assumption: experiment directory name is the experiment_name/model_version.
    """
    base = models_root / task
    if not base.exists() or not base.is_dir():
        return []

    experiments: list[dict[str, Path | str]] = []
    for child in sorted(base.iterdir(), key=lambda p: p.name):
        if not child.is_dir():
            continue
        # minimum requirement for compare-experiments
        if not (child / "model_metrics.json").exists():
            continue
        experiments.append(
            {
                "experiment_name": child.name,
                "model_dir": child,
            }
        )
    return experiments
