from __future__ import annotations

import hashlib
import json
from typing import Any


def stable_fingerprint(obj: Any) -> str:
    """
    Deterministic fingerprint for audit.
    Use for configs/metadata; for large datasets fingerprint should be table-level.
    """
    payload = json.dumps(obj, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
