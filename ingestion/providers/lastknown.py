"""Tiny shared store of the last genuinely-real observation per key.

The simulated fallback walks from here rather than from a hard-coded constant,
so a gap in a free tier degrades into "plausible drift from the last real
print" instead of a cliff in the chart.
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path

_STATE_PATH = Path(os.getenv("LAST_KNOWN_STATE_PATH", "/tmp/gdep_last_known.json"))


class LastKnownStore:
    def __init__(self, path: Path = _STATE_PATH) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._values: dict[str, float] = {}
        self._load()

    def _load(self) -> None:
        try:
            self._values = {k: float(v) for k, v in json.loads(self._path.read_text()).items()}
        except Exception:
            self._values = {}

    def get(self, key: str, default: float) -> float:
        return self._values.get(key, default)

    def set(self, key: str, value: float) -> None:
        with self._lock:
            self._values[key] = float(value)
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                self._path.write_text(json.dumps(self._values))
            except OSError:
                pass  # best effort; the in-memory value still works for this process


STORE = LastKnownStore()
