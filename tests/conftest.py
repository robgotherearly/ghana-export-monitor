"""Test setup.

The last-known-price store picks its path up at import time, so it has to be
redirected before anything from `ingestion` is imported.
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

os.environ.setdefault(
    "LAST_KNOWN_STATE_PATH",
    str(Path(tempfile.gettempdir()) / "gdep_test_last_known.json"),
)
