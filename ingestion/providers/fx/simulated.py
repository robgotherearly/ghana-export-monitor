"""Simulated FX fallback - random walk from the last real cedi print.

Free FX sources that carry GHS refresh daily, which is fine for a trend but
gives a streaming demo nothing to chew on between refreshes. When the live
sources go quiet this keeps the cedi panel moving, always tagged as simulated.
"""
from __future__ import annotations

import random
from collections.abc import Sequence

from ... import config
from ...models import FxRate, now_ms
from ..base import Provider
from ..commodities.simulated import walk
from ..lastknown import STORE


class SimulatedFxProvider(Provider[FxRate]):
    name = "simulated"
    min_interval_s = 0.0

    def __init__(self) -> None:
        super().__init__()
        self._rng = random.Random()

    def is_configured(self) -> bool:
        return config.ALLOW_SIMULATED_FALLBACK

    def fetch(self, targets: Sequence[str]) -> list[FxRate]:
        observed = now_ms()
        out: list[FxRate] = []
        for pair in targets:
            meta = config.FX_PAIRS.get(pair)
            if not meta:
                continue
            key = f"fx:{pair}"
            last = STORE.get(key, meta.anchor_rate)
            rate = walk(last, meta.anchor_rate, meta.daily_vol, self._rng)
            STORE.set(key, rate)
            out.append(
                FxRate(
                    pair=pair,
                    base=meta.base,
                    quote=meta.quote,
                    rate=rate,
                    source=self.name,
                    ts_event=observed,
                    is_simulated=True,
                )
            )
        return out
