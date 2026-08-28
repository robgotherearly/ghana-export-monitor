"""Last-resort fallback: a mean-reverting random walk from the last real print.

This exists so a dead free tier degrades the demo instead of flat-lining it. It
is deliberately the last link in every chain, every event it emits carries
`source="simulated"` and `is_simulated=true`, and the whole provider can be
switched off with ALLOW_SIMULATED_FALLBACK=false.
"""
from __future__ import annotations

import math
import random
from collections.abc import Sequence

from ... import config
from ...models import PriceTick, now_ms
from ..base import Provider
from ..lastknown import STORE

#: how many polls make up a trading day, used to scale daily vol down to a tick
_BARS_PER_DAY = max(1.0, 86_400.0 / max(1, config.COMMODITY_POLL_SECONDS))
#: pull back towards the anchor so a long run cannot drift to nonsense
_REVERSION = 0.02


def walk(last: float, anchor: float, daily_vol: float, rng: random.Random) -> float:
    """One step of an Ornstein-Uhlenbeck-flavoured geometric walk."""
    sigma = daily_vol / math.sqrt(_BARS_PER_DAY)
    shock = rng.gauss(0.0, sigma)
    drift = _REVERSION * math.log(anchor / last) if last > 0 else 0.0
    return round(max(0.01, last * math.exp(drift + shock)), 4)


class SimulatedCommodityProvider(Provider[PriceTick]):
    name = "simulated"
    min_interval_s = 0.0

    def __init__(self) -> None:
        super().__init__()
        self._rng = random.Random()

    def is_configured(self) -> bool:
        return config.ALLOW_SIMULATED_FALLBACK

    def fetch(self, targets: Sequence[str]) -> list[PriceTick]:
        observed = now_ms()
        ticks: list[PriceTick] = []
        for canonical in targets:
            meta = config.COMMODITIES.get(canonical)
            if not meta:
                continue
            key = f"commodity:{canonical}"
            last = STORE.get(key, meta.anchor_price)
            price = walk(last, meta.anchor_price, meta.daily_vol, self._rng)
            STORE.set(key, price)
            ticks.append(
                PriceTick(
                    symbol=canonical,
                    name=meta.name,
                    asset_class=meta.asset_class,
                    price=price,
                    currency=meta.currency,
                    unit=meta.unit,
                    source=self.name,
                    ts_event=observed,
                    is_simulated=True,
                )
            )
        return ticks
