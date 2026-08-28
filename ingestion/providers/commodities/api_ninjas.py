"""API Ninjas commodity price - simple free key, one call per symbol."""
from __future__ import annotations

from collections.abc import Sequence

from ... import config
from ...models import PriceTick, now_ms
from ..base import Provider

_URL = "https://api.api-ninjas.com/v1/commodityprice"


class ApiNinjasProvider(Provider[PriceTick]):
    name = "api_ninjas"
    requires_key = True
    min_interval_s = 30.0

    def is_configured(self) -> bool:
        return bool(config.API_NINJAS_API_KEY)

    def fetch(self, targets: Sequence[str]) -> list[PriceTick]:
        headers = {"X-Api-Key": config.API_NINJAS_API_KEY}
        observed = now_ms()
        ticks: list[PriceTick] = []
        for canonical in targets:
            meta = config.COMMODITIES.get(canonical)
            if not meta or self.name not in meta.provider_symbols:
                continue
            resp = self.get(_URL, params={"name": meta.provider_symbols[self.name]}, headers=headers)
            payload = resp.json()
            try:
                price = float(payload["price"])
            except (KeyError, TypeError, ValueError):
                continue
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
                    ts_source=int(payload.get("updated") or 0) * 1000 or None,
                )
            )
        return ticks
