"""Twelve Data - 800 req/day, 8 req/min on the free tier. Needs a key."""
from __future__ import annotations

from collections.abc import Sequence

from ... import config
from ...models import PriceTick, now_ms
from ..base import Provider, ProviderUnavailable

_URL = "https://api.twelvedata.com/price"


class TwelveDataCommodityProvider(Provider[PriceTick]):
    name = "twelvedata"
    requires_key = True
    min_interval_s = 8.0  # one batched call per cycle keeps us inside 8 req/min

    def is_configured(self) -> bool:
        return bool(config.TWELVEDATA_API_KEY)

    def fetch(self, targets: Sequence[str]) -> list[PriceTick]:
        wanted = {
            s: config.COMMODITIES[s].provider_symbols[self.name]
            for s in targets
            if s in config.COMMODITIES and self.name in config.COMMODITIES[s].provider_symbols
        }
        if not wanted:
            return []

        resp = self.get(
            _URL,
            params={"symbol": ",".join(wanted.values()), "apikey": config.TWELVEDATA_API_KEY},
        )
        payload = resp.json()
        if isinstance(payload, dict) and payload.get("status") == "error":
            raise ProviderUnavailable(f"twelvedata: {payload.get('message')}")

        observed = now_ms()
        ticks: list[PriceTick] = []
        for canonical, vendor_symbol in wanted.items():
            # A single-symbol request returns the bare object, a batch returns a map.
            node = payload.get(vendor_symbol) if len(wanted) > 1 else payload
            if not isinstance(node, dict):
                continue
            try:
                price = float(node["price"])
            except (KeyError, TypeError, ValueError):
                continue
            meta = config.COMMODITIES[canonical]
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
                )
            )
        return ticks
