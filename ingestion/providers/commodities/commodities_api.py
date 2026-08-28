"""commodities-api.com - roughly 100 calls/month free, so it is budgeted hard."""
from __future__ import annotations

import os
from collections.abc import Sequence

from ... import config
from ...models import PriceTick, now_ms
from ..base import Provider, ProviderUnavailable

_URL = "https://commodities-api.com/api/latest"


class CommoditiesApiProvider(Provider[PriceTick]):
    name = "commodities_api"
    requires_key = True
    # 100 calls/month is one call every ~7.5 hours. Default to hourly with a
    # lifetime cap so a crash-restart loop can never burn the month's quota.
    min_interval_s = float(os.getenv("COMMODITIES_API_MIN_INTERVAL_S", "3600"))
    max_calls = int(os.getenv("COMMODITIES_API_MAX_CALLS", "60"))

    def is_configured(self) -> bool:
        return bool(config.COMMODITIES_API_KEY)

    def fetch(self, targets: Sequence[str]) -> list[PriceTick]:
        wanted = {
            config.COMMODITIES[s].provider_symbols[self.name]: s
            for s in targets
            if s in config.COMMODITIES and self.name in config.COMMODITIES[s].provider_symbols
        }
        if not wanted:
            return []

        resp = self.get(
            _URL,
            params={
                "access_key": config.COMMODITIES_API_KEY,
                "base": "USD",
                "symbols": ",".join(wanted),
            },
        )
        payload = resp.json()
        if not payload.get("success", False):
            raise ProviderUnavailable(f"commodities_api: {payload.get('error')}")
        rates = (payload.get("data") or payload).get("rates", {})
        source_ts = int(payload.get("timestamp") or 0) * 1000 or None

        observed = now_ms()
        ticks: list[PriceTick] = []
        for vendor_symbol, canonical in wanted.items():
            try:
                rate = float(rates.get(vendor_symbol))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                continue
            if rate <= 0:
                continue
            meta = config.COMMODITIES[canonical]
            ticks.append(
                PriceTick(
                    symbol=canonical,
                    name=meta.name,
                    asset_class=meta.asset_class,
                    # The API quotes units-per-USD; invert for the USD price.
                    price=round(1.0 / rate, 6),
                    currency=meta.currency,
                    unit=meta.unit,
                    source=self.name,
                    ts_event=observed,
                    ts_source=source_ts,
                )
            )
        return ticks
