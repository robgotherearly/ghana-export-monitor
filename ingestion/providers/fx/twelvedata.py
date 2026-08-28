"""Twelve Data FX - intraday quotes when a key is configured."""
from __future__ import annotations

from collections.abc import Sequence

from ... import config
from ...models import FxRate, now_ms
from ..base import Provider, ProviderUnavailable

_URL = "https://api.twelvedata.com/price"


class TwelveDataFxProvider(Provider[FxRate]):
    name = "twelvedata"
    requires_key = True
    min_interval_s = 8.0

    def is_configured(self) -> bool:
        return bool(config.TWELVEDATA_API_KEY)

    def fetch(self, targets: Sequence[str]) -> list[FxRate]:
        pairs = [p for p in targets if p in config.FX_PAIRS]
        if not pairs:
            return []

        resp = self.get(
            _URL, params={"symbol": ",".join(pairs), "apikey": config.TWELVEDATA_API_KEY}
        )
        payload = resp.json()
        if isinstance(payload, dict) and payload.get("status") == "error":
            raise ProviderUnavailable(f"twelvedata: {payload.get('message')}")

        observed = now_ms()
        out: list[FxRate] = []
        for pair in pairs:
            node = payload.get(pair) if len(pairs) > 1 else payload
            if not isinstance(node, dict):
                continue
            try:
                rate = float(node["price"])
            except (KeyError, TypeError, ValueError):
                continue
            meta = config.FX_PAIRS[pair]
            out.append(
                FxRate(
                    pair=pair,
                    base=meta.base,
                    quote=meta.quote,
                    rate=round(rate, 6),
                    source=self.name,
                    ts_event=observed,
                )
            )
        return out
