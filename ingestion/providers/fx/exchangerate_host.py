"""exchangerate.host - free tier, now key-gated. Secondary FX source."""
from __future__ import annotations

from collections.abc import Sequence

from ... import config
from ...models import FxRate, now_ms
from ..base import Provider, ProviderUnavailable
from .open_er_api import cross

_URL = "https://api.exchangerate.host/live"


class ExchangeRateHostProvider(Provider[FxRate]):
    name = "exchangerate_host"
    requires_key = True
    min_interval_s = 30.0

    def is_configured(self) -> bool:
        return bool(config.EXCHANGERATE_HOST_API_KEY)

    def fetch(self, targets: Sequence[str]) -> list[FxRate]:
        currencies = sorted(
            {c for pair in targets if pair in config.FX_PAIRS
             for c in (config.FX_PAIRS[pair].base, config.FX_PAIRS[pair].quote)} - {"USD"}
        )
        if not currencies:
            return []

        resp = self.get(
            _URL,
            params={
                "access_key": config.EXCHANGERATE_HOST_API_KEY,
                "source": "USD",
                "currencies": ",".join(currencies),
            },
        )
        payload = resp.json()
        if not payload.get("success", False):
            raise ProviderUnavailable(f"exchangerate_host: {payload.get('error')}")

        # Quotes come back as {"USDGHS": 11.9, ...} - normalise to {"GHS": 11.9}.
        rates = {k[3:]: float(v) for k, v in (payload.get("quotes") or {}).items() if len(k) == 6}
        source_ts = int(payload.get("timestamp") or 0) * 1000 or None

        observed = now_ms()
        out: list[FxRate] = []
        for pair in targets:
            meta = config.FX_PAIRS.get(pair)
            if not meta:
                continue
            rate = cross(rates, meta.base, meta.quote)
            if not rate:
                continue
            out.append(
                FxRate(
                    pair=pair,
                    base=meta.base,
                    quote=meta.quote,
                    rate=round(rate, 6),
                    source=self.name,
                    ts_event=observed,
                    ts_source=source_ts,
                )
            )
        return out
