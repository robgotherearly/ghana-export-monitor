"""open.er-api.com - free, no key, and it actually carries GHS.

One call returns the whole USD table, so every configured pair (including the
EUR/GHS and GBP/GHS crosses) is derived from a single request.
"""
from __future__ import annotations

from collections.abc import Sequence

from ... import config
from ...models import FxRate, now_ms
from ..base import Provider, ProviderUnavailable

_URL = "https://open.er-api.com/v6/latest/USD"


def cross(rates: dict[str, float], base: str, quote: str) -> float | None:
    """base/quote from a USD-based table: (USD/quote) / (USD/base)."""
    per_usd_quote = 1.0 if quote == "USD" else rates.get(quote)
    per_usd_base = 1.0 if base == "USD" else rates.get(base)
    if not per_usd_quote or not per_usd_base:
        return None
    return per_usd_quote / per_usd_base


class OpenErApiProvider(Provider[FxRate]):
    name = "open_er_api"
    min_interval_s = 15.0

    def fetch(self, targets: Sequence[str]) -> list[FxRate]:
        resp = self.get(_URL)
        payload = resp.json()
        if payload.get("result") != "success":
            raise ProviderUnavailable(f"open_er_api: {payload.get('error-type', 'bad result')}")
        rates = {k: float(v) for k, v in (payload.get("rates") or {}).items()}
        source_ts = int(payload.get("time_last_update_unix") or 0) * 1000 or None

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
