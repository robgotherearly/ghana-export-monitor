"""Frankfurter (ECB reference rates) - free, no key.

Caveat worth knowing: the ECB set does not include GHS, so this provider can
only resolve the hard-currency crosses. It stays in the chain as a cheap sanity
source for EUR/USD-style pairs and is skipped for the cedi.
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from ... import config
from ...models import FxRate, now_ms
from ..base import Provider
from .open_er_api import cross

_URL = "https://api.frankfurter.dev/v1/latest"


class FrankfurterProvider(Provider[FxRate]):
    name = "frankfurter"
    min_interval_s = 30.0

    def fetch(self, targets: Sequence[str]) -> list[FxRate]:
        currencies = sorted(
            {c for pair in targets if pair in config.FX_PAIRS
             for c in (config.FX_PAIRS[pair].base, config.FX_PAIRS[pair].quote)} - {"USD"}
        )
        if not currencies:
            return []

        resp = self.get(_URL, params={"base": "USD", "symbols": ",".join(currencies)})
        payload = resp.json()
        rates = {k: float(v) for k, v in (payload.get("rates") or {}).items()}
        source_ts = _parse_date(payload.get("date"))

        observed = now_ms()
        out: list[FxRate] = []
        for pair in targets:
            meta = config.FX_PAIRS.get(pair)
            if not meta:
                continue
            rate = cross(rates, meta.base, meta.quote)
            if not rate:
                continue  # GHS lands here: not in the ECB set
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


def _parse_date(value: str | None) -> int | None:
    if not value:
        return None
    try:
        day = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None
    return int(day.timestamp() * 1000)
