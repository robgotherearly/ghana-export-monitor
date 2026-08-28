"""Yahoo Finance chart endpoint - free, no key, real exchange quotes.

Default primary source for the commodity leg: cocoa (CC=F, ICE), gold (GC=F,
COMEX) and WTI (CL=F, NYMEX). It is an undocumented public endpoint, which is
exactly why the provider chain exists - if it changes shape the poller demotes
to the next source instead of falling over.
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ... import config
from ...models import PriceTick, now_ms
from ..base import Provider, ProviderUnavailable

_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
_BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ghana-dep-monitor/1.0"


def _num(value: Any) -> float | None:
    try:
        out = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return out if out > 0 else None


def _day_open(result: dict) -> float | None:
    """First non-null open in today's bars, when the payload carries them."""
    try:
        opens = result["indicators"]["quote"][0]["open"]
    except (KeyError, IndexError, TypeError):
        return None
    for value in opens or []:
        parsed = _num(value)
        if parsed is not None:
            return parsed
    return None


class YahooFinanceProvider(Provider[PriceTick]):
    name = "yahoo"
    #: one HTTP call per symbol, so keep a floor between them
    min_interval_s = 5.0

    def __init__(self) -> None:
        super().__init__()
        self._session.headers.update({"User-Agent": _BROWSER_UA})

    def fetch(self, targets: Sequence[str]) -> list[PriceTick]:
        observed = now_ms()
        ticks: list[PriceTick] = []
        for canonical in targets:
            meta = config.COMMODITIES.get(canonical)
            if not meta or self.name not in meta.provider_symbols:
                continue

            resp = self.get(
                _URL.format(symbol=meta.provider_symbols[self.name]),
                params={"interval": "1d", "range": "1d"},
            )
            chart = (resp.json() or {}).get("chart") or {}
            if chart.get("error"):
                raise ProviderUnavailable(f"yahoo: {chart['error']}")
            results = chart.get("result") or []
            if not results:
                continue

            quote_meta = results[0].get("meta") or {}
            price = _num(quote_meta.get("regularMarketPrice"))
            if price is None:
                continue
            source_ts = int(quote_meta.get("regularMarketTime") or 0) * 1000 or None
            ticks.append(
                PriceTick(
                    symbol=canonical,
                    name=meta.name,
                    asset_class=meta.asset_class,
                    price=price,
                    currency=str(quote_meta.get("currency") or meta.currency),
                    unit=meta.unit,
                    source=self.name,
                    ts_event=observed,
                    ts_source=source_ts,
                    open=_day_open(results[0]),
                    high=_num(quote_meta.get("regularMarketDayHigh")),
                    low=_num(quote_meta.get("regularMarketDayLow")),
                    # daily bars expose chartPreviousClose, intraday ones previousClose
                    prev_close=_num(quote_meta.get("previousClose"))
                    or _num(quote_meta.get("chartPreviousClose")),
                )
            )
        return ticks
