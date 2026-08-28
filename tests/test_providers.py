"""Provider-chain behaviour: the part that has to survive a free tier going dark."""
from __future__ import annotations

import random

import pytest
import requests

from ingestion import config
from ingestion.models import PriceTick, now_ms
from ingestion.providers.base import Provider, ProviderChain, ProviderUnavailable
from ingestion.providers.commodities.simulated import SimulatedCommodityProvider, walk
from ingestion.providers.fx.open_er_api import cross
from ingestion.providers.weather.open_meteo import describe


def _tick(symbol: str = "COCOA", source: str = "test") -> PriceTick:
    meta = config.COMMODITIES[symbol]
    return PriceTick(
        symbol=symbol, name=meta.name, asset_class=meta.asset_class, price=1234.5,
        currency="USD", unit=meta.unit, source=source, ts_event=now_ms(),
    )


class _Stub(Provider[PriceTick]):
    """Provider that does exactly what the test tells it to."""

    def __init__(self, name: str, behaviour: str = "ok") -> None:
        super().__init__()
        self.name = name
        self.behaviour = behaviour
        self.calls = 0

    def fetch(self, targets):  # noqa: ANN001
        self.calls += 1
        if self.behaviour == "unavailable":
            raise ProviderUnavailable(f"{self.name}: down")
        if self.behaviour == "network":
            raise requests.ConnectionError("no route to host")
        if self.behaviour == "boom":
            raise ValueError("unexpected payload shape")
        if self.behaviour == "empty":
            return []
        return [_tick(source=self.name)]


class TestProviderChain:
    def test_first_healthy_provider_wins(self):
        primary, backup = _Stub("primary"), _Stub("backup")
        chain = ProviderChain([primary, backup], "commodities")

        events = chain.fetch(["COCOA"])

        assert [e.source for e in events] == ["primary"]
        assert backup.calls == 0, "backup should not be touched while primary works"

    @pytest.mark.parametrize("failure", ["unavailable", "network", "boom", "empty"])
    def test_falls_through_every_failure_mode(self, failure):
        primary, backup = _Stub("primary", failure), _Stub("backup")
        chain = ProviderChain([primary, backup], "commodities")

        events = chain.fetch(["COCOA"])

        assert [e.source for e in events] == ["backup"]
        assert chain.stats == {"primary": 0, "backup": 1}

    def test_returns_empty_when_everything_fails(self):
        chain = ProviderChain([_Stub("a", "unavailable"), _Stub("b", "network")], "commodities")
        assert chain.fetch(["COCOA"]) == []

    def test_rate_limited_provider_is_skipped_not_called(self):
        primary = _Stub("primary")
        primary.min_interval_s = 3600
        primary._last_call = __import__("time").time()  # just called
        backup = _Stub("backup")

        events = ProviderChain([primary, backup], "commodities").fetch(["COCOA"])

        assert primary.calls == 0
        assert [e.source for e in events] == ["backup"]

    def test_budget_cap_retires_a_provider(self):
        provider = _Stub("budgeted")
        provider.max_calls = 1
        provider._calls = 1
        assert provider.is_ready() is False


class TestCrossRates:
    def test_usd_base_pair_reads_straight_off_the_table(self):
        assert cross({"GHS": 11.2}, "USD", "GHS") == pytest.approx(11.2)

    def test_non_usd_base_is_derived(self):
        # 1 EUR = 1.16 USD, 1 USD = 11.2 GHS  ->  1 EUR = 12.99 GHS
        rates = {"GHS": 11.2, "EUR": 0.8621}
        assert cross(rates, "EUR", "GHS") == pytest.approx(11.2 / 0.8621)

    def test_missing_currency_returns_none(self):
        assert cross({"EUR": 0.86}, "USD", "GHS") is None


class TestSimulatedFallback:
    def test_walk_stays_positive_and_near_the_anchor(self):
        rng = random.Random(7)
        price = 5900.0
        for _ in range(500):
            price = walk(price, anchor=5900.0, daily_vol=0.03, rng=rng)
            assert price > 0
        # mean reversion should keep a long run inside a sane band
        assert 0.5 < price / 5900.0 < 2.0

    def test_every_simulated_event_is_labelled(self):
        provider = SimulatedCommodityProvider()
        ticks = provider.fetch(["COCOA", "GOLD"])

        assert len(ticks) == 2
        assert all(t.is_simulated for t in ticks)
        assert all(t.source == "simulated" for t in ticks)
        assert all(b'"is_simulated":true' in t.to_json() for t in ticks)

    def test_can_be_switched_off(self, monkeypatch):
        monkeypatch.setattr(config, "ALLOW_SIMULATED_FALLBACK", False)
        assert SimulatedCommodityProvider().is_configured() is False


class TestWeatherCodes:
    @pytest.mark.parametrize("code,expected", [(0, "Clear sky"), (63, "Rain"), (95, "Thunderstorm")])
    def test_known_wmo_codes(self, code, expected):
        assert describe(code) == expected

    def test_unknown_code_still_renders(self):
        assert describe(4242) == "WMO 4242"
        assert describe(None) == "Unknown"
