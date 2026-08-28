"""Contracts the rest of the pipeline depends on.

The Spark schemas are written by hand against these payloads, so a drift here
would silently produce all-null columns downstream. These tests pin the shape.
"""
from __future__ import annotations

import json

import pytest

from ingestion import config
from ingestion.kafka_sink import StdoutSink
from ingestion.models import FxRate, PriceTick, WeatherReading, now_ms
from ingestion.poller import Poller, PollerSpec
from ingestion.providers.base import ProviderChain
from ingestion.providers.lastknown import STORE


def _price_tick(**overrides) -> PriceTick:
    base = {
        "symbol": "COCOA", "name": "Cocoa", "asset_class": "softs", "price": 5844.0,
        "currency": "USD", "unit": "USD/tonne", "source": "yahoo", "ts_event": now_ms(),
    }
    base.update(overrides)
    return PriceTick(**base)


class TestWireFormat:
    def test_price_tick_carries_the_fields_spark_reads(self):
        payload = json.loads(_price_tick().to_json())
        assert set(payload) >= {
            "schema_version", "event_type", "symbol", "name", "asset_class", "price",
            "currency", "unit", "source", "ts_event", "ts_ingest", "ts_source",
            "open", "high", "low", "prev_close", "is_simulated",
        }
        assert payload["event_type"] == "commodity_price"
        assert payload["schema_version"] == config.SCHEMA_VERSION

    def test_fx_rate_inverts_itself(self):
        rate = FxRate(pair="USD/GHS", base="USD", quote="GHS", rate=11.2,
                      source="open_er_api", ts_event=now_ms())
        assert rate.inverse_rate == pytest.approx(1 / 11.2, rel=1e-6)
        assert json.loads(rate.to_json())["event_type"] == "fx_rate"

    def test_partition_keys_are_the_entity(self):
        assert _price_tick().key() == "COCOA"
        assert FxRate(pair="EUR/GHS", base="EUR", quote="GHS", rate=13.0,
                      source="x", ts_event=now_ms()).key() == "EUR/GHS"
        assert WeatherReading(
            region_id="ashanti_kumasi", station="Kumasi", admin_region="Ashanti",
            latitude=6.6, longitude=-1.6, cocoa_belt=True, temp_c=25.0,
            humidity_pct=88.0, precip_mm=0.2, wind_kph=9.0, cloud_pct=70.0,
            condition="Drizzle", source="open_meteo", ts_event=now_ms(),
        ).key() == "ashanti_kumasi"

    def test_event_time_is_observation_time_not_stale_quote_time(self):
        """Windowing depends on this: a weekend close must not drive the watermark."""
        observed = now_ms()
        tick = _price_tick(ts_event=observed, ts_source=observed - 3 * 86_400_000)
        assert tick.ts_event == observed
        assert tick.ts_source < tick.ts_event


class TestReferenceData:
    def test_every_active_commodity_is_defined(self):
        for symbol in config.ACTIVE_COMMODITIES:
            assert symbol in config.COMMODITIES

    def test_every_active_fx_pair_is_quoted_in_cedi(self):
        for pair in config.ACTIVE_FX_PAIRS:
            assert pair in config.FX_PAIRS
            # the depreciation rule in the Spark job assumes GHS is the quote side
            assert config.FX_PAIRS[pair].quote == "GHS"

    def test_cocoa_belt_regions_are_inside_ghana(self):
        for region in config.REGIONS.values():
            assert 4.0 < region.latitude < 11.5, region.region_id
            assert -3.5 < region.longitude < 1.5, region.region_id
        assert sum(r.cocoa_belt for r in config.REGIONS.values()) >= 5


class _OneShotChain(ProviderChain):
    def __init__(self, events):
        super().__init__([], "test")
        self._events = events

    def fetch(self, targets):  # noqa: ANN001
        return self._events


class TestPoller:
    def test_real_prices_feed_the_simulated_fallback(self):
        tick = _price_tick(price=6100.0, source="yahoo")
        poller = Poller(
            PollerSpec("commodities", "commodity.prices.raw", ["COCOA"], 60),
            _OneShotChain([tick]),
            StdoutSink(),
        )
        poller.cycle()
        assert STORE.get("commodity:COCOA", 0.0) == pytest.approx(6100.0)

    def test_simulated_prices_do_not_poison_the_anchor(self):
        STORE.set("commodity:GOLD", 4700.0)
        fake = _price_tick(symbol="GOLD", price=1.0, source="simulated", is_simulated=True)
        poller = Poller(
            PollerSpec("commodities", "commodity.prices.raw", ["GOLD"], 60),
            _OneShotChain([fake]),
            StdoutSink(),
        )
        poller.cycle()
        assert STORE.get("commodity:GOLD", 0.0) == pytest.approx(4700.0)

    def test_empty_cycle_is_survivable(self):
        poller = Poller(
            PollerSpec("fx", "fx.rates.raw", ["USD/GHS"], 30),
            _OneShotChain([]),
            StdoutSink(),
        )
        assert poller.cycle() == 0
