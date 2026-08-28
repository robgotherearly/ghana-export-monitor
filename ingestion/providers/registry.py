"""Registry that turns a comma-separated env chain into a ProviderChain.

Adding a new upstream is one class plus one line in the tables below - nothing
in the pollers, Kafka topics or Spark jobs has to know about it.
"""
from __future__ import annotations

import logging
from collections.abc import Callable

from .. import config
from ..models import FxRate, PriceTick, WeatherReading
from .base import Provider, ProviderChain
from .commodities.api_ninjas import ApiNinjasProvider
from .commodities.commodities_api import CommoditiesApiProvider
from .commodities.simulated import SimulatedCommodityProvider
from .commodities.twelvedata import TwelveDataCommodityProvider
from .commodities.yahoo import YahooFinanceProvider
from .fx.exchangerate_host import ExchangeRateHostProvider
from .fx.frankfurter import FrankfurterProvider
from .fx.open_er_api import OpenErApiProvider
from .fx.simulated import SimulatedFxProvider
from .fx.twelvedata import TwelveDataFxProvider
from .weather.open_meteo import OpenMeteoProvider
from .weather.openweathermap import OpenWeatherMapProvider
from .weather.simulated import SimulatedWeatherProvider

log = logging.getLogger(__name__)

COMMODITY_REGISTRY: dict[str, Callable[[], Provider[PriceTick]]] = {
    "yahoo": YahooFinanceProvider,
    "twelvedata": TwelveDataCommodityProvider,
    "commodities_api": CommoditiesApiProvider,
    "api_ninjas": ApiNinjasProvider,
    "simulated": SimulatedCommodityProvider,
}

FX_REGISTRY: dict[str, Callable[[], Provider[FxRate]]] = {
    "open_er_api": OpenErApiProvider,
    "exchangerate_host": ExchangeRateHostProvider,
    "twelvedata": TwelveDataFxProvider,
    "frankfurter": FrankfurterProvider,
    "simulated": SimulatedFxProvider,
}

WEATHER_REGISTRY: dict[str, Callable[[], Provider[WeatherReading]]] = {
    "open_meteo": OpenMeteoProvider,
    "openweathermap": OpenWeatherMapProvider,
    "simulated": SimulatedWeatherProvider,
}


def _build(registry: dict[str, Callable[[], Provider]], names: list[str], label: str) -> ProviderChain:
    providers: list[Provider] = []
    for name in names:
        factory = registry.get(name)
        if factory is None:
            log.warning("%s: unknown provider %r in chain, ignoring", label, name)
            continue
        provider = factory()
        if not provider.is_configured():
            reason = "no API key" if provider.requires_key else "disabled"
            log.info("%s: skipping %s (%s)", label, name, reason)
            continue
        providers.append(provider)
    if not providers:
        raise SystemExit(f"{label}: no usable providers in chain {names}")
    log.info("%s: provider chain = %s", label, " -> ".join(p.name for p in providers))
    return ProviderChain(providers, label)


def commodity_chain() -> ProviderChain[PriceTick]:
    return _build(COMMODITY_REGISTRY, config.COMMODITY_PROVIDERS, "commodities")


def fx_chain() -> ProviderChain[FxRate]:
    return _build(FX_REGISTRY, config.FX_PROVIDERS, "fx")


def weather_chain() -> ProviderChain[WeatherReading]:
    return _build(WEATHER_REGISTRY, config.WEATHER_PROVIDERS, "weather")
