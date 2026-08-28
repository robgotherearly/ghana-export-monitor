"""Central configuration for the ingestion layer.

Everything is environment-driven so the same image runs all three pollers.
Reference data (symbols, FX pairs, cocoa regions) lives here so the pollers,
the Spark jobs and the tests all agree on one vocabulary.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _env_int(key: str, default: int) -> int:
    try:
        return int(_env(key) or default)
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(_env(key) or default)
    except ValueError:
        return default


def _env_bool(key: str, default: bool = False) -> bool:
    raw = _env(key).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_list(key: str, default: str) -> list[str]:
    return [item.strip() for item in (_env(key) or default).split(",") if item.strip()]


# --------------------------------------------------------------------------
# Kafka
# --------------------------------------------------------------------------
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")

TOPIC_COMMODITY_RAW = _env("TOPIC_COMMODITY_RAW", "commodity.prices.raw")
TOPIC_FX_RAW = _env("TOPIC_FX_RAW", "fx.rates.raw")
TOPIC_WEATHER_RAW = _env("TOPIC_WEATHER_RAW", "weather.regions.raw")
TOPIC_COMMODITY_METRICS = _env("TOPIC_COMMODITY_METRICS", "commodity.metrics")
TOPIC_ALERTS = _env("TOPIC_ALERTS", "alerts.flagged")

SCHEMA_VERSION = 1

# --------------------------------------------------------------------------
# Poll cadence (seconds). Free tiers are the constraint, not the pipeline.
# --------------------------------------------------------------------------
COMMODITY_POLL_SECONDS = _env_int("COMMODITY_POLL_SECONDS", 60)
FX_POLL_SECONDS = _env_int("FX_POLL_SECONDS", 30)
WEATHER_POLL_SECONDS = _env_int("WEATHER_POLL_SECONDS", 120)
POLL_JITTER_PCT = _env_float("POLL_JITTER_PCT", 0.1)

HTTP_TIMEOUT_SECONDS = _env_float("HTTP_TIMEOUT_SECONDS", 12.0)

# Provider chains: first one that is configured and not rate-limited wins.
COMMODITY_PROVIDERS = _env_list("COMMODITY_PROVIDERS", "yahoo,twelvedata,api_ninjas,commodities_api,simulated")
FX_PROVIDERS = _env_list("FX_PROVIDERS", "open_er_api,frankfurter,twelvedata,exchangerate_host,simulated")
WEATHER_PROVIDERS = _env_list("WEATHER_PROVIDERS", "open_meteo,openweathermap,simulated")

# The simulated provider never invents a market out of nothing: it random-walks
# from the last real price it saw. It is always tagged source="simulated" in the
# payload so nothing downstream can mistake it for a live quote.
ALLOW_SIMULATED_FALLBACK = _env_bool("ALLOW_SIMULATED_FALLBACK", True)

# --------------------------------------------------------------------------
# API keys (all optional - the default chain runs key-free)
# --------------------------------------------------------------------------
TWELVEDATA_API_KEY = _env("TWELVEDATA_API_KEY")
COMMODITIES_API_KEY = _env("COMMODITIES_API_KEY")
API_NINJAS_API_KEY = _env("API_NINJAS_API_KEY")
EXCHANGERATE_HOST_API_KEY = _env("EXCHANGERATE_HOST_API_KEY")
OPENWEATHERMAP_API_KEY = _env("OPENWEATHERMAP_API_KEY")


# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------
@dataclass(frozen=True)
class Commodity:
    symbol: str            # canonical symbol used across the whole pipeline
    name: str
    unit: str
    asset_class: str = "other"
    currency: str = "USD"
    anchor_price: float = 100.0   # cold-start seed for the simulated provider
    daily_vol: float = 0.02       # rough annualised-ish daily sigma for the walk
    provider_symbols: dict[str, str] = field(default_factory=dict)


COMMODITIES: dict[str, Commodity] = {
    "COCOA": Commodity(
        symbol="COCOA",
        name="Cocoa",
        unit="USD/tonne",
        asset_class="softs",
        anchor_price=5900.0,
        daily_vol=0.030,
        provider_symbols={
            "yahoo": "CC=F",
            "twelvedata": "CC",
            "commodities_api": "COCOA",
            "api_ninjas": "cocoa",
        },
    ),
    "GOLD": Commodity(
        symbol="GOLD",
        name="Gold",
        unit="USD/troy ounce",
        asset_class="metals",
        anchor_price=4700.0,
        daily_vol=0.012,
        provider_symbols={
            "yahoo": "GC=F",
            "twelvedata": "XAU/USD",
            "commodities_api": "XAU",
            "api_ninjas": "gold",
        },
    ),
    "CRUDE": Commodity(
        symbol="CRUDE",
        name="Crude oil (WTI)",
        unit="USD/barrel",
        asset_class="energy",
        anchor_price=81.0,
        daily_vol=0.022,
        provider_symbols={
            "yahoo": "CL=F",
            "twelvedata": "WTI/USD",
            "commodities_api": "WTI",
            "api_ninjas": "crude_oil_wti",
        },
    ),
}

ACTIVE_COMMODITIES = _env_list("COMMODITY_SYMBOLS", "COCOA,GOLD,CRUDE")


@dataclass(frozen=True)
class FxPair:
    base: str
    quote: str
    anchor_rate: float
    daily_vol: float = 0.006

    @property
    def pair(self) -> str:
        return f"{self.base}/{self.quote}"


FX_PAIRS: dict[str, FxPair] = {
    "USD/GHS": FxPair("USD", "GHS", 11.20),
    "EUR/GHS": FxPair("EUR", "GHS", 13.05),
    "GBP/GHS": FxPair("GBP", "GHS", 15.25),
}

ACTIVE_FX_PAIRS = _env_list("FX_PAIRS", "USD/GHS,EUR/GHS,GBP/GHS")


@dataclass(frozen=True)
class Region:
    region_id: str      # kafka key
    name: str           # town / station
    admin_region: str   # Ghanaian administrative region
    latitude: float
    longitude: float
    cocoa_belt: bool = True


# The cocoa belt: Ashanti, Western, Western North, Eastern, Bono, Central.
REGIONS: dict[str, Region] = {
    "ashanti_kumasi": Region("ashanti_kumasi", "Kumasi", "Ashanti", 6.6885, -1.6244),
    "western_sefwi": Region("western_sefwi", "Sefwi Wiawso", "Western North", 6.2103, -2.4854),
    "western_takoradi": Region("western_takoradi", "Takoradi", "Western", 4.8975, -1.7603),
    "eastern_koforidua": Region("eastern_koforidua", "Koforidua", "Eastern", 6.0940, -0.2591),
    "bono_sunyani": Region("bono_sunyani", "Sunyani", "Bono", 7.3399, -2.3269),
    "central_twifo": Region("central_twifo", "Twifo Praso", "Central", 5.6086, -1.5497),
    "greater_accra": Region("greater_accra", "Accra", "Greater Accra", 5.6037, -0.1870, cocoa_belt=False),
}

ACTIVE_REGIONS = _env_list("WEATHER_REGIONS", ",".join(REGIONS))
