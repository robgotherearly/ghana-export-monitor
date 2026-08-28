"""Wire format for everything that goes onto Kafka.

One dataclass per topic. `key()` decides the Kafka partition key, which is what
gives us per-symbol / per-region ordering and parallelism downstream.

Timestamp convention (matters for windowing): `ts_event` is when *we observed*
the quote, i.e. poll time. Polled snapshot APIs are not a trade feed, so the
upstream-reported quote time is carried separately as `ts_source` and never
drives the watermark - otherwise a stale weekend close would stall every window.
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field

from . import config


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class Envelope:
    schema_version: int = field(default=config.SCHEMA_VERSION, init=False)
    event_type: str = field(default="unknown", init=False)

    def key(self) -> str:  # pragma: no cover - overridden
        raise NotImplementedError

    def to_json(self) -> bytes:
        payload = asdict(self)
        payload["schema_version"] = self.schema_version
        payload["event_type"] = self.event_type
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")


@dataclass
class PriceTick(Envelope):
    symbol: str
    name: str
    asset_class: str
    price: float
    currency: str
    unit: str
    source: str
    ts_event: int
    ts_ingest: int = field(default_factory=now_ms)
    ts_source: int | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    prev_close: float | None = None
    is_simulated: bool = False

    def __post_init__(self) -> None:
        self.event_type = "commodity_price"

    def key(self) -> str:
        return self.symbol


@dataclass
class FxRate(Envelope):
    pair: str
    base: str
    quote: str
    rate: float
    source: str
    ts_event: int
    ts_ingest: int = field(default_factory=now_ms)
    ts_source: int | None = None
    inverse_rate: float | None = None
    is_simulated: bool = False

    def __post_init__(self) -> None:
        self.event_type = "fx_rate"
        if self.inverse_rate is None and self.rate:
            self.inverse_rate = round(1.0 / self.rate, 8)

    def key(self) -> str:
        return self.pair


@dataclass
class WeatherReading(Envelope):
    region_id: str
    station: str
    admin_region: str
    latitude: float
    longitude: float
    cocoa_belt: bool
    temp_c: float
    humidity_pct: float | None
    precip_mm: float | None
    wind_kph: float | None
    cloud_pct: float | None
    condition: str
    source: str
    ts_event: int
    ts_ingest: int = field(default_factory=now_ms)
    ts_source: int | None = None
    is_simulated: bool = False

    def __post_init__(self) -> None:
        self.event_type = "weather_reading"

    def key(self) -> str:
        return self.region_id
