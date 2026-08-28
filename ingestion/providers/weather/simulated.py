"""Simulated weather fallback - a crude tropical diurnal cycle per region."""
from __future__ import annotations

import math
import random
import time
from collections.abc import Sequence

from ... import config
from ...models import WeatherReading, now_ms
from ..base import Provider


class SimulatedWeatherProvider(Provider[WeatherReading]):
    name = "simulated"
    min_interval_s = 0.0

    def __init__(self) -> None:
        super().__init__()
        self._rng = random.Random()

    def is_configured(self) -> bool:
        return config.ALLOW_SIMULATED_FALLBACK

    def fetch(self, targets: Sequence[str]) -> list[WeatherReading]:
        observed = now_ms()
        hour = (time.time() % 86_400) / 3_600.0
        # Peak heat mid-afternoon, coolest just before dawn.
        diurnal = math.sin((hour - 9.0) / 24.0 * 2 * math.pi)

        readings: list[WeatherReading] = []
        for region_id in targets:
            region = config.REGIONS.get(region_id)
            if not region:
                continue
            temp = 27.0 + 4.0 * diurnal + self._rng.gauss(0, 0.4)
            humidity = max(35.0, min(99.0, 78.0 - 12.0 * diurnal + self._rng.gauss(0, 3)))
            precip = round(max(0.0, self._rng.gauss(0.2, 1.2)), 2)
            readings.append(
                WeatherReading(
                    region_id=region.region_id,
                    station=region.name,
                    admin_region=region.admin_region,
                    latitude=region.latitude,
                    longitude=region.longitude,
                    cocoa_belt=region.cocoa_belt,
                    temp_c=round(temp, 2),
                    humidity_pct=round(humidity, 1),
                    precip_mm=precip,
                    wind_kph=round(abs(self._rng.gauss(8, 3)), 2),
                    cloud_pct=round(max(0.0, min(100.0, self._rng.gauss(55, 20))), 1),
                    condition="Showers" if precip > 1.5 else "Partly cloudy",
                    source=self.name,
                    ts_event=observed,
                    is_simulated=True,
                )
            )
        return readings
