"""OpenWeatherMap - 60 req/min free tier, one call per region."""
from __future__ import annotations

from collections.abc import Sequence

from ... import config
from ...models import WeatherReading, now_ms
from ..base import Provider

_URL = "https://api.openweathermap.org/data/2.5/weather"


class OpenWeatherMapProvider(Provider[WeatherReading]):
    name = "openweathermap"
    requires_key = True
    min_interval_s = 10.0

    def is_configured(self) -> bool:
        return bool(config.OPENWEATHERMAP_API_KEY)

    def fetch(self, targets: Sequence[str]) -> list[WeatherReading]:
        observed = now_ms()
        readings: list[WeatherReading] = []
        for region_id in targets:
            region = config.REGIONS.get(region_id)
            if not region:
                continue
            resp = self.get(
                _URL,
                params={
                    "lat": region.latitude,
                    "lon": region.longitude,
                    "units": "metric",
                    "appid": config.OPENWEATHERMAP_API_KEY,
                },
            )
            payload = resp.json()
            main = payload.get("main") or {}
            if "temp" not in main:
                continue
            weather = (payload.get("weather") or [{}])[0]
            rain = payload.get("rain") or {}
            readings.append(
                WeatherReading(
                    region_id=region.region_id,
                    station=region.name,
                    admin_region=region.admin_region,
                    latitude=region.latitude,
                    longitude=region.longitude,
                    cocoa_belt=region.cocoa_belt,
                    temp_c=float(main["temp"]),
                    humidity_pct=_num(main.get("humidity")),
                    precip_mm=_num(rain.get("1h")) or 0.0,
                    # OWM reports m/s in metric units; the pipeline speaks km/h.
                    wind_kph=round((_num((payload.get("wind") or {}).get("speed")) or 0.0) * 3.6, 2),
                    cloud_pct=_num((payload.get("clouds") or {}).get("all")),
                    condition=str(weather.get("main") or "Unknown"),
                    source=self.name,
                    ts_event=observed,
                    ts_source=int(payload.get("dt") or 0) * 1000 or None,
                )
            )
        return readings


def _num(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
