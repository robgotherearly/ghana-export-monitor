"""Open-Meteo - free, no key, no signup, and it takes every region in one call."""
from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from ... import config
from ...models import WeatherReading, now_ms
from ..base import Provider, ProviderUnavailable

_URL = "https://api.open-meteo.com/v1/forecast"
_CURRENT = (
    "temperature_2m,relative_humidity_2m,precipitation,rain,"
    "weather_code,cloud_cover,wind_speed_10m"
)

# WMO 4677 weather codes, collapsed to labels a dashboard panel can show.
WMO_CODES: dict[int, str] = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Rime fog",
    51: "Light drizzle", 53: "Drizzle", 55: "Dense drizzle",
    56: "Freezing drizzle", 57: "Dense freezing drizzle",
    61: "Light rain", 63: "Rain", 65: "Heavy rain",
    66: "Freezing rain", 67: "Heavy freezing rain",
    71: "Light snow", 73: "Snow", 75: "Heavy snow", 77: "Snow grains",
    80: "Light showers", 81: "Showers", 82: "Violent showers",
    85: "Snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Severe thunderstorm with hail",
}


def describe(code: Any) -> str:
    try:
        return WMO_CODES.get(int(code), f"WMO {int(code)}")
    except (TypeError, ValueError):
        return "Unknown"


class OpenMeteoProvider(Provider[WeatherReading]):
    name = "open_meteo"
    min_interval_s = 30.0

    def fetch(self, targets: Sequence[str]) -> list[WeatherReading]:
        regions = [config.REGIONS[r] for r in targets if r in config.REGIONS]
        if not regions:
            return []

        resp = self.get(
            _URL,
            params={
                "latitude": ",".join(f"{r.latitude}" for r in regions),
                "longitude": ",".join(f"{r.longitude}" for r in regions),
                "current": _CURRENT,
                "timezone": "UTC",
            },
        )
        payload = resp.json()
        blocks = payload if isinstance(payload, list) else [payload]
        if len(blocks) != len(regions):
            raise ProviderUnavailable(
                f"open_meteo: asked for {len(regions)} locations, got {len(blocks)}"
            )

        observed = now_ms()
        readings: list[WeatherReading] = []
        for region, block in zip(regions, blocks, strict=True):
            current = block.get("current") or {}
            if "temperature_2m" not in current:
                continue
            readings.append(
                WeatherReading(
                    region_id=region.region_id,
                    station=region.name,
                    admin_region=region.admin_region,
                    latitude=region.latitude,
                    longitude=region.longitude,
                    cocoa_belt=region.cocoa_belt,
                    temp_c=float(current["temperature_2m"]),
                    humidity_pct=_num(current.get("relative_humidity_2m")),
                    precip_mm=_num(current.get("precipitation")),
                    wind_kph=_num(current.get("wind_speed_10m")),
                    cloud_pct=_num(current.get("cloud_cover")),
                    condition=describe(current.get("weather_code")),
                    source=self.name,
                    ts_event=observed,
                    ts_source=_parse_iso(current.get("time")),
                )
            )
        return readings


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso(value: str | None) -> int | None:
    if not value:
        return None
    try:
        stamp = datetime.fromisoformat(value)
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=UTC)
    return int(stamp.timestamp() * 1000)
