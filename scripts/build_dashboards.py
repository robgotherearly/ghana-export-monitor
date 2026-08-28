"""Generate the Grafana dashboard JSON.

Dashboards are generated rather than hand-edited so panel geometry, datasource
uids and refresh settings stay consistent across the two boards. The generated
files are committed - Grafana provisions them straight from disk.

    python scripts/build_dashboards.py
"""
from __future__ import annotations

import json
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent.parent / "grafana" / "dashboards"
DS = {"type": "postgres", "uid": "ghana-dep-pg"}

GREEN_RED = [
    {"color": "green", "value": None},
    {"color": "#EAB839", "value": 1},
    {"color": "red", "value": 2.5},
]


def target(sql: str, fmt: str = "time_series", ref: str = "A") -> dict:
    return {
        "refId": ref,
        "datasource": DS,
        "format": fmt,
        "rawQuery": True,
        "rawSql": sql.strip(),
        "editorMode": "code",
    }


def panel(
    kind: str,
    title: str,
    sql: str,
    x: int,
    y: int,
    w: int,
    h: int,
    pid: int,
    fmt: str = "time_series",
    unit: str | None = None,
    decimals: int | None = None,
    options: dict | None = None,
    custom: dict | None = None,
    thresholds: list | None = None,
    overrides: list | None = None,
    description: str = "",
    gauge_min: float | None = None,
    gauge_max: float | None = None,
) -> dict:
    defaults: dict = {"custom": custom or {}}
    if unit:
        defaults["unit"] = unit
    if decimals is not None:
        defaults["decimals"] = decimals
    if gauge_min is not None:
        defaults["min"] = gauge_min
    if gauge_max is not None:
        defaults["max"] = gauge_max
    defaults["thresholds"] = {
        "mode": "absolute",
        "steps": thresholds or [{"color": "green", "value": None}],
    }
    defaults["mappings"] = []
    return {
        "id": pid,
        "type": kind,
        "title": title,
        "description": description,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
        "options": options or {},
        "targets": [target(sql, fmt)],
    }


def row(title: str, y: int, pid: int) -> dict:
    return {
        "id": pid,
        "type": "row",
        "title": title,
        "collapsed": False,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "panels": [],
    }


LINE = {
    "drawStyle": "line",
    "lineWidth": 2,
    "fillOpacity": 8,
    "gradientMode": "opacity",
    "showPoints": "never",
    "spanNulls": True,
    "pointSize": 5,
    "axisBorderShow": False,
    "scaleDistribution": {"type": "linear"},
}

#: Daily-granularity series can legitimately hold a single point, and a line
#: with hidden points draws literally nothing. Always show markers on the batch
#: board so one day of history is still visible.
DAILY_LINE = {**LINE, "showPoints": "always", "pointSize": 7, "lineWidth": 2}

TS_OPTIONS = {
    "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True, "calcs": []},
    "tooltip": {"mode": "multi", "sort": "none"},
}


def stat_options(graph: str = "area", text_size: int = 34) -> dict:
    return {
        "colorMode": "value",
        "graphMode": graph,
        "justifyMode": "auto",
        "orientation": "auto",
        "textMode": "auto",
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "text": {"valueSize": text_size},
    }


def table_options() -> dict:
    return {
        "showHeader": True,
        "cellHeight": "sm",
        "footer": {"show": False, "reducer": ["sum"], "countRows": False, "fields": ""},
    }


# ---------------------------------------------------------------------------
# Dashboard 1: the live streaming board
# ---------------------------------------------------------------------------
def live_dashboard() -> dict:
    panels: list[dict] = []
    pid = iter(range(1, 500))

    # -- headline stats ----------------------------------------------------
    panels.append(row("Cedi & commodities — live", 0, next(pid)))

    panels.append(panel(
        "stat", "USD / GHS", """
SELECT ts_event AS time, rate
FROM market.fx_ticks
WHERE pair = 'USD/GHS' AND $__timeFilter(ts_event)
ORDER BY 1
""", x=0, y=1, w=4, h=5, pid=next(pid), unit="none", decimals=4,
        options=stat_options(), custom={},
        description="Cedi per US dollar. Rising = the cedi is weakening.",
    ))

    for i, (symbol, label, unit_hint) in enumerate([
        ("COCOA", "Cocoa", "USD / tonne"),
        ("GOLD", "Gold", "USD / troy oz"),
        ("CRUDE", "Crude (WTI)", "USD / barrel"),
    ]):
        panels.append(panel(
            "stat", f"{label}", f"""
SELECT ts_event AS time, price
FROM market.commodity_ticks
WHERE symbol = '{symbol}' AND $__timeFilter(ts_event)
ORDER BY 1
""", x=4 + i * 4, y=1, w=4, h=5, pid=next(pid), unit="currencyUSD", decimals=2,
            options=stat_options(), description=unit_hint,
        ))

    panels.append(panel(
        "stat", "Ticks ingested (last hour)", """
SELECT count(*) AS "ticks"
FROM (
  SELECT ts_event FROM market.commodity_ticks WHERE ts_event > now() - interval '1 hour'
  UNION ALL
  SELECT ts_event FROM market.fx_ticks WHERE ts_event > now() - interval '1 hour'
  UNION ALL
  SELECT ts_event FROM market.weather_readings WHERE ts_event > now() - interval '1 hour'
) t
""", x=16, y=1, w=4, h=5, pid=next(pid), fmt="table", unit="none",
        options=stat_options(graph="none"),
        description="Total events written by the streaming job in the last hour.",
    ))

    panels.append(panel(
        "stat", "Open alerts (1h)", """
SELECT count(*) AS "alerts"
FROM market.alerts
WHERE ts_raised > now() - interval '1 hour'
""", x=20, y=1, w=4, h=5, pid=next(pid), fmt="table", unit="none",
        options=stat_options(graph="none"),
        thresholds=[{"color": "green", "value": None},
                    {"color": "#EAB839", "value": 1},
                    {"color": "red", "value": 5}],
    ))

    # -- price charts ------------------------------------------------------
    panels.append(row("Price streams", 6, next(pid)))

    panels.append(panel(
        "timeseries", "Cocoa — live price", """
SELECT ts_event AS time, price AS "Cocoa (USD/t)"
FROM market.commodity_ticks
WHERE symbol = 'COCOA' AND $__timeFilter(ts_event)
ORDER BY 1
""", x=0, y=7, w=12, h=8, pid=next(pid), unit="currencyUSD", decimals=2,
        custom=LINE, options=TS_OPTIONS,
        description="Ghana's #1 export. Raw ticks straight off commodity.prices.raw.",
    ))

    panels.append(panel(
        "timeseries", "Cedi crosses — live", """
SELECT ts_event AS time, rate AS value, pair AS metric
FROM market.fx_ticks
WHERE $__timeFilter(ts_event)
ORDER BY 1
""", x=12, y=7, w=12, h=8, pid=next(pid), unit="none", decimals=4,
        custom=LINE, options=TS_OPTIONS,
        description="GHS per unit of USD / EUR / GBP.",
    ))

    panels.append(panel(
        "timeseries", "Gold — live price", """
SELECT ts_event AS time, price AS "Gold (USD/oz)"
FROM market.commodity_ticks
WHERE symbol = 'GOLD' AND $__timeFilter(ts_event)
ORDER BY 1
""", x=0, y=15, w=8, h=7, pid=next(pid), unit="currencyUSD", decimals=2,
        custom=LINE, options=TS_OPTIONS, description="Ghana's #2 export.",
    ))

    panels.append(panel(
        "timeseries", "Crude (WTI) — live price", """
SELECT ts_event AS time, price AS "WTI (USD/bbl)"
FROM market.commodity_ticks
WHERE symbol = 'CRUDE' AND $__timeFilter(ts_event)
ORDER BY 1
""", x=8, y=15, w=8, h=7, pid=next(pid), unit="currencyUSD", decimals=2,
        custom=LINE, options=TS_OPTIONS,
        description="Ghana exports crude and imports refined fuel — it cuts both ways.",
    ))

    panels.append(panel(
        "timeseries", "Rolling 5-min % change", """
SELECT window_start AS time, pct_change AS value, symbol AS metric
FROM market.commodity_metrics
WHERE $__timeFilter(window_start)
ORDER BY 1
""", x=16, y=15, w=8, h=7, pid=next(pid), unit="percent", decimals=3,
        custom=LINE, options=TS_OPTIONS,
        description="Spark Structured Streaming output: (last - first) / first over the window.",
    ))

    # -- streaming metrics -------------------------------------------------
    panels.append(row("Streaming metrics — Spark rolling windows", 22, next(pid)))

    panels.append(panel(
        "gauge", "Volatility (5-min coeff. of variation)", """
-- The newest sliding window has only just opened and usually holds a single
-- tick, so its stddev is 0. Read the newest *closed* window that actually has
-- enough ticks to have a spread.
SELECT DISTINCT ON (symbol) symbol, volatility_pct
FROM market.commodity_metrics
WHERE window_start > now() - interval '30 minutes'
  AND window_end <= now()
  AND tick_count >= 3
ORDER BY symbol, window_start DESC
""", x=0, y=23, w=8, h=7, pid=next(pid), fmt="table", unit="percent", decimals=3,
        options={
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": True},
            "showThresholdLabels": False,
            "showThresholdMarkers": True,
            "minVizWidth": 75,
            "minVizHeight": 75,
        },
        # measured range for these instruments is ~0-0.3% per 5-min window
        gauge_min=0,
        gauge_max=0.4,
        thresholds=[{"color": "green", "value": None},
                    {"color": "#EAB839", "value": 0.1},
                    {"color": "red", "value": 0.25}],
        description="stddev / mean over the rolling window, per symbol.",
    ))

    panels.append(panel(
        "timeseries", "Anomaly z-score vs. threshold", """
SELECT window_start AS time, zscore AS value, symbol AS metric
FROM market.commodity_metrics
WHERE $__timeFilter(window_start) AND zscore IS NOT NULL
ORDER BY 1
""", x=8, y=23, w=16, h=7, pid=next(pid), unit="none", decimals=2,
        custom=LINE, options=TS_OPTIONS,
        description="(last - mean) / stddev per rolling window. Past ±2.5σ raises an alert.",
    ))

    # -- alerts ------------------------------------------------------------
    panels.append(row("Alerts — alerts.flagged", 30, next(pid)))

    panels.append(panel(
        "table", "Flagged events (most recent first)", """
SELECT ts_raised AS "raised",
       severity   AS "severity",
       entity     AS "entity",
       rule       AS "rule",
       message    AS "what happened",
       round(observed::numeric, 3)  AS "observed",
       round(threshold::numeric, 3) AS "threshold"
FROM market.alerts
WHERE $__timeFilter(ts_raised)
ORDER BY ts_raised DESC
LIMIT 100
""", x=0, y=31, w=24, h=9, pid=next(pid), fmt="table",
        options=table_options(),
        custom={"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False},
        overrides=[
            {
                "matcher": {"id": "byName", "options": "severity"},
                "properties": [
                    {"id": "custom.cellOptions", "value": {"type": "color-background"}},
                    {"id": "mappings", "value": [{
                        "type": "value",
                        "options": {
                            "critical": {"color": "red", "index": 0, "text": "CRITICAL"},
                            "warning": {"color": "orange", "index": 1, "text": "WARNING"},
                            "info": {"color": "blue", "index": 2, "text": "INFO"},
                        },
                    }]},
                    {"id": "custom.width", "value": 110},
                ],
            },
            {
                "matcher": {"id": "byName", "options": "raised"},
                "properties": [{"id": "custom.width", "value": 190}],
            },
        ],
        description="Written by the Spark job to Postgres and republished to the alerts.flagged topic.",
    ))

    # -- weather -----------------------------------------------------------
    panels.append(row("Cocoa belt weather", 40, next(pid)))

    panels.append(panel(
        "geomap", "Live conditions across the cocoa regions", """
SELECT station, admin_region, latitude, longitude, temp_c, humidity_pct, precip_mm, condition
FROM market.weather_latest
ORDER BY station
""", x=0, y=41, w=12, h=10, pid=next(pid), fmt="table", unit="celsius", decimals=1,
        options={
            "view": {"id": "coords", "lat": 6.4, "lon": -1.4, "zoom": 6.6},
            # key-free tiles; Grafana's "default" basemap serves API-KEY-REQUIRED images
            "basemap": {"type": "osm-standard", "name": "OpenStreetMap", "config": {}},
            "controls": {"showZoom": True, "showAttribution": True, "mouseWheelZoom": False},
            "tooltip": {"mode": "details"},
            "layers": [{
                "type": "markers",
                "name": "Stations",
                "location": {"mode": "coords", "latitude": "latitude", "longitude": "longitude"},
                "tooltip": True,
                "config": {
                    "showLegend": True,
                    "style": {
                        "color": {"field": "temp_c", "fixed": "dark-green"},
                        "opacity": 0.8,
                        "rotation": {"fixed": 0, "max": 360, "min": -360, "mode": "mod"},
                        "size": {"field": "precip_mm", "fixed": 6, "max": 18, "min": 6},
                        "symbol": {"fixed": "img/icons/marker/circle.svg", "mode": "fixed"},
                        "symbolAlign": {"horizontal": "center", "vertical": "center"},
                        "textConfig": {"fontSize": 12, "offsetX": 0, "offsetY": -14,
                                       "textAlign": "center", "textBaseline": "middle"},
                        "text": {"field": "station", "fixed": ""},
                    },
                },
            }],
        },
        description="Marker size = precipitation, colour = temperature. Open-Meteo, live.",
    ))

    panels.append(panel(
        "table", "Region readings", """
SELECT station        AS "station",
       admin_region   AS "region",
       round(temp_c::numeric, 1)       AS "temp °C",
       round(humidity_pct::numeric, 0) AS "humidity %",
       round(precip_mm::numeric, 2)    AS "precip mm",
       condition      AS "conditions",
       ts_event       AS "observed"
FROM market.weather_latest
ORDER BY cocoa_belt DESC, station
""", x=12, y=41, w=12, h=10, pid=next(pid), fmt="table",
        options=table_options(),
        custom={"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False},
    ))

    panels.append(panel(
        "timeseries", "Cocoa price vs. cocoa-belt rainfall", """
SELECT window_start AS time,
       last_price           AS "Cocoa (USD/t)",
       belt_total_precip_mm AS "Belt rainfall (mm)"
FROM market.cocoa_weather_context
WHERE $__timeFilter(window_start)
ORDER BY 1
""", x=0, y=51, w=24, h=8, pid=next(pid), custom=LINE, options=TS_OPTIONS,
        overrides=[{
            "matcher": {"id": "byName", "options": "Belt rainfall (mm)"},
            "properties": [
                {"id": "custom.axisPlacement", "value": "right"},
                {"id": "unit", "value": "lengthmm"},
                {"id": "custom.drawStyle", "value": "bars"},
                {"id": "custom.fillOpacity", "value": 40},
                {"id": "color", "value": {"mode": "fixed", "fixedColor": "semi-dark-blue"}},
            ],
        }],
        description="Stream-static join: cocoa price windows enriched with the latest belt weather.",
    ))

    return {
        "id": None,
        "uid": "gdep-live",
        "title": "Ghana Live Market Monitor",
        "description": "Live cocoa, gold, crude and cedi telemetry from the Kafka → Spark → Postgres pipeline.",
        "tags": ["ghana", "streaming", "kafka", "spark"],
        "timezone": "utc",
        "schemaVersion": 39,
        "version": 1,
        "editable": True,
        "graphTooltip": 1,
        "refresh": "10s",
        "time": {"from": "now-3h", "to": "now"},
        "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m", "15m"]},
        "panels": panels,
    }


# ---------------------------------------------------------------------------
# Dashboard 2: the analytical / batch board
# ---------------------------------------------------------------------------
def batch_dashboard() -> dict:
    panels: list[dict] = []
    pid = iter(range(1, 500))

    panels.append(row("Daily bars — Spark batch", 0, next(pid)))

    panels.append(panel(
        "candlestick", "Cocoa daily OHLC", """
SELECT trade_date AS time, open_price AS open, high_price AS high,
       low_price AS low, close_price AS close
FROM market.daily_ohlc
WHERE symbol = 'COCOA' AND $__timeFilter(trade_date)
ORDER BY 1
""", x=0, y=1, w=12, h=9, pid=next(pid), unit="currencyUSD", decimals=2,
        custom={"drawStyle": "line", "lineWidth": 1, "pointSize": 5},
        options={
            "mode": "candles",
            "candleStyle": "candles",
            "colorStrategy": "open-close",
            "colors": {"down": "red", "up": "green", "flat": "gray"},
            "includeAllFields": False,
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True, "calcs": []},
            "fields": {"open": "open", "high": "high", "low": "low", "close": "close"},
        },
        description="Recomputed nightly from the Delta lake. Re-running the job is idempotent.",
    ))

    panels.append(panel(
        "candlestick", "Gold daily OHLC", """
SELECT trade_date AS time, open_price AS open, high_price AS high,
       low_price AS low, close_price AS close
FROM market.daily_ohlc
WHERE symbol = 'GOLD' AND $__timeFilter(trade_date)
ORDER BY 1
""", x=12, y=1, w=12, h=9, pid=next(pid), unit="currencyUSD", decimals=2,
        custom={"drawStyle": "line", "lineWidth": 1, "pointSize": 5},
        options={
            "mode": "candles",
            "candleStyle": "candles",
            "colorStrategy": "open-close",
            "colors": {"down": "red", "up": "green", "flat": "gray"},
            "includeAllFields": False,
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True, "calcs": []},
            "fields": {"open": "open", "high": "high", "low": "low", "close": "close"},
        },
    ))

    panels.append(row("Cedi trend", 10, next(pid)))

    panels.append(panel(
        "timeseries", "USD/GHS close with 7d & 30d moving averages", """
SELECT trade_date AS time,
       close_rate AS "close",
       ma_7       AS "7-day MA",
       ma_30      AS "30-day MA"
FROM market.cedi_depreciation
WHERE pair = 'USD/GHS' AND $__timeFilter(trade_date)
ORDER BY 1
""", x=0, y=11, w=14, h=8, pid=next(pid), unit="none", decimals=4,
        custom=DAILY_LINE, options=TS_OPTIONS,
        description="Moving averages need history; with one day in the lake they equal the close.",
    ))

    panels.append(panel(
        "timeseries", "Cedi depreciation over 1d / 7d / 30d", """
SELECT trade_date AS time,
       change_1d_pct  AS "1 day",
       change_7d_pct  AS "7 day",
       change_30d_pct AS "30 day"
FROM market.cedi_depreciation
WHERE pair = 'USD/GHS' AND $__timeFilter(trade_date)
ORDER BY 1
""", x=14, y=11, w=10, h=8, pid=next(pid), unit="percent", decimals=3,
        custom={**DAILY_LINE, "drawStyle": "bars", "fillOpacity": 60},
        options=TS_OPTIONS,
        description=("Positive = the cedi lost ground over that horizon. Each series "
                     "stays empty until the lake holds that many days of closes."),
    ))

    panels.append(row("Cocoa seasonality — main crop Oct–Mar, light crop Apr–Sep", 19, next(pid)))

    panels.append(panel(
        "barchart", "Monthly average cocoa price by crop phase", """
SELECT to_char(month_start, 'YYYY-MM') AS "month",
       round(avg_price::numeric, 2)    AS "avg price",
       crop_phase                      AS "phase"
FROM market.cocoa_seasonal
ORDER BY month_start
""", x=0, y=20, w=14, h=9, pid=next(pid), fmt="table", unit="currencyUSD", decimals=2,
        options={
            "orientation": "auto",
            "xTickLabelRotation": -45,
            "xTickLabelSpacing": 0,
            "showValue": "auto",
            "stacking": "none",
            "groupWidth": 0.7,
            "barWidth": 0.85,
            "fullHighlight": False,
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True, "calcs": []},
            "tooltip": {"mode": "single", "sort": "none"},
        },
        custom={"axisBorderShow": False, "fillOpacity": 80, "gradientMode": "none",
                "lineWidth": 1, "thresholdsStyle": {"mode": "off"}},
    ))

    panels.append(panel(
        "table", "Season summary", """
SELECT season                        AS "season",
       crop_phase                    AS "phase",
       to_char(month_start,'YYYY-MM') AS "month",
       round(avg_price::numeric, 2)  AS "avg",
       round(min_price::numeric, 2)  AS "low",
       round(max_price::numeric, 2)  AS "high",
       observations                  AS "obs"
FROM market.cocoa_seasonal
ORDER BY month_start DESC
LIMIT 24
""", x=14, y=20, w=10, h=9, pid=next(pid), fmt="table",
        options=table_options(),
        custom={"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False},
    ))

    panels.append(row("Weather / price correlation & pipeline health", 29, next(pid)))

    panels.append(panel(
        "table", "Cocoa price vs. regional weather (hourly buckets)", """
SELECT region_id    AS "region",
       metric       AS "weather metric",
       round(correlation::numeric, 3) AS "correlation",
       observations AS "n",
       computed_at  AS "computed"
FROM market.weather_price_correlation
WHERE symbol = 'COCOA'
ORDER BY abs(coalesce(correlation, 0)) DESC
LIMIT 40
""", x=0, y=30, w=14, h=9, pid=next(pid), fmt="table",
        options=table_options(),
        custom={"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False},
        overrides=[{
            "matcher": {"id": "byName", "options": "correlation"},
            "properties": [
                {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "gradient"}},
                {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                    {"color": "semi-dark-blue", "value": None},
                    {"color": "text", "value": -0.2},
                    {"color": "semi-dark-orange", "value": 0.2},
                ]}},
            ],
        }],
        description="n is written alongside so a correlation built on thin history is visibly thin.",
    ))

    panels.append(panel(
        "table", "Batch runs", """
SELECT job_name    AS "job",
       started_at  AS "started",
       status      AS "status",
       rows_written AS "rows",
       detail      AS "detail"
FROM market.pipeline_runs
ORDER BY started_at DESC
LIMIT 20
""", x=14, y=30, w=10, h=9, pid=next(pid), fmt="table",
        options=table_options(),
        custom={"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False},
        overrides=[{
            "matcher": {"id": "byName", "options": "status"},
            "properties": [
                {"id": "custom.cellOptions", "value": {"type": "color-background"}},
                {"id": "mappings", "value": [{
                    "type": "value",
                    "options": {
                        "succeeded": {"color": "green", "index": 0},
                        "failed": {"color": "red", "index": 1},
                    },
                }]},
            ],
        }],
    ))

    return {
        "id": None,
        "uid": "gdep-batch",
        "title": "Ghana Analytical Layer",
        "description": "Nightly Spark batch output: daily OHLC, cedi trend, cocoa seasonality, weather correlations.",
        "tags": ["ghana", "batch", "spark", "delta"],
        "timezone": "utc",
        "schemaVersion": 39,
        "version": 1,
        "editable": True,
        "graphTooltip": 1,
        "refresh": "5m",
        "time": {"from": "now-90d", "to": "now"},
        "timepicker": {"refresh_intervals": ["1m", "5m", "15m", "30m", "1h"]},
        "panels": panels,
    }


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for filename, builder in [
        ("live-market-monitor.json", live_dashboard),
        ("analytical-layer.json", batch_dashboard),
    ]:
        path = OUT_DIR / filename
        payload = builder()
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {path} ({len(payload['panels'])} panels)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
