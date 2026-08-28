-- ---------------------------------------------------------------------------
-- Ghana Export Commodity & Cedi Monitoring Platform - serving store
--
-- Two layers live here:
--   * streaming  - raw ticks, rolling window metrics, alerts (written by Spark
--                  Structured Streaming, upserted every micro-batch)
--   * analytical - daily OHLC, seasonal cocoa tables, cedi trend, correlations
--                  (written by the nightly Spark batch job, idempotent per
--                  partition)
-- Grafana reads everything through a SELECT-only role.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS market;
SET search_path TO market, public;

-- ---------------------------------------------------------------------------
-- Streaming layer: raw ticks
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS commodity_ticks (
    symbol        TEXT        NOT NULL,
    ts_event      TIMESTAMPTZ NOT NULL,
    name          TEXT        NOT NULL,
    asset_class   TEXT        NOT NULL,
    price         DOUBLE PRECISION NOT NULL,
    currency      TEXT        NOT NULL,
    unit          TEXT        NOT NULL,
    open_price    DOUBLE PRECISION,
    high_price    DOUBLE PRECISION,
    low_price     DOUBLE PRECISION,
    prev_close    DOUBLE PRECISION,
    source        TEXT        NOT NULL,
    is_simulated  BOOLEAN     NOT NULL DEFAULT FALSE,
    ts_source     TIMESTAMPTZ,
    ts_ingest     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol, ts_event)
);
CREATE INDEX IF NOT EXISTS idx_commodity_ticks_ts ON commodity_ticks (ts_event DESC);

CREATE TABLE IF NOT EXISTS fx_ticks (
    pair          TEXT        NOT NULL,
    ts_event      TIMESTAMPTZ NOT NULL,
    base          TEXT        NOT NULL,
    quote         TEXT        NOT NULL,
    rate          DOUBLE PRECISION NOT NULL,
    inverse_rate  DOUBLE PRECISION,
    source        TEXT        NOT NULL,
    is_simulated  BOOLEAN     NOT NULL DEFAULT FALSE,
    ts_source     TIMESTAMPTZ,
    ts_ingest     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pair, ts_event)
);
CREATE INDEX IF NOT EXISTS idx_fx_ticks_ts ON fx_ticks (ts_event DESC);

CREATE TABLE IF NOT EXISTS weather_readings (
    region_id     TEXT        NOT NULL,
    ts_event      TIMESTAMPTZ NOT NULL,
    station       TEXT        NOT NULL,
    admin_region  TEXT        NOT NULL,
    latitude      DOUBLE PRECISION NOT NULL,
    longitude     DOUBLE PRECISION NOT NULL,
    cocoa_belt    BOOLEAN     NOT NULL,
    temp_c        DOUBLE PRECISION,
    humidity_pct  DOUBLE PRECISION,
    precip_mm     DOUBLE PRECISION,
    wind_kph      DOUBLE PRECISION,
    cloud_pct     DOUBLE PRECISION,
    condition     TEXT,
    source        TEXT        NOT NULL,
    is_simulated  BOOLEAN     NOT NULL DEFAULT FALSE,
    ts_ingest     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (region_id, ts_event)
);
CREATE INDEX IF NOT EXISTS idx_weather_ts ON weather_readings (ts_event DESC);

-- Latest reading per region - what the geomap / region table panel binds to.
CREATE OR REPLACE VIEW weather_latest AS
SELECT DISTINCT ON (region_id) *
FROM weather_readings
ORDER BY region_id, ts_event DESC;

-- ---------------------------------------------------------------------------
-- Streaming layer: rolling window metrics
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS commodity_metrics (
    symbol         TEXT        NOT NULL,
    window_start   TIMESTAMPTZ NOT NULL,
    window_end     TIMESTAMPTZ NOT NULL,
    tick_count     BIGINT      NOT NULL,
    first_price    DOUBLE PRECISION,
    last_price     DOUBLE PRECISION,
    avg_price      DOUBLE PRECISION,
    min_price      DOUBLE PRECISION,
    max_price      DOUBLE PRECISION,
    stddev_price   DOUBLE PRECISION,
    pct_change     DOUBLE PRECISION,   -- (last - first) / first * 100
    volatility_pct DOUBLE PRECISION,   -- stddev / avg * 100 (coeff. of variation)
    range_pct      DOUBLE PRECISION,   -- (max - min) / avg * 100
    zscore         DOUBLE PRECISION,   -- (last - avg) / stddev
    is_simulated   BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_start)
);
CREATE INDEX IF NOT EXISTS idx_commodity_metrics_ts ON commodity_metrics (window_start DESC);

CREATE TABLE IF NOT EXISTS fx_metrics (
    pair             TEXT        NOT NULL,
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    tick_count       BIGINT      NOT NULL,
    first_rate       DOUBLE PRECISION,
    last_rate        DOUBLE PRECISION,
    avg_rate         DOUBLE PRECISION,
    min_rate         DOUBLE PRECISION,
    max_rate         DOUBLE PRECISION,
    stddev_rate      DOUBLE PRECISION,
    pct_change       DOUBLE PRECISION,
    -- positive = the cedi lost ground over the window
    depreciation_pct DOUBLE PRECISION,
    volatility_pct   DOUBLE PRECISION,
    zscore           DOUBLE PRECISION,
    is_simulated     BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pair, window_start)
);
CREATE INDEX IF NOT EXISTS idx_fx_metrics_ts ON fx_metrics (window_start DESC);

CREATE TABLE IF NOT EXISTS weather_metrics (
    region_id      TEXT        NOT NULL,
    window_start   TIMESTAMPTZ NOT NULL,
    window_end     TIMESTAMPTZ NOT NULL,
    admin_region   TEXT,
    station        TEXT,
    cocoa_belt     BOOLEAN,
    reading_count  BIGINT      NOT NULL,
    avg_temp_c     DOUBLE PRECISION,
    max_temp_c     DOUBLE PRECISION,
    avg_humidity   DOUBLE PRECISION,
    total_precip_mm DOUBLE PRECISION,
    max_wind_kph   DOUBLE PRECISION,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (region_id, window_start)
);
CREATE INDEX IF NOT EXISTS idx_weather_metrics_ts ON weather_metrics (window_start DESC);

-- Cocoa price windows enriched with cocoa-belt weather (stream-static join).
CREATE TABLE IF NOT EXISTS cocoa_weather_context (
    window_start      TIMESTAMPTZ NOT NULL,
    window_end        TIMESTAMPTZ NOT NULL,
    symbol            TEXT        NOT NULL,
    last_price        DOUBLE PRECISION,
    pct_change        DOUBLE PRECISION,
    belt_avg_temp_c   DOUBLE PRECISION,
    belt_avg_humidity DOUBLE PRECISION,
    belt_total_precip_mm DOUBLE PRECISION,
    wettest_region    TEXT,
    wettest_precip_mm DOUBLE PRECISION,
    regions_reporting INT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_start)
);

-- ---------------------------------------------------------------------------
-- Streaming layer: alerts (also published to the alerts.flagged Kafka topic)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alerts (
    alert_id     TEXT        PRIMARY KEY,   -- deterministic: rule|entity|window
    ts_raised    TIMESTAMPTZ NOT NULL,
    entity_type  TEXT        NOT NULL,      -- commodity | fx
    entity       TEXT        NOT NULL,      -- COCOA, USD/GHS, ...
    rule         TEXT        NOT NULL,      -- price_zscore | price_jump | cedi_depreciation
    severity     TEXT        NOT NULL,      -- info | warning | critical
    message      TEXT        NOT NULL,
    observed     DOUBLE PRECISION,
    threshold    DOUBLE PRECISION,
    zscore       DOUBLE PRECISION,
    pct_change   DOUBLE PRECISION,
    window_start TIMESTAMPTZ,
    window_end   TIMESTAMPTZ,
    is_simulated BOOLEAN     NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts (ts_raised DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_entity ON alerts (entity, ts_raised DESC);

-- ---------------------------------------------------------------------------
-- Analytical layer: written nightly by the Spark batch job
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_ohlc (
    symbol       TEXT NOT NULL,
    trade_date   DATE NOT NULL,
    open_price   DOUBLE PRECISION,
    high_price   DOUBLE PRECISION,
    low_price    DOUBLE PRECISION,
    close_price  DOUBLE PRECISION,
    avg_price    DOUBLE PRECISION,
    stddev_price DOUBLE PRECISION,
    range_pct    DOUBLE PRECISION,
    tick_count   BIGINT,
    real_tick_count BIGINT,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, trade_date)
);

CREATE TABLE IF NOT EXISTS fx_daily (
    pair         TEXT NOT NULL,
    trade_date   DATE NOT NULL,
    open_rate    DOUBLE PRECISION,
    high_rate    DOUBLE PRECISION,
    low_rate     DOUBLE PRECISION,
    close_rate   DOUBLE PRECISION,
    avg_rate     DOUBLE PRECISION,
    pct_change   DOUBLE PRECISION,
    tick_count   BIGINT,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pair, trade_date)
);

-- Ghana's cocoa year: main crop Oct-Mar, light crop Apr-Sep.
CREATE TABLE IF NOT EXISTS cocoa_seasonal (
    season        TEXT NOT NULL,      -- e.g. 2025/26
    crop_phase    TEXT NOT NULL,      -- main_crop | light_crop
    month_start   DATE NOT NULL,
    avg_price     DOUBLE PRECISION,
    min_price     DOUBLE PRECISION,
    max_price     DOUBLE PRECISION,
    close_price   DOUBLE PRECISION,
    observations  BIGINT,
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (season, month_start)
);

CREATE TABLE IF NOT EXISTS cedi_depreciation (
    pair          TEXT NOT NULL,
    trade_date    DATE NOT NULL,
    close_rate    DOUBLE PRECISION,
    change_1d_pct DOUBLE PRECISION,
    change_7d_pct DOUBLE PRECISION,
    change_30d_pct DOUBLE PRECISION,
    ma_7          DOUBLE PRECISION,
    ma_30         DOUBLE PRECISION,
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pair, trade_date)
);

CREATE TABLE IF NOT EXISTS weather_price_correlation (
    symbol        TEXT NOT NULL,
    region_id     TEXT NOT NULL,
    metric        TEXT NOT NULL,      -- precip_mm | temp_c | humidity_pct
    window_days   INT  NOT NULL,
    correlation   DOUBLE PRECISION,
    observations  BIGINT,
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, region_id, metric, window_days)
);

-- Audit trail so a re-run is visibly idempotent rather than just claimed to be.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id       TEXT PRIMARY KEY,
    job_name     TEXT NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ,
    status       TEXT NOT NULL,
    rows_written BIGINT,
    detail       TEXT
);

-- ---------------------------------------------------------------------------
-- Read-only role for Grafana
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_ro') THEN
        CREATE ROLE grafana_ro LOGIN PASSWORD 'grafana_ro';
    END IF;
    -- database name comes from POSTGRES_DB, so grant against whatever we are in
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO grafana_ro', current_database());
END
$$;
GRANT USAGE ON SCHEMA market TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA market TO grafana_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA market GRANT SELECT ON TABLES TO grafana_ro;
ALTER ROLE grafana_ro SET search_path TO market, public;
