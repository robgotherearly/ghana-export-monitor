"""Env-driven settings shared by the streaming and batch Spark jobs."""
from __future__ import annotations

import os


def env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def env_int(key: str, default: int) -> int:
    try:
        return int(env(key) or default)
    except ValueError:
        return default


def env_float(key: str, default: float) -> float:
    try:
        return float(env(key) or default)
    except ValueError:
        return default


# -- Kafka -------------------------------------------------------------------
KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TOPIC_COMMODITY_RAW = env("TOPIC_COMMODITY_RAW", "commodity.prices.raw")
TOPIC_FX_RAW = env("TOPIC_FX_RAW", "fx.rates.raw")
TOPIC_WEATHER_RAW = env("TOPIC_WEATHER_RAW", "weather.regions.raw")
TOPIC_COMMODITY_METRICS = env("TOPIC_COMMODITY_METRICS", "commodity.metrics")
TOPIC_ALERTS = env("TOPIC_ALERTS", "alerts.flagged")
STARTING_OFFSETS = env("STARTING_OFFSETS", "latest")

# -- Postgres ----------------------------------------------------------------
PG_HOST = env("POSTGRES_HOST", "postgres")
PG_PORT = env_int("POSTGRES_PORT", 5432)
PG_DB = env("POSTGRES_DB", "ghana_dep")
PG_USER = env("POSTGRES_USER", "ghana")
PG_PASSWORD = env("POSTGRES_PASSWORD", "ghana")
PG_SCHEMA = env("POSTGRES_SCHEMA", "market")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"


def pg_conn_params() -> dict[str, object]:
    """Plain dict so it can ride along in a Spark closure to the executors."""
    return {
        "host": PG_HOST,
        "port": PG_PORT,
        "dbname": PG_DB,
        "user": PG_USER,
        "password": PG_PASSWORD,
        "options": f"-c search_path={PG_SCHEMA},public -c timezone=UTC",
    }


# -- Storage -----------------------------------------------------------------
LAKE_PATH = env("LAKE_PATH", "/data/lake")
CHECKPOINT_PATH = env("CHECKPOINT_PATH", "/data/checkpoints")
LAKE_FORMAT = env("LAKE_FORMAT", "delta")
#: Delta OPTIMIZE over the raw tables. Off by default: rewriting a day of
#: micro-batch files costs far more than the analytical pass it precedes, so
#: it belongs on its own (weekly) schedule, not on every run.
COMPACT_LAKE = env("COMPACT_LAKE", "false").lower() in {"1", "true", "yes", "on"}

# -- Windowing ---------------------------------------------------------------
PRICE_WINDOW = env("PRICE_WINDOW", "5 minutes")
PRICE_SLIDE = env("PRICE_SLIDE", "1 minute")
WEATHER_WINDOW = env("WEATHER_WINDOW", "15 minutes")
WATERMARK_DELAY = env("WATERMARK_DELAY", "2 minutes")
#: base micro-batch cadence. Each query gets its own offset from this (see
#: stream_metrics.TRIGGER_OFFSETS) so six foreachBatch callbacks do not all fire
#: on the same wall-clock boundary and stampede the single py4j gateway.
TRIGGER_SECONDS = env_int("TRIGGER_SECONDS", 20)

# -- Anomaly thresholds ------------------------------------------------------
#: |(last - mean) / stddev| over the rolling window
ZSCORE_THRESHOLD = env_float("ZSCORE_THRESHOLD", 2.5)
#: minimum ticks in a window before a z-score means anything
MIN_TICKS_FOR_ZSCORE = env_int("MIN_TICKS_FOR_ZSCORE", 5)
#: absolute % move inside one window that is flagged regardless of z-score
PRICE_JUMP_PCT = env_float("PRICE_JUMP_PCT", 1.5)
#: cedi weakening inside one window, in %
CEDI_DEPRECIATION_PCT = env_float("CEDI_DEPRECIATION_PCT", 0.5)
#: anything past this multiple of the threshold is critical rather than warning
CRITICAL_MULTIPLIER = env_float("CRITICAL_MULTIPLIER", 2.0)

SHUFFLE_PARTITIONS = env_int("SPARK_SHUFFLE_PARTITIONS", 4)
#: partitions (and therefore Python workers + PG connections) per sink write
SINK_PARTITIONS = env_int("SINK_PARTITIONS", 1)
SPARK_MASTER = env("SPARK_MASTER_URL", "local[*]")
