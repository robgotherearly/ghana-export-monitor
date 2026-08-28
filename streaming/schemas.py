"""Explicit Kafka payload schemas.

Schema inference on a stream is a trap: it needs a pass over data that has not
arrived yet, and it silently changes shape when an upstream field goes missing.
These mirror ingestion/models.py exactly.
"""
from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

COMMODITY_TICK_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("event_type", StringType()),
    StructField("symbol", StringType()),
    StructField("name", StringType()),
    StructField("asset_class", StringType()),
    StructField("price", DoubleType()),
    StructField("currency", StringType()),
    StructField("unit", StringType()),
    StructField("source", StringType()),
    StructField("ts_event", LongType()),
    StructField("ts_ingest", LongType()),
    StructField("ts_source", LongType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("prev_close", DoubleType()),
    StructField("is_simulated", BooleanType()),
])

FX_RATE_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("event_type", StringType()),
    StructField("pair", StringType()),
    StructField("base", StringType()),
    StructField("quote", StringType()),
    StructField("rate", DoubleType()),
    StructField("source", StringType()),
    StructField("ts_event", LongType()),
    StructField("ts_ingest", LongType()),
    StructField("ts_source", LongType()),
    StructField("inverse_rate", DoubleType()),
    StructField("is_simulated", BooleanType()),
])

WEATHER_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("event_type", StringType()),
    StructField("region_id", StringType()),
    StructField("station", StringType()),
    StructField("admin_region", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("cocoa_belt", BooleanType()),
    StructField("temp_c", DoubleType()),
    StructField("humidity_pct", DoubleType()),
    StructField("precip_mm", DoubleType()),
    StructField("wind_kph", DoubleType()),
    StructField("cloud_pct", DoubleType()),
    StructField("condition", StringType()),
    StructField("source", StringType()),
    StructField("ts_event", LongType()),
    StructField("ts_ingest", LongType()),
    StructField("ts_source", LongType()),
    StructField("is_simulated", BooleanType()),
])
