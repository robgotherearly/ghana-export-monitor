"""Spark Structured Streaming: the real-time brain.

Reads the three raw Kafka topics and, per symbol / pair / region:

  * lands every tick in the Delta lake (replayable source for the batch layer)
    and in Postgres (what the live Grafana line charts read),
  * computes rolling 5-minute windowed metrics sliding every minute -
    moving average, stddev, coefficient of variation, % change, z-score,
  * flags anomalies (price z-score, absolute price jump, cedi depreciation)
    and publishes them to both Postgres and the alerts.flagged Kafka topic,
  * enriches cocoa price windows with cocoa-belt weather via a stream-static
    join that re-reads the latest readings on every micro-batch.

Watermarks bound the state so late ticks are still accepted for a while but
memory does not grow without limit.
"""
from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

sys.path.insert(0, "/opt/app")  # so the package resolves under spark-submit

from streaming import settings  # noqa: E402
from streaming.pg_sink import read_table, upsert  # noqa: E402
from streaming.schemas import (  # noqa: E402
    COMMODITY_TICK_SCHEMA,
    FX_RATE_SCHEMA,
    WEATHER_SCHEMA,
)

logging.basicConfig(
    level="INFO",
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
# py4j narrates every gateway command at INFO, which buries the job's own output
logging.getLogger("py4j").setLevel(logging.WARNING)

log = logging.getLogger("stream_metrics")


# ---------------------------------------------------------------------------
# Session + sources
# ---------------------------------------------------------------------------
def build_spark(app_name: str = "ghana-dep-streaming") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", settings.SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.metricsEnabled", "true")
        # several small queries share one context; fair scheduling stops a slow
        # one from starving the others
        .config("spark.scheduler.mode", "FAIR")
    )
    if settings.LAKE_FORMAT == "delta":
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(settings.env("SPARK_LOG_LEVEL", "WARN"))
    return spark


def read_topic(spark: SparkSession, topic: str, schema: StructType) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", settings.STARTING_OFFSETS)
        # a compacted/expired offset should not kill the job
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(
            F.from_json(F.col("value").cast("string"), schema).alias("payload"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("payload.*", "kafka_ts")
        .withColumn("event_time", (F.col("ts_event") / 1000).cast("timestamp"))
        .withColumn("ingest_time", (F.col("ts_ingest") / 1000).cast("timestamp"))
        .withColumn(
            "source_time",
            F.when(F.col("ts_source").isNotNull(), (F.col("ts_source") / 1000).cast("timestamp")),
        )
        .withColumn("is_simulated", F.coalesce(F.col("is_simulated"), F.lit(False)))
        .filter(F.col("event_time").isNotNull())
    )


def to_kafka(df: DataFrame, topic: str, key_col: str) -> None:
    (
        df.select(
            F.col(key_col).cast("string").alias("key"),
            F.to_json(F.struct([F.col(c) for c in df.columns])).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .save()
    )


def to_lake(df: DataFrame, table: str) -> None:
    (
        df.withColumn("dt", F.to_date("event_time"))
        .write.format(settings.LAKE_FORMAT)
        .mode("append")
        .partitionBy("dt")
        .save(f"{settings.LAKE_PATH}/{table}")
    )


# ---------------------------------------------------------------------------
# Rolling window metrics
# ---------------------------------------------------------------------------
def rolling_metrics(ticks: DataFrame, key_col: str, value_col: str) -> DataFrame:
    """5-minute window sliding every minute, per key.

    first/last inside a window come from min/max over a (time, value) struct -
    ordinary first()/last() are non-deterministic on a shuffled aggregation.
    """
    agg = (
        ticks.withWatermark("event_time", settings.WATERMARK_DELAY)
        .groupBy(
            F.window("event_time", settings.PRICE_WINDOW, settings.PRICE_SLIDE).alias("w"),
            F.col(key_col),
        )
        .agg(
            F.count(F.lit(1)).alias("tick_count"),
            F.avg(value_col).alias("avg_value"),
            F.min(value_col).alias("min_value"),
            F.max(value_col).alias("max_value"),
            F.coalesce(F.stddev_samp(value_col), F.lit(0.0)).alias("stddev_value"),
            F.min(F.struct(F.col("event_time"), F.col(value_col))).alias("first_rec"),
            F.max(F.struct(F.col("event_time"), F.col(value_col))).alias("last_rec"),
            F.max(F.col("is_simulated").cast("int")).alias("sim_flag"),
        )
    )

    first_value = F.col("first_rec").getField(value_col)
    last_value = F.col("last_rec").getField(value_col)

    return (
        agg.select(
            F.col(key_col),
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("tick_count"),
            first_value.alias("first_value"),
            last_value.alias("last_value"),
            F.col("avg_value"),
            F.col("min_value"),
            F.col("max_value"),
            F.col("stddev_value"),
            (F.col("sim_flag") > 0).alias("is_simulated"),
        )
        .withColumn(
            "pct_change",
            F.when(F.col("first_value") > 0,
                   (F.col("last_value") - F.col("first_value")) / F.col("first_value") * 100),
        )
        .withColumn(
            "volatility_pct",
            F.when(F.col("avg_value") > 0, F.col("stddev_value") / F.col("avg_value") * 100),
        )
        .withColumn(
            "range_pct",
            F.when(F.col("avg_value") > 0,
                   (F.col("max_value") - F.col("min_value")) / F.col("avg_value") * 100),
        )
        .withColumn(
            "zscore",
            F.when(F.col("stddev_value") > 0,
                   (F.col("last_value") - F.col("avg_value")) / F.col("stddev_value")),
        )
    )


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------
def _severity(magnitude, threshold: float):
    return F.when(
        F.abs(magnitude) >= threshold * settings.CRITICAL_MULTIPLIER, F.lit("critical")
    ).otherwise(F.lit("warning"))


def _alert(df: DataFrame, entity_type: str, entity_col: str, rule: str,
           message, observed, threshold: float, magnitude) -> DataFrame:
    return df.select(
        F.concat_ws("|", F.lit(rule), F.col(entity_col),
                    F.date_format("window_start", "yyyyMMddHHmm")).alias("alert_id"),
        F.current_timestamp().alias("ts_raised"),
        F.lit(entity_type).alias("entity_type"),
        F.col(entity_col).alias("entity"),
        F.lit(rule).alias("rule"),
        _severity(magnitude, threshold).alias("severity"),
        message.alias("message"),
        observed.alias("observed"),
        F.lit(threshold).alias("threshold"),
        F.col("zscore"),
        F.col("pct_change"),
        F.col("window_start"),
        F.col("window_end"),
        F.col("is_simulated"),
    )


def commodity_alerts(metrics: DataFrame) -> DataFrame:
    zscore_hits = _alert(
        metrics.filter(
            (F.abs(F.col("zscore")) >= settings.ZSCORE_THRESHOLD)
            & (F.col("tick_count") >= settings.MIN_TICKS_FOR_ZSCORE)
        ),
        entity_type="commodity",
        entity_col="symbol",
        rule="price_zscore",
        message=F.concat(
            F.col("symbol"), F.lit(" moved "), F.round(F.col("zscore"), 2),
            F.lit("σ from its 5-min mean (last "), F.round(F.col("last_value"), 2),
            F.lit(" vs avg "), F.round(F.col("avg_value"), 2), F.lit(")"),
        ),
        observed=F.col("zscore"),
        threshold=settings.ZSCORE_THRESHOLD,
        magnitude=F.col("zscore"),
    )

    jump_hits = _alert(
        metrics.filter(F.abs(F.col("pct_change")) >= settings.PRICE_JUMP_PCT),
        entity_type="commodity",
        entity_col="symbol",
        rule="price_jump",
        message=F.concat(
            F.col("symbol"), F.lit(" jumped "), F.round(F.col("pct_change"), 2),
            F.lit("% inside the 5-min window"),
        ),
        observed=F.col("pct_change"),
        threshold=settings.PRICE_JUMP_PCT,
        magnitude=F.col("pct_change"),
    )
    return zscore_hits.unionByName(jump_hits)


def fx_alerts(metrics: DataFrame) -> DataFrame:
    # Every pair is quoted X/GHS, so a rising rate means a weakening cedi.
    weakening = metrics.filter(
        (F.col("quote") == "GHS") & (F.col("pct_change") >= settings.CEDI_DEPRECIATION_PCT)
    )
    depreciation = _alert(
        weakening,
        entity_type="fx",
        entity_col="pair",
        rule="cedi_depreciation",
        message=F.concat(
            F.lit("Cedi weakened "), F.round(F.col("pct_change"), 3),
            F.lit("% against "), F.col("base"), F.lit(" in 5 min (now "),
            F.round(F.col("last_value"), 4), F.lit(")"),
        ),
        observed=F.col("pct_change"),
        threshold=settings.CEDI_DEPRECIATION_PCT,
        magnitude=F.col("pct_change"),
    )

    zscore_hits = _alert(
        metrics.filter(
            (F.abs(F.col("zscore")) >= settings.ZSCORE_THRESHOLD)
            & (F.col("tick_count") >= settings.MIN_TICKS_FOR_ZSCORE)
        ),
        entity_type="fx",
        entity_col="pair",
        rule="fx_zscore",
        message=F.concat(
            F.col("pair"), F.lit(" moved "), F.round(F.col("zscore"), 2),
            F.lit("σ from its 5-min mean"),
        ),
        observed=F.col("zscore"),
        threshold=settings.ZSCORE_THRESHOLD,
        magnitude=F.col("zscore"),
    )
    return depreciation.unionByName(zscore_hits)


def publish_alerts(alerts: DataFrame) -> int:
    if alerts.rdd.isEmpty():
        return 0
    alerts = alerts.cache()
    count = alerts.count()
    upsert(alerts, "alerts", conflict_cols=["alert_id"])
    to_kafka(alerts, settings.TOPIC_ALERTS, key_col="entity")
    alerts.unpersist()
    return count


# ---------------------------------------------------------------------------
# Stream-static enrichment: cocoa price windows + cocoa-belt weather
# ---------------------------------------------------------------------------
BELT_QUERY = """
    SELECT region_id, station, admin_region, temp_c, humidity_pct, precip_mm
    FROM market.weather_latest
    WHERE cocoa_belt IS TRUE
      AND ts_event > now() - interval '2 hours'
"""


def enrich_cocoa_with_weather(spark: SparkSession, metrics: DataFrame) -> None:
    """Join the cocoa windows against the freshest cocoa-belt readings.

    The static side is re-read every micro-batch, which is the point: it is a
    small dimension table that changes slowly relative to the price stream.
    """
    cocoa = metrics.filter(F.col("symbol") == "COCOA")
    if cocoa.rdd.isEmpty():
        return

    belt = read_table(spark, BELT_QUERY)
    belt_summary = belt.agg(
        F.avg("temp_c").alias("belt_avg_temp_c"),
        F.avg("humidity_pct").alias("belt_avg_humidity"),
        F.sum("precip_mm").alias("belt_total_precip_mm"),
        F.max(F.struct(F.col("precip_mm"), F.col("station"))).alias("wettest"),
        F.count(F.lit(1)).alias("regions_reporting"),
    ).filter(F.col("regions_reporting") > 0)

    if belt_summary.rdd.isEmpty():
        return

    enriched = cocoa.crossJoin(F.broadcast(belt_summary)).select(
        F.col("window_start"),
        F.col("window_end"),
        F.col("symbol"),
        F.col("last_value").alias("last_price"),
        F.col("pct_change"),
        F.col("belt_avg_temp_c"),
        F.col("belt_avg_humidity"),
        F.col("belt_total_precip_mm"),
        F.col("wettest.station").alias("wettest_region"),
        F.col("wettest.precip_mm").alias("wettest_precip_mm"),
        F.col("regions_reporting").cast("int").alias("regions_reporting"),
        F.current_timestamp().alias("updated_at"),
    )
    upsert(enriched, "cocoa_weather_context", conflict_cols=["symbol", "window_start"])


# ---------------------------------------------------------------------------
# Micro-batch handlers
# ---------------------------------------------------------------------------
def handle_commodity_ticks(batch: DataFrame, batch_id: int) -> None:
    if batch.rdd.isEmpty():
        return
    batch = batch.cache()
    to_lake(batch, "commodity_ticks")
    rows = batch.select(
        "symbol", F.col("event_time").alias("ts_event"), "name", "asset_class", "price",
        "currency", "unit",
        F.col("open").alias("open_price"), F.col("high").alias("high_price"),
        F.col("low").alias("low_price"), "prev_close", "source", "is_simulated",
        F.col("source_time").alias("ts_source"), F.col("ingest_time").alias("ts_ingest"),
    )
    upsert(rows, "commodity_ticks", conflict_cols=["symbol", "ts_event"])
    log.info("batch %s: %d commodity tick(s) landed", batch_id, batch.count())
    batch.unpersist()


def handle_fx_ticks(batch: DataFrame, batch_id: int) -> None:
    if batch.rdd.isEmpty():
        return
    batch = batch.cache()
    to_lake(batch, "fx_ticks")
    rows = batch.select(
        "pair", F.col("event_time").alias("ts_event"), "base", "quote", "rate",
        "inverse_rate", "source", "is_simulated",
        F.col("source_time").alias("ts_source"), F.col("ingest_time").alias("ts_ingest"),
    )
    upsert(rows, "fx_ticks", conflict_cols=["pair", "ts_event"])
    log.info("batch %s: %d fx tick(s) landed", batch_id, batch.count())
    batch.unpersist()


def handle_weather_readings(batch: DataFrame, batch_id: int) -> None:
    if batch.rdd.isEmpty():
        return
    batch = batch.cache()
    to_lake(batch, "weather_readings")
    rows = batch.select(
        "region_id", F.col("event_time").alias("ts_event"), "station", "admin_region",
        "latitude", "longitude", "cocoa_belt", "temp_c", "humidity_pct", "precip_mm",
        "wind_kph", "cloud_pct", "condition", "source", "is_simulated",
        F.col("ingest_time").alias("ts_ingest"),
    )
    upsert(rows, "weather_readings", conflict_cols=["region_id", "ts_event"])
    log.info("batch %s: %d weather reading(s) landed", batch_id, batch.count())
    batch.unpersist()


def handle_commodity_metrics(spark: SparkSession):
    def _handler(batch: DataFrame, batch_id: int) -> None:
        if batch.rdd.isEmpty():
            return
        batch = batch.cache()
        rows = batch.select(
            "symbol", "window_start", "window_end", "tick_count",
            F.col("first_value").alias("first_price"),
            F.col("last_value").alias("last_price"),
            F.col("avg_value").alias("avg_price"),
            F.col("min_value").alias("min_price"),
            F.col("max_value").alias("max_price"),
            F.col("stddev_value").alias("stddev_price"),
            "pct_change", "volatility_pct", "range_pct", "zscore", "is_simulated",
            F.current_timestamp().alias("updated_at"),
        )
        upsert(rows, "commodity_metrics", conflict_cols=["symbol", "window_start"])
        to_kafka(rows, settings.TOPIC_COMMODITY_METRICS, key_col="symbol")

        raised = publish_alerts(commodity_alerts(batch))
        enrich_cocoa_with_weather(spark, batch)
        log.info("batch %s: %d commodity metric row(s), %d alert(s)",
                 batch_id, rows.count(), raised)
        batch.unpersist()

    return _handler


def handle_fx_metrics(batch: DataFrame, batch_id: int) -> None:
    if batch.rdd.isEmpty():
        return
    batch = batch.cache()
    rows = batch.select(
        "pair", "window_start", "window_end", "tick_count",
        F.col("first_value").alias("first_rate"),
        F.col("last_value").alias("last_rate"),
        F.col("avg_value").alias("avg_rate"),
        F.col("min_value").alias("min_rate"),
        F.col("max_value").alias("max_rate"),
        F.col("stddev_value").alias("stddev_rate"),
        "pct_change",
        # quoted X/GHS, so a positive move is the cedi losing ground
        F.when(F.col("quote") == "GHS", F.col("pct_change")).alias("depreciation_pct"),
        "volatility_pct", "zscore", "is_simulated",
        F.current_timestamp().alias("updated_at"),
    )
    upsert(rows, "fx_metrics", conflict_cols=["pair", "window_start"])
    raised = publish_alerts(fx_alerts(batch))
    log.info("batch %s: %d fx metric row(s), %d alert(s)", batch_id, rows.count(), raised)
    batch.unpersist()


def handle_weather_metrics(batch: DataFrame, batch_id: int) -> None:
    if batch.rdd.isEmpty():
        return
    upsert(batch, "weather_metrics", conflict_cols=["region_id", "window_start"])
    log.info("batch %s: %d weather metric row(s)", batch_id, batch.count())


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------
#: Seconds added to the base cadence, per query. Structured Streaming fires a
#: processingTime trigger on wall-clock multiples of its interval, so giving each
#: query a different (and mutually non-harmonic) interval keeps the six
#: foreachBatch callbacks from landing on the same instant. They share one py4j
#: gateway into the Python driver, and a synchronised stampede across it resets
#: the connection and kills the queries.
TRIGGER_OFFSETS = {
    "commodity_ticks": 0,
    "fx_ticks": 5,
    "commodity_metrics": 10,
    "fx_metrics": 17,
    "weather_readings": 23,
    "weather_metrics": 40,
}


def trigger_for(query: str) -> dict:
    return {"processingTime": f"{settings.TRIGGER_SECONDS + TRIGGER_OFFSETS[query]} seconds"}


def start(spark: SparkSession) -> list:
    queries = []

    commodity_ticks = read_topic(spark, settings.TOPIC_COMMODITY_RAW, COMMODITY_TICK_SCHEMA)
    fx_ticks = read_topic(spark, settings.TOPIC_FX_RAW, FX_RATE_SCHEMA)
    weather = read_topic(spark, settings.TOPIC_WEATHER_RAW, WEATHER_SCHEMA)

    queries.append(
        commodity_ticks.writeStream.queryName("commodity_ticks")
        .outputMode("append")
        .foreachBatch(handle_commodity_ticks)
        .option("checkpointLocation", f"{settings.CHECKPOINT_PATH}/commodity_ticks")
        .trigger(**trigger_for("commodity_ticks")).start()
    )

    queries.append(
        rolling_metrics(commodity_ticks, "symbol", "price")
        .writeStream.queryName("commodity_metrics")
        .outputMode("update")
        .foreachBatch(handle_commodity_metrics(spark))
        .option("checkpointLocation", f"{settings.CHECKPOINT_PATH}/commodity_metrics")
        .trigger(**trigger_for("commodity_metrics")).start()
    )

    queries.append(
        fx_ticks.writeStream.queryName("fx_ticks")
        .outputMode("append")
        .foreachBatch(handle_fx_ticks)
        .option("checkpointLocation", f"{settings.CHECKPOINT_PATH}/fx_ticks")
        .trigger(**trigger_for("fx_ticks")).start()
    )

    # carry base/quote through the aggregation so the alert text can name them
    fx_metrics = rolling_metrics(
        fx_ticks.withColumn("pair_key", F.concat_ws("|", "pair", "base", "quote")),
        "pair_key", "rate",
    ).select(
        F.split(F.col("pair_key"), "\\|").getItem(0).alias("pair"),
        F.split(F.col("pair_key"), "\\|").getItem(1).alias("base"),
        F.split(F.col("pair_key"), "\\|").getItem(2).alias("quote"),
        "window_start", "window_end", "tick_count", "first_value", "last_value",
        "avg_value", "min_value", "max_value", "stddev_value", "pct_change",
        "volatility_pct", "range_pct", "zscore", "is_simulated",
    )
    queries.append(
        fx_metrics.writeStream.queryName("fx_metrics")
        .outputMode("update")
        .foreachBatch(handle_fx_metrics)
        .option("checkpointLocation", f"{settings.CHECKPOINT_PATH}/fx_metrics")
        .trigger(**trigger_for("fx_metrics")).start()
    )

    queries.append(
        weather.writeStream.queryName("weather_readings")
        .outputMode("append")
        .foreachBatch(handle_weather_readings)
        .option("checkpointLocation", f"{settings.CHECKPOINT_PATH}/weather_readings")
        .trigger(**trigger_for("weather_readings")).start()
    )

    weather_windows = (
        weather.withWatermark("event_time", settings.WATERMARK_DELAY)
        .groupBy(
            F.window("event_time", settings.WEATHER_WINDOW).alias("w"),
            F.col("region_id"),
        )
        .agg(
            F.max("admin_region").alias("admin_region"),
            F.max("station").alias("station"),
            (F.max(F.col("cocoa_belt").cast("int")) > 0).alias("cocoa_belt"),
            F.count(F.lit(1)).alias("reading_count"),
            F.avg("temp_c").alias("avg_temp_c"),
            F.max("temp_c").alias("max_temp_c"),
            F.avg("humidity_pct").alias("avg_humidity"),
            F.sum("precip_mm").alias("total_precip_mm"),
            F.max("wind_kph").alias("max_wind_kph"),
        )
        .select(
            "region_id",
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "admin_region", "station", "cocoa_belt", "reading_count",
            "avg_temp_c", "max_temp_c", "avg_humidity", "total_precip_mm", "max_wind_kph",
            F.current_timestamp().alias("updated_at"),
        )
    )
    queries.append(
        weather_windows.writeStream.queryName("weather_metrics")
        .outputMode("update")
        .foreachBatch(handle_weather_metrics)
        .option("checkpointLocation", f"{settings.CHECKPOINT_PATH}/weather_metrics")
        .trigger(**trigger_for("weather_metrics")).start()
    )

    return queries


def main() -> int:
    spark = build_spark()
    log.info("streaming from %s -> postgres %s / lake %s",
             settings.KAFKA_BOOTSTRAP, settings.JDBC_URL, settings.LAKE_PATH)
    queries = start(spark)
    log.info("%d streaming queries started: %s",
             len(queries), ", ".join(q.name for q in queries))

    try:
        spark.streams.awaitAnyTermination()
    except Exception:
        log.exception("a streaming query failed")
        return 1

    # awaitAnyTermination also returns normally when a query merely stops. Either
    # way the job is no longer doing its job, so name the casualty and exit
    # non-zero rather than letting the restart policy loop on a silent exit 0.
    stopped = [q.name for q in queries if not q.isActive]
    log.error("streaming halted; inactive queries: %s", ", ".join(stopped) or "unknown")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
