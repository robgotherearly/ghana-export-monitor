"""Nightly Spark batch job: the analytical layer.

Source of truth is the Delta lake the streaming job appends to, never Postgres -
so this can be re-run over any historical range without re-hitting a single
free-tier API. Everything it writes is keyed and upserted, which makes a re-run
idempotent: run it twice for the same day and the tables are identical.

Produces:
  * daily OHLC per commodity                      -> market.daily_ohlc
  * daily FX bars                                 -> market.fx_daily
  * cedi depreciation trend (1d / 7d / 30d, MAs)  -> market.cedi_depreciation
  * cocoa seasonality by crop phase               -> market.cocoa_seasonal
  * weather/price correlation features            -> market.weather_price_correlation

Usage:
  spark-submit batch/daily_batch.py --days 30
  spark-submit batch/daily_batch.py --date 2026-08-25
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import UTC, date, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/app")  # so `streaming` is importable under spark-submit

from streaming import settings  # noqa: E402
from streaming.pg_sink import record_run, upsert  # noqa: E402

logging.basicConfig(
    level="INFO",
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("daily_batch")

#: Ghana's cocoa year - main crop Oct-Mar, light crop Apr-Sep
MAIN_CROP_MONTHS = [10, 11, 12, 1, 2, 3]


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("ghana-dep-batch")
        .master(settings.SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", settings.SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        # partition-scoped overwrites, so a re-run replaces only the days it touched
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
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


RAW_TABLES = ["commodity_ticks", "fx_ticks", "weather_readings"]


def compact_lake(spark: SparkSession) -> None:
    """Delta OPTIMIZE over the raw tables - the small-files problem, handled.

    Structured Streaming writes one file per partition per micro-batch: at a
    20-second trigger that is ~800 files of ~17 KB after a few hours, and read
    planning starts to swamp the actual data.

    Measured on this stack, though, OPTIMIZE over that many files takes longer
    than the entire analytical pass it precedes, so it is off by default and
    belongs on its own slower schedule:

        COMPACT_LAKE=true docker compose run --rm spark-batch
    """
    for table in RAW_TABLES:
        path = f"{settings.LAKE_PATH}/{table}"
        try:
            spark.sql(f"OPTIMIZE delta.`{path}`")
            log.info("compacted %s", table)
        except Exception as exc:
            # Never let housekeeping fail the run that produces the tables.
            log.warning("could not compact %s (%s)", table, type(exc).__name__)


def read_lake(spark: SparkSession, table: str, since: date) -> DataFrame | None:
    path = f"{settings.LAKE_PATH}/{table}"
    try:
        df = spark.read.format(settings.LAKE_FORMAT).load(path)
    except Exception as exc:  # nothing landed yet on a fresh deployment
        log.warning("lake table %s unavailable (%s)", table, type(exc).__name__)
        return None
    return df.filter(F.col("dt") >= F.lit(since.isoformat()))


# ---------------------------------------------------------------------------
# Daily bars
# ---------------------------------------------------------------------------
def daily_ohlc(ticks: DataFrame) -> DataFrame:
    """Open/high/low/close per symbol per day.

    open/close come from min/max over a (timestamp, price) struct rather than
    first()/last(), which have no defined order after a shuffle.
    """
    return (
        ticks.groupBy(F.col("symbol"), F.col("dt").alias("trade_date"))
        .agg(
            F.min(F.struct("event_time", "price")).alias("first_rec"),
            F.max(F.struct("event_time", "price")).alias("last_rec"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
            F.avg("price").alias("avg_price"),
            F.coalesce(F.stddev_samp("price"), F.lit(0.0)).alias("stddev_price"),
            F.count(F.lit(1)).alias("tick_count"),
            F.sum(F.when(~F.col("is_simulated"), 1).otherwise(0)).alias("real_tick_count"),
        )
        .select(
            "symbol", "trade_date",
            F.col("first_rec.price").alias("open_price"),
            "high_price", "low_price",
            F.col("last_rec.price").alias("close_price"),
            "avg_price", "stddev_price",
            F.when(F.col("avg_price") > 0,
                   (F.col("high_price") - F.col("low_price")) / F.col("avg_price") * 100
                   ).alias("range_pct"),
            "tick_count", "real_tick_count",
            F.current_timestamp().alias("computed_at"),
        )
    )


def fx_daily(ticks: DataFrame) -> DataFrame:
    return (
        ticks.groupBy(F.col("pair"), F.col("dt").alias("trade_date"))
        .agg(
            F.min(F.struct("event_time", "rate")).alias("first_rec"),
            F.max(F.struct("event_time", "rate")).alias("last_rec"),
            F.max("rate").alias("high_rate"),
            F.min("rate").alias("low_rate"),
            F.avg("rate").alias("avg_rate"),
            F.count(F.lit(1)).alias("tick_count"),
        )
        .select(
            "pair", "trade_date",
            F.col("first_rec.rate").alias("open_rate"),
            "high_rate", "low_rate",
            F.col("last_rec.rate").alias("close_rate"),
            "avg_rate",
            F.when(F.col("first_rec.rate") > 0,
                   (F.col("last_rec.rate") - F.col("first_rec.rate"))
                   / F.col("first_rec.rate") * 100).alias("pct_change"),
            "tick_count",
            F.current_timestamp().alias("computed_at"),
        )
    )


# ---------------------------------------------------------------------------
# Cedi trend
# ---------------------------------------------------------------------------
def cedi_depreciation(fx_bars: DataFrame) -> DataFrame:
    """Rolling depreciation and moving averages, positive = cedi weakening."""
    by_pair = Window.partitionBy("pair").orderBy("trade_date")

    def change_over(days: int):
        prior = F.lag("close_rate", days).over(by_pair)
        return F.when(prior > 0, (F.col("close_rate") - prior) / prior * 100)

    return (
        fx_bars.select("pair", "trade_date", "close_rate")
        .withColumn("change_1d_pct", change_over(1))
        .withColumn("change_7d_pct", change_over(7))
        .withColumn("change_30d_pct", change_over(30))
        .withColumn("ma_7", F.avg("close_rate").over(by_pair.rowsBetween(-6, 0)))
        .withColumn("ma_30", F.avg("close_rate").over(by_pair.rowsBetween(-29, 0)))
        .withColumn("computed_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Cocoa seasonality
# ---------------------------------------------------------------------------
def cocoa_seasonal(ticks: DataFrame) -> DataFrame:
    """Monthly cocoa aggregates labelled by Ghanaian cocoa year and crop phase."""
    month = F.month("dt")
    year = F.year("dt")
    # A cocoa year starts in October: Oct 2025 - Sep 2026 is season "2025/26".
    season_start_year = F.when(month >= 10, year).otherwise(year - 1)
    season = F.concat(
        season_start_year.cast("string"), F.lit("/"),
        F.lpad(((season_start_year + 1) % 100).cast("string"), 2, "0"),
    )

    return (
        ticks.filter(F.col("symbol") == "COCOA")
        .withColumn("season", season)
        .withColumn(
            "crop_phase",
            F.when(month.isin(MAIN_CROP_MONTHS), F.lit("main_crop")).otherwise(F.lit("light_crop")),
        )
        .withColumn("month_start", F.trunc("dt", "month"))
        .groupBy("season", "crop_phase", "month_start")
        .agg(
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.max(F.struct("event_time", "price")).alias("last_rec"),
            F.count(F.lit(1)).alias("observations"),
        )
        .select(
            "season", "crop_phase", "month_start", "avg_price", "min_price", "max_price",
            F.col("last_rec.price").alias("close_price"),
            "observations",
            F.current_timestamp().alias("computed_at"),
        )
    )


# ---------------------------------------------------------------------------
# Weather / price correlation features
# ---------------------------------------------------------------------------
def weather_price_correlation(
    commodity_ticks: DataFrame, weather: DataFrame, window_days: int
) -> DataFrame:
    """Correlate hourly cocoa price against hourly weather per region.

    Hourly rather than daily buckets: a portfolio deployment has hours of
    history, not months, and hourly still answers "does rain in Ashanti move
    with the cocoa price". `observations` is written alongside so a thin
    correlation is visibly thin.
    """
    price_hourly = (
        commodity_ticks.filter(F.col("symbol") == "COCOA")
        .groupBy(F.date_trunc("hour", "event_time").alias("bucket"), "symbol")
        .agg(F.avg("price").alias("price"))
    )
    weather_hourly = (
        weather.groupBy(F.date_trunc("hour", "event_time").alias("bucket"), "region_id")
        .agg(
            F.avg("temp_c").alias("temp_c"),
            F.sum("precip_mm").alias("precip_mm"),
            F.avg("humidity_pct").alias("humidity_pct"),
        )
    )

    joined = price_hourly.join(weather_hourly, on="bucket", how="inner")
    metrics = ["precip_mm", "temp_c", "humidity_pct"]

    frames = [
        joined.groupBy("symbol", "region_id")
        .agg(
            F.corr("price", metric).alias("correlation"),
            F.count(F.lit(1)).alias("observations"),
        )
        .select(
            "symbol", "region_id",
            F.lit(metric).alias("metric"),
            F.lit(window_days).alias("window_days"),
            "correlation", "observations",
            F.current_timestamp().alias("computed_at"),
        )
        for metric in metrics
    ]
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    return out


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run(spark: SparkSession, since: date, window_days: int, compact: bool = False) -> int:
    written = 0

    if settings.LAKE_FORMAT == "delta" and (compact or settings.COMPACT_LAKE):
        compact_lake(spark)

    commodity = read_lake(spark, "commodity_ticks", since)
    fx = read_lake(spark, "fx_ticks", since)
    weather = read_lake(spark, "weather_readings", since)

    if commodity is not None and not commodity.rdd.isEmpty():
        bars = daily_ohlc(commodity).cache()
        upsert(bars, "daily_ohlc", conflict_cols=["symbol", "trade_date"])
        written += bars.count()
        log.info("daily_ohlc: %d row(s)", bars.count())

        # Idempotent overwrite-by-partition into the lake as well: re-running
        # for a date replaces exactly that date's partition and nothing else.
        if settings.LAKE_FORMAT == "delta":
            (
                bars.withColumn("dt", F.col("trade_date"))
                .write.format("delta").mode("overwrite")
                .option("replaceWhere", f"dt >= '{since.isoformat()}'")
                .partitionBy("dt")
                .save(f"{settings.LAKE_PATH}/daily_ohlc")
            )

        seasonal = cocoa_seasonal(commodity)
        if not seasonal.rdd.isEmpty():
            upsert(seasonal, "cocoa_seasonal", conflict_cols=["season", "month_start"])
            written += seasonal.count()
            log.info("cocoa_seasonal: %d row(s)", seasonal.count())
        bars.unpersist()
    else:
        log.warning("no commodity ticks in the lake since %s", since)

    if fx is not None and not fx.rdd.isEmpty():
        fx_bars = fx_daily(fx).cache()
        upsert(fx_bars, "fx_daily", conflict_cols=["pair", "trade_date"])
        written += fx_bars.count()
        log.info("fx_daily: %d row(s)", fx_bars.count())

        trend = cedi_depreciation(fx_bars)
        upsert(trend, "cedi_depreciation", conflict_cols=["pair", "trade_date"])
        written += trend.count()
        log.info("cedi_depreciation: %d row(s)", trend.count())
        fx_bars.unpersist()
    else:
        log.warning("no fx ticks in the lake since %s", since)

    if (commodity is not None and weather is not None
            and not commodity.rdd.isEmpty() and not weather.rdd.isEmpty()):
        corr = weather_price_correlation(commodity, weather, window_days)
        if not corr.rdd.isEmpty():
            upsert(corr, "weather_price_correlation",
                   conflict_cols=["symbol", "region_id", "metric", "window_days"])
            written += corr.count()
            log.info("weather_price_correlation: %d row(s)", corr.count())

    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ghana DEP nightly batch")
    parser.add_argument("--days", type=int, default=30,
                        help="how many days back to recompute (default 30)")
    parser.add_argument("--date", type=str, default=None,
                        help="recompute a single YYYY-MM-DD instead")
    parser.add_argument("--compact", action="store_true",
                        help="run Delta OPTIMIZE on the raw tables first (slow)")
    args = parser.parse_args(argv)

    if args.date:
        since = datetime.strptime(args.date, "%Y-%m-%d").date()
        window_days = 1
    else:
        since = (datetime.now(UTC) - timedelta(days=args.days)).date()
        window_days = args.days

    run_id = f"daily_batch-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    spark = build_spark()
    log.info("batch run %s over lake %s since %s", run_id, settings.LAKE_PATH, since)

    try:
        rows = run(spark, since, window_days, compact=args.compact)
    except Exception as exc:
        record_run(run_id, "daily_batch", "failed", 0, f"{type(exc).__name__}: {exc}")
        log.exception("batch run failed")
        return 1

    record_run(run_id, "daily_batch", "succeeded", rows, f"since={since}")
    log.info("batch run %s finished, %d row(s) written", run_id, rows)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
