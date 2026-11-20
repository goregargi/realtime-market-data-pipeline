"""
Pure DataFrame transformations used by the streaming job.

Kept separate from the I/O code so the same logic runs on streaming and batch
DataFrames (useful for backfills and for running locally against sample data).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", LongType()),
        StructField("product_id", StringType()),
        StructField("price", DoubleType()),
        StructField("size", DoubleType()),
        StructField("side", StringType()),
        StructField("trade_time", StringType()),
        StructField("ingest_time", StringType()),
        StructField("source", StringType()),
    ]
)

DLQ_ENVELOPE_SCHEMA = StructType(
    [
        StructField("reason", StringType()),
        StructField("payload", StringType()),
        StructField("layer", StringType()),
        StructField("received_at", StringType()),
    ]
)


def _validation_error() -> Column:
    """Return the first rule a row breaks, or null if the row is valid."""
    t = F.col("t")
    return (
        F.when(
            t.isNull() | (t.trade_id.isNull() & t.product_id.isNull() & t.price.isNull()),
            F.lit("unparseable json"),
        )
        .when(t.trade_id.isNull() | t.product_id.isNull(), F.lit("missing trade_id or product_id"))
        .when(t.price.isNull() | (t.price <= 0), F.lit("invalid price"))
        .when(t.size.isNull() | (t.size <= 0), F.lit("invalid size"))
        .when(~t.side.isin("buy", "sell"), F.lit("invalid side"))
        .when(F.col("event_ts").isNull(), F.lit("invalid trade_time"))
    )


def parse_trades(kafka_df: DataFrame) -> DataFrame:
    """
    Parse raw Kafka records into typed trades plus a `validation_error` column.

    Expects the standard Kafka source columns: value, topic, partition, offset.
    """
    parsed = (
        kafka_df.select(
            F.col("value").cast("string").alias("raw"),
            "topic",
            "partition",
            "offset",
        )
        .withColumn("t", F.from_json("raw", TRADE_SCHEMA))
        .withColumn("event_ts", F.try_to_timestamp(F.col("t.trade_time")))
        .withColumn("validation_error", _validation_error())
    )
    return parsed


def valid_trades(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.col("validation_error").isNull()).select(
        F.col("t.trade_id").alias("trade_id"),
        F.col("t.product_id").alias("product_id"),
        F.col("t.price").alias("price"),
        F.col("t.size").alias("size"),
        F.col("t.side").alias("side"),
        F.col("event_ts").alias("trade_time"),
        F.try_to_timestamp(F.col("t.ingest_time")).alias("ingest_time"),
        F.col("t.source").alias("source"),
    )


def invalid_trades(parsed: DataFrame) -> DataFrame:
    """Rows that failed validation in Spark, shaped for the dead-letter table."""
    return parsed.where(F.col("validation_error").isNotNull()).select(
        F.lit("spark").alias("layer"),
        F.col("validation_error").alias("reason"),
        F.col("raw").alias("payload"),
        "topic",
        "partition",
        "offset",
    )


def producer_dead_letters(kafka_dlq_df: DataFrame) -> DataFrame:
    """Records the producer already rejected, read from the DLQ topic."""
    env = F.from_json(F.col("value").cast("string"), DLQ_ENVELOPE_SCHEMA)
    return kafka_dlq_df.select(
        F.coalesce(env.layer, F.lit("producer")).alias("layer"),
        F.coalesce(env.reason, F.lit("unknown")).alias("reason"),
        F.coalesce(env.payload, F.col("value").cast("string")).alias("payload"),
        "topic",
        "partition",
        "offset",
    )


def build_candles(trades: DataFrame, window: str = "1 minute", watermark: str = "2 minutes") -> DataFrame:
    """
    Aggregate trades into OHLCV candles with VWAP and order-flow metrics.

    The watermark bounds how long Spark keeps window state open for late
    events. Anything later than the watermark is dropped and reported in the
    pipeline metrics table.
    """
    return (
        trades.withWatermark("trade_time", watermark)
        .groupBy(F.window("trade_time", window).alias("w"), "product_id")
        .agg(
            F.min_by("price", "trade_time").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "trade_time").alias("close"),
            F.sum("size").alias("volume"),
            F.sum(F.col("price") * F.col("size")).alias("notional"),
            F.sum(F.when(F.col("side") == "buy", F.col("size")).otherwise(0.0)).alias("buy_volume"),
            F.sum(F.when(F.col("side") == "sell", F.col("size")).otherwise(0.0)).alias("sell_volume"),
            F.count(F.lit(1)).alias("trade_count"),
            F.stddev_samp("price").alias("price_stddev"),
        )
        .select(
            "product_id",
            F.col("w.start").alias("bucket"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            (F.col("notional") / F.col("volume")).alias("vwap"),
            "buy_volume",
            "sell_volume",
            "trade_count",
            F.coalesce("price_stddev", F.lit(0.0)).alias("price_stddev"),
        )
    )