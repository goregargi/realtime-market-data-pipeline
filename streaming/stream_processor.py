"""
Spark Structured Streaming job.

Reads trades from Kafka and runs three streaming queries:
  1. trades      deduplicated raw trades       -> TimescaleDB `trades`
  2. candles_1m  1-minute OHLCV + VWAP candles -> TimescaleDB `candles_1m`
  3. dead_letter rejected records (producer + Spark validation) -> `dead_letter_events`

A monitor loop records per-batch throughput, latency and late-row drops into
`pipeline_metrics`, so pipeline health is visible next to the market data.
"""

from __future__ import annotations

import logging
import os
import time

from pyspark.sql import SparkSession

import sinks
from transformations import (
    build_candles,
    invalid_trades,
    parse_trades,
    producer_dead_letters,
    valid_trades,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [spark-job] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = os.getenv("TOPIC_TRADES_RAW", "trades.raw")
TOPIC_DLQ = os.getenv("TOPIC_TRADES_DLQ", "trades.dlq")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "10 seconds")
WATERMARK = os.getenv("WATERMARK_DELAY", "2 minutes")
CANDLE_WINDOW = os.getenv("CANDLE_WINDOW", "1 minute")
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("realtime-market-data-pipeline")
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .config("spark.jars.ivy", os.getenv("IVY_DIR", "/opt/ivy"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "6"))
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession, topic: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50_000)
        .load()
    )


def start_queries(spark: SparkSession):
    parsed = parse_trades(read_kafka(spark, TOPIC_RAW))
    trades = valid_trades(parsed)

    deduped = trades.withWatermark("trade_time", WATERMARK).dropDuplicatesWithinWatermark(
        ["product_id", "trade_id"]
    )
    candles = build_candles(trades, window=CANDLE_WINDOW, watermark=WATERMARK)
    dead_letters = invalid_trades(parsed).unionByName(producer_dead_letters(read_kafka(spark, TOPIC_DLQ)))

    def start(df, name, sink, mode="append"):
        return (
            df.writeStream.queryName(name)
            .outputMode(mode)
            .foreachBatch(sink)
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/{name}")
            .trigger(processingTime=TRIGGER)
            .start()
        )

    return [
        start(deduped, "trades", sinks.write_trades),
        start(candles, "candles_1m", sinks.write_candles, mode="update"),
        start(dead_letters, "dead_letter", sinks.write_dead_letters),
    ]


def collect_metrics(queries, seen: dict[str, int]) -> list[tuple]:
    rows = []
    for q in queries:
        p = q.lastProgress
        if not p or seen.get(q.name) == p["batchId"]:
            continue
        seen[q.name] = p["batchId"]
        late = sum(op.get("numRowsDroppedByWatermark", 0) for op in p.get("stateOperators", []))
        rows.append(
            (
                q.name,
                p["batchId"],
                p["timestamp"],
                p.get("numInputRows", 0),
                p.get("inputRowsPerSecond") or 0.0,
                p.get("processedRowsPerSecond") or 0.0,
                p.get("durationMs", {}).get("triggerExecution", 0),
                late,
            )
        )
    return rows


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    queries = start_queries(spark)
    log.info("started queries: %s", ", ".join(q.name for q in queries))

    seen: dict[str, int] = {}
    while all(q.isActive for q in queries):
        time.sleep(5)
        try:
            sinks.write_metrics(collect_metrics(queries, seen))
        except Exception as exc:  # metrics must never take the pipeline down
            log.warning("could not write pipeline metrics: %s", exc)

    for q in queries:
        if q.exception():
            log.error("query %s failed: %s", q.name, q.exception())
    spark.stop()
    raise SystemExit(1)


if __name__ == "__main__":
    main()
