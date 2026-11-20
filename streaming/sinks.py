"""
TimescaleDB sinks for Spark foreachBatch.

Every write is idempotent (upsert or insert-ignore on a natural key), so a
micro-batch that Spark retries after a failure never creates duplicates.
Writes happen per partition, so they scale out with the executors.
"""

from __future__ import annotations

import os
from collections.abc import Iterable

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, Row

DB_PARAMS = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "market"),
    "user": os.getenv("POSTGRES_USER", "market"),
    "password": os.getenv("POSTGRES_PASSWORD", "market"),
    "options": "-c timezone=UTC",
}

TRADES_SQL = """
    INSERT INTO trades (trade_id, product_id, price, size, side, trade_time, ingest_time, source)
    VALUES %s
    ON CONFLICT DO NOTHING
"""

CANDLES_SQL = """
    INSERT INTO candles_1m (product_id, bucket, open, high, low, close, volume, vwap,
                            buy_volume, sell_volume, trade_count, price_stddev)
    VALUES %s
    ON CONFLICT (product_id, bucket) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume, vwap = EXCLUDED.vwap,
        buy_volume = EXCLUDED.buy_volume, sell_volume = EXCLUDED.sell_volume,
        trade_count = EXCLUDED.trade_count, price_stddev = EXCLUDED.price_stddev,
        updated_at = now()
"""

DLQ_SQL = """
    INSERT INTO dead_letter_events (layer, reason, payload, kafka_topic, kafka_partition, kafka_offset)
    VALUES %s
    ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
"""

METRICS_SQL = """
    INSERT INTO pipeline_metrics (query_name, batch_id, recorded_at, input_rows,
        input_rows_per_sec, processed_rows_per_sec, batch_duration_ms, late_rows_dropped)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (query_name, batch_id) DO NOTHING
"""


def _write_partition(sql: str, columns: list[str], rows: Iterable[Row]) -> None:
    values = [tuple(r[c] for c in columns) for r in rows]
    if not values:
        return
    with psycopg2.connect(**DB_PARAMS) as conn, conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=1000)


def _sink(sql: str, columns: list[str]):
    def write_batch(batch_df: DataFrame, _batch_id: int) -> None:
        batch_df.select(*columns).foreachPartition(lambda rows: _write_partition(sql, columns, rows))

    return write_batch


write_trades = _sink(
    TRADES_SQL,
    ["trade_id", "product_id", "price", "size", "side", "trade_time", "ingest_time", "source"],
)

write_candles = _sink(
    CANDLES_SQL,
    [
        "product_id",
        "bucket",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "buy_volume",
        "sell_volume",
        "trade_count",
        "price_stddev",
    ],
)

write_dead_letters = _sink(
    DLQ_SQL,
    ["layer", "reason", "payload", "topic", "partition", "offset"],
)


def write_metrics(rows: list[tuple]) -> None:
    if not rows:
        return
    with psycopg2.connect(**DB_PARAMS) as conn, conn.cursor() as cur:
        cur.executemany(METRICS_SQL, rows)
