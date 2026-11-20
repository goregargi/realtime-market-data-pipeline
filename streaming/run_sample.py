"""
Run the transformation logic on a few hand-made trades, without Kafka.

    docker run --rm -v "$PWD/streaming:/app" market-spark python run_sample.py
"""

import json

from pyspark.sql import SparkSession

from transformations import build_candles, invalid_trades, parse_trades, valid_trades


def trade(trade_id, price, size, side, ts):
    return json.dumps(
        {
            "trade_id": trade_id,
            "product_id": "BTC-USD",
            "price": price,
            "size": size,
            "side": side,
            "trade_time": ts,
        }
    )


records = [
    trade(1, 100.0, 1.0, "buy", "2026-11-20T17:00:05Z"),
    trade(2, 102.0, 3.0, "sell", "2026-11-20T17:00:30Z"),
    trade(3, 101.0, 1.0, "buy", "2026-11-20T17:00:59Z"),
    trade(4, 105.0, 2.0, "buy", "2026-11-20T17:01:10Z"),
    trade(5, -1.0, 1.0, "buy", "2026-11-20T17:01:10Z"),  # bad price
    trade(6, 100.0, 1.0, "hold", "2026-11-20T17:01:10Z"),  # bad side
    trade(7, 100.0, 1.0, "buy", "not-a-time"),  # bad timestamp
    "{this is not json",
]

spark = SparkSession.builder.master("local[2]").config("spark.sql.session.timeZone", "UTC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

kafka_like = spark.createDataFrame(
    [(r.encode(), "trades.raw", 0, i) for i, r in enumerate(records)],
    "value binary, topic string, partition int, offset long",
)
parsed = parse_trades(kafka_like)

print("Rejected records:")
invalid_trades(parsed).select("offset", "reason").show(truncate=False)

print("1-minute candles:")
candles = build_candles(valid_trades(parsed)).orderBy("bucket")
candles.show(truncate=False)

first = candles.first()
assert (first.open, first.high, first.low, first.close) == (100.0, 102.0, 100.0, 101.0)
assert abs(first.vwap - 101.4) < 1e-9  # (100*1 + 102*3 + 101*1) / 5
print("Candle math is correct.")
